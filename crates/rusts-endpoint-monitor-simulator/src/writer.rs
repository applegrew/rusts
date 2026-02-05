//! REST API writer using InfluxDB line protocol.

use crate::config::WriteConfig;
use crate::fleet::{Application, Device};
use crate::metrics::{
    AppPerformanceMetrics, DeviceHealthMetrics, ExperienceScoreMetrics, MetricGenerator,
    NetworkHealthMetrics,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Task join failed: {0}")]
    JoinError(#[from] JoinError),

    #[error("Server returned error: {status} - {body}")]
    ServerError { status: u16, body: String },

    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

/// Statistics for write operations.
#[derive(Debug, Default)]
pub struct WriteStats {
    pub points_written: AtomicU64,
    pub batches_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub errors: AtomicU64,
    pub retries: AtomicU64,
}

impl WriteStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self, points: u64, bytes: u64) {
        self.points_written.fetch_add(points, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }
}

/// Writer that sends data to RusTs via REST API.
#[derive(Clone)]
pub struct Writer {
    client: reqwest::Client,
    write_url: String,
    config: WriteConfig,
    stats: Arc<WriteStats>,
    latency_sender: mpsc::UnboundedSender<Duration>,
}

impl Writer {
    pub fn new(
        server_url: &str,
        config: WriteConfig,
        stats: Arc<WriteStats>,
        latency_sender: mpsc::UnboundedSender<Duration>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("Failed to create HTTP client");

        let write_url = format!("{}/write", server_url.trim_end_matches('/'));

        Self {
            client,
            write_url,
            config,
            stats,
            latency_sender,
        }
    }

    /// Writes a batch of data points with retry logic.
    pub async fn write_batch(&self, batch: &str) -> Result<(), WriteError> {
        let points_count = batch.lines().count() as u64;
        let bytes_count = batch.len() as u64;

        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                self.stats.record_retry();
                let delay = self.config.retry_delay * 2u32.pow(attempt - 1);
                tokio::time::sleep(delay).await;
            }

            let start = Instant::now();
            match self
                .client
                .post(&self.write_url)
                .header("Content-Type", "text/plain")
                .body(batch.to_string())
                .send()
                .await
            {
                Ok(response) => {
                    let latency = start.elapsed();
                    let _ = self.latency_sender.send(latency);

                    if response.status().is_success() {
                        self.stats.record_success(points_count, bytes_count);
                        return Ok(());
                    } else {
                        let status = response.status().as_u16();
                        let body = response.text().await.unwrap_or_default();
                        last_error = Some(WriteError::ServerError { status, body });
                    }
                }
                Err(e) => {
                    last_error = Some(WriteError::HttpError(e));
                }
            }
        }

        self.stats.record_error();
        Err(last_error.unwrap_or(WriteError::MaxRetriesExceeded))
    }
}

/// Generates a batch of line protocol data for the fleet at a given timestamp.
pub fn generate_batch(
    fleet: &[Device],
    generator: &mut MetricGenerator,
    timestamp_ns: i64,
) -> String {
    let mut lines = Vec::with_capacity(fleet.len() * 4); // ~4 measurements per device

    for device in fleet {
        // Device health metrics
        let health = generator.device_health(device, timestamp_ns);
        lines.push(format_device_health(device, &health, timestamp_ns));

        // App performance metrics for each application on the device
        let mut app_metrics = Vec::new();
        for app in &device.applications {
            let app_perf = generator.app_performance(device, *app, timestamp_ns);
            lines.push(format_app_performance(device, *app, &app_perf, timestamp_ns));
            app_metrics.push(app_perf);
        }

        // Network health metrics
        let network = generator.network_health(device, timestamp_ns);
        lines.push(format_network_health(device, &network, timestamp_ns));

        // Experience score metrics
        let experience = generator.experience_score(&health, &app_metrics, &network);
        lines.push(format_experience_score(device, &experience, timestamp_ns));
    }

    lines.join("\n")
}

/// Formats device health metrics as line protocol.
fn format_device_health(device: &Device, metrics: &DeviceHealthMetrics, timestamp_ns: i64) -> String {
    let mut fields = format!(
        "cpu_usage={:.2},memory_usage={:.2},disk_usage={:.2},disk_io_read={}i,disk_io_write={}i,boot_time_sec={:.2},login_time_sec={:.2}",
        metrics.cpu_usage,
        metrics.memory_usage,
        metrics.disk_usage,
        metrics.disk_io_read,
        metrics.disk_io_write,
        metrics.boot_time_sec,
        metrics.login_time_sec
    );

    if let Some(battery) = metrics.battery_pct {
        fields.push_str(&format!(",battery_pct={:.2}", battery));
    }

    format!(
        "device_health,device_id={},device_type={},os_version={},department={},location={} {} {}",
        device.id,
        device.device_type.as_str(),
        escape_tag_value(device.os_version.as_str()),
        device.department.as_str(),
        device.location.as_str(),
        fields,
        timestamp_ns
    )
}

/// Formats application performance metrics as line protocol.
fn format_app_performance(
    device: &Device,
    app: Application,
    metrics: &AppPerformanceMetrics,
    timestamp_ns: i64,
) -> String {
    format!(
        "app_performance,device_id={},application_name={},department={} response_time_ms={:.2},availability_pct={:.2},error_count={}i,crash_count={}i,page_load_ms={:.2} {}",
        device.id,
        app.as_str(),
        device.department.as_str(),
        metrics.response_time_ms,
        metrics.availability_pct,
        metrics.error_count,
        metrics.crash_count,
        metrics.page_load_ms,
        timestamp_ns
    )
}

/// Formats network health metrics as line protocol.
fn format_network_health(
    device: &Device,
    metrics: &NetworkHealthMetrics,
    timestamp_ns: i64,
) -> String {
    let mut fields = format!(
        "latency_ms={:.2},packet_loss_pct={:.2},bandwidth_mbps={:.2}",
        metrics.latency_ms, metrics.packet_loss_pct, metrics.bandwidth_mbps
    );

    if let Some(signal) = metrics.wifi_signal_dbm {
        fields.push_str(&format!(",wifi_signal_dbm={}i", signal));
    }

    format!(
        "network_health,device_id={},connection_type={},department={},location={} {} {}",
        device.id,
        device.connection_type.as_str(),
        device.department.as_str(),
        device.location.as_str(),
        fields,
        timestamp_ns
    )
}

/// Formats experience score metrics as line protocol.
fn format_experience_score(
    device: &Device,
    metrics: &ExperienceScoreMetrics,
    timestamp_ns: i64,
) -> String {
    format!(
        "experience_score,device_id={},department={},location={} overall_score={:.2},device_score={:.2},app_score={:.2},network_score={:.2} {}",
        device.id,
        device.department.as_str(),
        device.location.as_str(),
        metrics.overall_score,
        metrics.device_score,
        metrics.app_score,
        metrics.network_score,
        timestamp_ns
    )
}

/// Escapes special characters in tag values for line protocol.
fn escape_tag_value(value: &str) -> String {
    value
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::generate_fleet;

    #[test]
    fn test_generate_batch() {
        let fleet = generate_fleet(10);
        let mut generator = MetricGenerator::new(42);
        let timestamp = 1700000000_000_000_000i64;

        let batch = generate_batch(&fleet, &mut generator, timestamp);

        // Should have multiple lines
        let lines: Vec<&str> = batch.lines().collect();
        assert!(!lines.is_empty());

        // Each line should have proper format
        for line in &lines {
            // Should contain measurement, tags, fields, timestamp
            assert!(line.contains(' '));
            assert!(line.ends_with(&timestamp.to_string()));
        }

        // Should have device_health, app_performance, network_health, experience_score
        assert!(batch.contains("device_health,"));
        assert!(batch.contains("app_performance,"));
        assert!(batch.contains("network_health,"));
        assert!(batch.contains("experience_score,"));
    }

    #[test]
    fn test_escape_tag_value() {
        assert_eq!(escape_tag_value("Windows 11"), "Windows\\ 11");
        assert_eq!(escape_tag_value("macOS 14"), "macOS\\ 14");
        assert_eq!(escape_tag_value("test,value"), "test\\,value");
    }
}
