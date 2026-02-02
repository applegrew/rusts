//! Query templates and generator for benchmark workloads.

use crate::fleet::Device;
use rand::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Server returned error: {status} - {body}")]
    ServerError { status: u16, body: String },
}

/// Types of queries for the workload mix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Dashboard,
    Alerting,
    Historical,
}

impl QueryType {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryType::Dashboard => "dashboard",
            QueryType::Alerting => "alerting",
            QueryType::Historical => "historical",
        }
    }
}

/// Dashboard queries - run frequently, show current state.
const DASHBOARD_QUERIES: &[&str] = &[
    // Top 10 devices by CPU
    "SELECT device_id, MAX(cpu_usage) as max_cpu FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id ORDER BY max_cpu DESC LIMIT 10",
    // Department summary
    "SELECT department, AVG(cpu_usage) as avg_cpu, AVG(memory_usage) as avg_mem FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY department",
    // App error counts
    "SELECT application_name, SUM(error_count) as total_errors FROM app_performance WHERE time > now() - INTERVAL '15 minutes' GROUP BY application_name ORDER BY total_errors DESC",
    // Overall experience by location
    "SELECT location, AVG(overall_score) as avg_score FROM experience_score WHERE time > now() - INTERVAL '5 minutes' GROUP BY location",
    // Device type distribution with health
    "SELECT device_type, COUNT(*) as count, AVG(cpu_usage) as avg_cpu FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_type",
    // Network health by connection type
    "SELECT connection_type, AVG(latency_ms) as avg_latency, AVG(packet_loss_pct) as avg_loss FROM network_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY connection_type",
    // App crash summary
    "SELECT application_name, SUM(crash_count) as crashes FROM app_performance WHERE time > now() - INTERVAL '1 hour' GROUP BY application_name ORDER BY crashes DESC LIMIT 5",
    // Memory usage by OS
    "SELECT os_version, AVG(memory_usage) as avg_mem FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY os_version",
];

/// Alerting queries - run frequently, detect threshold breaches.
const ALERTING_QUERIES: &[&str] = &[
    // High CPU devices
    "SELECT device_id, MAX(cpu_usage) as max_cpu FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING max_cpu > 80",
    // High memory devices
    "SELECT device_id, MAX(memory_usage) as max_mem FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING max_mem > 85",
    // High latency
    "SELECT device_id, AVG(latency_ms) as avg_latency FROM network_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING avg_latency > 100",
    // High packet loss
    "SELECT device_id, AVG(packet_loss_pct) as avg_loss FROM network_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING avg_loss > 2",
    // Low experience score
    "SELECT device_id, MIN(overall_score) as min_score FROM experience_score WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING min_score < 50",
    // App errors threshold
    "SELECT device_id, application_name, SUM(error_count) as errors FROM app_performance WHERE time > now() - INTERVAL '15 minutes' GROUP BY device_id, application_name HAVING errors > 5",
    // Disk usage warning
    "SELECT device_id, MAX(disk_usage) as max_disk FROM device_health WHERE time > now() - INTERVAL '5 minutes' GROUP BY device_id HAVING max_disk > 90",
];

/// Historical queries - run less frequently, retrieve trends.
/// These templates contain {device_id} placeholders to be replaced with random devices.
const HISTORICAL_QUERIES: &[&str] = &[
    // 24h CPU trend for a device (5-minute buckets would need time bucketing - simplified)
    "SELECT time, cpu_usage, memory_usage FROM device_health WHERE device_id = '{device_id}' AND time > now() - INTERVAL '24 hours' ORDER BY time",
    // Device experience over time
    "SELECT time, overall_score, device_score, app_score, network_score FROM experience_score WHERE device_id = '{device_id}' AND time > now() - INTERVAL '24 hours' ORDER BY time",
    // Network latency history
    "SELECT time, latency_ms, packet_loss_pct FROM network_health WHERE device_id = '{device_id}' AND time > now() - INTERVAL '12 hours' ORDER BY time",
    // App performance history
    "SELECT time, response_time_ms, error_count FROM app_performance WHERE device_id = '{device_id}' AND time > now() - INTERVAL '6 hours' ORDER BY time",
    // Boot time trends (longer period)
    "SELECT time, boot_time_sec, login_time_sec FROM device_health WHERE device_id = '{device_id}' AND time > now() - INTERVAL '7 days' ORDER BY time",
];

/// Statistics for query operations.
#[derive(Debug, Default)]
pub struct QueryStats {
    pub dashboard_count: AtomicU64,
    pub alerting_count: AtomicU64,
    pub historical_count: AtomicU64,
    pub errors: AtomicU64,
}

impl QueryStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self, query_type: QueryType) {
        match query_type {
            QueryType::Dashboard => self.dashboard_count.fetch_add(1, Ordering::Relaxed),
            QueryType::Alerting => self.alerting_count.fetch_add(1, Ordering::Relaxed),
            QueryType::Historical => self.historical_count.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_queries(&self) -> u64 {
        self.dashboard_count.load(Ordering::Relaxed)
            + self.alerting_count.load(Ordering::Relaxed)
            + self.historical_count.load(Ordering::Relaxed)
    }
}

/// Query executor that sends SQL queries to RusTs.
pub struct QueryExecutor {
    client: reqwest::Client,
    sql_url: String,
    stats: Arc<QueryStats>,
    latency_senders: LatencySenders,
    fleet: Arc<Vec<Device>>,
    rng: rand::rngs::StdRng,
    dashboard_pct: u8,
    alerting_pct: u8,
}

/// Latency recording channels per query type.
pub struct LatencySenders {
    pub dashboard: mpsc::UnboundedSender<Duration>,
    pub alerting: mpsc::UnboundedSender<Duration>,
    pub historical: mpsc::UnboundedSender<Duration>,
}

impl QueryExecutor {
    pub fn new(
        server_url: &str,
        timeout: Duration,
        stats: Arc<QueryStats>,
        latency_senders: LatencySenders,
        fleet: Arc<Vec<Device>>,
        dashboard_pct: u8,
        alerting_pct: u8,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to create HTTP client");

        let sql_url = format!("{}/sql", server_url.trim_end_matches('/'));

        Self {
            client,
            sql_url,
            stats,
            latency_senders,
            fleet,
            rng: rand::rngs::StdRng::from_entropy(),
            dashboard_pct,
            alerting_pct,
        }
    }

    /// Selects a query type based on configured mix.
    pub fn select_query_type(&mut self) -> QueryType {
        let roll: u8 = self.rng.gen_range(0..100);
        if roll < self.dashboard_pct {
            QueryType::Dashboard
        } else if roll < self.dashboard_pct + self.alerting_pct {
            QueryType::Alerting
        } else {
            QueryType::Historical
        }
    }

    /// Generates a query of the specified type.
    pub fn generate_query(&mut self, query_type: QueryType) -> String {
        let template = match query_type {
            QueryType::Dashboard => *DASHBOARD_QUERIES.choose(&mut self.rng).unwrap(),
            QueryType::Alerting => *ALERTING_QUERIES.choose(&mut self.rng).unwrap(),
            QueryType::Historical => *HISTORICAL_QUERIES.choose(&mut self.rng).unwrap(),
        };

        // Replace placeholders
        if template.contains("{device_id}") {
            let device = self.fleet.choose(&mut self.rng).unwrap();
            template.replace("{device_id}", &device.id)
        } else {
            template.to_string()
        }
    }

    /// Executes a query and records statistics.
    pub async fn execute_query(&mut self, query_type: QueryType) -> Result<(), QueryError> {
        let query = self.generate_query(query_type);

        let start = Instant::now();
        let result = self
            .client
            .post(&self.sql_url)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await;

        let latency = start.elapsed();

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    self.stats.record_success(query_type);
                    self.record_latency(query_type, latency);
                    Ok(())
                } else {
                    self.stats.record_error();
                    let status = response.status().as_u16();
                    let body = response.text().await.unwrap_or_default();
                    Err(QueryError::ServerError { status, body })
                }
            }
            Err(e) => {
                self.stats.record_error();
                Err(QueryError::HttpError(e))
            }
        }
    }

    fn record_latency(&self, query_type: QueryType, latency: Duration) {
        let sender = match query_type {
            QueryType::Dashboard => &self.latency_senders.dashboard,
            QueryType::Alerting => &self.latency_senders.alerting,
            QueryType::Historical => &self.latency_senders.historical,
        };
        let _ = sender.send(latency);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_templates() {
        // Ensure all templates are valid SQL
        for query in DASHBOARD_QUERIES {
            assert!(query.contains("SELECT"));
            assert!(query.contains("FROM"));
        }

        for query in ALERTING_QUERIES {
            assert!(query.contains("SELECT"));
            assert!(query.contains("HAVING"));
        }

        for query in HISTORICAL_QUERIES {
            assert!(query.contains("{device_id}"));
            assert!(query.contains("ORDER BY time"));
        }
    }
}
