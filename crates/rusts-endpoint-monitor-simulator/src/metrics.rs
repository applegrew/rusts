//! Metric value generators with realistic patterns.

use crate::fleet::{Application, ConnectionType, Device, DeviceType};
use rand::prelude::*;

/// Generates metric values for a device at a given timestamp.
pub struct MetricGenerator {
    rng: rand::rngs::StdRng,
}

impl MetricGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
        }
    }

    /// Returns a diurnal multiplier based on simulated time of day.
    /// Higher values during business hours (9am-6pm), lower at night.
    fn diurnal_factor(&self, timestamp_ns: i64) -> f64 {
        // Convert timestamp to hour of day (0-23)
        let secs = timestamp_ns / 1_000_000_000;
        let hour = ((secs / 3600) % 24) as f64;

        // Peak at 2pm (14:00), lowest at 4am
        let factor = 0.5 + 0.5 * ((hour - 14.0) * std::f64::consts::PI / 12.0).cos().abs();

        // Business hours boost
        if (9.0..18.0).contains(&hour) {
            factor * 1.3
        } else {
            factor * 0.7
        }
    }

    /// Generates device health metrics.
    pub fn device_health(&mut self, device: &Device, timestamp_ns: i64) -> DeviceHealthMetrics {
        let diurnal = self.diurnal_factor(timestamp_ns);
        let anomaly_boost = if device.is_anomalous { 1.5 } else { 1.0 };

        // CPU usage: base + diurnal pattern + noise + anomaly
        let cpu_usage = (device.base_cpu * diurnal * anomaly_boost
            + self.rng.gen_range(-5.0..5.0))
        .clamp(0.0, 100.0);

        // Memory usage: correlates with CPU, desktops use more
        let device_memory_factor = match device.device_type {
            DeviceType::Desktop => 1.2,
            DeviceType::Virtual => 0.9,
            DeviceType::Laptop => 1.0,
        };
        let memory_usage = (device.base_memory * diurnal * anomaly_boost * device_memory_factor
            + self.rng.gen_range(-3.0..3.0))
        .clamp(0.0, 100.0);

        // Disk usage: stable with slow growth
        let disk_usage =
            (device.base_disk + self.rng.gen_range(-1.0..1.0)).clamp(0.0, 100.0);

        // Disk I/O: correlates with CPU activity
        let io_factor = cpu_usage / 50.0;
        let disk_io_read = ((5000.0 * io_factor + self.rng.gen_range(0.0..2000.0)) as i64)
            .clamp(0, 100000);
        let disk_io_write = ((3000.0 * io_factor + self.rng.gen_range(0.0..1500.0)) as i64)
            .clamp(0, 100000);

        // Battery: only for laptops, slowly drains
        let battery_pct = if device.device_type.has_battery() {
            Some(self.rng.gen_range(20.0..100.0))
        } else {
            None
        };

        // Boot time: stable per device, slight variance
        let boot_time_sec = 30.0 + (device.base_cpu * 2.0) + self.rng.gen_range(-5.0..5.0);

        // Login time: correlates with system load
        let login_time_sec = 10.0 + (cpu_usage / 10.0) + self.rng.gen_range(-2.0..2.0);

        DeviceHealthMetrics {
            cpu_usage,
            memory_usage,
            disk_usage,
            disk_io_read,
            disk_io_write,
            battery_pct,
            boot_time_sec: boot_time_sec.clamp(10.0, 300.0),
            login_time_sec: login_time_sec.clamp(5.0, 120.0),
        }
    }

    /// Generates application performance metrics for a specific app.
    pub fn app_performance(
        &mut self,
        device: &Device,
        app: Application,
        timestamp_ns: i64,
    ) -> AppPerformanceMetrics {
        let diurnal = self.diurnal_factor(timestamp_ns);
        let anomaly_factor = if device.is_anomalous { 2.0 } else { 1.0 };

        // Base response time varies by app type
        let base_response = match app {
            Application::Chrome | Application::Slack => 50.0,
            Application::Zoom | Application::Teams => 100.0,
            Application::Docker | Application::IntelliJ => 200.0,
            Application::Salesforce | Application::Jira => 300.0,
            _ => 150.0,
        };

        let response_time_ms = (base_response * diurnal * anomaly_factor
            + self.rng.gen_range(-30.0..100.0))
        .clamp(10.0, 5000.0);

        // Availability inversely correlates with response time
        let availability_pct =
            (100.0 - (response_time_ms / 100.0) + self.rng.gen_range(-0.5..0.5)).clamp(90.0, 100.0);

        // Errors: rare, more likely when anomalous
        let error_count = if device.is_anomalous {
            self.rng.gen_range(0..10)
        } else if self.rng.gen_bool(0.1) {
            self.rng.gen_range(0..3)
        } else {
            0
        };

        // Crashes: very rare
        let crash_count = if device.is_anomalous && self.rng.gen_bool(0.2) {
            self.rng.gen_range(0..3)
        } else if self.rng.gen_bool(0.01) {
            1
        } else {
            0
        };

        // Page load time: varies by app
        let page_load_ms = (response_time_ms * 3.0 + self.rng.gen_range(0.0..500.0))
            .clamp(100.0, 10000.0);

        AppPerformanceMetrics {
            response_time_ms,
            availability_pct,
            error_count,
            crash_count,
            page_load_ms,
        }
    }

    /// Generates network health metrics.
    pub fn network_health(&mut self, device: &Device, timestamp_ns: i64) -> NetworkHealthMetrics {
        let diurnal = self.diurnal_factor(timestamp_ns);
        let anomaly_factor = if device.is_anomalous { 3.0 } else { 1.0 };

        // Base latency by connection type
        let base_latency = match device.connection_type {
            ConnectionType::Ethernet => 5.0,
            ConnectionType::Wifi => 15.0,
            ConnectionType::Vpn => 50.0,
        };

        let latency_ms =
            (base_latency * diurnal * anomaly_factor + self.rng.gen_range(-5.0..20.0))
                .clamp(1.0, 500.0);

        // Packet loss: rare for ethernet, more common for wifi
        let base_loss = match device.connection_type {
            ConnectionType::Ethernet => 0.01,
            ConnectionType::Wifi => 0.5,
            ConnectionType::Vpn => 0.3,
        };
        let packet_loss_pct =
            (base_loss * anomaly_factor + self.rng.gen_range(0.0..0.5)).clamp(0.0, 10.0);

        // Bandwidth
        let base_bandwidth = match device.connection_type {
            ConnectionType::Ethernet => 800.0,
            ConnectionType::Wifi => 200.0,
            ConnectionType::Vpn => 100.0,
        };
        let bandwidth_mbps =
            (base_bandwidth / anomaly_factor + self.rng.gen_range(-50.0..50.0)).clamp(1.0, 1000.0);

        // WiFi signal: only for wifi connections
        let wifi_signal_dbm = if device.connection_type == ConnectionType::Wifi {
            Some(self.rng.gen_range(-80..-40) as i32)
        } else {
            None
        };

        NetworkHealthMetrics {
            latency_ms,
            packet_loss_pct,
            bandwidth_mbps,
            wifi_signal_dbm,
        }
    }

    /// Generates experience score metrics computed from device, app, and network health.
    pub fn experience_score(
        &mut self,
        device_health: &DeviceHealthMetrics,
        app_metrics: &[AppPerformanceMetrics],
        network_health: &NetworkHealthMetrics,
    ) -> ExperienceScoreMetrics {
        // Device score: based on CPU, memory, and boot time
        let device_score = 100.0
            - (device_health.cpu_usage / 2.0)
            - (device_health.memory_usage / 4.0)
            - (device_health.boot_time_sec / 30.0);
        let device_score = device_score.clamp(0.0, 100.0);

        // App score: average across all apps
        let app_score = if app_metrics.is_empty() {
            80.0
        } else {
            let avg_availability: f64 =
                app_metrics.iter().map(|m| m.availability_pct).sum::<f64>() / app_metrics.len() as f64;
            let avg_response: f64 =
                app_metrics.iter().map(|m| m.response_time_ms).sum::<f64>() / app_metrics.len() as f64;
            let total_errors: i64 = app_metrics.iter().map(|m| m.error_count).sum();

            (avg_availability - (avg_response / 100.0) - (total_errors as f64 * 2.0)).clamp(0.0, 100.0)
        };

        // Network score: based on latency and packet loss
        let network_score = 100.0
            - (network_health.latency_ms / 5.0)
            - (network_health.packet_loss_pct * 10.0);
        let network_score = network_score.clamp(0.0, 100.0);

        // Overall score: weighted average
        let overall_score = device_score * 0.3 + app_score * 0.4 + network_score * 0.3;

        ExperienceScoreMetrics {
            overall_score: overall_score + self.rng.gen_range(-2.0..2.0),
            device_score: device_score + self.rng.gen_range(-1.0..1.0),
            app_score: app_score + self.rng.gen_range(-1.0..1.0),
            network_score: network_score + self.rng.gen_range(-1.0..1.0),
        }
    }
}

/// Device health metrics.
#[derive(Debug, Clone)]
pub struct DeviceHealthMetrics {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub disk_io_read: i64,
    pub disk_io_write: i64,
    pub battery_pct: Option<f64>,
    pub boot_time_sec: f64,
    pub login_time_sec: f64,
}

/// Application performance metrics.
#[derive(Debug, Clone)]
pub struct AppPerformanceMetrics {
    pub response_time_ms: f64,
    pub availability_pct: f64,
    pub error_count: i64,
    pub crash_count: i64,
    pub page_load_ms: f64,
}

/// Network health metrics.
#[derive(Debug, Clone)]
pub struct NetworkHealthMetrics {
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub bandwidth_mbps: f64,
    pub wifi_signal_dbm: Option<i32>,
}

/// Experience score metrics.
#[derive(Debug, Clone)]
pub struct ExperienceScoreMetrics {
    pub overall_score: f64,
    pub device_score: f64,
    pub app_score: f64,
    pub network_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::generate_fleet;

    #[test]
    fn test_metric_generation() {
        let fleet = generate_fleet(10);
        let mut gen = MetricGenerator::new(42);
        let timestamp = 1700000000_000_000_000i64; // Some fixed timestamp

        for device in &fleet {
            let health = gen.device_health(device, timestamp);
            assert!(health.cpu_usage >= 0.0 && health.cpu_usage <= 100.0);
            assert!(health.memory_usage >= 0.0 && health.memory_usage <= 100.0);
            assert!(health.disk_usage >= 0.0 && health.disk_usage <= 100.0);

            // Battery only for laptops
            if device.device_type.has_battery() {
                assert!(health.battery_pct.is_some());
            } else {
                assert!(health.battery_pct.is_none());
            }

            let network = gen.network_health(device, timestamp);
            assert!(network.latency_ms >= 1.0 && network.latency_ms <= 500.0);

            // WiFi signal only for wifi connections
            if device.connection_type == crate::fleet::ConnectionType::Wifi {
                assert!(network.wifi_signal_dbm.is_some());
            } else {
                assert!(network.wifi_signal_dbm.is_none());
            }
        }
    }
}
