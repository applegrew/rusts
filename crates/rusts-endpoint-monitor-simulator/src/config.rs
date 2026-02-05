//! Configuration structs for the endpoint monitor simulator.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration for the simulator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server URL (e.g., "http://localhost:8086")
    pub server_url: String,

    /// Number of devices to simulate
    pub device_count: usize,

    /// Duration to run the workload
    pub duration: Duration,

    /// Write configuration
    pub write: WriteConfig,

    /// Query configuration
    pub query: QueryConfig,

    /// Whether to include warmup period
    pub warmup_secs: u64,

    /// Output file for the report (optional)
    pub output_file: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:8086".to_string(),
            device_count: 1000,
            duration: Duration::from_secs(300),
            write: WriteConfig::default(),
            query: QueryConfig::default(),
            warmup_secs: 30,
            output_file: None,
        }
    }
}

/// Configuration for write workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfig {
    /// Interval between write batches
    pub interval: Duration,

    /// Maximum points per batch
    pub batch_size: usize,

    /// Number of parallel write workers
    pub workers: usize,

    /// Maximum number of in-flight HTTP write requests per worker
    pub max_in_flight: usize,

    /// Number of retry attempts for failed writes
    pub max_retries: u32,

    /// Base delay between retries (exponential backoff)
    pub retry_delay: Duration,

    /// Request timeout
    pub timeout: Duration,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            batch_size: 5000,
            workers: 1,
            max_in_flight: 4,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            timeout: Duration::from_secs(30),
        }
    }
}

/// Configuration for query workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Target queries per second
    pub rate_per_sec: f64,

    /// Query mix percentages (must sum to 100)
    pub dashboard_pct: u8,
    pub alerting_pct: u8,
    pub historical_pct: u8,

    /// Request timeout
    pub timeout: Duration,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            rate_per_sec: 5.0,
            dashboard_pct: 60,
            alerting_pct: 30,
            historical_pct: 10,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Mode of operation for the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadMode {
    /// Write data only (no queries)
    WriteOnly,
    /// Query existing data only (no writes)
    QueryOnly,
    /// Combined read/write benchmark
    Benchmark,
}

impl std::fmt::Display for WorkloadMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadMode::WriteOnly => write!(f, "write-only"),
            WorkloadMode::QueryOnly => write!(f, "query-only"),
            WorkloadMode::Benchmark => write!(f, "benchmark"),
        }
    }
}
