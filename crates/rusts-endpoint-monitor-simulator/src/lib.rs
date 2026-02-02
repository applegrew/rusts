//! Endpoint monitoring workload simulator for RusTs benchmarking.
//!
//! This crate simulates realistic endpoint monitoring workloads similar to
//! enterprise Digital Employee Experience (DEX) products to benchmark the
//! RusTs time series database.
//!
//! # Target Scale
//! - 1,000 devices
//! - ~100K series
//! - REST API writes only
//!
//! # Measurements
//! - `device_health`: CPU, memory, disk metrics per device
//! - `app_performance`: Application response times and errors
//! - `network_health`: Latency, packet loss, bandwidth
//! - `experience_score`: Computed experience scores
//!
//! # Usage
//! ```bash
//! # Write-only workload
//! rusts-endpoint-monitor-simulator write --devices 1000 --duration 300
//!
//! # Query-only workload
//! rusts-endpoint-monitor-simulator query --duration 60 --rate 10
//!
//! # Combined benchmark
//! rusts-endpoint-monitor-simulator benchmark --devices 1000 --duration 300
//! ```

pub mod config;
pub mod fleet;
pub mod metrics;
pub mod queries;
pub mod report;
pub mod workload;
pub mod writer;

pub use config::{Config, QueryConfig, WorkloadMode, WriteConfig};
pub use fleet::{generate_fleet, Device};
pub use report::Report;
pub use workload::run_workload;
