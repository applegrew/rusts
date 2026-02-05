//! CLI entry point for the endpoint monitor workload simulator.

use anyhow::Result;
use clap::{Parser, Subcommand};
use rusts_endpoint_monitor_simulator::{
    config::{Config, QueryConfig, WorkloadMode, WriteConfig},
    workload::run_workload,
};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(name = "rusts-endpoint-monitor-simulator")]
#[command(about = "Endpoint monitoring workload simulator for RusTs benchmarking")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate data and write (no queries)
    Write {
        /// Number of devices to simulate
        #[arg(short, long, default_value = "1000")]
        devices: usize,

        /// Duration in seconds
        #[arg(short = 'D', long, default_value = "300")]
        duration: u64,

        /// Write interval in seconds
        #[arg(short, long, default_value = "15")]
        interval: u64,

        /// Write interval in milliseconds (overrides --interval)
        #[arg(long)]
        interval_ms: Option<u64>,

        /// Server URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        server: String,

        /// Warmup period in seconds
        #[arg(short, long, default_value = "30")]
        warmup: u64,

        /// Batch size (max points per write)
        #[arg(short, long, default_value = "5000")]
        batch_size: usize,

        /// Number of parallel write workers
        #[arg(long, default_value = "1")]
        workers: usize,

        /// Maximum number of in-flight write requests per worker
        #[arg(long, default_value = "4")]
        max_in_flight: usize,

        /// Output file for report (markdown)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Run queries against existing data (no writes)
    Query {
        /// Duration in seconds
        #[arg(short = 'D', long, default_value = "60")]
        duration: u64,

        /// Queries per second
        #[arg(short, long, default_value = "5")]
        rate: f64,

        /// Server URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        server: String,

        /// Number of devices (for parameterized queries)
        #[arg(short, long, default_value = "1000")]
        devices: usize,

        /// Dashboard query percentage
        #[arg(long, default_value = "60")]
        dashboard_pct: u8,

        /// Alerting query percentage
        #[arg(long, default_value = "30")]
        alerting_pct: u8,

        /// Output file for report (markdown)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Combined workload (read + write)
    Benchmark {
        /// Number of devices to simulate
        #[arg(short, long, default_value = "1000")]
        devices: usize,

        /// Duration in seconds
        #[arg(short = 'D', long, default_value = "300")]
        duration: u64,

        /// Write interval in seconds
        #[arg(long, default_value = "15")]
        write_interval: u64,

        /// Write interval in milliseconds (overrides --write-interval)
        #[arg(long)]
        write_interval_ms: Option<u64>,

        /// Batch size (max points per write)
        #[arg(long, default_value = "5000")]
        batch_size: usize,

        /// Number of parallel write workers
        #[arg(long, default_value = "1")]
        workers: usize,

        /// Maximum number of in-flight write requests per worker
        #[arg(long, default_value = "4")]
        max_in_flight: usize,

        /// Queries per second
        #[arg(long, default_value = "5")]
        query_rate: f64,

        /// Server URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        server: String,

        /// Warmup period in seconds
        #[arg(short, long, default_value = "30")]
        warmup: u64,

        /// Output file for report (markdown)
        #[arg(short, long)]
        output: Option<String>,

        /// Also output JSON report
        #[arg(long)]
        json: bool,
    },

    /// Run a self-contained performance test (spawns server with temp config)
    PerfTest {
        /// Number of devices to simulate
        #[arg(short, long, default_value = "10000")]
        devices: usize,

        /// Duration in seconds
        #[arg(short = 'D', long, default_value = "60")]
        duration: u64,

        /// Write interval in milliseconds
        #[arg(long, default_value = "100")]
        interval_ms: u64,

        /// Batch size (max points per write)
        #[arg(short, long, default_value = "5000")]
        batch_size: usize,

        /// Number of parallel write workers
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Maximum number of in-flight write requests per worker
        #[arg(long, default_value = "8")]
        max_in_flight: usize,

        /// Server port to use
        #[arg(short, long, default_value = "18086")]
        port: u16,

        /// Warmup period in seconds
        #[arg(short, long, default_value = "5")]
        warmup: u64,

        /// Output file for report (markdown)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();

    let (config, mode) = match cli.command {
        Commands::Write {
            devices,
            duration,
            interval,
            interval_ms,
            server,
            warmup,
            batch_size,
            workers,
            max_in_flight,
            output,
        } => {
            info!("Starting write-only workload");
            let interval = match interval_ms {
                Some(ms) => Duration::from_millis(ms),
                None => Duration::from_secs(interval),
            };
            let config = Config {
                server_url: server,
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig {
                    interval,
                    batch_size,
                    workers,
                    max_in_flight,
                    ..Default::default()
                },
                query: QueryConfig::default(),
                warmup_secs: warmup,
                output_file: output,
            };
            (config, WorkloadMode::WriteOnly)
        }

        Commands::Query {
            duration,
            rate,
            server,
            devices,
            dashboard_pct,
            alerting_pct,
            output,
        } => {
            info!("Starting query-only workload");

            // Validate percentages
            let historical_pct = 100u8.saturating_sub(dashboard_pct).saturating_sub(alerting_pct);
            if dashboard_pct + alerting_pct > 100 {
                anyhow::bail!("Dashboard and alerting percentages cannot exceed 100%");
            }

            let config = Config {
                server_url: server,
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig::default(),
                query: QueryConfig {
                    rate_per_sec: rate,
                    dashboard_pct,
                    alerting_pct,
                    historical_pct,
                    ..Default::default()
                },
                warmup_secs: 0, // No warmup for query-only
                output_file: output,
            };
            (config, WorkloadMode::QueryOnly)
        }

        Commands::Benchmark {
            devices,
            duration,
            write_interval,
            write_interval_ms,
            query_rate,
            server,
            warmup,
            output,
            json,
            batch_size,
            workers,
            max_in_flight,
        } => {
            info!("Starting combined benchmark workload");
            let write_interval = match write_interval_ms {
                Some(ms) => Duration::from_millis(ms),
                None => Duration::from_secs(write_interval),
            };
            let mut config = Config {
                server_url: server,
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig {
                    interval: write_interval,
                    batch_size,
                    workers,
                    max_in_flight,
                    ..Default::default()
                },
                query: QueryConfig {
                    rate_per_sec: query_rate,
                    ..Default::default()
                },
                warmup_secs: warmup,
                output_file: output,
            };

            // Store json flag in a way we can use it later
            if json {
                // We'll handle this after the workload runs
                config.output_file = config.output_file.or(Some("report".to_string()));
            }

            (config, WorkloadMode::Benchmark)
        }

        Commands::PerfTest {
            devices,
            duration,
            interval_ms,
            batch_size,
            workers,
            max_in_flight,
            port,
            warmup,
            output,
        } => {
            info!("Starting self-contained performance test");

            // Create temp directory in current directory for data and config
            let cwd = std::env::current_dir()?;
            let perf_test_dir = cwd.join(".perf_test_data");

            // Clean up any leftover data from previous runs
            if perf_test_dir.exists() {
                info!("Cleaning up previous test data at {:?}", perf_test_dir);
                std::fs::remove_dir_all(&perf_test_dir)?;
            }

            // Estimate required disk space and check availability
            let estimated_bytes = estimate_disk_usage(
                devices,
                duration + warmup,
                interval_ms,
            );
            let available_bytes = get_available_disk_space(&cwd)?;

            let estimated_gb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let available_gb = available_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

            info!(
                "Estimated disk usage: {:.2} GB, Available: {:.2} GB",
                estimated_gb, available_gb
            );

            // Require 20% safety margin
            let required_bytes = (estimated_bytes as f64 * 1.2) as u64;
            if available_bytes < required_bytes {
                let required_gb = required_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                anyhow::bail!(
                    "Insufficient disk space for performance test.\n\
                     Estimated usage: {:.2} GB\n\
                     Required (with 20% margin): {:.2} GB\n\
                     Available: {:.2} GB\n\
                     \n\
                     Options:\n\
                     - Free up at least {:.2} GB of disk space\n\
                     - Reduce --duration (currently {}s)\n\
                     - Reduce --devices (currently {})",
                    estimated_gb,
                    required_gb,
                    available_gb,
                    required_gb - available_gb,
                    duration,
                    devices
                );
            }

            // Create the test data directory
            std::fs::create_dir_all(&perf_test_dir)?;
            let data_dir = perf_test_dir.join("data");
            let config_path = perf_test_dir.join("rusts-perf.yml");

            // Write temporary config file with perf settings
            let config_content = format!(
                r#"server:
  host: "127.0.0.1"
  port: {port}
  max_body_size: 104857600
  request_timeout_secs: 120
  query_timeout_secs: 120
  max_concurrent_queries: 100
storage:
  data_dir: "{data_dir}"
  wal_durability: none
  compression: none
  memtable:
    max_size_mb: 128
    max_points: 2000000
    max_age_secs: 60
    out_of_order_lag_ms: 100
  partition_duration_hours: 24
auth:
  enabled: false
logging:
  level: warn
postgres:
  enabled: false
"#,
                port = port,
                data_dir = data_dir.display(),
            );

            std::fs::write(&config_path, &config_content)?;
            info!("Created temp config at: {:?}", config_path);
            info!("Data directory: {:?}", data_dir);

            // Find the rusts-server binary
            let server_bin = find_server_binary()?;
            info!("Using server binary: {:?}", server_bin);

            // Start the server
            info!("Starting rusts-server on port {}...", port);
            let mut server_process = Command::new(&server_bin)
                .arg("--config")
                .arg(&config_path)
                .kill_on_drop(true)
                .spawn()?;

            // Wait for server to be ready
            let server_url = format!("http://127.0.0.1:{}", port);
            wait_for_server_ready(&server_url, Duration::from_secs(30)).await?;
            info!("Server is ready");

            // Build workload config
            let config = Config {
                server_url: server_url.clone(),
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig {
                    interval: Duration::from_millis(interval_ms),
                    batch_size,
                    workers,
                    max_in_flight,
                    ..Default::default()
                },
                query: QueryConfig::default(),
                warmup_secs: warmup,
                output_file: output.clone(),
            };

            // Run the workload
            let report = run_workload(config, WorkloadMode::WriteOnly).await;

            // Print summary
            report.print_summary();

            // Always save report to current directory
            let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let output_base = output.unwrap_or_else(|| format!("perf_test_{}", timestamp));
            let md_path = if output_base.ends_with(".md") {
                output_base.clone()
            } else {
                format!("{}.md", output_base)
            };
            let json_path = md_path.replace(".md", ".json");

            std::fs::write(&md_path, report.to_markdown())?;
            std::fs::write(&json_path, report.to_json())?;
            info!("Markdown report saved to: {}", md_path);
            info!("JSON report saved to: {}", json_path);

            // Shutdown server
            info!("Shutting down server...");
            server_process.kill().await?;

            // Clean up test data directory
            info!("Cleaning up test data at {:?}", perf_test_dir);
            if let Err(e) = std::fs::remove_dir_all(&perf_test_dir) {
                tracing::warn!("Failed to clean up test data: {}", e);
            }

            info!("Performance test complete");
            return Ok(());
        }
    };

    // Run the workload
    let report = run_workload(config.clone(), mode).await;

    // Print summary to console
    report.print_summary();

    // Save report if output file specified
    if let Some(output_path) = &config.output_file {
        let md_path = if output_path.ends_with(".md") {
            output_path.clone()
        } else {
            format!("{}.md", output_path)
        };

        std::fs::write(&md_path, report.to_markdown())?;
        info!("Markdown report saved to: {}", md_path);

        // Save JSON if this was a benchmark with --json flag
        if matches!(mode, WorkloadMode::Benchmark) {
            let json_path = md_path.replace(".md", ".json");
            std::fs::write(&json_path, report.to_json())?;
            info!("JSON report saved to: {}", json_path);
        }
    }

    Ok(())
}

/// Finds the rusts-server binary in target directory or PATH.
fn find_server_binary() -> Result<PathBuf> {
    // First try release build
    let release_path = PathBuf::from("target/release/rusts");
    if release_path.exists() {
        return Ok(release_path);
    }

    // Then try debug build
    let debug_path = PathBuf::from("target/debug/rusts");
    if debug_path.exists() {
        return Ok(debug_path);
    }

    // Try to find in PATH
    if let Ok(path) = which::which("rusts") {
        return Ok(path);
    }

    anyhow::bail!(
        "Could not find rusts-server binary. Please build it first with:\n\
         cargo build -p rusts-server --release"
    )
}

/// Waits for the server to become ready by polling the health endpoint.
async fn wait_for_server_ready(server_url: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let health_url = format!("{}/health", server_url);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for server to become ready");
        }

        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                // Check if server reports ready
                if let Ok(body) = resp.text().await {
                    if body.contains("\"status\":\"healthy\"") {
                        return Ok(());
                    }
                }
            }
            _ => {}
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Estimates disk usage for a performance test.
///
/// Based on observed data from actual runs:
/// - 38M points used ~7.15 GB = ~188 bytes/point
/// - Using 250 bytes/point as conservative estimate (includes WAL overhead, indexes, etc.)
/// Each device emits ~8 points per tick (3 base metrics + ~5 apps average).
fn estimate_disk_usage(devices: usize, duration_secs: u64, interval_ms: u64) -> u64 {
    const BYTES_PER_POINT: u64 = 250;
    const POINTS_PER_DEVICE_PER_TICK: u64 = 8;

    let ticks_per_second = 1000 / interval_ms.max(1);
    let total_ticks = duration_secs * ticks_per_second;
    let points_per_tick = devices as u64 * POINTS_PER_DEVICE_PER_TICK;
    let total_points = total_ticks * points_per_tick;

    total_points * BYTES_PER_POINT
}

/// Gets available disk space for a given path.
#[cfg(unix)]
fn get_available_disk_space(path: &std::path::Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path_cstr = CString::new(path.as_os_str().as_bytes())?;

    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path_cstr.as_ptr(), &mut stat) == 0 {
            Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
        } else {
            anyhow::bail!("Failed to get disk space for {:?}", path);
        }
    }
}

#[cfg(not(unix))]
fn get_available_disk_space(_path: &std::path::Path) -> Result<u64> {
    // On non-Unix systems, return a large value to skip the check
    Ok(u64::MAX)
}
