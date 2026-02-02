//! CLI entry point for the endpoint monitor workload simulator.

use anyhow::Result;
use clap::{Parser, Subcommand};
use rusts_endpoint_monitor_simulator::{
    config::{Config, QueryConfig, WorkloadMode, WriteConfig},
    workload::run_workload,
};
use std::time::Duration;
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

        /// Server URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        server: String,

        /// Warmup period in seconds
        #[arg(short, long, default_value = "30")]
        warmup: u64,

        /// Batch size (max points per write)
        #[arg(short, long, default_value = "5000")]
        batch_size: usize,

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
            server,
            warmup,
            batch_size,
            output,
        } => {
            info!("Starting write-only workload");
            let config = Config {
                server_url: server,
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig {
                    interval: Duration::from_secs(interval),
                    batch_size,
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
            query_rate,
            server,
            warmup,
            output,
            json,
        } => {
            info!("Starting combined benchmark workload");
            let mut config = Config {
                server_url: server,
                device_count: devices,
                duration: Duration::from_secs(duration),
                write: WriteConfig {
                    interval: Duration::from_secs(write_interval),
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
