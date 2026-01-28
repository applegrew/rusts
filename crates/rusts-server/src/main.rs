//! RusTs Server - Main binary for the time series database
//!
//! Usage:
//!   rusts [OPTIONS]
//!
//! Options:
//!   --data-dir <PATH>   Data directory (default: ./data)
//!   --port <PORT>       HTTP port (default: 8086)
//!   --host <HOST>       Bind address (default: 0.0.0.0)

use rusts_api::{create_router, handlers::AppState};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Server configuration
#[derive(Debug, Clone)]
struct Config {
    /// Data directory
    data_dir: PathBuf,
    /// Bind host
    host: String,
    /// Bind port
    port: u16,
    /// WAL durability mode
    wal_durability: WalDurability,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            host: "0.0.0.0".to_string(),
            port: 8086,
            wal_durability: WalDurability::default(),
        }
    }
}

fn parse_args() -> Config {
    let mut config = Config::default();
    let args: Vec<String> = std::env::args().collect();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" | "-d" => {
                if i + 1 < args.len() {
                    config.data_dir = PathBuf::from(&args[i + 1]);
                    i += 1;
                }
            }
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    if let Ok(port) = args[i + 1].parse() {
                        config.port = port;
                    }
                    i += 1;
                }
            }
            "--host" | "-h" => {
                if i + 1 < args.len() {
                    config.host = args[i + 1].clone();
                    i += 1;
                }
            }
            "--wal-sync" => {
                if i + 1 < args.len() {
                    config.wal_durability = match args[i + 1].as_str() {
                        "every" | "every-write" => WalDurability::EveryWrite,
                        "periodic" => WalDurability::Periodic { interval_ms: 100 },
                        "os" | "os-default" => WalDurability::OsDefault,
                        "none" => WalDurability::None,
                        _ => config.wal_durability,
                    };
                    i += 1;
                }
            }
            "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--version" => {
                println!("rusts {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    config
}

fn print_help() {
    println!(
        r#"RusTs - State-of-the-Art Time Series Database

USAGE:
    rusts [OPTIONS]

OPTIONS:
    -d, --data-dir <PATH>    Data directory (default: ./data)
    -h, --host <HOST>        Bind address (default: 0.0.0.0)
    -p, --port <PORT>        HTTP port (default: 8086)
    --wal-sync <MODE>        WAL sync mode: every, periodic, os, none
                             (default: periodic)
    --help                   Print help information
    --version                Print version information

EXAMPLES:
    # Start server with default settings
    rusts

    # Start with custom data directory and port
    rusts --data-dir /var/lib/rusts --port 9086

    # Start with maximum durability
    rusts --wal-sync every

API ENDPOINTS:
    POST /write              Write data using InfluxDB line protocol
    POST /query              Query data with JSON request
    GET  /health             Health check
    GET  /ready              Readiness check
    GET  /stats              Database statistics

WRITE EXAMPLE:
    curl -X POST http://localhost:8086/write \
      -d 'cpu,host=server01,region=us-west usage=64.5 1609459200000000000'

QUERY EXAMPLE:
    curl -X POST http://localhost:8086/query \
      -H "Content-Type: application/json" \
      -d '{{"measurement": "cpu", "time_range": {{"start": 0, "end": 9999999999999999999}}}}'
"#
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set subscriber");

    // Parse configuration
    let config = parse_args();

    info!("Starting RusTs v{}", env!("CARGO_PKG_VERSION"));
    info!("Data directory: {:?}", config.data_dir);
    info!("WAL durability: {:?}", config.wal_durability);

    // Initialize storage engine
    let storage_config = StorageEngineConfig {
        data_dir: config.data_dir.clone(),
        wal_durability: config.wal_durability,
        ..Default::default()
    };

    info!("Initializing storage engine...");
    let storage = Arc::new(StorageEngine::new(storage_config)?);
    info!("Storage engine initialized");

    // Initialize indexes
    let series_index = Arc::new(SeriesIndex::new());
    let tag_index = Arc::new(TagIndex::new());

    // Create application state
    let app_state = Arc::new(AppState::new(
        Arc::clone(&storage),
        Arc::clone(&series_index),
        Arc::clone(&tag_index),
    ));

    // Create router
    let app = create_router(app_state);

    // Start server
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("Server listening on http://{}", addr);
    info!("Write endpoint: POST http://{}/write", addr);
    info!("Query endpoint: POST http://{}/query", addr);
    info!("Health check: GET http://{}/health", addr);

    // Handle shutdown gracefully
    let storage_clone = Arc::clone(&storage);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        if let Err(e) = storage_clone.shutdown() {
            tracing::error!("Error during shutdown: {}", e);
        }
        std::process::exit(0);
    });

    // Run server
    axum::serve(listener, app).await?;

    Ok(())
}
