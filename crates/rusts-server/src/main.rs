//! RusTs Server - Main binary for the time series database
//!
//! Usage:
//!   rusts [OPTIONS]
//!
//! Options:
//!   --config <PATH>     Configuration file (default: rusts.yml)
//!   --data-dir <PATH>   Data directory (overrides config)
//!   --port <PORT>       HTTP port (overrides config)
//!   --host <HOST>       Bind address (overrides config)

use rusts_api::auth::AuthConfig;
use rusts_api::{create_router, handlers::AppState, StartupPhase, StartupState};
use rusts_core::ParallelConfig;
use rusts_storage::memtable::FlushTrigger;
use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Complete server configuration - can be loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Server configuration
    pub server: ServerSettings,
    /// Storage engine configuration
    pub storage: StorageSettings,
    /// Authentication configuration
    pub auth: AuthSettings,
    /// Logging configuration
    pub logging: LoggingSettings,
    /// Parallel query execution configuration
    pub parallel: ParallelSettings,
    /// PostgreSQL wire protocol configuration
    pub postgres: PostgresSettings,
    /// Retention policies
    #[serde(default)]
    pub retention_policies: Vec<RetentionPolicyConfig>,
}

/// Server network settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerSettings {
    /// Bind host
    pub host: String,
    /// Bind port
    pub port: u16,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    /// Query execution timeout in seconds
    pub query_timeout_secs: u64,
    /// Maximum number of concurrent queries
    pub max_concurrent_queries: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8086,
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout_secs: 30,
            query_timeout_secs: 30,
            max_concurrent_queries: 100,
        }
    }
}

/// Storage engine settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageSettings {
    /// Data directory
    pub data_dir: PathBuf,
    /// WAL directory (defaults to data_dir/wal)
    pub wal_dir: Option<PathBuf>,
    /// WAL durability mode: "every_write", "periodic", "os_default", "none"
    pub wal_durability: String,
    /// WAL periodic sync interval in milliseconds (only for "periodic" mode)
    pub wal_sync_interval_ms: u64,
    /// WAL retention in seconds (None = retain forever, useful for CDC)
    pub wal_retention_secs: Option<u64>,
    /// MemTable settings
    pub memtable: MemTableSettings,
    /// Partition duration in hours
    pub partition_duration_hours: u64,
    /// Compression level: "none", "fast", "default", "best"
    pub compression: String,
}

impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_dir: None,
            wal_durability: "periodic".to_string(),
            wal_sync_interval_ms: 100,
            wal_retention_secs: Some(7 * 24 * 60 * 60), // 7 days
            memtable: MemTableSettings::default(),
            partition_duration_hours: 24,
            compression: "default".to_string(),
        }
    }
}

/// MemTable flush settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemTableSettings {
    /// Maximum memory size in MB
    pub max_size_mb: usize,
    /// Maximum number of points
    pub max_points: usize,
    /// Maximum age in seconds
    pub max_age_secs: u64,
    /// Out-of-order commit lag in milliseconds.
    /// When set, the memtable waits this long after the last write before flushing,
    /// allowing late-arriving out-of-order data to be sorted correctly within segments.
    /// This is similar to QuestDB's cairo.o3.lag.millis setting.
    /// Default: 1000ms (1 second)
    pub out_of_order_lag_ms: u64,
}

impl Default for MemTableSettings {
    fn default() -> Self {
        Self {
            max_size_mb: 64,
            max_points: 1_000_000,
            max_age_secs: 60,
            out_of_order_lag_ms: 1000, // 1 second default
        }
    }
}

/// Authentication settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthSettings {
    /// Enable authentication
    pub enabled: bool,
    /// JWT secret key (change this in production!)
    pub jwt_secret: String,
    /// Token expiration in seconds
    pub token_expiration_secs: u64,
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            jwt_secret: "change-me-in-production".to_string(),
            token_expiration_secs: 3600,
        }
    }
}

/// Logging settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingSettings {
    /// Log level: "trace", "debug", "info", "warn", "error"
    pub level: String,
    /// Include target in logs
    pub show_target: bool,
    /// Include thread IDs in logs
    pub show_thread_ids: bool,
    /// Include file and line numbers
    pub show_location: bool,
}

impl Default for LoggingSettings {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            show_target: true,
            show_thread_ids: false,
            show_location: false,
        }
    }
}

/// Retention policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicyConfig {
    /// Policy name
    pub name: String,
    /// Measurement pattern (supports wildcards)
    pub pattern: String,
    /// Retention duration (e.g., "30d", "1w", "6h")
    pub duration: String,
    /// Is this the default policy
    #[serde(default)]
    pub is_default: bool,
}

/// Parallel query execution settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ParallelSettings {
    /// Minimum number of series to trigger parallel processing (default: 4)
    pub series_threshold: usize,
    /// Minimum number of partitions to trigger parallel scanning (default: 2)
    pub partition_threshold: usize,
    /// Maximum number of series to process in parallel (0 = unlimited)
    pub max_parallel_series: usize,
    /// Maximum number of partitions to scan in parallel (0 = unlimited)
    pub max_parallel_partitions: usize,
    /// Number of threads in the query thread pool (0 = number of CPU cores)
    pub thread_pool_size: usize,
}

/// PostgreSQL wire protocol settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresSettings {
    /// Enable PostgreSQL wire protocol
    pub enabled: bool,
    /// Bind host for PostgreSQL connections
    pub host: String,
    /// Port for PostgreSQL connections (default: 5432)
    pub port: u16,
    /// Maximum concurrent connections (not currently enforced)
    pub max_connections: usize,
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "0.0.0.0".to_string(),
            port: 5432,
            max_connections: 100,
        }
    }
}

impl Default for ParallelSettings {
    fn default() -> Self {
        Self {
            series_threshold: 4,
            partition_threshold: 2,
            max_parallel_series: 0,
            max_parallel_partitions: 0,
            thread_pool_size: 0,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: ServerSettings::default(),
            storage: StorageSettings::default(),
            auth: AuthSettings::default(),
            logging: LoggingSettings::default(),
            parallel: ParallelSettings::default(),
            postgres: PostgresSettings::default(),
            retention_policies: Vec::new(),
        }
    }
}

impl ServerConfig {
    /// Load configuration from a YAML file
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let config: ServerConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Convert to StorageEngineConfig
    pub fn to_storage_config(&self) -> StorageEngineConfig {
        let wal_durability = match self.storage.wal_durability.to_lowercase().as_str() {
            "every_write" | "every-write" | "every" => WalDurability::EveryWrite,
            "periodic" => WalDurability::Periodic {
                interval_ms: self.storage.wal_sync_interval_ms,
            },
            "os_default" | "os-default" | "os" => WalDurability::OsDefault,
            "none" => WalDurability::None,
            _ => WalDurability::default(),
        };

        let compression = match self.storage.compression.to_lowercase().as_str() {
            "none" => rusts_compression::CompressionLevel::None,
            "fast" => rusts_compression::CompressionLevel::Fast,
            "best" => rusts_compression::CompressionLevel::Best,
            _ => rusts_compression::CompressionLevel::Default,
        };

        StorageEngineConfig {
            data_dir: self.storage.data_dir.clone(),
            wal_dir: self.storage.wal_dir.clone(),
            wal_durability,
            wal_retention_secs: self.storage.wal_retention_secs,
            flush_trigger: FlushTrigger {
                max_size: self.storage.memtable.max_size_mb * 1024 * 1024,
                max_points: self.storage.memtable.max_points,
                max_age_nanos: self.storage.memtable.max_age_secs as i64 * 1_000_000_000,
                out_of_order_lag_ms: self.storage.memtable.out_of_order_lag_ms,
            },
            partition_duration: self.storage.partition_duration_hours as i64 * 60 * 60 * 1_000_000_000,
            compression,
        }
    }

    /// Convert to AuthConfig
    pub fn to_auth_config(&self) -> AuthConfig {
        AuthConfig {
            enabled: self.auth.enabled,
            jwt_secret: self.auth.jwt_secret.clone(),
            token_expiration: std::time::Duration::from_secs(self.auth.token_expiration_secs),
        }
    }

    /// Get log level
    pub fn log_level(&self) -> Level {
        match self.logging.level.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        }
    }

    /// Convert to ParallelConfig
    pub fn to_parallel_config(&self) -> ParallelConfig {
        ParallelConfig {
            series_threshold: self.parallel.series_threshold,
            partition_threshold: self.parallel.partition_threshold,
            max_parallel_series: self.parallel.max_parallel_series,
            max_parallel_partitions: self.parallel.max_parallel_partitions,
            thread_pool_size: self.parallel.thread_pool_size,
        }
    }

    /// Write default config to a file (for generating example config)
    pub fn write_default(path: impl AsRef<Path>) -> anyhow::Result<()> {
        let config = Self::default();
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(path, yaml)?;
        Ok(())
    }
}

/// Command line arguments
struct CliArgs {
    config_path: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    host: Option<String>,
    port: Option<u16>,
    generate_config: bool,
}

fn parse_args() -> CliArgs {
    let mut args = CliArgs {
        config_path: None,
        data_dir: None,
        host: None,
        port: None,
        generate_config: false,
    };

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;

    while i < argv.len() {
        match argv[i].as_str() {
            "--config" | "-c" => {
                if i + 1 < argv.len() {
                    args.config_path = Some(PathBuf::from(&argv[i + 1]));
                    i += 1;
                }
            }
            "--data-dir" | "-d" => {
                if i + 1 < argv.len() {
                    args.data_dir = Some(PathBuf::from(&argv[i + 1]));
                    i += 1;
                }
            }
            "--port" | "-p" => {
                if i + 1 < argv.len() {
                    if let Ok(port) = argv[i + 1].parse() {
                        args.port = Some(port);
                    }
                    i += 1;
                }
            }
            "--host" | "-H" => {
                if i + 1 < argv.len() {
                    args.host = Some(argv[i + 1].clone());
                    i += 1;
                }
            }
            "--generate-config" => {
                args.generate_config = true;
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

    args
}

fn print_help() {
    println!(
        r#"RusTs - State-of-the-Art Time Series Database

USAGE:
    rusts [OPTIONS]

OPTIONS:
    -c, --config <PATH>      Configuration file (default: rusts.yml)
    -d, --data-dir <PATH>    Data directory (overrides config)
    -H, --host <HOST>        Bind address (overrides config)
    -p, --port <PORT>        HTTP port (overrides config)
    --generate-config        Generate default config file (rusts.yml)
    --help                   Print help information
    --version                Print version information

CONFIGURATION:
    RusTs reads configuration from rusts.yml in the current directory,
    or from the path specified with --config. Command line arguments
    override values in the config file.

    Generate a default config file with:
        rusts --generate-config

EXAMPLE CONFIG (rusts.yml):
    server:
      host: "0.0.0.0"
      port: 8086

    storage:
      data_dir: "./data"
      wal_durability: "periodic"
      wal_retention_secs: 604800  # 7 days
      memtable:
        max_size_mb: 64
        max_points: 1000000
        out_of_order_lag_ms: 1000  # Wait 1s for late data

    auth:
      enabled: false

    logging:
      level: "info"

API ENDPOINTS:
    POST /write              Write data using InfluxDB line protocol
    POST /query              Query data with JSON request
    GET  /health             Health check
    GET  /ready              Readiness check
    GET  /stats              Database statistics
"#
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let args = parse_args();

    // Generate config and exit if requested
    if args.generate_config {
        let path = "rusts.yml";
        ServerConfig::write_default(path)?;
        println!("Generated default configuration: {}", path);
        return Ok(());
    }

    // Load configuration
    let config_path = args.config_path.clone().unwrap_or_else(|| PathBuf::from("rusts.yml"));
    let mut config = if config_path.exists() {
        match ServerConfig::from_file(&config_path) {
            Ok(c) => {
                println!("Loaded configuration from: {}", config_path.display());
                c
            }
            Err(e) => {
                eprintln!("Warning: Failed to load {}: {}", config_path.display(), e);
                eprintln!("Using default configuration");
                ServerConfig::default()
            }
        }
    } else {
        ServerConfig::default()
    };

    // Apply command line overrides
    if let Some(data_dir) = args.data_dir {
        config.storage.data_dir = data_dir;
    }
    if let Some(host) = args.host {
        config.server.host = host;
    }
    if let Some(port) = args.port {
        config.server.port = port;
    }

    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(config.log_level())
        .with_target(config.logging.show_target)
        .with_thread_ids(config.logging.show_thread_ids)
        .with_file(config.logging.show_location)
        .with_line_number(config.logging.show_location)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    info!("Starting RusTs v{}", env!("CARGO_PKG_VERSION"));
    info!("Data directory: {:?}", config.storage.data_dir);
    info!("WAL durability: {}", config.storage.wal_durability);
    if let Some(retention) = config.storage.wal_retention_secs {
        info!("WAL retention: {} seconds ({} days)", retention, retention / 86400);
    } else {
        info!("WAL retention: forever (CDC mode)");
    }

    // Create startup state for tracking initialization progress
    let startup_state = Arc::new(StartupState::new());

    // Query protection settings
    let query_timeout = std::time::Duration::from_secs(config.server.query_timeout_secs);
    let max_concurrent_queries = config.server.max_concurrent_queries;
    let parallel_config = config.to_parallel_config();
    info!("Query timeout: {} seconds", config.server.query_timeout_secs);
    info!("Max concurrent queries: {}", max_concurrent_queries);
    info!(
        "Parallel query config: series_threshold={}, partition_threshold={}, max_series={}, max_partitions={}, threads={}",
        parallel_config.series_threshold,
        parallel_config.partition_threshold,
        if parallel_config.max_parallel_series == 0 { "unlimited".to_string() } else { parallel_config.max_parallel_series.to_string() },
        if parallel_config.max_parallel_partitions == 0 { "unlimited".to_string() } else { parallel_config.max_parallel_partitions.to_string() },
        if parallel_config.thread_pool_size == 0 { "auto".to_string() } else { parallel_config.thread_pool_size.to_string() },
    );

    // Create application state in initializing mode (no storage yet)
    // This allows health/ready endpoints to respond immediately
    let app_state = Arc::new(AppState::new_initializing(
        query_timeout,
        max_concurrent_queries,
        Arc::clone(&startup_state),
        parallel_config,
    ));

    // Create router with request timeout
    let request_timeout = std::time::Duration::from_secs(config.server.request_timeout_secs);
    let app = create_router(Arc::clone(&app_state), request_timeout);

    // Start HTTP server FIRST - before storage initialization
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("Server listening on http://{}", addr);
    info!("Write endpoint: POST http://{}/write", addr);
    info!("Query endpoint: POST http://{}/query", addr);
    info!("Health check: GET http://{}/health", addr);
    info!("Server is starting up - health/ready endpoints are available");

    // Spawn background task for storage initialization and index rebuilding
    let storage_config = config.to_storage_config();
    let app_state_for_init = Arc::clone(&app_state);
    let startup_state_for_init = Arc::clone(&startup_state);
    tokio::spawn(async move {
        // Initialize storage engine (this includes WAL recovery)
        startup_state_for_init.set_phase(StartupPhase::WalRecovery);
        info!("Initializing storage engine...");

        // Run blocking storage initialization in spawn_blocking
        let storage_result = tokio::task::spawn_blocking(move || {
            StorageEngine::new(storage_config)
        }).await;

        let storage = match storage_result {
            Ok(Ok(storage)) => Arc::new(storage),
            Ok(Err(e)) => {
                tracing::error!("Failed to initialize storage engine: {}", e);
                std::process::exit(1);
            }
            Err(e) => {
                tracing::error!("Storage initialization task panicked: {}", e);
                std::process::exit(1);
            }
        };
        info!("Storage engine initialized");

        // Set storage in app state
        app_state_for_init.set_storage(Arc::clone(&storage));

        // Now rebuild indexes
        startup_state_for_init.set_phase(StartupPhase::IndexRebuilding);

        // Get all series (this can be slow for large datasets)
        let all_series = storage.get_all_series();

        if !all_series.is_empty() {
            info!("Rebuilding indexes from {} series (memtable + partitions)", all_series.len());
            let total = all_series.len();
            for (i, (series_id, measurement, tags)) in all_series.into_iter().enumerate() {
                app_state_for_init.series_index.upsert(series_id, &measurement, &tags, 0);
                app_state_for_init.tag_index.index_series(series_id, &tags);

                // Log progress every 10000 series
                if (i + 1) % 10000 == 0 {
                    info!("Index rebuild progress: {}/{} series", i + 1, total);
                }
            }
            info!("Index rebuild complete");
        }

        // Mark server as ready
        startup_state_for_init.set_phase(StartupPhase::Ready);
        info!("Server is ready to accept requests");
    });

    // Create shutdown token for coordinating graceful shutdown
    let shutdown_token = CancellationToken::new();

    // Start PostgreSQL wire protocol server if enabled
    if config.postgres.enabled {
        let pg_state = Arc::clone(&app_state);
        let pg_shutdown = shutdown_token.clone();
        let pg_host = config.postgres.host.clone();
        let pg_port = config.postgres.port;

        info!("Starting PostgreSQL wire protocol server on {}:{}", pg_host, pg_port);

        tokio::spawn(async move {
            if let Err(e) = rusts_pgwire::run_postgres_server(
                pg_state,
                query_timeout,
                &pg_host,
                pg_port,
                pg_shutdown,
            ).await {
                tracing::error!("PostgreSQL server error: {}", e);
            }
        });
    }

    // Handle shutdown gracefully (SIGINT and SIGTERM)
    let app_state_for_shutdown = Arc::clone(&app_state);
    let shutdown_token_for_handler = shutdown_token.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("SIGINT received, shutting down gracefully...");
            }
            _ = terminate => {
                info!("SIGTERM received, shutting down gracefully...");
            }
        }

        // Signal all servers to shut down
        shutdown_token_for_handler.cancel();

        if let Some(storage) = app_state_for_shutdown.get_storage() {
            if let Err(e) = storage.shutdown() {
                tracing::error!("Error during shutdown: {}", e);
            }
        }
        info!("Shutdown complete");
        std::process::exit(0);
    });

    // Run server
    axum::serve(listener, app).await?;

    Ok(())
}
