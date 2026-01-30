//! RusTs Importer CLI
//!
//! A command-line tool for importing data into RusTs from various file formats.
//!
//! ## Usage
//!
//! ```bash
//! # Import a Parquet file (uses data_dir from rusts.yml)
//! rusts-import parquet data.parquet --measurement cpu --direct
//!
//! # Specify tag columns
//! rusts-import parquet data.parquet -m metrics --tags host,region --direct
//!
//! # Use custom config file
//! rusts-import --config /etc/rusts.yml parquet data.parquet --direct
//!
//! # Override data directory
//! rusts-import parquet data.parquet --direct --data-dir /custom/path
//!
//! # Inspect Parquet schema
//! rusts-import parquet data.parquet --schema-only
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use rusts_core::{FieldValue, Point};
use rusts_importer::{inspect_parquet_schema, ParquetReader, ParquetReaderConfig, RustsWriter};
use rusts_storage::memtable::FlushTrigger;
use rusts_storage::wal::WalDurability;
use rusts_storage::{StorageEngine, StorageEngineConfig};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Minimal config structure to read data_dir from rusts.yml
#[derive(Debug, Deserialize)]
struct RustsConfig {
    #[serde(default)]
    storage: StorageConfig,
}

#[derive(Debug, Deserialize, Default)]
struct StorageConfig {
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

impl RustsConfig {
    fn load(path: &PathBuf) -> Option<Self> {
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(content) => match serde_yaml::from_str(&content) {
                    Ok(config) => Some(config),
                    Err(e) => {
                        eprintln!("Warning: Failed to parse {}: {}", path.display(), e);
                        None
                    }
                },
                Err(e) => {
                    eprintln!("Warning: Failed to read {}: {}", path.display(), e);
                    None
                }
            }
        } else {
            None
        }
    }
}

#[derive(Parser)]
#[command(name = "rusts-import")]
#[command(author, version, about = "Import data into RusTs time series database")]
struct Cli {
    /// Path to rusts.yml config file (default: ./rusts.yml)
    #[arg(short, long, global = true, default_value = "rusts.yml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import data from a Parquet file
    Parquet {
        /// Path to the Parquet file
        file: PathBuf,

        /// Measurement name for the imported data
        #[arg(short, long, default_value = "imported")]
        measurement: String,

        /// RusTs server URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        server: String,

        /// Column name containing timestamps
        #[arg(short = 't', long, default_value = "timestamp")]
        timestamp_column: String,

        /// Columns to treat as tags (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,

        /// Batch size for writing
        #[arg(short, long, default_value = "10000")]
        batch_size: usize,

        /// Only show the schema, don't import
        #[arg(long)]
        schema_only: bool,

        /// Dry run - read the file but don't send to server
        #[arg(long)]
        dry_run: bool,

        /// Column name to use for deduplication against existing DB records.
        /// Only imports records whose dedup column value doesn't already exist in the database.
        /// For direct mode, only field columns are supported (not tags).
        #[arg(long)]
        dedup_column: Option<String>,

        /// Write directly to storage engine (bypasses REST API, uses streaming)
        #[arg(long)]
        direct: bool,

        /// Data directory for direct mode (overrides config file)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load config file for data_dir
    let rusts_config = RustsConfig::load(&cli.config);

    match cli.command {
        Commands::Parquet {
            file,
            measurement,
            server,
            timestamp_column,
            tags,
            batch_size,
            schema_only,
            dry_run,
            dedup_column,
            direct,
            data_dir,
        } => {
            // Use data_dir from CLI if provided, otherwise from config, otherwise default
            let effective_data_dir = if let Some(dir) = data_dir {
                dir
            } else if let Some(ref config) = rusts_config {
                info!("Using data_dir from {}: {:?}", cli.config.display(), config.storage.data_dir);
                config.storage.data_dir.clone()
            } else {
                default_data_dir()
            };

            import_parquet(
                file,
                measurement,
                server,
                timestamp_column,
                tags,
                batch_size,
                schema_only,
                dry_run,
                dedup_column,
                direct,
                effective_data_dir,
            )
            .await
        }
    }
}

async fn import_parquet(
    file: PathBuf,
    measurement: String,
    server: String,
    timestamp_column: String,
    tags: Vec<String>,
    batch_size: usize,
    schema_only: bool,
    dry_run: bool,
    dedup_column: Option<String>,
    direct: bool,
    data_dir: PathBuf,
) -> Result<()> {
    // Check file exists
    if !file.exists() {
        anyhow::bail!("File not found: {}", file.display());
    }

    info!("Reading Parquet file: {}", file.display());

    // If schema-only, just print the schema and exit
    if schema_only {
        let columns = inspect_parquet_schema(&file).context("Failed to read Parquet schema")?;
        println!("\nParquet Schema:");
        println!("{:-<60}", "");
        println!("{:<30} {:<30}", "Column", "Type");
        println!("{:-<60}", "");
        for (name, dtype) in columns {
            println!("{:<30} {:<30}", name, dtype);
        }
        println!("{:-<60}", "");
        return Ok(());
    }

    // Configure the reader
    let config = ParquetReaderConfig::new(&measurement)
        .with_timestamp_column(&timestamp_column)
        .with_tag_columns(tags.clone())
        .with_batch_size(batch_size);

    // Determine mode
    let mode_str = if direct {
        if dedup_column.is_some() {
            "direct (streaming with DB dedup)"
        } else {
            "direct (streaming)"
        }
    } else if dedup_column.is_some() {
        "REST API (streaming with DB dedup)"
    } else {
        "REST API (streaming)"
    };

    info!("Configuration:");
    info!("  Measurement: {}", measurement);
    info!("  Timestamp column: {}", timestamp_column);
    info!("  Tag columns: {:?}", tags);
    info!("  Batch size: {}", batch_size);
    info!("  Mode: {}", mode_str);
    if let Some(ref col) = dedup_column {
        info!("  Dedup column: {} (checks against existing DB records)", col);
    }

    let reader = ParquetReader::new(config);
    let start = Instant::now();

    if direct {
        // STREAMING DIRECT MODE - read and write batches without loading all into memory
        import_direct_streaming(&reader, &file, &data_dir, batch_size, dry_run, dedup_column.clone(), &measurement, start).await
    } else {
        // STREAMING REST MODE - read and send batches without loading all into memory
        import_via_rest_streaming(&reader, &file, &server, batch_size, dry_run, dedup_column.clone(), &measurement, start).await
    }
}

/// Streaming direct import - reads batches from parquet and writes directly to storage
async fn import_direct_streaming(
    reader: &ParquetReader,
    file: &PathBuf,
    data_dir: &PathBuf,
    batch_size: usize,
    dry_run: bool,
    dedup_column: Option<String>,
    measurement: &str,
    start: Instant,
) -> Result<()> {
    if dry_run {
        // For dry run, just count batches
        info!("Dry run - counting records...");
        let batches = reader.read_file_batched(file).context("Failed to read Parquet file")?;
        let mut total_points = 0;
        for batch_result in batches {
            let batch = batch_result?;
            total_points += batch.len();
        }
        println!("\nDry run complete:");
        println!("  Points counted: {}", total_points);
        println!("  Would write to: {}", data_dir.display());
        return Ok(());
    }

    info!("Opening storage engine at: {}", data_dir.display());

    let storage_config = StorageEngineConfig {
        data_dir: data_dir.clone(),
        wal_dir: None,
        wal_durability: WalDurability::None, // Skip WAL - source file is our recovery
        wal_retention_secs: None,            // No retention needed for import
        flush_trigger: FlushTrigger {
            max_size: 256 * 1024 * 1024, // 256MB memtable
            max_points: 10_000_000,      // 10M points
            max_age_nanos: i64::MAX,     // Don't flush on age during import
            out_of_order_lag_ms: 0,      // No lag needed for bulk import
        },
        partition_duration: 24 * 60 * 60 * 1_000_000_000, // 1 day
        compression: rusts_compression::CompressionLevel::Default,
    };

    let engine = StorageEngine::new(storage_config).context("Failed to open storage engine")?;

    // If dedup is enabled, query existing keys from the database
    let existing_keys: HashSet<String> = if let Some(ref dedup_col) = dedup_column {
        info!("Querying existing records for deduplication (column: {})...", dedup_col);
        let query_start = Instant::now();
        let keys = query_existing_keys_direct(&engine, measurement, dedup_col)?;
        info!(
            "Found {} existing records in {:.2}s",
            keys.len(),
            query_start.elapsed().as_secs_f64()
        );
        keys
    } else {
        HashSet::new()
    };

    info!("Streaming import started (batch size: {})...", batch_size);

    // Create a spinner since we don't know total count upfront
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("Invalid progress style template"),
    );

    let batches = reader.read_file_batched(file).context("Failed to read Parquet file")?;

    let mut total_written = 0;
    let mut total_skipped = 0;
    let mut batch_count = 0;

    for batch_result in batches {
        let batch = batch_result.context("Failed to read batch")?;

        // Filter out duplicates if dedup is enabled
        let batch_to_write = if let Some(ref dedup_col) = dedup_column {
            let (new_points, skipped) = filter_existing_points(batch, dedup_col, &existing_keys);
            total_skipped += skipped;
            new_points
        } else {
            batch
        };

        if !batch_to_write.is_empty() {
            engine.write_batch(&batch_to_write).context("Failed to write batch")?;
            total_written += batch_to_write.len();
            batch_count += 1;

            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                total_written as f64 / elapsed
            } else {
                0.0
            };
            let skip_msg = if total_skipped > 0 {
                format!(", {} skipped", total_skipped)
            } else {
                String::new()
            };
            progress.set_message(format!(
                "{} points written ({} batches{}) - {:.0} pts/sec",
                total_written, batch_count, skip_msg, rate
            ));
        }
    }

    // Flush and shutdown cleanly
    info!("Flushing data to disk...");
    engine.flush().context("Failed to flush")?;
    engine.shutdown().context("Failed to shutdown engine")?;

    progress.finish_with_message(format!("{} points written - done", total_written));

    let total_duration = start.elapsed();

    println!("\nImport complete:");
    println!("  Points imported: {}", total_written);
    if total_skipped > 0 {
        println!("  Points skipped (duplicates): {}", total_skipped);
    }
    println!("  Batches processed: {}", batch_count);
    println!("  Total time: {:.2}s", total_duration.as_secs_f64());
    println!(
        "  Throughput: {:.0} points/sec",
        total_written as f64 / total_duration.as_secs_f64()
    );

    Ok(())
}

/// Streaming REST API import - reads and sends batches without loading all into memory
async fn import_via_rest_streaming(
    reader: &ParquetReader,
    file: &PathBuf,
    server: &str,
    batch_size: usize,
    dry_run: bool,
    dedup_column: Option<String>,
    measurement: &str,
    start: Instant,
) -> Result<()> {
    if dry_run {
        info!("Dry run - counting records...");
        let batches = reader.read_file_batched(file).context("Failed to read Parquet file")?;
        let mut total_points = 0;
        for batch_result in batches {
            let batch = batch_result?;
            total_points += batch.len();
        }
        println!("\nDry run complete:");
        println!("  Points counted: {}", total_points);
        println!("  Would write to: {}", server);
        return Ok(());
    }

    // Connect to server first
    info!("Connecting to server: {}", server);
    let writer = RustsWriter::new(server).context("Failed to create writer")?;

    if !writer.health_check().await.unwrap_or(false) {
        anyhow::bail!("Server is not healthy or unreachable: {}", server);
    }
    info!("Server is healthy");

    // If dedup is enabled, query existing keys from the database
    let existing_keys: HashSet<String> = if let Some(ref dedup_col) = dedup_column {
        info!("Querying existing records for deduplication (column: {})...", dedup_col);
        let query_start = Instant::now();
        let keys = query_existing_keys_rest(&writer, measurement, dedup_col).await?;
        info!(
            "Found {} existing records in {:.2}s",
            keys.len(),
            query_start.elapsed().as_secs_f64()
        );
        keys
    } else {
        HashSet::new()
    };

    info!("Streaming import started (batch size: {})...", batch_size);

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("Invalid progress style template"),
    );

    let batches = reader.read_file_batched(file).context("Failed to read Parquet file")?;

    let mut total_written = 0;
    let mut total_skipped = 0;
    let mut batch_count = 0;

    for batch_result in batches {
        let batch = batch_result.context("Failed to read batch")?;

        // Filter out duplicates if dedup is enabled
        let batch_to_write = if let Some(ref dedup_col) = dedup_column {
            let (new_points, skipped) = filter_existing_points(batch, dedup_col, &existing_keys);
            total_skipped += skipped;
            new_points
        } else {
            batch
        };

        if !batch_to_write.is_empty() {
            let result = writer.write(&batch_to_write).await.context("Failed to write batch")?;
            total_written += result.points_written;
            batch_count += 1;

            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                total_written as f64 / elapsed
            } else {
                0.0
            };
            let skip_msg = if total_skipped > 0 {
                format!(", {} skipped", total_skipped)
            } else {
                String::new()
            };
            progress.set_message(format!(
                "{} points written ({} batches{}) - {:.0} pts/sec",
                total_written, batch_count, skip_msg, rate
            ));
        }
    }

    progress.finish_with_message(format!("{} points written - done", total_written));

    let total_duration = start.elapsed();

    println!("\nImport complete:");
    println!("  Points imported: {}", total_written);
    if total_skipped > 0 {
        println!("  Points skipped (duplicates): {}", total_skipped);
    }
    println!("  Batches processed: {}", batch_count);
    println!("  Total time: {:.2}s", total_duration.as_secs_f64());
    println!(
        "  Throughput: {:.0} points/sec",
        total_written as f64 / total_duration.as_secs_f64()
    );

    Ok(())
}

/// Query existing keys from the database using REST API
async fn query_existing_keys_rest(
    writer: &RustsWriter,
    measurement: &str,
    dedup_column: &str,
) -> Result<HashSet<String>> {
    let mut existing_keys = HashSet::new();

    // Query all records from the measurement, selecting only the dedup column
    // Use a large time range to get all records
    let response = writer
        .query(
            measurement,
            Some((0, i64::MAX)),
            Some(vec![dedup_column.to_string()]),
            None, // No limit - get all records
        )
        .await;

    match response {
        Ok(query_response) => {
            for row in query_response.results {
                // Check in fields
                if let Some(value) = row.fields.get(dedup_column) {
                    if let Some(key_str) = json_value_to_string(value) {
                        existing_keys.insert(key_str);
                    }
                }
                // Also check in tags
                if let Some(value) = row.tags.get(dedup_column) {
                    existing_keys.insert(value.clone());
                }
            }
        }
        Err(e) => {
            // If measurement doesn't exist yet, that's OK - no duplicates
            warn!("Query for existing records failed (may be empty measurement): {}", e);
        }
    }

    Ok(existing_keys)
}

/// Query existing keys from the database using direct storage access
fn query_existing_keys_direct(
    engine: &StorageEngine,
    measurement: &str,
    dedup_column: &str,
) -> Result<HashSet<String>> {
    use rusts_core::TimeRange;

    let mut existing_keys = HashSet::new();

    // Query all records from the measurement
    let time_range = TimeRange::new(0, i64::MAX);
    let results = engine
        .query_measurement(measurement, &time_range)
        .context("Failed to query existing records")?;

    // Note: Direct mode can only deduplicate by fields, not tags.
    // Tags are encoded in the series_id and not directly accessible from MemTablePoint.
    let mut found_column = false;

    for (_series_id, points) in results {
        for point in points {
            // Check in fields (MemTablePoint only has fields, not tags)
            if let Some((_, field_value)) = point.fields.iter().find(|(k, _)| k == dedup_column) {
                existing_keys.insert(field_value_to_string(field_value));
                found_column = true;
            }
        }
    }

    if !found_column && existing_keys.is_empty() {
        // No records found with this column - might be a tag or measurement doesn't exist yet
        warn!(
            "Column '{}' not found in existing records (may be a tag - direct mode can only dedup by fields)",
            dedup_column
        );
    }

    Ok(existing_keys)
}

/// Filter out points that already exist in the database based on the dedup column
fn filter_existing_points(
    points: Vec<Point>,
    dedup_column: &str,
    existing_keys: &HashSet<String>,
) -> (Vec<Point>, usize) {
    let mut result = Vec::with_capacity(points.len());
    let mut skipped = 0;

    for point in points {
        // Try to find the key in tags first
        let key = point
            .tags
            .iter()
            .find(|t| t.key == dedup_column)
            .map(|t| t.value.clone())
            .or_else(|| {
                // Try to find in fields
                point
                    .fields
                    .iter()
                    .find(|f| f.key == dedup_column)
                    .map(|f| field_value_to_string(&f.value))
            });

        if let Some(key) = key {
            if existing_keys.contains(&key) {
                skipped += 1;
            } else {
                result.push(point);
            }
        } else {
            // Column not found, include the point anyway
            result.push(point);
        }
    }

    (result, skipped)
}

/// Convert a field value to a string for deduplication comparison
fn field_value_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Float(v) => v.to_string(),
        FieldValue::Integer(v) => v.to_string(),
        FieldValue::UnsignedInteger(v) => v.to_string(),
        FieldValue::String(v) => v.clone(),
        FieldValue::Boolean(v) => v.to_string(),
    }
}

/// Convert a JSON value to a string for deduplication comparison
fn json_value_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
