//! RusTs Importer CLI
//!
//! A command-line tool for importing data into RusTs from various file formats.
//!
//! ## Usage
//!
//! ```bash
//! # Import a Parquet file
//! rusts-import parquet data.parquet --measurement cpu --server http://localhost:8086
//!
//! # Specify tag columns
//! rusts-import parquet data.parquet -m metrics --tags host,region
//!
//! # Inspect Parquet schema
//! rusts-import parquet data.parquet --schema-only
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use rusts_core::{FieldValue, Point};
use rusts_importer::{inspect_parquet_schema, ParquetReader, ParquetReaderConfig, RustsWriter};
use rusts_storage::{StorageEngine, StorageEngineConfig};
use rusts_storage::memtable::FlushTrigger;
use rusts_storage::wal::WalDurability;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rusts-import")]
#[command(author, version, about = "Import data into RusTs time series database")]
struct Cli {
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

        /// Column name to use for deduplication (removes duplicate values)
        #[arg(long)]
        dedup_column: Option<String>,

        /// Write directly to storage engine (bypasses REST API, much faster)
        #[arg(long)]
        direct: bool,

        /// Data directory for direct mode (default: ./data)
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
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
                data_dir,
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

    info!("Configuration:");
    info!("  Measurement: {}", measurement);
    info!("  Timestamp column: {}", timestamp_column);
    info!("  Tag columns: {:?}", tags);
    info!("  Batch size: {}", batch_size);
    if let Some(ref col) = dedup_column {
        info!("  Dedup column: {}", col);
    }

    // Read the file
    let reader = ParquetReader::new(config);
    let start = Instant::now();

    info!("Reading data...");
    let points = reader
        .read_file(&file)
        .context("Failed to read Parquet file")?;

    let read_duration = start.elapsed();
    let original_count = points.len();
    info!(
        "Read {} points in {:.2}s ({:.0} points/sec)",
        original_count,
        read_duration.as_secs_f64(),
        original_count as f64 / read_duration.as_secs_f64()
    );

    // Deduplicate if requested
    let points = if let Some(ref dedup_col) = dedup_column {
        info!("Deduplicating by column: {}", dedup_col);
        let deduped = deduplicate_points(points, dedup_col);
        let removed = original_count - deduped.len();
        info!(
            "Deduplication complete: {} duplicates removed, {} unique points",
            removed,
            deduped.len()
        );
        deduped
    } else {
        points
    };

    if points.is_empty() {
        warn!("No points to import");
        return Ok(());
    }

    // Show sample point
    if let Some(sample) = points.first() {
        info!("Sample point:");
        info!("  Measurement: {}", sample.measurement);
        info!("  Tags: {:?}", sample.tags);
        info!(
            "  Fields: {:?}",
            sample
                .fields
                .iter()
                .map(|f| &f.key)
                .collect::<Vec<_>>()
        );
        info!("  Timestamp: {}", sample.timestamp);
    }

    if dry_run {
        info!("Dry run - skipping write");
        println!("\nDry run complete:");
        println!("  Points read: {}", points.len());
        if direct {
            println!("  Would write to: {}", data_dir.display());
        } else {
            println!("  Would write to: {}", server);
        }
        return Ok(());
    }

    // Write data - either direct or via REST
    let write_start = Instant::now();
    let points_written;

    if direct {
        // Direct mode - write to storage engine
        info!("Opening storage engine at: {}", data_dir.display());

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_dir: None,
            wal_durability: WalDurability::None, // Skip WAL - source file is our recovery
            flush_trigger: FlushTrigger {
                max_size: 256 * 1024 * 1024,    // 256MB memtable
                max_points: 10_000_000,          // 10M points
                max_age_nanos: i64::MAX,         // Don't flush on age during import
            },
            partition_duration: 24 * 60 * 60 * 1_000_000_000, // 1 day
            compression: rusts_compression::CompressionLevel::Default,
        };

        let engine = StorageEngine::new(config).context("Failed to open storage engine")?;

        info!("Writing {} points in batches of {}...", points.len(), batch_size);

        let progress = ProgressBar::new(points.len() as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
                .expect("Invalid progress style template")
                .progress_chars("#>-"),
        );

        let mut written = 0;
        for batch in points.chunks(batch_size) {
            engine.write_batch(batch).context("Failed to write batch")?;
            written += batch.len();
            progress.set_position(written as u64);
            let elapsed = write_start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let rate = written as f64 / elapsed;
                progress.set_message(format!("{:.0} pts/sec", rate));
            }
        }

        // Flush and shutdown cleanly
        engine.flush().context("Failed to flush")?;
        engine.shutdown().context("Failed to shutdown engine")?;

        progress.finish_with_message("done");
        points_written = written;
    } else {
        // REST mode - write via HTTP
        info!("Connecting to server: {}", server);
        let writer = RustsWriter::new(&server).context("Failed to create writer")?;

        // Check server health
        if !writer.health_check().await.unwrap_or(false) {
            anyhow::bail!("Server is not healthy or unreachable: {}", server);
        }
        info!("Server is healthy");

        info!("Writing {} points in batches of {}...", points.len(), batch_size);

        let progress = ProgressBar::new(points.len() as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
                .expect("Invalid progress style template")
                .progress_chars("#>-"),
        );

        let result = writer
            .write_batched_with_progress(&points, batch_size, |written, _total| {
                progress.set_position(written as u64);
                let elapsed = write_start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    let rate = written as f64 / elapsed;
                    progress.set_message(format!("{:.0} pts/sec", rate));
                }
            })
            .await
            .context("Failed to write to server")?;

        progress.finish_with_message("done");
        points_written = result.points_written;
    }

    let write_duration = write_start.elapsed();
    let total_duration = start.elapsed();

    info!(
        "Wrote {} points in {:.2}s ({:.0} points/sec)",
        points_written,
        write_duration.as_secs_f64(),
        points_written as f64 / write_duration.as_secs_f64()
    );

    println!("\nImport complete:");
    println!("  Points imported: {}", points_written);
    println!("  Read time: {:.2}s", read_duration.as_secs_f64());
    println!("  Write time: {:.2}s", write_duration.as_secs_f64());
    println!("  Total time: {:.2}s", total_duration.as_secs_f64());
    println!(
        "  Throughput: {:.0} points/sec",
        points_written as f64 / total_duration.as_secs_f64()
    );

    Ok(())
}

/// Deduplicate points based on a column value.
/// The column can be a tag or a field. First occurrence is kept.
fn deduplicate_points(points: Vec<Point>, dedup_column: &str) -> Vec<Point> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut result = Vec::with_capacity(points.len());

    for point in points {
        // Try to find the value in tags first
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
            if seen.insert(key) {
                result.push(point);
            }
        } else {
            // Column not found, include the point anyway
            result.push(point);
        }
    }

    result
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
