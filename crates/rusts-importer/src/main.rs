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
use rusts_importer::{inspect_parquet_schema, ParquetReader, ParquetReaderConfig, RustsWriter};
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

    // Read the file
    let reader = ParquetReader::new(config);
    let start = Instant::now();

    info!("Reading data...");
    let points = reader
        .read_file(&file)
        .context("Failed to read Parquet file")?;

    let read_duration = start.elapsed();
    info!(
        "Read {} points in {:.2}s ({:.0} points/sec)",
        points.len(),
        read_duration.as_secs_f64(),
        points.len() as f64 / read_duration.as_secs_f64()
    );

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
        info!("Dry run - skipping server write");
        println!("\nDry run complete:");
        println!("  Points read: {}", points.len());
        println!("  Would write to: {}", server);
        return Ok(());
    }

    // Connect to server
    info!("Connecting to server: {}", server);
    let writer = RustsWriter::new(&server).context("Failed to create writer")?;

    // Check server health
    if !writer.health_check().await.unwrap_or(false) {
        anyhow::bail!("Server is not healthy or unreachable: {}", server);
    }
    info!("Server is healthy");

    // Write data
    info!("Writing {} points in batches of {}...", points.len(), batch_size);
    let write_start = Instant::now();

    let result = writer
        .write_batched(&points, batch_size)
        .await
        .context("Failed to write to server")?;

    let write_duration = write_start.elapsed();
    let total_duration = start.elapsed();

    info!(
        "Wrote {} points in {:.2}s ({:.0} points/sec)",
        result.points_written,
        write_duration.as_secs_f64(),
        result.points_written as f64 / write_duration.as_secs_f64()
    );

    println!("\nImport complete:");
    println!("  Points imported: {}", result.points_written);
    println!("  Bytes sent: {} KB", result.bytes_sent / 1024);
    println!("  Read time: {:.2}s", read_duration.as_secs_f64());
    println!("  Write time: {:.2}s", write_duration.as_secs_f64());
    println!("  Total time: {:.2}s", total_duration.as_secs_f64());
    println!(
        "  Throughput: {:.0} points/sec",
        result.points_written as f64 / total_duration.as_secs_f64()
    );

    Ok(())
}
