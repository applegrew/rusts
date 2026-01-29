//! RusTs Importer - Data import utilities for RusTs time series database
//!
//! This crate provides tools for importing data from various formats into RusTs.
//!
//! ## Supported Formats
//!
//! - **Parquet**: Apache Parquet columnar format
//!
//! ## Usage
//!
//! ```no_run
//! use rusts_importer::{ParquetReader, ParquetReaderConfig, RustsWriter};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Configure the reader
//!     let config = ParquetReaderConfig::new("metrics")
//!         .with_timestamp_column("timestamp")
//!         .with_tag_columns(vec!["host".to_string(), "region".to_string()]);
//!
//!     // Read the Parquet file
//!     let reader = ParquetReader::new(config);
//!     let points = reader.read_file("data.parquet")?;
//!
//!     // Write to RusTs server
//!     let writer = RustsWriter::new("http://localhost:8086")?;
//!     let result = writer.write(&points).await?;
//!
//!     println!("Imported {} points", result.points_written);
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod parquet;
pub mod writer;

pub use error::{ImportError, Result};
pub use parquet::{inspect_parquet_schema, ParquetReader, ParquetReaderConfig};
pub use writer::{RustsWriter, WriteResult};
