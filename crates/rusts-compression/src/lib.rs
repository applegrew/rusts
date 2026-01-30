//! RusTs Compression - Compression algorithms for time series data
//!
//! This crate provides specialized compression algorithms optimized for time series:
//! - **Gorilla XOR**: Facebook's algorithm for float compression (10-12x)
//! - **Delta-of-delta**: For timestamps (10-15x)
//! - **Dictionary encoding**: For strings/tags (3-10x)
//! - **Block compression**: LZ4 (fast) / Zstd (ratio)

pub mod bits;
pub mod block;
pub mod dictionary;
pub mod error;
pub mod float;
pub mod integer;
pub mod timestamp;

pub use block::{BlockCompressor, CompressionLevel};
pub use dictionary::{DictionaryDecoder, DictionaryEncoder};
pub use error::{CompressionError, Result};
pub use float::{GorillaDecoder, GorillaEncoder};
pub use integer::{IntegerDecoder, IntegerEncoder};
pub use timestamp::{TimestampDecoder, TimestampEncoder};
