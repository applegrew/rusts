//! RusTs Index - Indexing engine for time series database
//!
//! This crate provides indexing capabilities:
//! - Series Index: SeriesId -> metadata mapping
//! - Tag Index: Inverted index with Roaring bitmaps
//! - Bloom filters for partition pruning

pub mod bloom;
pub mod error;
pub mod series;
pub mod tag;

pub use bloom::BloomFilter;
pub use error::{IndexError, Result};
pub use series::SeriesIndex;
pub use tag::TagIndex;
