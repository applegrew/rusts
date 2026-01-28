//! RusTs Core - Core types for the time series database
//!
//! This crate provides the fundamental data types used throughout the RusTs TSDB:
//! - `Timestamp`: Nanosecond-precision Unix epoch timestamps
//! - `SeriesId`: Unique identifier for a time series (measurement + tags)
//! - `Tag`: Key-value pair for series identification
//! - `FieldValue`: Typed field values (Float, Integer, String, Boolean, etc.)
//! - `Field`: Named field with a value
//! - `Point`: A single data point with timestamp, tags, and fields
//! - `Series`: Metadata about a time series

pub mod error;
pub mod types;

pub use error::{CoreError, Result};
pub use types::*;
