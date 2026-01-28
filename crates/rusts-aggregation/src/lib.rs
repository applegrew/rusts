//! RusTs Aggregation - Continuous aggregates and downsampling
//!
//! This crate provides:
//! - Continuous aggregates (materialized views)
//! - Downsampling engine
//! - Rollup definitions

pub mod continuous;
pub mod downsample;
pub mod error;

pub use continuous::{ContinuousAggregate, ContinuousAggregateEngine};
pub use downsample::{DownsampleConfig, DownsampleEngine};
pub use error::{AggregationError, Result};
