//! RusTs Query - Query engine for time series database
//!
//! This crate provides query capabilities:
//! - Query model and parsing
//! - Query planning with partition pruning
//! - Parallel segment scanning
//! - Aggregation functions

pub mod aggregation;
pub mod error;
pub mod executor;
pub mod model;
pub mod planner;

pub use aggregation::{AggregateFunction, Aggregator};
pub use error::{QueryError, Result};
pub use executor::QueryExecutor;
pub use model::{Query, QueryBuilder, QueryResult};
pub use planner::QueryPlanner;
