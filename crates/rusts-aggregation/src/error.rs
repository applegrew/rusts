//! Aggregation error types

use thiserror::Error;

/// Aggregation errors
#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("Invalid aggregate definition: {0}")]
    InvalidDefinition(String),

    #[error("Aggregate not found: {0}")]
    NotFound(String),

    #[error("Computation error: {0}")]
    Computation(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type for aggregation operations
pub type Result<T> = std::result::Result<T, AggregationError>;
