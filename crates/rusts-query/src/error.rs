//! Query error types

use thiserror::Error;

/// Query errors
#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Measurement not found: {0}")]
    MeasurementNotFound(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Invalid time range: start {start} >= end {end}")]
    InvalidTimeRange { start: i64, end: i64 },

    #[error("Invalid aggregation: {0}")]
    InvalidAggregation(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Query timeout: exceeded {0} seconds")]
    Timeout(u64),

    #[error("Query cancelled")]
    Cancelled,

    #[error("Window function error: {0}")]
    WindowError(String),
}

/// Result type for query operations
pub type Result<T> = std::result::Result<T, QueryError>;

impl From<rusts_storage::StorageError> for QueryError {
    fn from(e: rusts_storage::StorageError) -> Self {
        QueryError::Storage(e.to_string())
    }
}

impl From<rusts_index::IndexError> for QueryError {
    fn from(e: rusts_index::IndexError) -> Self {
        QueryError::Index(e.to_string())
    }
}
