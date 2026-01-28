//! Error types for rusts-core

use thiserror::Error;

/// Core error types
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    #[error("Empty measurement name")]
    EmptyMeasurement,

    #[error("Empty tag key")]
    EmptyTagKey,

    #[error("Empty field key")]
    EmptyFieldKey,

    #[error("No fields provided")]
    NoFields,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Invalid field type: expected {expected}, got {actual}")]
    InvalidFieldType { expected: String, actual: String },
}

/// Result type alias for core operations
pub type Result<T> = std::result::Result<T, CoreError>;

impl From<bincode::Error> for CoreError {
    fn from(e: bincode::Error) -> Self {
        CoreError::Serialization(e.to_string())
    }
}
