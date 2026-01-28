//! Index error types

use thiserror::Error;

/// Index errors
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Series not found: {0}")]
    SeriesNotFound(u64),

    #[error("Tag not found: {key}={value}")]
    TagNotFound { key: String, value: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Result type for index operations
pub type Result<T> = std::result::Result<T, IndexError>;

impl From<bincode::Error> for IndexError {
    fn from(e: bincode::Error) -> Self {
        IndexError::Serialization(e.to_string())
    }
}
