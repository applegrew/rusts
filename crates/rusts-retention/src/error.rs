//! Retention error types

use thiserror::Error;

/// Retention errors
#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("Policy not found: {0}")]
    PolicyNotFound(String),

    #[error("Invalid policy: {0}")]
    InvalidPolicy(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Enforcement error: {0}")]
    Enforcement(String),
}

/// Result type for retention operations
pub type Result<T> = std::result::Result<T, RetentionError>;
