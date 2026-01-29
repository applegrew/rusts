//! Error types for the importer

use thiserror::Error;

/// Importer errors
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Server error: {status} - {message}")]
    Server { status: u16, message: String },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Data conversion error: {0}")]
    Conversion(String),
}

/// Result type for importer operations
pub type Result<T> = std::result::Result<T, ImportError>;
