//! Compression error types

use thiserror::Error;

/// Compression errors
#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Buffer overflow: needed {needed} bytes, had {available}")]
    BufferOverflow { needed: usize, available: usize },

    #[error("Buffer underflow: unexpected end of data")]
    BufferUnderflow,

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Dictionary full: max {max} entries")]
    DictionaryFull { max: usize },

    #[error("Invalid dictionary index: {index} (max: {max})")]
    InvalidDictionaryIndex { index: u32, max: u32 },

    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for compression operations
pub type Result<T> = std::result::Result<T, CompressionError>;
