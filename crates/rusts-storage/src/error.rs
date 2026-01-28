//! Storage error types

use thiserror::Error;

/// Storage errors
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),

    #[error("WAL write failed: {0}")]
    WalWriteFailed(String),

    #[error("Segment not found: {0}")]
    SegmentNotFound(String),

    #[error("Partition not found: {0}")]
    PartitionNotFound(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("MemTable full")]
    MemTableFull,

    #[error("Storage engine not initialized")]
    NotInitialized,

    #[error("Storage engine already initialized")]
    AlreadyInitialized,

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Core error: {0}")]
    Core(#[from] rusts_core::CoreError),

    #[error("Channel send error")]
    ChannelSend,
}

/// Result type for storage operations
pub type Result<T> = std::result::Result<T, StorageError>;

impl From<bincode::Error> for StorageError {
    fn from(e: bincode::Error) -> Self {
        StorageError::Serialization(e.to_string())
    }
}

impl From<rusts_compression::CompressionError> for StorageError {
    fn from(e: rusts_compression::CompressionError) -> Self {
        StorageError::Compression(e.to_string())
    }
}
