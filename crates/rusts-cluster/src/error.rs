//! Cluster error types

use thiserror::Error;

/// Cluster errors
#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Shard not found: {0}")]
    ShardNotFound(u32),

    #[error("Replication failed: {0}")]
    ReplicationFailed(String),

    #[error("Quorum not reached: got {got} of {needed}")]
    QuorumNotReached { got: usize, needed: usize },

    #[error("Network error: {0}")]
    Network(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for cluster operations
pub type Result<T> = std::result::Result<T, ClusterError>;
