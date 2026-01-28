//! Replication protocol
//!
//! Defines replication modes and quorum checking logic.
//! Actual replication coordination is handled by the storage layer.

use crate::error::{ClusterError, Result};
use serde::{Deserialize, Serialize};

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// All replicas must acknowledge
    Synchronous,
    /// Majority of replicas must acknowledge
    #[default]
    Quorum,
    /// Fire-and-forget (no acknowledgment required)
    Asynchronous,
}

impl ReplicationMode {
    /// Calculate required acknowledgments
    pub fn required_acks(&self, replica_count: usize) -> usize {
        match self {
            ReplicationMode::Synchronous => replica_count,
            ReplicationMode::Quorum => (replica_count / 2) + 1,
            ReplicationMode::Asynchronous => 0,
        }
    }
}

/// Replication acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationAck {
    /// Node ID
    pub node_id: String,
    /// Success flag
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Latency in milliseconds
    pub latency_ms: u64,
}

/// Replication result
#[derive(Debug, Clone)]
pub struct ReplicationResult {
    /// Number of successful replications
    pub success_count: usize,
    /// Total replica count
    pub total_count: usize,
    /// Individual acknowledgments
    pub acks: Vec<ReplicationAck>,
    /// Whether quorum was reached
    pub quorum_reached: bool,
}

impl ReplicationResult {
    /// Check if all replications succeeded
    pub fn all_success(&self) -> bool {
        self.success_count == self.total_count
    }

    /// Get failed nodes
    pub fn failed_nodes(&self) -> Vec<&str> {
        self.acks
            .iter()
            .filter(|a| !a.success)
            .map(|a| a.node_id.as_str())
            .collect()
    }
}

/// Replication protocol handler
pub struct ReplicationProtocol {
    /// Replication mode
    mode: ReplicationMode,
    /// Replication factor
    replication_factor: usize,
}

impl ReplicationProtocol {
    /// Create a new replication protocol
    pub fn new(mode: ReplicationMode, replication_factor: usize) -> Self {
        Self {
            mode,
            replication_factor,
        }
    }

    /// Get replication mode
    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }

    /// Get replication factor
    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// Check if enough acknowledgments received
    pub fn check_quorum(&self, acks: &[ReplicationAck]) -> Result<ReplicationResult> {
        let success_count = acks.iter().filter(|a| a.success).count();
        let required = self.mode.required_acks(acks.len());
        let quorum_reached = success_count >= required;

        let result = ReplicationResult {
            success_count,
            total_count: acks.len(),
            acks: acks.to_vec(),
            quorum_reached,
        };

        if self.mode != ReplicationMode::Asynchronous && !quorum_reached {
            return Err(ClusterError::QuorumNotReached {
                got: success_count,
                needed: required,
            });
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_mode_required_acks() {
        // 3 replicas
        assert_eq!(ReplicationMode::Synchronous.required_acks(3), 3);
        assert_eq!(ReplicationMode::Quorum.required_acks(3), 2);
        assert_eq!(ReplicationMode::Asynchronous.required_acks(3), 0);

        // 5 replicas
        assert_eq!(ReplicationMode::Synchronous.required_acks(5), 5);
        assert_eq!(ReplicationMode::Quorum.required_acks(5), 3);
        assert_eq!(ReplicationMode::Asynchronous.required_acks(5), 0);
    }

    #[test]
    fn test_quorum_check_sync() {
        let protocol = ReplicationProtocol::new(ReplicationMode::Synchronous, 3);

        let acks = vec![
            ReplicationAck {
                node_id: "node1".to_string(),
                success: true,
                error: None,
                latency_ms: 10,
            },
            ReplicationAck {
                node_id: "node2".to_string(),
                success: true,
                error: None,
                latency_ms: 15,
            },
            ReplicationAck {
                node_id: "node3".to_string(),
                success: true,
                error: None,
                latency_ms: 20,
            },
        ];

        let result = protocol.check_quorum(&acks).unwrap();
        assert!(result.quorum_reached);
        assert!(result.all_success());

        // One failure should fail for sync
        let acks_with_failure = vec![
            ReplicationAck {
                node_id: "node1".to_string(),
                success: true,
                error: None,
                latency_ms: 10,
            },
            ReplicationAck {
                node_id: "node2".to_string(),
                success: false,
                error: Some("timeout".to_string()),
                latency_ms: 100,
            },
            ReplicationAck {
                node_id: "node3".to_string(),
                success: true,
                error: None,
                latency_ms: 20,
            },
        ];

        assert!(protocol.check_quorum(&acks_with_failure).is_err());
    }

    #[test]
    fn test_quorum_check_quorum() {
        let protocol = ReplicationProtocol::new(ReplicationMode::Quorum, 3);

        // 2/3 success should pass for quorum
        let acks = vec![
            ReplicationAck {
                node_id: "node1".to_string(),
                success: true,
                error: None,
                latency_ms: 10,
            },
            ReplicationAck {
                node_id: "node2".to_string(),
                success: false,
                error: Some("timeout".to_string()),
                latency_ms: 100,
            },
            ReplicationAck {
                node_id: "node3".to_string(),
                success: true,
                error: None,
                latency_ms: 20,
            },
        ];

        let result = protocol.check_quorum(&acks).unwrap();
        assert!(result.quorum_reached);
        assert!(!result.all_success());
        assert_eq!(result.failed_nodes(), vec!["node2"]);

        // 1/3 should fail
        let acks_insufficient = vec![
            ReplicationAck {
                node_id: "node1".to_string(),
                success: true,
                error: None,
                latency_ms: 10,
            },
            ReplicationAck {
                node_id: "node2".to_string(),
                success: false,
                error: Some("timeout".to_string()),
                latency_ms: 100,
            },
            ReplicationAck {
                node_id: "node3".to_string(),
                success: false,
                error: Some("error".to_string()),
                latency_ms: 50,
            },
        ];

        assert!(protocol.check_quorum(&acks_insufficient).is_err());
    }

    #[test]
    fn test_quorum_check_async() {
        let protocol = ReplicationProtocol::new(ReplicationMode::Asynchronous, 3);

        // All failures should still pass for async
        let acks = vec![
            ReplicationAck {
                node_id: "node1".to_string(),
                success: false,
                error: Some("error".to_string()),
                latency_ms: 10,
            },
            ReplicationAck {
                node_id: "node2".to_string(),
                success: false,
                error: Some("error".to_string()),
                latency_ms: 100,
            },
        ];

        let result = protocol.check_quorum(&acks).unwrap();
        assert!(result.quorum_reached); // Async doesn't require any acks
    }
}
