//! Static cluster configuration
//!
//! Provides cluster configuration via TOML files or environment variables.
//! This is the simplest deployment model - no external dependencies required.
//!
//! For dynamic clustering (auto-discovery, failover), external tools like
//! etcd, Consul, or Kubernetes can be integrated via the NodeRegistry trait.

use crate::error::{ClusterError, Result};
use crate::replication::ReplicationMode;
use crate::shard::ShardingStrategy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's unique identifier
    pub node_id: String,
    /// Cluster name (for isolation)
    #[serde(default = "default_cluster_name")]
    pub cluster_name: String,
    /// Replication mode
    #[serde(default)]
    pub replication_mode: ReplicationMode,
    /// Replication factor (number of copies of each shard)
    #[serde(default = "default_replication_factor")]
    pub replication_factor: usize,
    /// Total number of shards
    #[serde(default = "default_shard_count")]
    pub shard_count: usize,
    /// Sharding strategy
    #[serde(default)]
    pub sharding_strategy: ShardingStrategy,
    /// All nodes in the cluster
    pub nodes: Vec<NodeConfig>,
}

fn default_cluster_name() -> String {
    "rusts".to_string()
}

fn default_replication_factor() -> usize {
    1
}

fn default_shard_count() -> usize {
    16
}

/// Configuration for a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node identifier
    pub id: String,
    /// Node address (host:port)
    pub address: String,
    /// Shards assigned to this node (primary)
    #[serde(default)]
    pub shards: Vec<u32>,
    /// Shards this node holds as replica
    #[serde(default)]
    pub replica_shards: Vec<u32>,
    /// Optional node tags/labels
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl NodeConfig {
    /// Create a new node configuration
    pub fn new(id: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            address: address.into(),
            shards: Vec::new(),
            replica_shards: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Add primary shards
    pub fn with_shards(mut self, shards: Vec<u32>) -> Self {
        self.shards = shards;
        self
    }

    /// Add replica shards
    pub fn with_replica_shards(mut self, shards: Vec<u32>) -> Self {
        self.replica_shards = shards;
        self
    }

    /// Parse the address into a SocketAddr
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        self.address
            .parse()
            .map_err(|e| ClusterError::Configuration(format!("Invalid address '{}': {}", self.address, e)))
    }
}

impl ClusterConfig {
    /// Load configuration from a TOML file
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| ClusterError::Configuration(format!("Failed to read config file: {}", e)))?;
        Self::from_toml(&content)
    }

    /// Parse configuration from TOML string
    pub fn from_toml(content: &str) -> Result<Self> {
        toml::from_str(content)
            .map_err(|e| ClusterError::Configuration(format!("Failed to parse config: {}", e)))
    }

    /// Create a single-node configuration (for standalone mode)
    pub fn single_node(node_id: impl Into<String>, address: impl Into<String>) -> Self {
        let node_id = node_id.into();
        let shard_count = default_shard_count();

        Self {
            node_id: node_id.clone(),
            cluster_name: default_cluster_name(),
            replication_mode: ReplicationMode::default(),
            replication_factor: 1,
            shard_count,
            sharding_strategy: ShardingStrategy::default(),
            nodes: vec![NodeConfig {
                id: node_id,
                address: address.into(),
                shards: (0..shard_count as u32).collect(),
                replica_shards: Vec::new(),
                tags: HashMap::new(),
            }],
        }
    }

    /// Get this node's configuration
    pub fn this_node(&self) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.id == self.node_id)
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: &str) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.id == node_id)
    }

    /// Get the primary node for a shard
    pub fn primary_for_shard(&self, shard_id: u32) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.shards.contains(&shard_id))
    }

    /// Get replica nodes for a shard
    pub fn replicas_for_shard(&self, shard_id: u32) -> Vec<&NodeConfig> {
        self.nodes
            .iter()
            .filter(|n| n.replica_shards.contains(&shard_id))
            .collect()
    }

    /// Get all nodes that hold a shard (primary + replicas)
    pub fn nodes_for_shard(&self, shard_id: u32) -> Vec<&NodeConfig> {
        self.nodes
            .iter()
            .filter(|n| n.shards.contains(&shard_id) || n.replica_shards.contains(&shard_id))
            .collect()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Check node_id exists in nodes
        if self.this_node().is_none() {
            return Err(ClusterError::Configuration(format!(
                "node_id '{}' not found in nodes list",
                self.node_id
            )));
        }

        // Check all shards are assigned
        let mut assigned_shards: Vec<bool> = vec![false; self.shard_count];
        for node in &self.nodes {
            for &shard in &node.shards {
                if shard as usize >= self.shard_count {
                    return Err(ClusterError::Configuration(format!(
                        "Shard {} exceeds shard_count {}",
                        shard, self.shard_count
                    )));
                }
                if assigned_shards[shard as usize] {
                    return Err(ClusterError::Configuration(format!(
                        "Shard {} assigned to multiple nodes as primary",
                        shard
                    )));
                }
                assigned_shards[shard as usize] = true;
            }
        }

        // Check all shards have a primary
        for (shard, assigned) in assigned_shards.iter().enumerate() {
            if !assigned {
                return Err(ClusterError::Configuration(format!(
                    "Shard {} has no primary node assigned",
                    shard
                )));
            }
        }

        // Check replication factor
        if self.replication_factor > self.nodes.len() {
            return Err(ClusterError::Configuration(format!(
                "Replication factor {} exceeds node count {}",
                self.replication_factor,
                self.nodes.len()
            )));
        }

        Ok(())
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::single_node("node1", "127.0.0.1:8086")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_config() {
        let config = ClusterConfig::single_node("test-node", "127.0.0.1:8086");

        assert_eq!(config.node_id, "test-node");
        assert_eq!(config.nodes.len(), 1);
        assert_eq!(config.shard_count, 16);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
            node_id = "node1"
            cluster_name = "test-cluster"
            replication_mode = "Quorum"
            replication_factor = 2
            shard_count = 4

            [[nodes]]
            id = "node1"
            address = "10.0.1.1:8086"
            shards = [0, 1]

            [[nodes]]
            id = "node2"
            address = "10.0.1.2:8086"
            shards = [2, 3]
            replica_shards = [0, 1]

            [[nodes]]
            id = "node3"
            address = "10.0.1.3:8086"
            replica_shards = [2, 3]
        "#;

        let config = ClusterConfig::from_toml(toml).unwrap();

        assert_eq!(config.cluster_name, "test-cluster");
        assert_eq!(config.replication_factor, 2);
        assert_eq!(config.nodes.len(), 3);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_shard_lookup() {
        let toml = r#"
            node_id = "node1"
            shard_count = 4

            [[nodes]]
            id = "node1"
            address = "10.0.1.1:8086"
            shards = [0, 1]

            [[nodes]]
            id = "node2"
            address = "10.0.1.2:8086"
            shards = [2, 3]
            replica_shards = [0]
        "#;

        let config = ClusterConfig::from_toml(toml).unwrap();

        // Primary lookup
        assert_eq!(config.primary_for_shard(0).unwrap().id, "node1");
        assert_eq!(config.primary_for_shard(2).unwrap().id, "node2");

        // Replica lookup
        let replicas = config.replicas_for_shard(0);
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].id, "node2");
    }

    #[test]
    fn test_validation_missing_shard() {
        let toml = r#"
            node_id = "node1"
            shard_count = 4

            [[nodes]]
            id = "node1"
            address = "10.0.1.1:8086"
            shards = [0, 1, 2]  # Missing shard 3
        "#;

        let config = ClusterConfig::from_toml(toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_duplicate_shard() {
        let toml = r#"
            node_id = "node1"
            shard_count = 2

            [[nodes]]
            id = "node1"
            address = "10.0.1.1:8086"
            shards = [0, 1]

            [[nodes]]
            id = "node2"
            address = "10.0.1.2:8086"
            shards = [1]  # Duplicate primary for shard 1
        "#;

        let config = ClusterConfig::from_toml(toml).unwrap();
        assert!(config.validate().is_err());
    }
}
