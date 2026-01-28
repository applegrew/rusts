//! Shard router for distributing requests
//!
//! Routes write and query requests to the appropriate shard based on
//! the configured sharding strategy.

use crate::config::ClusterConfig;
use crate::error::{ClusterError, Result};
use crate::shard::{Shard, ShardKey, ShardingStrategy};
use dashmap::DashMap;

/// Shard router for distributing requests across shards
pub struct ShardRouter {
    /// Sharding strategy
    strategy: ShardingStrategy,
    /// Number of shards
    num_shards: u32,
    /// Shard map
    shards: DashMap<u32, Shard>,
    /// This node's ID
    node_id: String,
}

impl ShardRouter {
    /// Create a new shard router
    pub fn new(strategy: ShardingStrategy, num_shards: u32, node_id: impl Into<String>) -> Self {
        Self {
            strategy,
            num_shards,
            shards: DashMap::new(),
            node_id: node_id.into(),
        }
    }

    /// Create router from cluster configuration
    pub fn from_config(config: &ClusterConfig) -> Self {
        let router = Self::new(
            config.sharding_strategy,
            config.shard_count as u32,
            &config.node_id,
        );

        // Register all shards from config
        for node in &config.nodes {
            for &shard_id in &node.shards {
                let mut shard = Shard::new(shard_id, node.id.clone());
                shard.address = Some(node.address.clone());

                // Add replicas
                for replica_node in &config.nodes {
                    if replica_node.replica_shards.contains(&shard_id) {
                        shard.add_replica(replica_node.id.clone());
                    }
                }

                router.register_shard(shard);
            }
        }

        router
    }

    /// Get this node's ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Check if this node is primary for a shard
    pub fn is_primary_for(&self, shard_id: u32) -> bool {
        self.shards
            .get(&shard_id)
            .map(|s| s.primary == self.node_id)
            .unwrap_or(false)
    }

    /// Check if this node holds a shard (primary or replica)
    pub fn holds_shard(&self, shard_id: u32) -> bool {
        self.shards
            .get(&shard_id)
            .map(|s| s.primary == self.node_id || s.replicas.contains(&self.node_id))
            .unwrap_or(false)
    }

    /// Get shards this node is primary for
    pub fn local_primary_shards(&self) -> Vec<Shard> {
        self.primary_shards_for_node(&self.node_id)
    }

    /// Get all shards this node holds
    pub fn local_shards(&self) -> Vec<Shard> {
        self.shards_for_node(&self.node_id)
    }

    /// Register a shard
    pub fn register_shard(&self, shard: Shard) {
        self.shards.insert(shard.id, shard);
    }

    /// Get shard for a key
    pub fn route(&self, key: &ShardKey) -> Result<Shard> {
        let shard_id = self.strategy.compute_shard(key, self.num_shards);

        self.shards
            .get(&shard_id)
            .map(|s| s.clone())
            .ok_or(ClusterError::ShardNotFound(shard_id))
    }

    /// Get shard by ID
    pub fn get_shard(&self, shard_id: u32) -> Option<Shard> {
        self.shards.get(&shard_id).map(|s| s.clone())
    }

    /// Get all shards
    pub fn all_shards(&self) -> Vec<Shard> {
        self.shards.iter().map(|r| r.clone()).collect()
    }

    /// Get shards for a node
    pub fn shards_for_node(&self, node_id: &str) -> Vec<Shard> {
        self.shards
            .iter()
            .filter(|s| s.primary == node_id || s.replicas.contains(&node_id.to_string()))
            .map(|s| s.clone())
            .collect()
    }

    /// Get primary shards for a node
    pub fn primary_shards_for_node(&self, node_id: &str) -> Vec<Shard> {
        self.shards
            .iter()
            .filter(|s| s.primary == node_id)
            .map(|s| s.clone())
            .collect()
    }

    /// Update shard
    pub fn update_shard(&self, shard: Shard) {
        self.shards.insert(shard.id, shard);
    }

    /// Remove shard
    pub fn remove_shard(&self, shard_id: u32) -> Option<Shard> {
        self.shards.remove(&shard_id).map(|(_, s)| s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_basic() {
        let router = ShardRouter::new(ShardingStrategy::BySeries, 3, "node1");

        // Register shards
        router.register_shard(Shard::new(0, "node1".to_string()));
        router.register_shard(Shard::new(1, "node2".to_string()));
        router.register_shard(Shard::new(2, "node3".to_string()));

        // Route keys
        let key = ShardKey::new("cpu".to_string(), 100, 1000);
        let shard = router.route(&key).unwrap();
        assert!(shard.id < 3);
    }

    #[test]
    fn test_router_consistency() {
        let router = ShardRouter::new(ShardingStrategy::BySeries, 10, "node0");

        for i in 0..10 {
            router.register_shard(Shard::new(i, format!("node{}", i)));
        }

        // Same key should always route to same shard
        let key = ShardKey::new("cpu".to_string(), 12345, 1000);

        let shard1 = router.route(&key).unwrap();
        let shard2 = router.route(&key).unwrap();

        assert_eq!(shard1.id, shard2.id);
    }

    #[test]
    fn test_router_node_shards() {
        let router = ShardRouter::new(ShardingStrategy::BySeries, 3, "node1");

        let mut shard0 = Shard::new(0, "node1".to_string());
        shard0.add_replica("node2".to_string());

        let mut shard1 = Shard::new(1, "node2".to_string());
        shard1.add_replica("node1".to_string());

        let shard2 = Shard::new(2, "node3".to_string());

        router.register_shard(shard0);
        router.register_shard(shard1);
        router.register_shard(shard2);

        // node1 is primary for shard0, replica for shard1
        let node1_primary = router.primary_shards_for_node("node1");
        assert_eq!(node1_primary.len(), 1);
        assert_eq!(node1_primary[0].id, 0);

        let node1_all = router.shards_for_node("node1");
        assert_eq!(node1_all.len(), 2);

        // Test local shard helpers
        assert!(router.is_primary_for(0));
        assert!(!router.is_primary_for(1));
        assert!(router.holds_shard(0));
        assert!(router.holds_shard(1)); // replica
        assert!(!router.holds_shard(2));
    }

    #[test]
    fn test_router_from_config() {
        let config = ClusterConfig::single_node("test-node", "127.0.0.1:8086");
        let router = ShardRouter::from_config(&config);

        assert_eq!(router.node_id(), "test-node");
        assert_eq!(router.all_shards().len(), 16);
        assert_eq!(router.local_primary_shards().len(), 16);
    }
}
