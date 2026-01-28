//! Sharding implementation
//!
//! Provides sharding strategies for distributing data across nodes.

use fxhash::FxHasher;
use rusts_core::{SeriesId, Timestamp};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Shard key for routing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    /// Measurement name
    pub measurement: String,
    /// Series ID
    pub series_id: SeriesId,
    /// Timestamp (for time-based sharding)
    pub timestamp: Timestamp,
}

impl ShardKey {
    /// Create a new shard key
    pub fn new(measurement: String, series_id: SeriesId, timestamp: Timestamp) -> Self {
        Self {
            measurement,
            series_id,
            timestamp,
        }
    }
}

/// Sharding strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ShardingStrategy {
    /// Shard by measurement name
    ByMeasurement,
    /// Shard by series ID (hash) - default, good for even distribution
    #[default]
    BySeries,
    /// Shard by time range
    ByTime { duration_nanos: i64 },
    /// Composite: time + series
    Composite { time_duration_nanos: i64 },
}

impl ShardingStrategy {
    /// Compute shard ID for a key
    pub fn compute_shard(&self, key: &ShardKey, num_shards: u32) -> u32 {
        match self {
            ShardingStrategy::ByMeasurement => {
                let mut hasher = FxHasher::default();
                key.measurement.hash(&mut hasher);
                (hasher.finish() % num_shards as u64) as u32
            }
            ShardingStrategy::BySeries => {
                (key.series_id % num_shards as u64) as u32
            }
            ShardingStrategy::ByTime { duration_nanos } => {
                let time_bucket = key.timestamp / duration_nanos;
                (time_bucket.unsigned_abs() % num_shards as u64) as u32
            }
            ShardingStrategy::Composite { time_duration_nanos } => {
                // Combine time and series
                let time_bucket = key.timestamp / time_duration_nanos;
                let combined = key.series_id.wrapping_add(time_bucket as u64);
                (combined % num_shards as u64) as u32
            }
        }
    }
}

/// Shard information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    /// Shard ID
    pub id: u32,
    /// Primary node ID
    pub primary: String,
    /// Primary node address (for routing)
    pub address: Option<String>,
    /// Replica node IDs
    pub replicas: Vec<String>,
    /// Shard state
    pub state: ShardState,
}

/// Shard state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardState {
    /// Shard is healthy and accepting writes
    Active,
    /// Shard is being rebalanced
    Rebalancing,
    /// Shard is read-only (during maintenance)
    ReadOnly,
    /// Shard is offline
    Offline,
}

impl Shard {
    /// Create a new shard
    pub fn new(id: u32, primary: String) -> Self {
        Self {
            id,
            primary,
            address: None,
            replicas: Vec::new(),
            state: ShardState::Active,
        }
    }

    /// Create a new shard with address
    pub fn with_address(id: u32, primary: String, address: String) -> Self {
        Self {
            id,
            primary,
            address: Some(address),
            replicas: Vec::new(),
            state: ShardState::Active,
        }
    }

    /// Add a replica
    pub fn add_replica(&mut self, node_id: String) {
        if !self.replicas.contains(&node_id) && node_id != self.primary {
            self.replicas.push(node_id);
        }
    }

    /// Remove a replica
    pub fn remove_replica(&mut self, node_id: &str) {
        self.replicas.retain(|n| n != node_id);
    }

    /// Get all nodes (primary + replicas)
    pub fn all_nodes(&self) -> Vec<&str> {
        let mut nodes = vec![self.primary.as_str()];
        nodes.extend(self.replicas.iter().map(|s| s.as_str()));
        nodes
    }

    /// Check if shard can accept writes
    pub fn can_write(&self) -> bool {
        self.state == ShardState::Active
    }

    /// Check if shard can accept reads
    pub fn can_read(&self) -> bool {
        self.state == ShardState::Active || self.state == ShardState::ReadOnly
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharding_by_measurement() {
        let strategy = ShardingStrategy::ByMeasurement;

        let key1 = ShardKey::new("cpu".to_string(), 1, 1000);
        let key2 = ShardKey::new("cpu".to_string(), 2, 2000);
        let key3 = ShardKey::new("mem".to_string(), 1, 1000);

        let num_shards = 10;

        // Same measurement should go to same shard
        assert_eq!(
            strategy.compute_shard(&key1, num_shards),
            strategy.compute_shard(&key2, num_shards)
        );

        // Different measurements may go to different shards
        // (not guaranteed, but likely with good hash)
    }

    #[test]
    fn test_sharding_by_series() {
        let strategy = ShardingStrategy::BySeries;
        let num_shards = 10;

        for i in 0..100 {
            let key = ShardKey::new("cpu".to_string(), i, 1000);
            let shard = strategy.compute_shard(&key, num_shards);
            assert!(shard < num_shards);
        }
    }

    #[test]
    fn test_sharding_by_time() {
        let strategy = ShardingStrategy::ByTime {
            duration_nanos: 86400_000_000_000, // 1 day
        };
        let num_shards = 10;

        // Same day should go to same shard
        let key1 = ShardKey::new("cpu".to_string(), 1, 1000_000_000);
        let key2 = ShardKey::new("cpu".to_string(), 2, 86399_000_000_000);

        assert_eq!(
            strategy.compute_shard(&key1, num_shards),
            strategy.compute_shard(&key2, num_shards)
        );

        // Different day goes to different shard (likely)
        let key3 = ShardKey::new("cpu".to_string(), 1, 86400_000_000_000);
        // May or may not be different depending on hash distribution
    }

    #[test]
    fn test_shard_management() {
        let mut shard = Shard::new(0, "node1".to_string());
        assert!(shard.can_write());
        assert!(shard.can_read());

        shard.add_replica("node2".to_string());
        shard.add_replica("node3".to_string());
        assert_eq!(shard.replicas.len(), 2);

        // Don't add primary as replica
        shard.add_replica("node1".to_string());
        assert_eq!(shard.replicas.len(), 2);

        // Don't add duplicates
        shard.add_replica("node2".to_string());
        assert_eq!(shard.replicas.len(), 2);

        assert_eq!(shard.all_nodes(), vec!["node1", "node2", "node3"]);

        shard.remove_replica("node2");
        assert_eq!(shard.replicas.len(), 1);

        shard.state = ShardState::ReadOnly;
        assert!(!shard.can_write());
        assert!(shard.can_read());

        shard.state = ShardState::Offline;
        assert!(!shard.can_write());
        assert!(!shard.can_read());
    }
}
