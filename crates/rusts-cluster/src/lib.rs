//! RusTs Cluster - Clustering support for time series database
//!
//! This crate provides clustering capabilities:
//! - Static cluster configuration (TOML-based)
//! - Shard routing (by measurement, series, time, or composite)
//! - Replication modes (sync/quorum/async)
//!
//! For dynamic clustering (auto-discovery, failover), integrate with
//! external tools like etcd, Consul, or Kubernetes.

pub mod config;
pub mod error;
pub mod replication;
pub mod router;
pub mod shard;

pub use config::{ClusterConfig, NodeConfig};
pub use error::{ClusterError, Result};
pub use replication::{ReplicationMode, ReplicationProtocol};
pub use router::ShardRouter;
pub use shard::{Shard, ShardKey, ShardingStrategy};
