# RusTs Cluster - Architecture

## Overview

The `rusts-cluster` crate implements static cluster configuration for the RusTs time series database. It provides sharding strategies, replication modes, and query routing without external coordination services.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── config.rs       # Static TOML-based cluster configuration
├── error.rs        # Error types
├── replication.rs  # Replication modes and quorum checking
├── router.rs       # Shard routing and distribution
└── shard.rs        # Sharding strategies and metadata
```

## Cluster Configuration (config.rs)

### ClusterConfig (config.rs:18-39)

```rust
pub struct ClusterConfig {
    pub node_id: String,
    pub cluster_name: String,           // Default: "rusts"
    pub replication_mode: ReplicationMode,
    pub replication_factor: usize,      // Default: 1
    pub shard_count: usize,             // Default: 16
    pub sharding_strategy: ShardingStrategy,
    pub nodes: Vec<NodeConfig>,
}
```

### NodeConfig (config.rs:54-69)

```rust
pub struct NodeConfig {
    pub id: String,
    pub address: String,                // host:port format
    pub shards: Vec<u32>,              // Primary shards
    pub replica_shards: Vec<u32>,      // Replica shards
    pub tags: HashMap<String, String>,  // Optional labels
}
```

### Key Methods (config.rs)

| Method | Lines | Purpose |
|--------|-------|---------|
| `from_file()` | 105-109 | Load from TOML file |
| `from_toml()` | 112-115 | Parse TOML string |
| `single_node()` | 118-137 | Create standalone config |
| `this_node()` | 140-142 | Get current node config |
| `get_node()` | 145-147 | Lookup node by ID |
| `primary_for_shard()` | 150-152 | Find primary node for shard |
| `replicas_for_shard()` | 155-160 | Find replica nodes |
| `validate()` | 171-220 | Comprehensive config validation |

### Example TOML Configuration

```toml
node_id = "node1"
cluster_name = "rusts-prod"
replication_mode = "Quorum"
replication_factor = 2
shard_count = 16

[[nodes]]
id = "node1"
address = "10.0.1.1:8086"
shards = [0, 1, 2, 3, 4, 5, 6, 7]

[[nodes]]
id = "node2"
address = "10.0.1.2:8086"
shards = [8, 9, 10, 11, 12, 13, 14, 15]
replica_shards = [0, 1, 2, 3, 4, 5, 6, 7]
```

### Validation Rules (config.rs:171-220)

- Node ID must exist in nodes list
- All shards 0..shard_count must be assigned
- No duplicate primary shard assignments
- All shards must have a primary node
- Replication factor must be ≤ node count

## Sharding Strategies (shard.rs)

### ShardingStrategy (shard.rs:33-44)

```rust
pub enum ShardingStrategy {
    ByMeasurement,                          // Hash measurement name
    #[default]
    BySeries,                               // Hash series_id (default)
    ByTime { duration_nanos: i64 },         // Time-range bucketing
    Composite { time_duration_nanos: i64 }, // Time + series hash
}
```

### ShardKey (shard.rs:11-30)

```rust
pub struct ShardKey {
    pub measurement: String,
    pub series_id: SeriesId,
    pub timestamp: Timestamp,
}
```

### compute_shard() (shard.rs:47-69)

| Strategy | Formula |
|----------|---------|
| ByMeasurement | `(hash(measurement) % num_shards)` |
| BySeries | `(series_id % num_shards)` |
| ByTime | `((timestamp / duration) % num_shards)` |
| Composite | `((series_id + time_bucket) % num_shards)` |

### Shard (shard.rs:73-85)

```rust
pub struct Shard {
    pub id: u32,
    pub primary: String,
    pub address: Option<String>,
    pub replicas: Vec<String>,
    pub state: ShardState,
}
```

### ShardState (shard.rs:88-98)

```rust
pub enum ShardState {
    Active,       // Healthy, accepting writes
    Rebalancing,  // Being rebalanced
    ReadOnly,     // Maintenance mode
    Offline,      // Unavailable
}
```

## Replication (replication.rs)

### ReplicationMode (replication.rs:10-19)

```rust
pub enum ReplicationMode {
    Synchronous,  // All replicas must acknowledge
    #[default]
    Quorum,       // Majority must acknowledge
    Asynchronous, // Fire-and-forget
}
```

### Required Acknowledgments (replication.rs:22-30)

| Mode | Formula | Example (3 replicas) |
|------|---------|----------------------|
| Synchronous | `replica_count` | 3 acks required |
| Quorum | `(replica_count / 2) + 1` | 2 acks required |
| Asynchronous | `0` | 0 acks required |

### ReplicationAck (replication.rs:32-43)

```rust
pub struct ReplicationAck {
    pub node_id: String,
    pub success: bool,
    pub error: Option<String>,
    pub latency_ms: u64,
}
```

### ReplicationResult (replication.rs:45-56)

```rust
pub struct ReplicationResult {
    pub success_count: usize,
    pub total_count: usize,
    pub acks: Vec<ReplicationAck>,
    pub quorum_reached: bool,
}
```

### ReplicationProtocol (replication.rs:74-123)

```rust
pub struct ReplicationProtocol {
    mode: ReplicationMode,
    replication_factor: usize,
}
```

**check_quorum() (lines 102-122):**
- Counts successful acks
- Calculates required acks based on mode
- Returns error if quorum not reached (except Async mode)

## Query Routing (router.rs)

### ShardRouter (router.rs:11-21)

```rust
pub struct ShardRouter {
    strategy: ShardingStrategy,
    num_shards: u32,
    shards: DashMap<u32, Shard>,
    node_id: String,
}
```

### Key Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `from_config()` | 34-60 | Initialize from ClusterConfig |
| `route()` | 99-106 | Route ShardKey to Shard |
| `is_primary_for()` | 68-73 | Check if node is primary |
| `holds_shard()` | 76-81 | Check if node holds shard |
| `local_primary_shards()` | 84-86 | Get all primary shards |
| `local_shards()` | 89-91 | Get all shards (primary + replicas) |
| `shards_for_node()` | 119-125 | All shards held by a node |
| `primary_shards_for_node()` | 128-134 | Only primary shards |

### Routing Flow

```
Client Request
    ↓
ShardRouter::route()
    ↓
ShardingStrategy::compute_shard()
    ↓
Shard Registry (DashMap)
    ↓
Returns: Shard ID, Primary Node, Replicas, Address, State
```

## Error Handling (error.rs)

```rust
pub enum ClusterError {
    Configuration(String),
    NodeNotFound(String),
    ShardNotFound(u32),
    ReplicationFailed(String),
    QuorumNotReached { got: usize, needed: usize },
    Network(String),
    Serialization(String),
}
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│                  Client Request                  │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │      ShardRouter::route()     │
         └────────────────┬──────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
   ShardingStrategy            Shard Registry
   compute_shard()             (DashMap)

   ├─ ByMeasurement            Returns:
   ├─ BySeries (default)       ├─ Shard ID
   ├─ ByTime                   ├─ Primary Node
   └─ Composite                ├─ Replicas
                               └─ State

         ┌────────────────────────────────┐
         │      Replication Phase         │
         │     (If Write Request)         │
         └────────────────┬───────────────┘
                          │
          ┌───────────────┴──────────────┐
          ▼                              ▼
   Primary Node          ReplicationProtocol
   (Write to WAL)        check_quorum()

                         ├─ Synchronous
                         ├─ Quorum
                         └─ Asynchronous
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | SeriesId, Timestamp types |
| `serde` | TOML/JSON serialization |
| `toml` | Configuration parsing |
| `dashmap` | Concurrent shard routing |
| `fxhash` | Fast measurement hashing |
| `thiserror` | Error types |

## Thread Safety

- `DashMap` provides lock-free concurrent access to shard registry
- No global state mutations
- Safe for multi-threaded request routing

## Design Decisions

1. **Static Configuration:** No external dependencies (etcd, Consul) required
2. **Consistent Routing:** Same ShardKey always routes to same shard
3. **Flexible Strategies:** Multiple sharding approaches for different workloads
4. **Quorum-based Writes:** Configurable consistency vs availability tradeoff
