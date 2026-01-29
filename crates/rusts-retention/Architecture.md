# RusTs Retention - Architecture

## Overview

The `rusts-retention` crate implements retention policies and storage tiering for the RusTs time series database. It provides policy-based data expiration and automatic data movement between storage tiers.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── error.rs        # Error types
├── policy.rs       # Retention policy management
└── tiering.rs      # Storage tiering implementation
```

## Retention Policies (policy.rs)

### RetentionPolicy (policy.rs:10-24)

```rust
pub struct RetentionPolicy {
    pub name: String,
    pub pattern: String,                    // Measurement pattern (wildcards)
    pub duration: i64,                      // Retention in nanoseconds
    pub shard_group_duration: Option<i64>,  // Time partitioning
    pub replication_factor: Option<u32>,
    pub is_default: bool,
}
```

### Key Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 27-37 | Create basic policy |
| `with_duration_str()` | 40-43 | Parse duration strings |
| `default_policy()` | 46-49 | Mark as default |
| `shard_group_duration()` | 52-55 | Set partitioning |
| `replication_factor()` | 58-61 | Set replication |
| `matches()` | 64-75 | Pattern matching |
| `expiration_time()` | 78-80 | Calculate expiration |
| `validate()` | 83-94 | Validate constraints |

### Pattern Matching (policy.rs:64-75)

Supports three patterns:
- **Exact match:** `"cpu"` matches `"cpu"`
- **Prefix wildcard:** `"cpu*"` matches `"cpu"`, `"cpu_usage"`, `"cpu.host.server01"`
- **Full wildcard:** `"*"` matches everything

### Duration Parsing (policy.rs:98-140)

Supported units:

| Unit | Multiplier |
|------|------------|
| `ns` | 1 |
| `us`, `µs` | 1,000 |
| `ms` | 1,000,000 |
| `s` | 1,000,000,000 |
| `m` | 60 × 10^9 |
| `h` | 3,600 × 10^9 |
| `d` | 86,400 × 10^9 |
| `w` | 604,800 × 10^9 |

### RetentionPolicyManager (policy.rs:143-240)

```rust
pub struct RetentionPolicyManager {
    policies: RwLock<HashMap<String, RetentionPolicy>>,
    default_policy: RwLock<Option<String>>,
}
```

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 152-157 | Create empty manager |
| `add_policy()` | 160-174 | Add/register policy |
| `remove_policy()` | 177-188 | Remove policy |
| `get_policy()` | 191-194 | Retrieve by name |
| `get_policy_for_measurement()` | 197-221 | Smart pattern matching |
| `list_policies()` | 224-227 | Return all policies |
| `default_policy()` | 230-233 | Get current default |

### Policy Resolution (policy.rs:197-221)

Priority order:
1. Exact match on measurement name
2. Most specific pattern match
3. Default policy fallback

## Storage Tiering (tiering.rs)

### StorageTier (tiering.rs:7-26)

```rust
pub enum StorageTier {
    Hot,    // Fast storage (SSD), recent data
    Warm,   // Medium storage, older data
    Cold,   // Slow/cheap storage (HDD/S3), archive
}
```

### Compression Recommendations (tiering.rs:18-25)

| Tier | Compression Level |
|------|-------------------|
| Hot | `CompressionLevel::Fast` |
| Warm | `CompressionLevel::Default` |
| Cold | `CompressionLevel::Best` |

### TieringPolicy (tiering.rs:29-43)

```rust
pub struct TieringPolicy {
    pub name: String,
    pub warm_after: i64,            // Age threshold (nanos)
    pub cold_after: i64,            // Age threshold (nanos)
    pub hot_path: PathBuf,          // Required
    pub warm_path: Option<PathBuf>, // Defaults to hot_path/warm
    pub cold_path: Option<PathBuf>, // Defaults to hot_path/cold
}
```

### Key Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 47-56 | Create policy with thresholds |
| `warm_path()` | 59-62 | Set custom warm path |
| `cold_path()` | 65-68 | Set custom cold path |
| `tier_for_age()` | 71-79 | Determine tier for data age |
| `path_for_tier()` | 82-90 | Get storage path for tier |

### Tier Determination (tiering.rs:71-79)

```rust
pub fn tier_for_age(&self, age_nanos: i64) -> StorageTier {
    if age_nanos >= self.cold_after {
        StorageTier::Cold
    } else if age_nanos >= self.warm_after {
        StorageTier::Warm
    } else {
        StorageTier::Hot
    }
}
```

### Default Paths

```
Hot:  /data
Warm: /data/warm (if not explicitly set)
Cold: /data/cold (if not explicitly set)
```

## Error Handling (error.rs)

```rust
pub enum RetentionError {
    PolicyNotFound(String),
    InvalidPolicy(String),
    Storage(String),
    Enforcement(String),
}
```

## Integration Points

### With Storage Engine

- `wal_retention_secs` configuration in storage engine
- WAL files older than retention eligible for cleanup
- Partition deletion based on policy expiration

### With Server

Server configuration:
```rust
pub struct RetentionPolicyConfig {
    pub name: String,
    pub pattern: String,
    pub duration: String,  // "30d", "1w", etc.
    pub is_default: bool,
}
```

### With Compression

`StorageTier::recommended_compression()` returns appropriate `CompressionLevel` for each tier.

## Thread Safety

Uses `parking_lot::RwLock` for:
- Multiple concurrent readers
- Exclusive writers for policy changes
- Lock-free reads for policy lookups

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Timestamp type |
| `rusts-storage` | Storage interface |
| `rusts-compression` | CompressionLevel enums |
| `tokio` | Async runtime |
| `serde` | Serialization |
| `thiserror` | Error types |
| `tracing` | Logging |
| `chrono` | Time utilities |
| `parking_lot` | RwLock |

## Design Patterns

### Builder Pattern

```rust
RetentionPolicy::new("30d", "*", duration)
    .default_policy()
    .shard_group_duration(86400 * 1_000_000_000)
    .replication_factor(3)
```

### Time-based Tiering

```rust
TieringPolicy::new("default", warm_after, cold_after, hot_path)
    .warm_path(PathBuf::from("/mnt/warm"))
    .cold_path(PathBuf::from("/mnt/archive"))
```

## Current Limitations

The crate provides **policy infrastructure only**:

1. **No Scheduler:** Background enforcement task not implemented
2. **No Partition Deletion:** Actual deletion logic in storage layer
3. **No Data Movement:** Tier migration not automated
4. **No Metrics:** Cleanup monitoring not implemented

These require implementation at the server layer as background tasks.
