# RusTs Aggregation - Architecture

## Overview

The `rusts-aggregation` crate implements continuous aggregates and downsampling for the RusTs time series database. It provides materialized view-like functionality and data resolution reduction for time series data.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── error.rs        # Error types
├── continuous.rs   # Continuous aggregates engine
└── downsample.rs   # Downsampling engine
```

## Continuous Aggregates (continuous.rs)

### AggregateFunction (continuous.rs:9-19)

```rust
pub enum AggregateFunction {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
}
```

### ContinuousAggregate (continuous.rs:21-99)

```rust
pub struct ContinuousAggregate {
    pub name: String,
    pub source_measurement: String,
    pub target_measurement: String,
    pub interval: i64,                              // Bucket size in nanos
    pub aggregations: HashMap<String, AggregateFunction>,
    pub group_by: Vec<String>,
    pub retention: Option<i64>,
    pub enabled: bool,
    pub last_processed: Timestamp,
}
```

### Key Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 46-62 | Constructor with defaults |
| `add_aggregation()` | 66-69 | Add field aggregation |
| `group_by()` | 72-75 | Set group by tags |
| `retention()` | 78-81 | Set retention policy |
| `validate()` | 84-98 | Validation logic |

### ContinuousAggregateEngine (continuous.rs:101-184)

```rust
pub struct ContinuousAggregateEngine {
    aggregates: RwLock<HashMap<String, ContinuousAggregate>>,
}
```

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 108-113 | Create new engine |
| `register()` | 116-122 | Register and validate aggregate |
| `unregister()` | 125-128 | Remove aggregate |
| `get()` | 131-134 | Retrieve by name |
| `list()` | 137-140 | List all aggregates |
| `enable()` | 143-151 | Enable aggregate |
| `disable()` | 154-162 | Disable aggregate |
| `get_pending()` | 165-172 | **Get aggregates needing processing** |
| `update_processed()` | 175-183 | Update last_processed timestamp |

### Scheduling Logic (continuous.rs:165-172)

```rust
pub fn get_pending(&self, current_time: Timestamp) -> Vec<ContinuousAggregate> {
    aggregates
        .values()
        .filter(|a| a.enabled && a.last_processed < current_time - a.interval)
        .cloned()
        .collect()
}
```

An aggregate needs processing when `current_time - last_processed > interval`.

## Downsampling (downsample.rs)

### DownsampleFunction (downsample.rs:29-46)

```rust
pub enum DownsampleFunction {
    First,
    Last,
    Mean,
    Min,
    Max,
    Sum,
    Count,
}
```

### DownsampleConfig (downsample.rs:8-18)

```rust
pub struct DownsampleConfig {
    pub source: String,
    pub target: String,
    pub interval: i64,
    pub fields: HashMap<String, DownsampleField>,
}
```

### DownsampleField (downsample.rs:20-27)

```rust
pub struct DownsampleField {
    pub function: DownsampleFunction,
    pub output_name: Option<String>,
}
```

### DownsampleEngine (downsample.rs:48-94)

```rust
pub struct DownsampleEngine {
    configs: HashMap<String, DownsampleConfig>,
}
```

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 56-60 | Create new engine |
| `register()` | 63-74 | Register configuration |
| `get_for_source()` | 77-82 | Query configs by source measurement |
| `list()` | 85-87 | List all configurations |

### DownsampleBuilder (downsample.rs:96-172)

Fluent builder pattern:

```rust
DownsampleBuilder::new("source", "target", interval)
    .mean("cpu_usage")
    .min("temperature")
    .max("temperature")
    .field("requests", DownsampleFunction::Sum, Some("total_requests"))
    .build()
```

| Method | Lines | Purpose |
|--------|-------|---------|
| `mean()` | 116-125 | Add mean aggregation |
| `min()` | 128-137 | Add min aggregation |
| `max()` | 140-149 | Add max aggregation |
| `field()` | 152-161 | Add custom field config |
| `build()` | 164-171 | Build final config |

## Error Handling (error.rs)

```rust
pub enum AggregationError {
    InvalidDefinition(String),
    NotFound(String),
    Computation(String),
    Storage(String),
}
```

## Integration Points

### Current Status

The aggregation crate is **intentionally decoupled** from the storage engine:

- No direct dependency on `rusts-storage` for execution
- Configuration layer only - defines aggregates and configs
- Actual computation happens in `rusts-query` crate
- Results written by server/API handlers

### With Query Engine

The `rusts-query` crate provides:
- `Aggregator` for single-pass aggregation
- `TimeBucketAggregator` for GROUP BY time queries
- All aggregate function implementations

### With Server

Server integration points (not yet implemented):
- Instantiate `ContinuousAggregateEngine::new()`
- Spawn background task calling `get_pending()`
- Execute aggregations and write results
- Update `last_processed` timestamps

## Thread Safety

- Uses `parking_lot::RwLock` for thread-safe access
- Multiple readers allowed for `get()`, `list()`, `get_pending()`
- Exclusive write for `register()`, `unregister()`, `enable()`, `disable()`

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Timestamp, FieldValue types |
| `rusts-storage` | Storage interface (declared) |
| `tokio` | Async runtime support |
| `async-trait` | Async trait definitions |
| `serde` | Serialization |
| `thiserror` | Error handling |
| `tracing` | Structured logging |
| `parking_lot` | RwLock for thread safety |

## Design Patterns

### Builder Pattern

Both `ContinuousAggregate` and `DownsampleBuilder` use fluent APIs:

```rust
ContinuousAggregate::new("hourly_cpu", "cpu", "cpu_hourly", interval)
    .add_aggregation("usage", AggregateFunction::Mean)
    .group_by(vec!["host".to_string()])
    .retention(Some(30 * 24 * 60 * 60 * 1_000_000_000))
```

### Serialization

All key structs implement `Serialize` + `Deserialize` for:
- YAML/JSON configuration file support
- Persistence across restarts
- API responses

## Current Limitations

1. **No Scheduler:** Background task executor not implemented
2. **No Persistence:** Aggregate state not persisted across restarts
3. **No API Endpoints:** REST endpoints for CRUD not implemented
4. **No Metrics:** Execution monitoring not implemented
