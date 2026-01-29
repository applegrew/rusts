# RusTs Query - Architecture

## Overview

The `rusts-query` crate implements the query engine for the RusTs time series database. It provides query building, planning, optimization, and execution with support for aggregations, time bucketing, and tag filtering.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── error.rs        # Error types
├── model.rs        # Query data structures and builders
├── planner.rs      # Query planning and optimization
├── executor.rs     # Query execution engine
└── aggregation.rs  # Aggregation functions
```

## Query Model (model.rs)

### TagFilter (model.rs:10-47)

Tag filtering operations:

```rust
pub enum TagFilter {
    Equals { key: String, value: String },
    NotEquals { key: String, value: String },
    Regex { key: String, pattern: String },
    In { key: String, values: Vec<String> },
    Exists { key: String },
}
```

### FieldSelection (model.rs:50-62)

Field selection modes:

```rust
pub enum FieldSelection {
    All,
    Fields(Vec<String>),
    Aggregate { field: String, function: AggregateFunction, alias: Option<String> },
}
```

### Query (model.rs:65-108)

Core query definition:

```rust
pub struct Query {
    pub measurement: String,
    pub time_range: TimeRange,
    pub tag_filters: Vec<TagFilter>,
    pub field_selection: FieldSelection,
    pub group_by: Vec<String>,
    pub group_by_interval: Option<i64>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
```

### QueryBuilder (model.rs:111-240)

Fluent builder API:

```rust
Query::builder("measurement")
    .time_range(start, end)
    .where_tag(key, value)
    .where_tag_not(key, value)
    .where_tag_in(key, values)
    .select_fields(vec![field_names])
    .select_aggregate(field, function, alias)
    .group_by_tags(vec![tag_names])
    .group_by_interval(nanos)
    .order_by(field, ascending)
    .limit(n)
    .offset(n)
    .build()?
```

### ResultRow (model.rs:243-253)

```rust
pub struct ResultRow {
    pub timestamp: Option<Timestamp>,
    pub series_id: Option<SeriesId>,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
}
```

### QueryResult (model.rs:256-288)

```rust
pub struct QueryResult {
    pub rows: Vec<ResultRow>,
    pub total_count: usize,
    pub execution_time_ms: f64,
}
```

## Query Planning (planner.rs)

### QueryPlan (planner.rs:8-18)

```rust
pub struct QueryPlan {
    pub query: Query,
    pub partition_ranges: Vec<TimeRange>,
    pub estimated_cost: f64,
}
```

### QueryPlanner (planner.rs:21-103)

**Partition Pruning (lines 75-81):**
- Filters partitions by time range overlap
- Eliminates irrelevant partitions before scanning

**Cost Estimation (lines 49-53):**
```
base_cost = partitions × 100
time_cost = (end - start) / 1_000_000_000
filter_discount = 0.5 if filters exist, else 1.0
final_cost = (base_cost + time_cost) × filter_discount
```

**Selectivity Estimation (lines 84-102):**

| Filter Type | Selectivity Factor |
|-------------|-------------------|
| Equals | 0.1× |
| NotEquals | 0.9× |
| Regex | 0.3× |
| In | min(values.len() × 0.1, 0.5)× |
| Exists | 0.7× |

## Query Execution (executor.rs)

### QueryExecutor (executor.rs:15-449)

Main execution engine with access to storage, series index, and tag index.

### Execution Pipeline (executor.rs:42-73)

```
Query Input
    ↓
execute() [line 42-73]
    ├─ query.validate()
    ├─ planner.plan(query)
    ├─ optimizer.optimize(plan)
    ├─ resolve_series_ids(&query)
    └─ Execute based on FieldSelection
        ├─ All/Fields → execute_select()
        └─ Aggregate → execute_aggregate()
    ├─ apply_limit_offset()
    └─ QueryResult
```

### Series ID Resolution (executor.rs:76-136)

1. Get all series for measurement from series_index
2. Apply tag filters sequentially:
   - `Equals`: Find exact matches, retain intersection
   - `NotEquals`: Exclude matching series
   - `In`: Find any matching values
   - `Exists`: Check tag presence in series metadata
   - `Regex`: Pattern matching on tag values

### Execution Paths

**SELECT Path (lines 139-171):**
1. For each series_id in filtered set, query storage
2. Filter fields based on FieldSelection
3. Create ResultRow for each point
4. Sort by timestamp
5. Return QueryResult

**Simple Aggregate Path (lines 173-229):**
1. Create Aggregator with function
2. For each series_id, get points and add to aggregator
3. Return single ResultRow with aggregated value

**Time-Bucketed Aggregate Path (lines 232-333):**
1. Create TimeBucketAggregator with interval
2. If group_by tags specified, each series gets own buckets
3. Return ResultRow for each non-empty bucket

**Grouped Aggregate Path (lines 336-405):**
1. Group series by group_by tags
2. For each group, aggregate all points
3. Return one ResultRow per group

## Aggregation Functions (aggregation.rs)

### AggregateFunction (aggregation.rs:8-30)

```rust
pub enum AggregateFunction {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
    StdDev,
    Variance,
    Percentile(u8),
}
```

### Aggregator (aggregation.rs:65-182)

Single-pass aggregation:

```rust
pub struct Aggregator {
    function: AggregateFunction,
    values: Vec<f64>,
}
```

**Computation (lines 97-171):**

| Function | Implementation |
|----------|----------------|
| Count | Count of non-NaN values |
| Sum | Sum of all values |
| Mean | Sum / count |
| Min | Minimum value |
| Max | Maximum value |
| First | First value in input order |
| Last | Last value in input order |
| StdDev | Sample standard deviation |
| Variance | Sample variance |
| Percentile(p) | Linear interpolation percentile |

### TimeBucketAggregator (aggregation.rs:185-229)

Time-bucketed GROUP BY aggregation:

```rust
pub struct TimeBucketAggregator {
    buckets: Vec<Aggregator>,
    start: Timestamp,
    interval: i64,
}
```

- Creates `num_buckets = (end - start + interval - 1) / interval` buckets
- `add(timestamp, value)`: Maps timestamp to bucket index
- `results()`: Returns `Vec<(bucket_start_timestamp, aggregated_value)>`

## Error Handling (error.rs)

```rust
pub enum QueryError {
    InvalidQuery(String),
    MeasurementNotFound(String),
    FieldNotFound(String),
    InvalidTimeRange { start, end },
    InvalidAggregation(String),
    Storage(String),
    Index(String),
    Execution(String),
    Parse(String),
    TypeMismatch { expected, actual },
}
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Core types (Point, SeriesId, Tag, TimeRange) |
| `rusts-storage` | StorageEngine for data retrieval |
| `rusts-index` | SeriesIndex and TagIndex for lookups |
| `regex` | Pattern matching for tag filters |
| `serde` | Serialization |
| `thiserror` | Error handling |
| `tracing` | Logging |

## Integration Points

| Component | Usage |
|-----------|-------|
| StorageEngine | Query points by series_id and time range |
| SeriesIndex | Lookup series metadata, get series by measurement |
| TagIndex | Find series IDs matching tag filters |

## Public API

```rust
pub use aggregation::{AggregateFunction, Aggregator};
pub use error::{QueryError, Result};
pub use executor::QueryExecutor;
pub use model::{Query, QueryBuilder, QueryResult};
pub use planner::QueryPlanner;
```
