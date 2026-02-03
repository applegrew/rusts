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

### QueryPlan

```rust
pub struct QueryPlan {
    pub query: Query,
    pub partition_ranges: Vec<TimeRange>,
    pub series_ids: Option<Vec<SeriesId>>,
    pub hints: ExecutionHints,
}
```

### ExecutionHints

Tracks which time-series optimizations can be applied:

```rust
pub struct ExecutionHints {
    pub memtable_only: bool,       // Query can be served from memtable alone
    pub use_segment_stats: bool,   // Aggregation can use segment statistics
    pub filters_reordered: bool,   // Filters reordered by selectivity
    pub filter_order: Vec<(String, usize)>,  // Filter descriptions with cardinalities
    pub partition_count: usize,
    pub estimated_series: usize,
    pub estimated_points: u64,
}
```

### EXPLAIN Output

`QueryPlan::explain()` returns a human-readable plan:

```rust
pub struct ExplainOutput {
    pub measurement: String,
    pub time_range: TimeRange,
    pub optimizations: Vec<String>,
    pub filter_order: Vec<(String, usize)>,
    pub partitions_to_scan: usize,
    pub estimated_series: usize,
    pub estimated_points: u64,
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

**Selectivity Estimation:**

| Filter Type | Selectivity Factor |
|-------------|-------------------|
| Equals | 0.1× |
| NotEquals | 0.9× |
| Regex | 0.3× |
| In | min(values.len() × 0.1, 0.5)× |
| Exists | 0.7× |

## Time-Series Optimizations

### Filter Ordering by Cardinality

Filters are reordered to apply most selective (lowest cardinality) first, shrinking bitmaps quickly:

```sql
-- Query: WHERE region='us-west' AND device_id='device-0001'
-- Cardinality: region=5, device_id=1000
-- Optimal: device_id first (shrinks to ~1 series), then region
```

### Hot Data Routing

Recent queries can skip partition scans if data is in memtable:

```rust
if storage.can_serve_from_memtable(&query.time_range) {
    // Fast path: memtable only, no disk I/O
}
```

### Segment Statistics Pushdown

Simple aggregations (COUNT, MIN, MAX, SUM) can use pre-computed segment statistics without decompressing data:

```sql
SELECT COUNT(*), MIN(value), MAX(value) FROM cpu
-- Returns sum(segment.count), min(segment.min), max(segment.max)
-- Zero data decompression needed
```

Conditions for segment stats pushdown:
- Simple aggregate function (COUNT, SUM, MIN, MAX)
- No GROUP BY tags
- No GROUP BY time
- Query covers complete segments

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

### LIMIT Query Optimization

For queries with LIMIT, the executor uses early termination to avoid scanning unnecessary data:

**TopKCollector:**
Maintains a bounded heap of points, sorted by timestamp:

```rust
// Ascending order (oldest first)
struct TopKCollector {
    heap: BinaryHeap<Reverse<(i64, ...)>>,  // min-heap behavior
    k: usize,
}

// Descending order (newest first)
struct TopKCollectorDesc {
    heap: BinaryHeap<(i64, ...)>,  // max-heap behavior
    k: usize,
}
```

**Early Termination:**
- Queries with LIMIT pass the limit to storage layer
- Memtable returns only needed points using binary search
- Segments use `read_range_with_limit()` for O(log n + k) reads
- Partitions stop scanning when limit is reached

**Performance Impact:**
- `SELECT * FROM trips ORDER BY time DESC LIMIT 100` benefits most (newest data in memtable)
- `SELECT * FROM trips ORDER BY time ASC LIMIT 100` still benefits from segment-level optimization

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
