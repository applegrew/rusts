# RusTs Core - Architecture

## Overview

The `rusts-core` crate provides the foundational data types and abstractions used throughout the RusTs time series database. It defines the core domain model including points, series, tags, fields, and time ranges.

## Module Structure

```
src/
├── lib.rs          # Module exports
├── types.rs        # Core data type definitions
└── error.rs        # Error handling
```

## Core Type Aliases

### Timestamp (types.rs:9-10)
```rust
pub type Timestamp = i64;
```
- Nanosecond-precision Unix epoch timestamps
- Allows ~292 years of range with nanosecond granularity
- Used throughout the database for all temporal operations

### SeriesId (types.rs:12-13)
```rust
pub type SeriesId = u64;
```
- Unique identifier for a time series (measurement + tags combination)
- Computed via FxHash for fast, deterministic hashing
- Ensures consistent series identification across writes and queries

## Data Structures

### Tag (types.rs:15-54)

**Purpose:** Key-value pair for series identification and indexing.

```rust
pub struct Tag {
    pub key: String,
    pub value: String,
}
```

**Key Methods:**
- `new(key, value)` - Constructor with Into trait support
- `validate()` - Ensures key is not empty
- `Ord` implementation - Enables deterministic sorting (by key, then value)

**Design Note:** Tag ordering is critical for consistent SeriesId computation. Tags are always sorted alphabetically before hashing.

### FieldValue (types.rs:56-160)

**Purpose:** Represents all supported field value types.

```rust
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UnsignedInteger(u64),
    String(String),
    Boolean(bool),
}
```

**Type Conversion Methods:**
- `as_f64()` - Numeric types convert to f64; others return None
- `as_i64()` - Integer/UnsignedInteger/Float convert to i64 (with overflow check)
- `as_str()` - Only String variant returns Some
- `as_bool()` - Only Boolean variant returns Some

**From Implementations:**
- `From<f64>`, `From<i64>`, `From<u64>`, `From<String>`, `From<&str>`, `From<bool>`

### Field (types.rs:162-186)

**Purpose:** Named value in a data point containing actual measurement data.

```rust
pub struct Field {
    pub key: String,
    pub value: FieldValue,
}
```

**Key Methods:**
- `new(key, value)` - Constructor with Into trait support
- `validate()` - Ensures key is not empty

**Design Note:** Fields are NOT indexed (unlike tags) and contain the actual measurements.

### Point (types.rs:188-246)

**Purpose:** Represents a single measurement at a specific time.

```rust
pub struct Point {
    pub measurement: String,
    pub timestamp: Timestamp,
    pub tags: Vec<Tag>,
    pub fields: Vec<Field>,
}
```

**Key Methods:**
- `builder(measurement)` - Returns PointBuilder for fluent construction
- `validate()` - Comprehensive validation (non-empty measurement, at least one field)
- `series_id()` - Computes SeriesId from measurement and tags
- `sort_tags()` - Sorts tags alphabetically (required for consistent SeriesId)
- `get_tag(key)` - Linear search for tag value
- `get_field(key)` - Linear search for field value

### PointBuilder (types.rs:248-305)

**Purpose:** Fluent builder pattern for safe Point construction.

```rust
pub struct PointBuilder {
    measurement: String,
    timestamp: Option<Timestamp>,
    tags: Vec<Tag>,
    fields: Vec<Field>,
}
```

**Builder Methods:**
- `timestamp(ts)` - Set timestamp (chainable)
- `tag(key, value)` - Add tag (chainable)
- `field(key, value)` - Add field (chainable)
- `build()` - Construct final Point with validation

**Example Usage:**
```rust
Point::builder("cpu")
    .timestamp(1609459200000000000)
    .tag("host", "server01")
    .field("usage", 64.5)
    .build()
    .unwrap()
```

**Build Process:**
1. Sort tags alphabetically (critical for SeriesId consistency)
2. Use current system time if timestamp not set
3. Validate the constructed point
4. Return Result<Point>

### Series (types.rs:307-343)

**Purpose:** Metadata about a time series (distinct from individual points).

```rust
pub struct Series {
    pub id: SeriesId,
    pub measurement: String,
    pub tags: Vec<Tag>,
}
```

**Key Methods:**
- `from_point(point)` - Creates Series from a Point (clones and sorts tags)
- `new(measurement, tags)` - Direct constructor (sorts tags, computes SeriesId)

### TimeRange (types.rs:358-396)

**Purpose:** Temporal bounds for queries with inclusive start, exclusive end semantics.

```rust
pub struct TimeRange {
    pub start: Timestamp,  // Inclusive
    pub end: Timestamp,    // Exclusive
}
```

**Key Methods:**
- `contains(ts)` - Returns `ts >= start && ts < end`
- `overlaps(other)` - Returns true if ranges overlap
- `duration_nanos()` - Returns `end - start`

**Default:** `start: i64::MIN`, `end: i64::MAX` (represents all-time range)

### DatabaseConfig (types.rs:398-423)

**Purpose:** Configuration parameters for database initialization.

```rust
pub struct DatabaseConfig {
    pub name: String,
    pub data_dir: String,
    pub wal_dir: Option<String>,
    pub max_memtable_size: usize,
    pub partition_duration: i64,
}
```

**Default Values:**
- `name`: "default"
- `data_dir`: "./data"
- `wal_dir`: None (defaults to data_dir/wal)
- `max_memtable_size`: 64MB
- `partition_duration`: 1 day in nanoseconds

## SeriesId Computation (types.rs:346-356)

```rust
pub fn compute_series_id(measurement: &str, tags: &[Tag]) -> SeriesId {
    let mut hasher = FxHasher::default();
    measurement.hash(&mut hasher);
    for tag in tags {
        tag.key.hash(&mut hasher);
        tag.value.hash(&mut hasher);
    }
    hasher.finish()
}
```

**Key Characteristics:**
- Uses `FxHasher` for speed over cryptographic strength
- **Order-dependent:** Tags MUST be sorted before hashing
- **Deterministic:** Same measurement + sorted tags = same SeriesId

**Critical Requirement:** PointBuilder and Series ensure tags are sorted before this function is called.

## Error Handling (error.rs)

```rust
pub enum CoreError {
    InvalidTimestamp(i64),
    EmptyMeasurement,
    EmptyTagKey,
    EmptyFieldKey,
    NoFields,
    Serialization(String),
    Deserialization(String),
    InvalidFieldType { expected: String, actual: String },
}

pub type Result<T> = std::result::Result<T, CoreError>;
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `serde` | Serialization/deserialization framework |
| `bincode` | Binary encoding (fast, compact) |
| `thiserror` | Ergonomic error handling |
| `fxhash` | Fast hashing for SeriesId computation |
| `bytes` | Efficient byte handling |
| `chrono` | Date/time manipulation |

## Design Patterns

1. **Builder Pattern:** PointBuilder enables fluent, safe Point construction with validation
2. **Type Safety:** Strong typing via enums (FieldValue) prevents runtime errors
3. **Trait Conversions:** Into implementations simplify API usage
4. **Deterministic Hashing:** Tag sorting before SeriesId computation ensures consistency
5. **Semantic Time Ranges:** Inclusive start, exclusive end (standard in databases)
6. **Serialization:** All types support both binary (bincode) and JSON serialization

## Integration Points

This crate is used by all other RusTs crates:
- `rusts-storage` - Point storage and retrieval
- `rusts-compression` - Field value compression
- `rusts-query` - Query result types
- `rusts-index` - Series and tag indexing
- `rusts-api` - Request/response types
- `rusts-importer` - Data import types
