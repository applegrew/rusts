# RusTs Index - Architecture

## Overview

The `rusts-index` crate implements high-performance indexing structures for the RusTs time series database. It provides series metadata lookup, tag-based filtering using Roaring bitmaps, and Bloom filters for partition pruning.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── error.rs        # Error types
├── series.rs       # Series index implementation
├── tag.rs          # Tag inverted index with Roaring bitmaps
└── bloom.rs        # Bloom filter for partition pruning
```

## Series Index (series.rs)

### SeriesMetadata (series.rs:11-24)

Stores metadata for individual time series:

```rust
pub struct SeriesMetadata {
    pub id: SeriesId,
    pub measurement: String,
    pub tags: Vec<Tag>,
    pub first_seen: i64,
    pub last_seen: i64,
}
```

### SeriesIndex (series.rs:26-34)

Concurrent hash map-based series registry:

```rust
pub struct SeriesIndex {
    series: DashMap<SeriesId, SeriesMetadata>,
    measurement_index: DashMap<String, Vec<SeriesId>>,
    count: AtomicUsize,
}
```

### Key Methods

| Method | Line | Purpose |
|--------|------|---------|
| `new()` | 38-44 | Create new empty index |
| `upsert()` | 47-82 | Insert or update series; returns `bool` (true if new) |
| `get()` | 85-87 | Retrieve series metadata by ID |
| `contains()` | 90-92 | Check series existence |
| `get_by_measurement()` | 95-100 | Get all series IDs for a measurement |
| `measurements()` | 103-108 | Get all measurement names |
| `all_series_ids()` | 111-113 | Get all series IDs |
| `remove()` | 126-139 | Remove series and update measurement index |
| `to_bytes()` | 142-145 | Serialize to bincode |
| `from_bytes()` | 148-163 | Deserialize from bincode |

### Concurrency Model

- `DashMap` for lock-free concurrent series access
- `AtomicUsize` with `Ordering::Relaxed` for count tracking
- Multiple readers allowed, writers get exclusive access per entry

## Tag Index (tag.rs)

### TagKey (tag.rs:13-27)

Internal representation of tag key-value pairs:

```rust
pub struct TagKey {
    pub key: String,
    pub value: String,
}
```

### TagIndex (tag.rs:29-40)

Inverted index using Roaring bitmaps for efficient set operations:

```rust
pub struct TagIndex {
    index: DashMap<TagKey, RoaringBitmap>,
    series_to_internal: DashMap<SeriesId, u32>,
    internal_to_series: RwLock<Vec<SeriesId>>,
    tag_values: DashMap<String, Vec<String>>,
}
```

**Why u32 mapping?** Roaring bitmaps use u32 indices, but RusTs SeriesIds are u64. The index maintains bidirectional mappings to bridge this limitation.

### Key Methods

| Method | Purpose |
|--------|---------|
| `new()` | Create new tag index |
| `index_series()` | Index tags for a series |
| `find_by_tag()` | Single tag lookup |
| `find_by_tags_all()` | AND operation - series matching ALL tags |
| `find_by_tags_any()` | OR operation - series matching ANY tag |
| `find_by_tag_not()` | Negation - series NOT matching tag |
| `get_tag_values()` | Get unique values for tag key |
| `get_cardinality()` | Get count of series matching tag (for filter ordering) |
| `remove_series()` | Remove series from all bitmaps |
| `to_bytes()` | Serialize to bincode |
| `from_bytes()` | Deserialize and reconstruct mappings |

### Bitmap Operations

```rust
// AND operation
result = result & bitmap.value();

// OR operation
result |= bitmap.value();
```

### Cardinality-Based Filter Ordering

The query planner uses `get_cardinality()` to order filters by selectivity:

```rust
// Lower cardinality = more selective = apply first
let cardinality = tag_index.get_cardinality("device_id", "device-0001");
// Returns: Some(1) - very selective, apply first

let cardinality = tag_index.get_cardinality("region", "us-west");
// Returns: Some(1000) - less selective, apply later
```

Applying high-selectivity filters first shrinks bitmaps faster, improving query performance.

## Bloom Filter (bloom.rs)

### BloomFilter (bloom.rs:10-21)

Probabilistic set membership for partition pruning:

```rust
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
    count: usize,
}
```

### Parameter Calculation (bloom.rs:33-40)

```
Optimal bits:   m = -n * ln(p) / (ln(2)²)
Optimal hashes: k = (m/n) * ln(2)
```
Where `p` = false positive rate, `n` = expected items.

### Constructors

| Constructor | Line | Purpose |
|-------------|------|---------|
| `new(expected_items, fpr)` | 29-52 | Optimal parameters calculation |
| `with_params(num_bits, num_hashes)` | 55-65 | Direct parameter specification |

### Key Methods

| Method | Line | Purpose |
|--------|------|---------|
| `insert()` | 68-79 | Add item using double hashing |
| `might_contain()` | 84-97 | Check membership (false=definitely not, true=possibly) |
| `estimated_fpr()` | 105-114 | Calculate actual FPR: `(1 - e^(-k*n/m))^k` |
| `fill_ratio()` | 117-120 | Percentage of bits set |
| `union()` | 139-147 | In-place OR with another filter |
| `union_new()` | 150-154 | Create union without mutation |

### Hashing Strategy (bloom.rs:156-173)

Double hashing: `h(i) = h1 + i * h2`

- Uses `FxHasher` for fast non-cryptographic hashing
- Pair generation computes h1, then derives h2 from h1
- Index computation: `(h1 + i * h2) % num_bits`

## Error Handling (error.rs)

```rust
pub enum IndexError {
    SeriesNotFound(u64),
    TagNotFound { key, value },
    Io(std::io::Error),
    Serialization(String),
    InvalidData(String),
}
```

## Integration Points

### With Storage Engine

- Called during write path to track new series metadata
- Used in query planning to resolve series names to IDs
- Provides measurement-based filtering for queries

### With Query Engine

- TagIndex enables fast tag-based filtering
- Supports complex boolean queries (AND/OR/NOT)
- Essential for InfluxDB-style tag filtering

### With Partitions

- Bloom filters used in partition pruning to skip partitions
- Reduces I/O by quickly eliminating irrelevant partitions
- Union operation combines filters from multiple partitions

## Thread Safety

| Component | Primitives |
|-----------|------------|
| SeriesIndex | `DashMap` + `AtomicUsize` |
| TagIndex | `DashMap` for tags, `RwLock` for internal ID vector |
| BloomFilter | No built-in concurrency; wrap in `Arc<Mutex>` if shared |

## Key Design Patterns

1. **Inverted Index:** TagIndex maps tags → series (not series → tags) for efficient filtering
2. **Roaring Bitmaps:** Enables O(k) set operations where k = number of tags in query
3. **u32/u64 Mapping:** Works around Roaring's u32 limitation while maintaining 64-bit SeriesIds
4. **Double Hashing:** Bloom filter uses h(i) = h1 + i*h2 for independent hash functions
5. **Lock-free Reads:** DashMap's concurrent iteration enables efficient bulk operations

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Core types (SeriesId, Tag) |
| `dashmap` | Lock-free concurrent hashmap |
| `roaring` | Roaring bitmap for set operations |
| `fxhash` | Fast hashing for bloom filters |
| `parking_lot` | RwLock for TagIndex |
| `serde` | Serialization framework |
| `bincode` | Binary serialization |
| `thiserror` | Error handling |

## Typical Compression Ratios

- Roaring bitmaps achieve 2-10x compression over raw bit arrays
- Bloom filters use ~10 bits per item for 1% FPR
