# RusTs Storage - Architecture

## Overview

The `rusts-storage` crate implements the storage engine for the RusTs time series database. It provides a LSM-tree inspired architecture with Write-Ahead Logging (WAL), in-memory buffering (MemTable), and columnar compressed storage (Segments).

## Module Structure

```
src/
├── lib.rs              # Public exports
├── error.rs            # Error types
├── engine.rs           # StorageEngine coordinator
├── memtable.rs         # In-memory write buffer
├── segment.rs          # Columnar compressed storage
├── partition.rs        # Time-based data organization
└── wal/
    ├── mod.rs          # WAL module
    ├── writer.rs       # WAL writing
    └── reader.rs       # WAL reading and recovery
```

## Storage Engine (engine.rs)

### Configuration (engine.rs:25-58)

```rust
pub struct StorageEngineConfig {
    pub data_dir: PathBuf,
    pub wal_dir: Option<PathBuf>,        // Defaults to data_dir/wal
    pub wal_durability: WalDurability,
    pub wal_retention_secs: Option<u64>, // Default: 7 days
    pub flush_trigger: FlushTrigger,
    pub partition_duration: i64,         // Default: 1 day in nanos
    pub compression: CompressionLevel,
}
```

### StorageEngine Structure (engine.rs:67-86)

```rust
pub struct StorageEngine {
    config: StorageEngineConfig,
    wal: WalWriter,
    active_memtable: RwLock<Arc<MemTable>>,
    immutable_memtables: RwLock<Vec<Arc<MemTable>>>,
    partitions: PartitionManager,
    flush_tx: UnboundedSender<FlushCommand>,
    last_flushed_sequence: AtomicU64,
    running: RwLock<bool>,
}
```

### Initialization (engine.rs:88-131)

1. Create data and WAL directories
2. Initialize WalWriter with durability mode
3. Create PartitionManager
4. Spawn background flusher thread
5. Load checkpoint from disk (if exists)
6. Perform WAL recovery (skips entries ≤ checkpoint)

### Shutdown (engine.rs:267-315)

Clean shutdown ensures no WAL recovery is needed on restart:

```
shutdown()
    ↓
1. Set running = false
    ↓
2. If memtable has data:
   ├─ Flush to partitions (synchronous)
   └─ Update checkpoint to current WAL sequence
    ↓
3. Send Shutdown command to background flusher
    ↓
4. Sync WAL to disk
```

**Result:** After clean shutdown, restart is instant (no WAL recovery needed).

## Write Path

### Flow Diagram

```
Client write_batch(points)
    ↓
1. Point validation
    ↓
2. WAL write (durability enforcement)
    ↓
3. MemTable insert
    ↓
4. Check flush triggers
    ↓
5. If triggered: rotate_memtable()
    ↓
6. Background flush to segments
```

### write_batch() (engine.rs:138-165)

1. Validate all points
2. Write to WAL (returns sequence number)
3. Insert into active MemTable
4. Check `should_flush()` triggers
5. If triggered, call `rotate_memtable()`

### rotate_memtable() (engine.rs:373-394)

1. Seal active memtable (no more writes)
2. Create new active memtable
3. Move old to immutable list
4. Send `FlushCommand::Flush` to background thread

## Read Path

### Flow Diagram

```
query(series_id, time_range)
    ↓
1. Query active MemTable
    ↓
2. Query immutable MemTables
    ↓
3. Get overlapping partitions
    ↓
4. Query each partition
    ↓
5. Merge, sort, deduplicate
    ↓
6. Return results
```

### query() (engine.rs:167-199)

Returns `Vec<MemTablePoint>` sorted by timestamp with duplicates removed.

### query_measurement() (engine.rs)

Queries by measurement name, returns `Vec<(SeriesId, Vec<MemTablePoint>)>`.

### Query Optimization Methods

**Hot Data Routing:**
```rust
// Check if query can be served from memtable alone
pub fn can_serve_from_memtable(&self, time_range: &TimeRange) -> bool;

// Get memtable's current time range
pub fn get_memtable_time_range(&self) -> Option<TimeRange>;
```

**Segment Statistics:**
```rust
// Get aggregated field stats from segments (for COUNT/MIN/MAX/SUM pushdown)
pub fn get_aggregated_field_stats(
    &self,
    series_ids: &[SeriesId],
    time_range: &TimeRange,
    field_name: &str,
) -> Option<FieldStats>;

// Get point count from segment metadata
pub fn get_segment_point_count(
    &self,
    series_ids: &[SeriesId],
    time_range: &TimeRange,
) -> (u64, bool);  // (count, is_complete)

// Check if all segments have complete stats
pub fn has_complete_segment_stats(
    &self,
    series_ids: &[SeriesId],
    time_range: &TimeRange,
    field_name: &str,
) -> bool;
```

## WAL (Write-Ahead Log)

### Durability Modes (wal/writer.rs:16-31)

```rust
pub enum WalDurability {
    EveryWrite,              // fsync after every write
    Periodic { interval_ms: u64 }, // fsync at intervals
    OsDefault,               // Let OS decide
    None,                    // No fsync (fastest)
}
```

### Entry Format (wal/writer.rs:34-71)

**Header (32 bytes):**
```
bytes 0-7:   sequence (u64)
bytes 8-15:  timestamp (i64)
bytes 16-19: point_count (u32)
bytes 20-23: data_len (u32)
bytes 24-27: checksum (u32, CRC32)
bytes 28-31: padding
```

### WalWriter (wal/writer.rs:73-237)

- Buffered I/O with configurable max file size (default: 64MB)
- Automatic file rotation at size threshold
- Atomic sequence number counter
- Durability enforcement based on mode

### WAL Recovery (wal/reader.rs)

- Uses binary search to efficiently skip already-flushed WAL files
- Only reads files that may contain entries after the checkpoint
- Validates checksums on read entries
- Handles truncated entries gracefully
- Supports `read_from(start_sequence)` for incremental reads
- `read_after_checkpoint(seq)` returns entries > checkpoint with file skip stats

### WAL Checkpoint (engine.rs)

The checkpoint file (`wal_checkpoint`) tracks the last WAL sequence number that has been successfully flushed to segments.

**File Location:** `{data_dir}/wal_checkpoint`

**Format:** Plain text containing the sequence number

**Usage:**
- Loaded on startup to determine which WAL entries need recovery
- Updated after each successful memtable flush to segments
- Updated during clean shutdown after final memtable flush

**Recovery Behavior:**
- If checkpoint exists: Only recover entries with sequence > checkpoint
- If no checkpoint: Recover all WAL entries (fresh start or crash recovery)
- Binary search efficiently skips files where all entries ≤ checkpoint

### WAL Retention (engine.rs:306-371)

```rust
pub fn cleanup_wal(&self) -> Result<usize>
```

**Safety Constraints:**
- Only deletes files where all entries have been flushed
- Only deletes files older than retention period
- Prevents data loss for CDC consumers

## MemTable (memtable.rs)

### Structure (memtable.rs:65-78)

```rust
pub struct MemTable {
    series: DashMap<SeriesId, SeriesData>,
    size: AtomicUsize,
    point_count: AtomicUsize,
    oldest_timestamp: RwLock<Option<Timestamp>>,
    sealed: RwLock<bool>,
}
```

### MemTablePoint (memtable.rs:14-17)

Lightweight point without measurement/tags (those are in SeriesData):

```rust
pub struct MemTablePoint {
    pub timestamp: Timestamp,
    pub fields: Vec<(String, FieldValue)>,
}
```

### Flush Triggers (memtable.rs:44-62)

```rust
pub struct FlushTrigger {
    pub max_size: usize,       // Default: 64MB
    pub max_points: usize,     // Default: 1M
    pub max_age_nanos: i64,    // Default: 60 seconds
}
```

### Sorted Insert Optimization

MemTable maintains sorted order of points within each series for efficient queries:

**Insert Algorithm:**
- If new point timestamp ≥ last point timestamp: O(1) append
- Otherwise: O(log n) binary search insertion to maintain sorted order

This optimization benefits time series workloads where data typically arrives in chronological order.

### Binary Search for Time Range Queries

Time range queries use binary search to find boundaries efficiently:

```rust
// O(log n + k) instead of O(n) where k = points in range
let start_idx = points.partition_point(|p| p.timestamp < time_range.start);
let end_idx = points.partition_point(|p| p.timestamp < time_range.end);
// Return points[start_idx..end_idx]
```

**Time Range Semantics:**
- `start` is inclusive
- `end` is exclusive

### LIMIT Query Optimization

MemTable supports efficient LIMIT queries without scanning all points:

```rust
pub fn query_with_limit(
    &self,
    series_id: SeriesId,
    time_range: &TimeRange,
    limit: usize,
    ascending: bool,
) -> Vec<MemTablePoint>
```

Uses binary search to find time range boundaries, then returns only the required number of points from the appropriate end (start for ascending, end for descending).

### Concurrency Model

- `DashMap` for lock-free concurrent series access
- `AtomicUsize` for size and point count
- `RwLock` for oldest timestamp and sealed state

## Segments (segment.rs)

### File Format

```
Magic: [0x52, 0x54, 0x53, 0x53] ("RTSS")
Version: u16 = 1
[metadata_len: u32][metadata: bincode]
[timestamp_len: u32][compressed_timestamps]
For each field:
  [field_len: u32][compressed_field_data]
```

### SegmentMeta (segment.rs)

```rust
pub struct SegmentMeta {
    pub id: u64,
    pub series_id: SeriesId,
    pub measurement: String,
    pub tags: Vec<Tag>,
    pub time_range: TimeRange,
    pub point_count: usize,
    pub fields: Vec<(String, FieldType)>,
    pub field_stats: HashMap<String, FieldStats>,  // Pre-computed stats
    pub compressed_size: usize,
    pub uncompressed_size: usize,
}
```

### FieldStats (segment.rs)

Pre-computed statistics per field for aggregation pushdown:

```rust
pub struct FieldStats {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}
```

Enables COUNT/SUM/MIN/MAX queries without decompressing data.

### Compression by Field Type

| Field Type | Algorithm |
|------------|-----------|
| Timestamps | TimestampEncoder + BlockCompressor |
| Float | GorillaEncoder |
| Integer | IntegerEncoder |
| String | DictionaryEncoder |
| Boolean | Bit-packed (8 per byte) |

### SegmentWriter (segment.rs:72-312)

1. Sort points by timestamp
2. Build field schema
3. Compress timestamps
4. Compress each field column separately
5. Write to file with structure above

### SegmentReader (segment.rs:320-511)

1. Verify magic and version
2. Read and deserialize metadata
3. Decompress timestamps on query
4. Decompress requested fields
5. Filter by time range during reconstruction

### LIMIT Query Optimization (segment.rs)

Segments support efficient LIMIT queries using binary search:

```rust
pub fn read_range_with_limit(
    &self,
    time_range: &TimeRange,
    limit: usize,
    ascending: bool,
) -> Result<Vec<MemTablePoint>>
```

**Algorithm:**
1. Binary search to find time range boundaries in timestamp array
2. Calculate which indices to read based on limit and order direction
3. Decompress only the required portion of each field column
4. Build points from the limited range

**Performance:** O(log n + k) where k = min(limit, points_in_range), instead of O(n) for full scan.

This optimization is particularly effective for queries like:
- `SELECT * FROM measurement ORDER BY time DESC LIMIT 100`
- Most recent data queries benefit from early termination

## Partitions (partition.rs)

### Time-Based Organization

Partitions group data by time windows (default: 24 hours).

### Partition Calculation (partition.rs:309-347)

```rust
partition_start = (timestamp / partition_duration) * partition_duration
partition_end = partition_start + partition_duration
```

### PartitionManager (partition.rs:249-393)

```rust
pub struct PartitionManager {
    data_dir: PathBuf,
    partition_duration: i64,
    partitions: RwLock<Vec<Partition>>,
    next_partition_id: AtomicU64,
}
```

**Key Methods:**
- `get_or_create_for_timestamp()` - Get partition for a timestamp
- `partitions_in_range()` - Get partitions overlapping a time range
- `delete_partitions_before()` - Retention cleanup

### Partition Structure (partition.rs:32-47)

```rust
pub struct Partition {
    id: u64,
    directory: PathBuf,
    time_range: TimeRange,
    segments: RwLock<HashMap<SeriesId, Vec<Segment>>>,
    compression: CompressionLevel,
}
```

## Background Flushing (engine.rs:396-437)

### FlushCommand

```rust
pub enum FlushCommand {
    Flush(Arc<MemTable>),
    Shutdown,
}
```

### Flusher Thread

1. Spawns in separate OS thread with Tokio runtime
2. Receives commands via mpsc channel
3. Calls `flush_memtable_to_partitions()` for each memtable
4. Handles errors without crashing

### flush_memtable_to_partitions() (engine.rs:471-499)

1. Iterate over all series in memtable
2. Group points by partition boundaries
3. For each partition: write segment

## Thread Safety

### Synchronization Primitives

| Primitive | Usage |
|-----------|-------|
| `parking_lot::RwLock` | MemTable, partitions |
| `dashmap::DashMap` | Series in MemTable |
| `std::sync::atomic::Atomic*` | Counters, sequence numbers |
| `tokio::sync::mpsc` | Flush command channel |
| `parking_lot::Mutex` | WAL file, sync timing |

## Error Handling (error.rs)

```rust
pub enum StorageError {
    Io(std::io::Error),
    WalCorrupted { details: String },
    WalWriteFailed { details: String },
    SegmentNotFound { id: u64 },
    PartitionNotFound { id: u64 },
    InvalidData { details: String },
    Serialization(bincode::Error),
    MemTableFull,
    NotInitialized,
    AlreadyInitialized,
    Compression(String),
    Core(rusts_core::CoreError),
    ChannelSend(String),
}
```

## File Formats

### WAL File Format

WAL (Write-Ahead Log) files store durable write records before they are flushed to segments.

**File Naming:** `wal_XXXXXXXX.log` (8-digit zero-padded sequence number)

**Example:** `wal_00000000.log`, `wal_00000001.log`, etc.

**File Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│ Entry 0                                                     │
├─────────────────────────────────────────────────────────────┤
│ Entry 1                                                     │
├─────────────────────────────────────────────────────────────┤
│ ...                                                         │
├─────────────────────────────────────────────────────────────┤
│ Entry N                                                     │
└─────────────────────────────────────────────────────────────┘
```

**Entry Format (Header + Data):**
```
Header (32 bytes):
┌──────────┬──────────────┬────────────────────────────────────┐
│ Offset   │ Size (bytes) │ Field                              │
├──────────┼──────────────┼────────────────────────────────────┤
│ 0        │ 8            │ sequence (u64, little-endian)      │
│ 8        │ 8            │ timestamp (i64, little-endian)     │
│ 16       │ 4            │ point_count (u32, little-endian)   │
│ 20       │ 4            │ data_len (u32, little-endian)      │
│ 24       │ 4            │ checksum (u32, CRC32)              │
│ 28       │ 4            │ padding (reserved)                 │
└──────────┴──────────────┴────────────────────────────────────┘

Data (variable length):
┌──────────────────────────────────────────────────────────────┐
│ bincode-serialized Vec<Point> (data_len bytes)               │
└──────────────────────────────────────────────────────────────┘
```

**CRC32 Coverage:**
```
┌──────────────────────────────────────────────────────────────┐
│                    CRC32 PROTECTED REGION                    │
├──────────────────────────────────────────────────────────────┤
│ Header bytes 0-23 (sequence, timestamp, point_count, data_len│
│ + all data bytes                                             │
└──────────────────────────────────────────────────────────────┘
```

The CRC32 checksum protects both the header fields and the serialized data, ensuring complete entry integrity. If any byte in the header or data is corrupted, validation will fail.

**Rotation:**
- Files rotate when size exceeds `max_file_size` (default: 64MB)
- New file gets incremented sequence number
- Old files retained based on `wal_retention_secs` configuration

### Segment File Format

Segments store compressed, immutable data flushed from MemTables.

**File Naming:** `segment_XXXXXXXXXXXXXXXX.rts` (16-digit hex segment ID)

**File Layout:**
```
┌──────────────────────────────────────────────────────────────┐
│ Magic: "RTSS" (4 bytes: 0x52 0x54 0x53 0x53)                 │
├──────────────────────────────────────────────────────────────┤
│ Version: u16 (2 bytes, little-endian, currently = 1)         │
├──────────────────────────────────────────────────────────────┤
│ Metadata Block                                               │
├──────────────────────────────────────────────────────────────┤
│ Timestamp Column                                             │
├──────────────────────────────────────────────────────────────┤
│ Field Column 0                                               │
├──────────────────────────────────────────────────────────────┤
│ Field Column 1                                               │
├──────────────────────────────────────────────────────────────┤
│ ...                                                          │
├──────────────────────────────────────────────────────────────┤
│ Field Column N                                               │
└──────────────────────────────────────────────────────────────┘
```

**Metadata Block:**
```
┌──────────┬──────────────┬────────────────────────────────────┐
│ Offset   │ Size (bytes) │ Field                              │
├──────────┼──────────────┼────────────────────────────────────┤
│ 0        │ 4            │ metadata_len (u32, little-endian)  │
│ 4        │ metadata_len │ bincode-serialized SegmentMeta     │
└──────────┴──────────────┴────────────────────────────────────┘

SegmentMeta contains:
- id: u64 (segment identifier)
- series_id: u64 (hash of measurement + tags)
- measurement: String
- tags: Vec<Tag>
- time_range: TimeRange (start, end timestamps)
- point_count: usize
- fields: Vec<(String, FieldType)>
- compressed_size: usize
- uncompressed_size: usize
```

**Column Block Format:**
```
┌──────────┬──────────────┬────────────────────────────────────┐
│ Offset   │ Size (bytes) │ Field                              │
├──────────┼──────────────┼────────────────────────────────────┤
│ 0        │ 4            │ column_len (u32, little-endian)    │
│ 4        │ column_len   │ compressed column data             │
└──────────┴──────────────┴────────────────────────────────────┘
```

**Compression by Field Type:**

| Field Type | Compression Algorithm |
|------------|----------------------|
| Timestamps | Delta-of-delta + LZ4/Zstd |
| Float | Gorilla XOR encoding |
| Integer | Delta + varint encoding |
| String | Dictionary encoding |
| Boolean | Bit-packing (8 per byte) |

### Partition Directory Structure

Partitions organize data by time windows (default: 24 hours).

**Directory Naming:** `partition_XXXXXXXXXXXXXXXX/` (16-digit hex partition ID)

**Directory Layout:**
```
data/
├── partition_0000000000000001/
│   ├── partition.meta           # Partition metadata
│   ├── segment_0000000000000001.rts
│   ├── segment_0000000000000002.rts
│   └── ...
├── partition_0000000000000002/
│   ├── partition.meta
│   └── ...
└── wal/
    ├── wal_00000000.log
    ├── wal_00000001.log
    └── ...
```

**partition.meta Format:**
```
bincode-serialized PartitionMeta:
- id: u64
- time_range: TimeRange (start, end timestamps)
- segment_count: usize
- total_points: usize
- created_at: i64 (nanosecond timestamp)
```

**Partition Time Calculation:**
```
partition_duration = 24 * 60 * 60 * 1_000_000_000  // 1 day in nanos
partition_start = (timestamp / partition_duration) * partition_duration
partition_end = partition_start + partition_duration
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `parking_lot` | High-performance locking |
| `dashmap` | Concurrent hash map |
| `crc32fast` | CRC32 checksums |
| `tokio` | Async runtime |
| `bincode` | Binary serialization |
| `tracing` | Structured logging |
| `rusts-compression` | Compression algorithms |
| `rusts-core` | Core types |

## Configuration Example

```rust
StorageEngineConfig {
    data_dir: PathBuf::from("./data"),
    wal_dir: None,
    wal_durability: WalDurability::Periodic { interval_ms: 100 },
    wal_retention_secs: Some(604800),  // 7 days
    flush_trigger: FlushTrigger {
        max_size: 64 * 1024 * 1024,    // 64MB
        max_points: 1_000_000,
        max_age_nanos: 60 * 1_000_000_000,
    },
    partition_duration: 24 * 60 * 60 * 1_000_000_000,  // 1 day
    compression: CompressionLevel::Default,
}
```

## Load Testing / Endpoint Monitor Simulator

For high-throughput ingestion benchmarks, the `rusts-endpoint-monitor-simulator` **test mode** (`test`) runs `rusts-server` with an isolated data directory under `./.perf_test_data/` and typically sets `wal_durability` to `none` (fastest) to measure raw ingestion throughput.

This does not change storage engine architecture, but it affects:

- **WAL durability**: `WalDurability::None` avoids `fsync` overhead during benchmarks.
- **Disk usage**: high-throughput tests can consume tens of GB quickly depending on run duration.
