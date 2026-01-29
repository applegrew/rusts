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
5. Perform WAL recovery

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

### query_measurement() (engine.rs:202-240)

Queries by measurement name, returns `Vec<(SeriesId, Vec<MemTablePoint>)>`.

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

- Reads all WAL files in sequence order
- Validates checksums
- Handles truncated entries gracefully
- Supports `read_from(start_sequence)` for incremental reads

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

### SegmentMeta (segment.rs:28-47)

```rust
pub struct SegmentMeta {
    pub id: u64,
    pub series_id: SeriesId,
    pub measurement: String,
    pub tags: Vec<Tag>,
    pub time_range: TimeRange,
    pub point_count: usize,
    pub fields: Vec<(String, FieldType)>,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
}
```

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
