# RusTs Importer - Architecture

## Overview

The `rusts-importer` crate provides a CLI tool for importing data from Parquet files into RusTs. It supports streaming imports via REST API or direct storage engine writes, with deduplication capabilities.

## Module Structure

```
src/
├── lib.rs          # Public exports
├── main.rs         # CLI entry point
├── error.rs        # Error types
├── parquet.rs      # Parquet file reading
└── writer.rs       # HTTP REST writer
```

## CLI Arguments (main.rs:78-139)

### Main CLI Structure

```rust
#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "rusts.yml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}
```

### Parquet Subcommand

| Argument | Flag | Default | Purpose |
|----------|------|---------|---------|
| file | (positional) | required | Parquet file path |
| measurement | `-m, --measurement` | `imported` | Measurement name |
| server | `-s, --server` | `http://localhost:8086` | Server URL |
| timestamp_column | `-t, --timestamp-column` | `timestamp` | Timestamp column |
| tags | `--tags` | empty | Tag columns (comma-separated) |
| batch_size | `-b, --batch-size` | `10000` | Records per batch |
| schema_only | `--schema-only` | false | Print schema only |
| dry_run | `--dry-run` | false | Count only |
| dedup_column | `--dedup-column` | none | Deduplication column |
| direct | `--direct` | false | Direct storage write |
| data_dir | `--data-dir` | from config | Storage directory |

## Parquet Reading (parquet.rs)

### ParquetReaderConfig (parquet.rs:30-80)

```rust
pub struct ParquetReaderConfig {
    pub measurement: String,
    pub timestamp_column: String,
    pub tag_columns: HashSet<String>,
    pub batch_size: usize,
}
```

Builder methods:
- `new(measurement)` - Create config
- `with_timestamp_column(column)` - Set timestamp column
- `with_tag_columns(columns)` - Set tag columns
- `with_batch_size(size)` - Set batch size

### Reading Methods

| Method | Lines | Returns | Purpose |
|--------|-------|---------|---------|
| `read_file()` | 94-199 | `Vec<Point>` | Full file read |
| `read_file_batched()` | 201-303 | `Iterator<Result<Vec<Point>>>` | Streaming batched read |

### Timestamp Extraction (parquet.rs:306-375)

Supports multiple Arrow timestamp types:

| Arrow Type | Conversion |
|------------|------------|
| `Timestamp(Nanosecond)` | Direct copy |
| `Timestamp(Microsecond)` | × 1,000 |
| `Timestamp(Millisecond)` | × 1,000,000 |
| `Timestamp(Second)` | × 1,000,000,000 |
| `Int64` | Direct copy |
| `Date64` | × 1,000,000 |

### Field Value Extraction (parquet.rs:401-462)

| Arrow Type | FieldValue |
|------------|------------|
| Float64/Float32 | `FieldValue::Float` |
| Int64/32/16/8 | `FieldValue::Integer` |
| UInt64/32/16/8 | `FieldValue::UnsignedInteger` |
| Boolean | `FieldValue::Boolean` |
| Utf8/LargeUtf8 | `FieldValue::String` |

### Schema Inspection (parquet.rs:464-477)

```rust
pub fn inspect_parquet_schema(path) -> Result<Vec<(String, String)>>
```

Returns column names and Arrow type strings for `--schema-only` mode.

## Import Modes

### Direct Mode (main.rs:272-404)

**Characteristics:**
- Writes directly to storage engine
- Bypasses REST API
- Disables WAL (source file is recovery mechanism)
- Uses larger memtable buffers (256MB, 10M points)
- Requires exclusive data directory access

**Storage Configuration:**
```rust
StorageEngineConfig {
    wal_durability: WalDurability::None,
    flush_trigger: FlushTrigger {
        max_size: 256 * 1024 * 1024,    // 256MB
        max_points: 10_000_000,
        max_age_nanos: i64::MAX,
    },
    ...
}
```

**Performance:** ~260k points/sec

### REST Mode (main.rs:406-522)

**Characteristics:**
- Streams batches via HTTP API
- Server manages WAL and durability
- Can run while server is active
- Supports deduplication by tags and fields

**Performance:** ~45k points/sec

## Deduplication (main.rs:524-667)

### REST Mode Dedup Query (lines 524-565)

- Queries `/query` endpoint for all existing records
- Extracts dedup column from both fields and tags
- Returns `HashSet<String>` of existing keys

### Direct Mode Dedup Query (lines 567-606)

- Queries storage engine directly
- Can only deduplicate by fields (not tags)
- Warns if column not found (may be a tag)

### Filtering Function (lines 608-646)

```rust
fn filter_existing_points(
    points: Vec<Point>,
    dedup_column: &str,
    existing_keys: &HashSet<String>,
) -> (Vec<Point>, usize)  // (filtered, skipped_count)
```

Priority:
1. Check tags first
2. Fall back to fields
3. Include with warning if column not found

## HTTP Writer (writer.rs)

### RustsWriter (writer.rs:9-13)

```rust
pub struct RustsWriter {
    client: reqwest::Client,
    base_url: String,
}
```

### Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `new()` | 49-63 | Create writer with 60s timeout |
| `health_check()` | 65-70 | GET /health |
| `query()` | 72-102 | POST /query |
| `write()` | 104-134 | POST /write |
| `write_batched()` | 136-172 | Batch writes with progress |

### Line Protocol Conversion (writer.rs:184-226)

Converts Points to InfluxDB line protocol:

```
measurement,tag1=val1,tag2=val2 field1=123i,field2=45.6 timestamp
```

Field type encoding:
- Float: `value`
- Integer: `valuei`
- Unsigned Integer: `valueu`
- String: `"value"`
- Boolean: `true`/`false`

## Error Handling (error.rs)

```rust
pub enum ImportError {
    Io(std::io::Error),
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow::error::ArrowError),
    Http(reqwest::Error),
    Server { status: u16, message: String },
    Config(String),
    Schema(String),
    Conversion(String),
}
```

## Dependencies

| Category | Crates | Purpose |
|----------|--------|---------|
| Internal | rusts-core, rusts-storage, rusts-compression | RusTs ecosystem |
| Data Format | arrow, parquet | Columnar data |
| CLI | clap | Argument parsing |
| Async | tokio | Runtime |
| HTTP | reqwest | REST client |
| Serialization | serde, serde_json, serde_yaml | Data/config |
| Error | thiserror, anyhow | Error handling |
| Utilities | tracing, indicatif, chrono | Logging, progress |

## Workflow Example

```
rusts-import parquet data.parquet -m cpu --tags host,region --direct --dedup-column id

1. Parse CLI arguments
2. Load rusts.yml for data_dir
3. Create ParquetReaderConfig
4. Route to import_direct_streaming()
5. Create StorageEngine
6. Query existing 'id' values
7. For each batch:
   ├─ reader.read_file_batched() yields Vec<Point>
   ├─ filter_existing_points() removes duplicates
   ├─ engine.write_batch() writes to storage
   └─ Update progress
8. Flush and shutdown
9. Print metrics
```

## Mode Comparison

| Aspect | Direct Mode | REST Mode |
|--------|------------|-----------|
| Write Path | Storage engine | HTTP POST |
| WAL | Disabled | Server config |
| Connection | None | TCP to server |
| Dedup Support | Fields only | Fields and tags |
| Performance | ~260k pts/sec | ~45k pts/sec |
