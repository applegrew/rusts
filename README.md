# RusTs

A high-performance time series database written in Rust, designed for monitoring, IoT, and real-time analytics workloads.

## Features

- **High Performance**: Optimized for time series workloads with columnar storage and specialized compression
- **InfluxDB Compatible**: Line protocol support for easy migration
- **Flexible Durability**: Configurable WAL modes (sync per write, periodic, or async)
- **Advanced Compression**: Gorilla XOR for floats, delta-of-delta for timestamps, dictionary encoding for strings
- **Horizontal Scaling**: Static cluster configuration with sharding and replication
- **Rich Query API**: Aggregations, time bucketing, tag filtering, and more
- **Retention Policies**: Automatic data expiration and storage tiering

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                      REST API Layer                        │
│         (InfluxDB Line Protocol, JSON Query API)           │
├────────────────────────────────────────────────────────────┤
│                     Query Engine                           │
│            (Planner, Optimizer, Executor)                  │
├──────────┬──────────┬───────────┬──────────┬───────────────┤
│  Index   │ Storage  │ Compress  │ Cluster  │  Retention    │
│  Engine  │  Engine  │  Engine   │  Router  │   Manager     │
└──────────┴──────────┴───────────┴──────────┴───────────────┘
```

### Data Model

```
Measurement: cpu
Tags:        host=server01, region=us-west
Fields:      usage=64.5, idle=35.5
Timestamp:   1609459200000000000 (nanoseconds)
```

- **Measurement**: Logical grouping of related data (like a table)
- **Tags**: Indexed metadata for filtering (string key-value pairs)
- **Fields**: Actual data values (float, integer, string, boolean)
- **Timestamp**: Nanosecond-precision Unix epoch

### Storage Engine

```
Write Path:
  Client → WAL → MemTable → Background Flush → Segments

Read Path:
  Query → Partition Pruning → Segment Scan → Aggregation
```

**Three-Tier Storage:**
1. **Active**: In-memory write buffer (MemTable) + Write-Ahead Log
2. **Hot**: Native columnar segments with LZ4 compression
3. **Cold**: High-compression segments with Zstd

### Compression

| Data Type | Algorithm | Typical Ratio |
|-----------|-----------|---------------|
| Timestamps | Delta-of-delta | 10-15x |
| Floats | Gorilla XOR | 8-12x |
| Integers | Zigzag + Varint | 4-8x |
| Strings | Dictionary encoding | 3-10x |

## Quick Start

### Prerequisites

- Rust 1.75 or later
- Cargo

### Build

```bash
# Clone the repository
git clone https://github.com/rusts/rusts.git
cd rusts

# Build in release mode
cargo build --release

# Run tests
cargo test --workspace
```

### Run the Server

```bash
# Start with default settings (standalone mode)
cargo run --release -p rusts-server

# Or with custom configuration
cargo run --release -p rusts-server -- \
  --data-dir /var/lib/rusts \
  --bind 0.0.0.0:8086 \
  --wal-mode periodic
```

### Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `./data` | Data storage directory |
| `--bind` | `127.0.0.1:8086` | HTTP bind address |
| `--wal-mode` | `periodic` | WAL durability: `every-write`, `periodic`, `os-default`, `none` |

## API Usage

### Write Data

Write data using InfluxDB line protocol:

```bash
# Single point
curl -X POST 'http://localhost:8086/write' \
  -d 'cpu,host=server01,region=us-west usage=64.5 1609459200000000000'

# Multiple points
curl -X POST 'http://localhost:8086/write' \
  -d 'cpu,host=server01 usage=64.5 1609459200000000000
cpu,host=server01 usage=62.3 1609459201000000000
cpu,host=server02 usage=71.2 1609459200000000000'
```

**Line Protocol Format:**
```
<measurement>,<tag_key>=<tag_value>,... <field_key>=<field_value>,... [timestamp]
```

### Query Data

```bash
# Query by measurement and time range
curl -X POST 'http://localhost:8086/query' \
  -H 'Content-Type: application/json' \
  -d '{
    "measurement": "cpu",
    "time_range": {
      "start": 1609459200000000000,
      "end": 1609459300000000000
    }
  }'

# Query with tag filter
curl -X POST 'http://localhost:8086/query' \
  -H 'Content-Type: application/json' \
  -d '{
    "measurement": "cpu",
    "time_range": {"start": 0, "end": 9223372036854775807},
    "tag_filters": [
      {"key": "host", "value": "server01", "op": "Eq"}
    ]
  }'

# Query with aggregation
curl -X POST 'http://localhost:8086/query' \
  -H 'Content-Type: application/json' \
  -d '{
    "measurement": "cpu",
    "time_range": {"start": 0, "end": 9223372036854775807},
    "aggregation": {
      "function": "Mean",
      "field": "usage",
      "interval_nanos": 60000000000
    }
  }'
```

### Health Check

```bash
curl http://localhost:8086/health
# {"status":"healthy"}

curl http://localhost:8086/ready
# {"ready":true}
```

### Statistics

```bash
curl http://localhost:8086/stats
# {"memtable":{"active_size":1024,"active_points":100,...},...}
```

## Clustering

RusTs supports horizontal scaling through static cluster configuration. For production deployments, integrate with external coordination services (etcd, Consul, or Kubernetes) for dynamic discovery.

### Cluster Configuration

Create a `cluster.toml` file:

```toml
node_id = "node1"
cluster_name = "rusts-prod"
replication_mode = "Quorum"    # Sync, Quorum, or Async
replication_factor = 2
shard_count = 16
sharding_strategy = "BySeries" # BySeries, ByMeasurement, ByTime

[[nodes]]
id = "node1"
address = "10.0.1.1:8086"
shards = [0, 1, 2, 3, 4, 5, 6, 7]

[[nodes]]
id = "node2"
address = "10.0.1.2:8086"
shards = [8, 9, 10, 11, 12, 13, 14, 15]
replica_shards = [0, 1, 2, 3, 4, 5, 6, 7]

[[nodes]]
id = "node3"
address = "10.0.1.3:8086"
replica_shards = [8, 9, 10, 11, 12, 13, 14, 15]
```

### Sharding Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| `BySeries` | Hash of measurement + tags | Even distribution |
| `ByMeasurement` | Hash of measurement name | Measurement-level isolation |
| `ByTime` | Time-based partitioning | Time-range queries |
| `Composite` | Time + Series combined | Balanced workloads |

### Replication Modes

| Mode | Consistency | Latency | Use Case |
|------|-------------|---------|----------|
| `Sync` | Strong | High | Financial data |
| `Quorum` | Eventual | Medium | General purpose |
| `Async` | Weak | Low | High-throughput metrics |

## Data Import

RusTs includes a standalone CLI tool for importing data from files.

### Parquet Import

```bash
# Build the importer
cargo build --release -p rusts-importer

# View Parquet file schema
./target/release/rusts-import parquet data.parquet --schema-only

# Import with default settings
./target/release/rusts-import parquet data.parquet \
  --measurement metrics \
  --server http://localhost:8086

# Specify tag columns and timestamp column
./target/release/rusts-import parquet data.parquet \
  --measurement cpu \
  --timestamp-column time \
  --tags host,region,datacenter \
  --batch-size 50000

# Dry run (read file, don't write to server)
./target/release/rusts-import parquet data.parquet --dry-run
```

### Importer Options

| Option | Default | Description |
|--------|---------|-------------|
| `-m, --measurement` | `imported` | Measurement name for all points |
| `-s, --server` | `http://localhost:8086` | RusTs server URL |
| `-t, --timestamp-column` | `timestamp` | Column containing timestamps |
| `--tags` | (none) | Comma-separated tag column names |
| `-b, --batch-size` | `10000` | Points per write batch |
| `--schema-only` | false | Only display schema, don't import |
| `--dry-run` | false | Read file but don't write to server |

### Supported Timestamp Formats

- `Timestamp(Nanosecond)` - Direct nanoseconds
- `Timestamp(Microsecond)` - Converted to nanoseconds
- `Timestamp(Millisecond)` - Converted to nanoseconds
- `Timestamp(Second)` - Converted to nanoseconds
- `Int64` - Interpreted as nanoseconds
- `Date64` - Milliseconds since epoch

## Project Structure

```
rusts/
├── Cargo.toml                 # Workspace configuration
├── crates/
│   ├── rusts-core/           # Core types (Point, Series, Field, Tag)
│   ├── rusts-compression/    # Compression algorithms
│   ├── rusts-storage/        # WAL, MemTable, Segments, Partitions
│   ├── rusts-index/          # Series index, Tag index, Bloom filters
│   ├── rusts-query/          # Query planning and execution
│   ├── rusts-api/            # REST API, line protocol parser
│   ├── rusts-cluster/        # Sharding, routing, replication
│   ├── rusts-aggregation/    # Continuous aggregates, downsampling
│   ├── rusts-retention/      # Retention policies, tiering
│   ├── rusts-importer/       # Data import CLI (Parquet, etc.)
│   └── rusts-server/         # Main server binary
└── README.md
```

## Development

### Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p rusts-compression

# With output
cargo test --workspace -- --nocapture
```

### Benchmarks

```bash
# Compression benchmarks
cargo bench -p rusts-compression

# Ingestion benchmarks
cargo bench -p rusts-server --bench ingestion

# Query benchmarks
cargo bench -p rusts-server --bench query
```

### Code Coverage

```bash
# Requires cargo-tarpaulin
cargo install cargo-tarpaulin
cargo tarpaulin --workspace --out Html
```

## Performance Considerations

### Write Optimization

- Batch writes when possible (reduces WAL overhead)
- Use appropriate WAL mode for your durability requirements
- Monitor MemTable size and flush frequency

### Query Optimization

- Use tag filters to reduce scan scope
- Leverage time range pruning
- Pre-aggregate data for dashboard queries

### Memory Tuning

Default MemTable flush triggers:
- Size: 64 MB
- Points: 1 million
- Age: 60 seconds

Adjust via configuration for your workload.

## Roadmap

- [ ] SQL query interface (via DataFusion)
- [ ] Kubernetes operator
- [ ] Continuous queries
- [ ] Materialized views

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
