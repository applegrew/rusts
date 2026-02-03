![Logo](logo.png)

# RusTs

A high-performance time series database written in Rust, designed for monitoring, IoT, and real-time analytics workloads.

[![Video Overview](https://img.youtube.com/vi/-aaLTkZcpJ8/maxresdefault.jpg)](https://youtu.be/-aaLTkZcpJ8)

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
# Start with default settings (reads rusts.yml if present)
cargo run --release -p rusts-server

# Generate a default configuration file
cargo run --release -p rusts-server -- --generate-config

# Use a custom configuration file
cargo run --release -p rusts-server -- --config /etc/rusts/rusts.yml

# Override config with command line arguments
cargo run --release -p rusts-server -- \
  --data-dir /var/lib/rusts \
  --host 0.0.0.0 \
  --port 8086
```

### Configuration File (rusts.yml)

RusTs reads configuration from `rusts.yml` in the current directory by default. Generate a default config with `--generate-config`.

```yaml
server:
  host: 0.0.0.0
  port: 8086
  max_body_size: 10485760    # 10MB max request body
  request_timeout_secs: 30

storage:
  data_dir: ./data
  wal_dir: null              # Defaults to data_dir/wal
  wal_durability: periodic   # every_write, periodic, os_default, none
  wal_sync_interval_ms: 100  # For periodic mode
  wal_retention_secs: 604800 # 7 days (null = forever for CDC)
  memtable:
    max_size_mb: 64
    max_points: 1000000
    max_age_secs: 60
  partition_duration_hours: 24
  compression: default       # none, fast, default, best

auth:
  enabled: false
  jwt_secret: change-me-in-production
  token_expiration_secs: 3600

logging:
  level: info                # trace, debug, info, warn, error
  show_target: true
  show_thread_ids: false
  show_location: false

retention_policies: []
```

### Command Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --config` | `rusts.yml` | Configuration file path |
| `-d, --data-dir` | (from config) | Data storage directory (overrides config) |
| `-H, --host` | (from config) | Bind address (overrides config) |
| `-p, --port` | (from config) | HTTP port (overrides config) |
| `--generate-config` | | Generate default rusts.yml and exit |

### WAL Durability Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `every_write` | Sync after each write | Maximum durability, slower |
| `periodic` | Sync at intervals (default 100ms) | Balanced durability/performance |
| `os_default` | Let OS decide when to sync | Higher throughput, some risk |
| `none` | No syncing (in-memory only) | Bulk imports, testing |

### WAL Retention

WAL files can be retained for:
- **Change Data Capture (CDC)**: Stream changes to downstream systems
- **Backup/Recovery**: Point-in-time recovery beyond memtable flushes

Set `wal_retention_secs: null` to retain WAL files indefinitely for CDC use cases.

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

RusTs includes a standalone CLI tool for importing data from files with streaming support for large datasets.

### Parquet Import

```bash
# Build the importer
cargo build --release -p rusts-importer

# View Parquet file schema
./target/release/rusts-import parquet data.parquet --schema-only

# Import via REST API (streaming)
./target/release/rusts-import parquet data.parquet \
  --measurement metrics \
  --server http://localhost:8086

# Direct mode - write directly to storage (faster, bypasses REST)
./target/release/rusts-import parquet data.parquet \
  --measurement metrics \
  --direct

# Specify tag columns and timestamp column
./target/release/rusts-import parquet data.parquet \
  --measurement cpu \
  --timestamp-column time \
  --tags host,region,datacenter \
  --batch-size 50000 \
  --direct

# Use custom config file (reads data_dir from config)
./target/release/rusts-import --config /etc/rusts.yml parquet data.parquet --direct

# Override data directory
./target/release/rusts-import parquet data.parquet --direct --data-dir /var/lib/rusts

# Deduplicate against existing database records
./target/release/rusts-import parquet data.parquet \
  --measurement metrics \
  --dedup-column record_id \
  --direct

# Dry run (read file, don't write to server)
./target/release/rusts-import parquet data.parquet --dry-run
```

### Import Modes

| Mode | Flag | Description | Performance |
|------|------|-------------|-------------|
| REST Streaming | (default) | Stream batches via HTTP API | ~45k pts/sec |
| Direct | `--direct` | Write directly to storage engine | ~260k pts/sec |

**Direct mode** bypasses the REST API and writes directly to the storage engine. It:
- Disables WAL (source file serves as recovery mechanism)
- Uses larger memtable buffers for bulk loading
- Requires exclusive access to the data directory (server should not be running)

### Deduplication

The `--dedup-column` option checks against **existing records in the database** before importing:

```bash
# Skip records where 'trip_id' already exists in the database
./target/release/rusts-import parquet trips.parquet \
  --measurement trips \
  --dedup-column trip_id \
  --direct
```

**Deduplication behavior:**
- Queries all existing values of the dedup column before import starts
- Filters each batch to exclude records that already exist
- Reports skipped duplicate count in progress and summary

**Limitations:**
- **Direct mode**: Can only deduplicate by field columns (not tags)
- **REST mode**: Can deduplicate by both tags and fields
- Loads all existing dedup keys into memory

### Importer Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config` | `rusts.yml` | Path to rusts.yml config file |
| `-m, --measurement` | `imported` | Measurement name for all points |
| `-s, --server` | `http://localhost:8086` | RusTs server URL (REST mode) |
| `-t, --timestamp-column` | `timestamp` | Column containing timestamps |
| `--tags` | (none) | Comma-separated tag column names |
| `-b, --batch-size` | `10000` | Points per write batch |
| `--direct` | false | Write directly to storage engine |
| `--data-dir` | (from config) | Data directory for direct mode |
| `--dedup-column` | (none) | Column for deduplication against existing DB records |
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
│   ├── rusts-sql/            # SQL query interface (sqlparser-rs)
│   ├── rusts-pgwire/         # PostgreSQL wire protocol (psql, DataGrip)
│   ├── rusts-cluster/        # Sharding, routing, replication
│   ├── rusts-aggregation/    # Continuous aggregates, downsampling
│   ├── rusts-retention/      # Retention policies, tiering
│   ├── rusts-importer/       # Data import CLI (Parquet, etc.)
│   ├── rusts-endpoint-monitor-simulator/  # DEX workload simulator
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

# Query benchmarks (SQL)
python3 benchmarks/benchmark.py --iterations 3
```

### Query Performance Results

Benchmark performed on **1.25 million points** with the following data shape:

| Attribute | Value |
|-----------|-------|
| **Measurement** | `trips` |
| **Tags** | `hvfhs_license_num`, `dispatching_base_num` (2 unique series) |
| **Fields** | 17 numeric/string fields (`trip_miles`, `trip_time`, `base_passenger_fare`, `driver_pay`, `tips`, `tolls`, `airport_fee`, `congestion_surcharge`, `sales_tax`, `bcf`, `PULocationID`, `DOLocationID`, etc.) |
| **Time range** | ~30 days |
| **Segments** | 4 segments across 2 partitions |

**Results (44 queries, single iteration):**

| Query Type | Avg Time | Example |
|------------|----------|---------|
| Simple aggregations | ~950ms | `SELECT COUNT(*) FROM trips` |
| Filtered aggregations | ~940ms | `SELECT SUM(trip_miles) FROM trips WHERE trip_miles > 0` |
| Tag-filtered queries | ~650ms | `SELECT COUNT(*) FROM trips WHERE hvfhs_license_num = 'HV0003'` |
| GROUP BY queries | ~940ms | `SELECT hvfhs_license_num, COUNT(*) FROM trips GROUP BY hvfhs_license_num` |
| LIMIT queries | ~250-700ms | `SELECT * FROM trips ORDER BY time DESC LIMIT 10` |

**Summary:**
- Total benchmark time: **37 seconds** (44 queries)
- Average query time: **841ms**
- All queries complete in **under 1 second**

Data source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (FHVHV trips, November 2025)

### Endpoint Monitoring Benchmark

Simulated Digital Employee Experience (DEX) workload using the `rusts-endpoint-monitor-simulator`:

**Configuration:**
| Parameter | Value |
|-----------|-------|
| Devices | 1,000 |
| Series | ~7,500 |
| Duration | 5 minutes |
| Write interval | 15 seconds |
| Query rate | 5 queries/sec |
| Measurements | `device_health`, `app_performance`, `network_health`, `experience_score` |

**Write Performance:**
| Metric | Value |
|--------|-------|
| Points Written | 382,653 |
| Throughput | 1,275 points/sec |
| Data Volume | 73.19 MB (249.78 KB/s) |
| p50 Latency | 26.5ms |
| p95 Latency | 51.6ms |
| p99 Latency | 109.5ms |

**Query Performance:**
| Query Type | Count | p50 | p95 | p99 |
|------------|-------|-----|-----|-----|
| Dashboard (aggregations, GROUP BY) | 901 | 13.7ms | 51.4ms | 62.5ms |
| Alerting (HAVING thresholds) | 451 | 12.6ms | 55.3ms | 62.6ms |
| Historical (single-device trends) | 149 | 0.9ms | 2.1ms | 4.0ms |

**Summary:**
- Zero errors during 5-minute benchmark
- Sustained 1,275 writes/sec + 5 queries/sec concurrently
- Sub-100ms p99 write latency under mixed read/write load
- Historical point queries complete in under 1ms (p50)

Run the benchmark yourself:
```bash
cargo run -p rusts-endpoint-monitor-simulator --release -- benchmark \
  --devices 1000 --duration 300 --write-interval 15 --query-rate 5
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

## PostgreSQL Wire Protocol

RusTs supports the PostgreSQL wire protocol, enabling connections from psql, DataGrip, SQLAlchemy, and other PostgreSQL clients. This provides ~5-15x faster queries compared to HTTP REST for many workloads.

### Configuration

Enable PostgreSQL wire protocol in `rusts.yml`:

```yaml
postgres:
  enabled: true
  host: 0.0.0.0
  port: 5432
  max_connections: 100
```

### Usage

```bash
# Connect with psql (PostgreSQL 12+ requires GSSAPI disabled)
PGGSSENCMODE=disable psql -h localhost -p 5432

# Run queries
PGGSSENCMODE=disable psql -h localhost -p 5432 -c "SHOW TABLES"
PGGSSENCMODE=disable psql -h localhost -p 5432 -c "SELECT * FROM cpu LIMIT 10"

# Or set environment variable for session
export PGGSSENCMODE=disable
psql -h localhost -p 5432
```

```python
# Python with SQLAlchemy
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://localhost:5432/rusts?gssencmode=disable')
with engine.connect() as conn:
    result = conn.execute(text('SELECT * FROM trips LIMIT 5'))
    for row in result:
        print(row)
```

### Limitations

- No parameterized queries (prepared statements work, but `$1`, `$2` bind variables are not supported)
- No authentication (trusts all connections)
- No TLS/SSL support
- No GSSAPI encryption (psql 12+ clients must set `PGGSSENCMODE=disable`)

## SQL Query Interface

RusTs supports SQL queries via the `/sql` endpoint. Queries are parsed using sqlparser-rs and translated to the native Query model for execution.

### SQL Usage

```bash
# Basic SELECT
curl -X POST 'http://localhost:8086/sql' \
  -d "SELECT * FROM cpu WHERE host = 'server01' LIMIT 10"

# Aggregation with GROUP BY
curl -X POST 'http://localhost:8086/sql' \
  -d "SELECT AVG(usage), MAX(usage) FROM cpu GROUP BY host"

# Time range filtering
curl -X POST 'http://localhost:8086/sql' \
  -d "SELECT * FROM cpu WHERE time >= '2024-01-01' AND time < '2024-01-02'"
```

### Supported SQL Features

| Feature | Example |
|---------|---------|
| Field selection | `SELECT usage, temperature FROM cpu` |
| Wildcards | `SELECT * FROM cpu` |
| Tag filtering | `WHERE host = 'server01'` |
| Tag IN | `WHERE region IN ('us-west', 'us-east')` |
| Tag NOT EQUALS | `WHERE host != 'server01'` |
| Tag EXISTS | `WHERE host IS NOT NULL` |
| Time range | `WHERE time >= '2024-01-01' AND time < '2024-01-02'` |
| Aggregations | `SELECT AVG(usage), COUNT(*), MAX(temp) FROM cpu` |
| GROUP BY tags | `GROUP BY host, region` |
| ORDER BY | `ORDER BY time DESC` |
| LIMIT/OFFSET | `LIMIT 100 OFFSET 50` |
| Show tables | `SHOW TABLES` |

**Supported aggregate functions:** COUNT, SUM, AVG/MEAN, MIN, MAX, FIRST, LAST, STDDEV, VARIANCE, PERCENTILE_N

**Not supported (v1):** JOINs, subqueries, CTEs, window functions, UNION, OR conditions

### SHOW TABLES

List all measurements (tables) in the database:

```bash
curl -X POST 'http://localhost:8086/sql' -d "SHOW TABLES"

# Response:
# {
#   "measurement": "_tables",
#   "results": [{"fields": {"name": "cpu"}}, {"fields": {"name": "memory"}}],
#   "total_rows": 2,
#   "execution_time_ms": 0.5
# }
```

## Roadmap

- [x] SQL query interface
  - [x] Architecture design (sqlparser-rs based)
  - [x] Create rusts-sql crate
  - [x] SQL parser wrapper
  - [x] SQL to Query translator
  - [x] Time-series functions (now(), time_bucket())
  - [x] /sql API endpoint
  - [x] Documentation and examples
  - [ ] DataFusion integration (future)
- [x] PostgreSQL wire protocol
  - [x] pgwire crate integration
  - [x] Simple query protocol support
  - [x] Extended query protocol support
  - [x] psql, DataGrip, SQLAlchemy compatibility
  - [x] pg_catalog support for JDBC clients
  - [ ] Parameterized queries (bind variables)
  - [ ] TLS/SSL support
  - [ ] Authentication
- [ ] Kubernetes operator
- [ ] Continuous queries
- [ ] Materialized views
- [ ] **Continuous Aggregates**: Pre-computed aggregations maintained automatically on write:
  ```sql
  CREATE CONTINUOUS AGGREGATE hourly_metrics AS
    SELECT time_bucket('1 hour', time) as bucket,
           COUNT(*), AVG(value), SUM(value)
    FROM metrics
    GROUP BY bucket;
  ```
  Query from the aggregate table for instant results on large time ranges.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
