# RusTs - Time Series Database

## Project Overview

RusTs is a production-grade time series database written in Rust with configurable durability, compression, downsampling, clustering, and retention policies.

## Build Commands

```bash
# Build all crates
cargo build --workspace

# Build release version
cargo build --workspace --release

# Run all tests
cargo test --workspace

# Run specific crate tests
cargo test -p rusts-compression --lib

# Run benchmarks
cargo bench -p rusts-server

# Run the server (reads rusts.yml if present)
cargo run -p rusts-server

# Generate default config file
cargo run -p rusts-server -- --generate-config

# Run with custom config
cargo run -p rusts-server -- --config /path/to/rusts.yml
```

## Configuration

Server configuration is managed via `rusts.yml` (YAML). Key sections:

- **server**: host, port, max_body_size, request_timeout
- **storage**: data_dir, wal_dir, wal_durability, wal_retention_secs, memtable settings, compression
- **auth**: enabled, jwt_secret, token_expiration
- **logging**: level, show_target, show_thread_ids, show_location
- **postgres**: enabled, host, port, max_connections (PostgreSQL wire protocol)

WAL durability modes: `every_write`, `periodic`, `os_default`, `none`

WAL retention: Set `wal_retention_secs` for automatic cleanup, or `null` to retain forever (for CDC)

## Crate Structure

```
crates/
├── rusts-core/           # Core types: Point, Series, Field, Tag, FieldValue, TimeRange
├── rusts-compression/    # Gorilla XOR, delta-of-delta, dictionary, LZ4/Zstd compression
├── rusts-storage/        # WAL, MemTable, Segment, Partition, StorageEngine
├── rusts-index/          # Series index, Tag index (Roaring bitmaps), Bloom filters
├── rusts-query/          # Query model, planner, executor, aggregation functions
├── rusts-api/            # REST API, line protocol parser, auth, rate limiting
├── rusts-sql/            # SQL query interface (sqlparser-rs, AST to Query translation)
├── rusts-pgwire/         # PostgreSQL wire protocol (pgwire crate, psql/DataGrip compatibility)
├── rusts-cluster/        # Static config, sharding strategies, query routing
├── rusts-aggregation/    # Continuous aggregates, downsampling
├── rusts-retention/      # Retention policies, storage tiering
├── rusts-importer/       # Data import CLI (Parquet, streaming, direct mode)
└── rusts-server/         # Main server binary
```

## Architecture

### Data Model
- `Timestamp` = i64 (nanosecond-precision Unix epoch)
- `SeriesId` = u64 (hash of measurement + tags)
- Points contain timestamp, tags, and fields (Float, Integer, UnsignedInteger, String, Boolean)

### Storage Engine
- **WAL**: Write-Ahead Log with configurable durability (EveryWrite, Periodic, OsDefault, None)
- **MemTable**: In-memory write buffer with flush triggers (size, points, age)
- **Segments**: Columnar compressed storage format
- **Partitions**: Time-based data organization
- **Checkpoint**: Tracks last flushed WAL sequence for efficient recovery
- **Clean Shutdown**: Flushes memtable and updates checkpoint (instant restart, no WAL recovery)

### Write Path
```
Client → WAL Writer → MemTable → Background Flusher → Segments
```

### Read Path
```
Query → Planner → Partition Pruner → Series Resolver → Parallel Scanner → Aggregator
```

## Key Files

- `crates/rusts-core/src/types.rs` - Core data types
- `crates/rusts-storage/src/engine.rs` - Storage engine coordinator
- `crates/rusts-compression/src/float.rs` - Gorilla XOR compression
- `crates/rusts-compression/src/timestamp.rs` - Delta-of-delta encoding
- `crates/rusts-query/src/executor.rs` - Query execution
- `crates/rusts-api/src/line_protocol.rs` - InfluxDB line protocol parser
- `crates/rusts-api/src/router.rs` - Axum REST API routes
- `crates/rusts-sql/src/translator.rs` - SQL to Query translation
- `crates/rusts-pgwire/src/backend.rs` - PostgreSQL wire protocol handler
- `crates/rusts-server/src/main.rs` - Server config parsing (rusts.yml)
- `crates/rusts-importer/src/main.rs` - Data import CLI
- `rusts.yml` - Default server configuration file

## API Endpoints

- `POST /write` - Write data (InfluxDB line protocol)
- `POST /query` - Query data (JSON)
- `POST /sql` - Query data (SQL)
- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /stats` - Database statistics

## PostgreSQL Wire Protocol

RusTs supports the PostgreSQL wire protocol, enabling connections from psql, DataGrip, SQLAlchemy, and other PostgreSQL clients. This provides ~5-15x faster queries compared to HTTP REST for many workloads due to binary protocol, persistent connections, and reduced serialization overhead.

### Configuration

Enable PostgreSQL wire protocol in `rusts.yml`:

```yaml
postgres:
  enabled: true
  host: 0.0.0.0
  port: 5432
  max_connections: 100
```

### Usage Examples

```bash
# Connect with psql
psql -h localhost -p 5432

# Run queries
psql -h localhost -p 5432 -c "SHOW TABLES"
psql -h localhost -p 5432 -c "SELECT * FROM cpu LIMIT 10"
psql -h localhost -p 5432 -c "SELECT COUNT(*) FROM trips"
```

```python
# Python with SQLAlchemy
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://localhost:5432/rusts')
with engine.connect() as conn:
    result = conn.execute(text('SELECT * FROM trips LIMIT 5'))
    for row in result:
        print(row)
```

### Type Mapping

| RusTs FieldValue | PostgreSQL Type |
|------------------|-----------------|
| Float(f64) | FLOAT8 |
| Integer(i64) | INT8 |
| UnsignedInteger(u64) | INT8 |
| String(String) | TEXT |
| Boolean(bool) | BOOL |
| Timestamp (i64 nanos) | TIMESTAMPTZ |

### Limitations

- Simple query protocol only (no prepared statements with parameters)
- No authentication (trusts all connections)
- No TLS/SSL support

## Cluster Configuration

Clustering uses static TOML configuration (no external dependencies like etcd required):

```toml
# cluster.toml
node_id = "node1"
cluster_name = "rusts-prod"
replication_mode = "Quorum"
replication_factor = 2
shard_count = 16
sharding_strategy = "BySeries"

[[nodes]]
id = "node1"
address = "10.0.1.1:8086"
shards = [0, 1, 2, 3, 4, 5, 6, 7]

[[nodes]]
id = "node2"
address = "10.0.1.2:8086"
shards = [8, 9, 10, 11, 12, 13, 14, 15]
replica_shards = [0, 1, 2, 3, 4, 5, 6, 7]
```

For dynamic clustering (auto-discovery, failover), integrate with external tools like etcd, Consul, or Kubernetes.

## Data Import CLI

The `rusts-import` CLI imports data from Parquet files with streaming support.

```bash
# Build
cargo build --release -p rusts-importer

# REST mode (streams to server via HTTP)
./target/release/rusts-import parquet data.parquet -m metrics

# Direct mode (writes directly to storage, ~5x faster)
./target/release/rusts-import parquet data.parquet -m metrics --direct

# With deduplication against existing DB records
./target/release/rusts-import parquet data.parquet -m metrics --dedup-column id --direct
```

Key options:
- `--direct`: Bypass REST API, write directly to storage engine
- `--dedup-column <COL>`: Skip records where column value exists in DB
- `--tags <COLS>`: Comma-separated columns to treat as tags
- `--timestamp-column <COL>`: Column for point timestamp
- `--config <PATH>`: Read data_dir from rusts.yml

Dedup limitations: Direct mode can only dedup by fields (not tags)

## Testing Notes

- Tests use small timestamps (0, 1000, 2000...) which can trigger age-based flush
- Some tests configure high flush thresholds to prevent auto-flush
- Bloom filter FPR tests have tolerance due to hash function variation

## Sample Test Data

For performance testing with real-world data, use NYC TLC trip data (Parquet format):

```bash
# Download sample data (~200MB, ~1.25M rows)
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-11.parquet

# Import into RusTs (direct mode for speed)
./target/release/rusts-import parquet fhvhv_tripdata_2025-11.parquet \
  -m trips \
  --tags hvfhs_license_num,dispatching_base_num \
  --timestamp-column pickup_datetime \
  --direct

# Example queries after import:
# SELECT * FROM trips LIMIT 10
# SELECT * FROM trips ORDER BY time DESC LIMIT 10
# SELECT COUNT(*) FROM trips
# SELECT COUNT(*) FROM trips GROUP BY hvfhs_license_num
```

Data characteristics:
- **Measurement**: `trips`
- **Tags**: `hvfhs_license_num`, `dispatching_base_num`
- **Fields**: `trip_miles`, `trip_time`, `base_passenger_fare`, `driver_pay`, `tips`, `tolls`, etc.
- **Row count**: ~1.25 million
- **Time range**: November 2025

More trip data (different months/years) available at: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
