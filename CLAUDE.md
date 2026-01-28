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

# Run the server
cargo run -p rusts-server
```

## Crate Structure

```
crates/
├── rusts-core/           # Core types: Point, Series, Field, Tag, FieldValue, TimeRange
├── rusts-compression/    # Gorilla XOR, delta-of-delta, dictionary, LZ4/Zstd compression
├── rusts-storage/        # WAL, MemTable, Segment, Partition, StorageEngine
├── rusts-index/          # Series index, Tag index (Roaring bitmaps), Bloom filters
├── rusts-query/          # Query model, planner, executor, aggregation functions
├── rusts-api/            # REST API, line protocol parser, auth, rate limiting
├── rusts-cluster/        # Static config, sharding strategies, query routing
├── rusts-aggregation/    # Continuous aggregates, downsampling
├── rusts-retention/      # Retention policies, storage tiering
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

## API Endpoints

- `POST /write` - Write data (InfluxDB line protocol)
- `POST /query` - Query data (JSON)
- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /stats` - Database statistics

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

## Testing Notes

- Tests use small timestamps (0, 1000, 2000...) which can trigger age-based flush
- Some tests configure high flush thresholds to prevent auto-flush
- Bloom filter FPR tests have tolerance due to hash function variation
