# RusTs Server - Architecture

## Overview

The `rusts-server` crate is the main entry point for the RusTs time series database. It orchestrates all components, handles configuration, and runs the HTTP server.

## Module Structure

```
src/
├── main.rs         # Main entry point and configuration
benches/
├── ingestion.rs    # Ingestion benchmarks
└── query.rs        # Query benchmarks
```

## Configuration Structures (main.rs)

### ServerConfig (main.rs:25-40)

```rust
pub struct ServerConfig {
    pub server: ServerSettings,
    pub storage: StorageSettings,
    pub auth: AuthSettings,
    pub logging: LoggingSettings,
    pub retention_policies: Vec<RetentionPolicyConfig>,
}
```

### ServerSettings (main.rs:42-65)

```rust
pub struct ServerSettings {
    pub host: String,               // Default: "0.0.0.0"
    pub port: u16,                  // Default: 8086
    pub max_body_size: usize,       // Default: 10MB
    pub request_timeout_secs: u64,  // Default: 30s
}
```

### StorageSettings (main.rs:67-102)

```rust
pub struct StorageSettings {
    pub data_dir: PathBuf,
    pub wal_dir: Option<PathBuf>,
    pub wal_durability: String,     // every_write|periodic|os_default|none
    pub wal_sync_interval_ms: u64,  // Default: 100ms
    pub wal_retention_secs: Option<u64>,  // Default: 7 days
    pub memtable: MemTableSettings,
    pub partition_duration_hours: u64,    // Default: 24
    pub compression: String,        // none|fast|default|best
}
```

### MemTableSettings (main.rs:104-124)

```rust
pub struct MemTableSettings {
    pub max_size_mb: usize,         // Default: 64MB
    pub max_points: usize,          // Default: 1,000,000
    pub max_age_secs: u64,          // Default: 60s
}
```

### AuthSettings (main.rs:126-146)

```rust
pub struct AuthSettings {
    pub enabled: bool,              // Default: false
    pub jwt_secret: String,         // Default: "change-me-in-production"
    pub token_expiration_secs: u64, // Default: 3600
}
```

### LoggingSettings (main.rs:148-171)

```rust
pub struct LoggingSettings {
    pub level: String,              // trace|debug|info|warn|error
    pub show_target: bool,          // Default: true
    pub show_thread_ids: bool,      // Default: false
    pub show_location: bool,        // Default: false
}
```

### RetentionPolicyConfig (main.rs:173-185)

```rust
pub struct RetentionPolicyConfig {
    pub name: String,
    pub pattern: String,
    pub duration: String,           // "30d"|"1w"|"6h"
    pub is_default: bool,
}
```

## Initialization Sequence (main.rs:391-497)

```
1. Parse CLI Arguments (line 394)
   ↓
2. Generate Config Option (lines 397-402)
   ↓
3. Load Configuration (lines 405-420)
   ├─ Try config file
   └─ Fall back to defaults
   ↓
4. Apply CLI Overrides (lines 423-431)
   ↓
5. Initialize Logging (lines 434-442)
   ↓
6. Initialize Storage Engine (lines 454-457)
   ↓
7. Initialize Indexes (lines 460-461)
   ├─ SeriesIndex
   └─ TagIndex
   ↓
8. Create AppState (lines 464-468)
   ↓
9. Create Router (line 471)
   ↓
10. Start TCP Server (lines 474-481)
    ↓
11. Setup Graceful Shutdown (lines 484-491)
    ↓
12. Run Server (line 494)
```

## CLI Arguments (main.rs:270-336)

### CliArgs Structure

```rust
struct CliArgs {
    config_path: Option<PathBuf>,   // -c, --config
    data_dir: Option<PathBuf>,      // -d, --data-dir
    host: Option<String>,           // -H, --host
    port: Option<u16>,              // -p, --port
    generate_config: bool,          // --generate-config
}
```

### Argument Priority

```
CLI Arguments > Config File > Defaults
```

## Component Wiring

```
┌─────────────────────────────────────────────────────────────┐
│                       main.rs                                │
│              Initialization & Orchestration                  │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
    ┌───────┐   ┌─────────┐   ┌────────┐
    │ CLI   │   │ Config  │   │ Default│
    │ Args  │   │ File    │   │ Config │
    └───┬───┘   └────┬────┘   └───┬────┘
        │            │            │
        └────────────┼────────────┘
                     │
                     ▼
            ┌────────────────────┐
            │   ServerConfig     │
            └────────┬───────────┘
                     │
    ┌────────────────┼────────────────┐
    │                │                │
    ▼                ▼                ▼
┌─────────────┐  ┌───────────┐  ┌──────────────┐
│ Logging     │  │ Storage   │  │ Indexes      │
│ Subscriber  │  │ Engine    │  │ - SeriesIdx  │
└─────────────┘  └───────────┘  │ - TagIdx     │
                     │          └──────────────┘
                     │                │
                     └────────────────┘
                          │
                          ▼
                    ┌───────────────┐
                    │   AppState    │
                    │  (Arc shared) │
                    └───────┬───────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │   Axum Router    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   HTTP Server    │
                    └──────────────────┘
```

## Configuration Conversion Methods

### to_storage_config() (main.rs:207-239)

Converts string configuration to typed enums:
- WAL durability string → `WalDurability` enum
- Compression string → `CompressionLevel` enum
- Size calculations (MB to bytes)
- Duration calculations (hours to nanoseconds)

### to_auth_config() (main.rs:241-248)

Creates `AuthConfig` from settings with Duration conversion.

### log_level() (main.rs:250-259)

Parses log level string to `tracing::Level`.

### write_default() (main.rs:261-267)

Generates default `rusts.yml` configuration file.

## API Integration

### AppState (from rusts-api)

```rust
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub series_index: Arc<SeriesIndex>,
    pub tag_index: Arc<TagIndex>,
    pub executor: Arc<QueryExecutor>,
}
```

### Routes (from create_router)

| Route | Method | Handler |
|-------|--------|---------|
| /health | GET | Health check |
| /ready | GET | Readiness check |
| /write | POST | Write data (line protocol) |
| /query | POST | Query data (JSON) |
| /stats | GET | Database statistics |

## Graceful Shutdown (main.rs:483-491)

```rust
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    info!("Shutdown signal received");
    storage_clone.shutdown().expect("Failed to shutdown storage");
    std::process::exit(0);
});
```

## Dependencies

### Internal Crates

| Crate | Purpose |
|-------|---------|
| rusts-core | Core types |
| rusts-storage | Storage engine |
| rusts-compression | Compression algorithms |
| rusts-index | Series and tag indexes |
| rusts-query | Query execution |
| rusts-cluster | Clustering (sharding, routing) |
| rusts-aggregation | Continuous aggregates |
| rusts-retention | Retention policies |
| rusts-api | REST API layer |

### External Dependencies

| Dependency | Purpose |
|------------|---------|
| tokio | Async runtime |
| axum | Web framework |
| serde | Serialization |
| serde_yaml | YAML config |
| tracing | Logging |
| tracing-subscriber | Log formatting |
| thiserror | Error handling |
| anyhow | Error context |

## Default Configuration Values

### Server
- Host: `0.0.0.0`
- Port: `8086`
- Max body size: `10MB`
- Request timeout: `30s`

### Storage
- Data dir: `./data`
- WAL durability: `periodic` (100ms)
- WAL retention: `7 days`
- MemTable max size: `64MB`
- MemTable max points: `1,000,000`
- MemTable max age: `60s`
- Partition duration: `24 hours`
- Compression: `default`

### Auth
- Enabled: `false`
- Token expiration: `1 hour`

### Logging
- Level: `info`
- Show target: `true`

## Example Configuration (rusts.yml)

```yaml
server:
  host: 0.0.0.0
  port: 8086
  max_body_size: 10485760
  request_timeout_secs: 30

storage:
  data_dir: ./data
  wal_durability: periodic
  wal_sync_interval_ms: 100
  wal_retention_secs: 604800
  memtable:
    max_size_mb: 64
    max_points: 1000000
    max_age_secs: 60
  partition_duration_hours: 24
  compression: default

auth:
  enabled: false
  jwt_secret: change-me-in-production
  token_expiration_secs: 3600

logging:
  level: info
  show_target: true
  show_thread_ids: false
  show_location: false

retention_policies: []
```
