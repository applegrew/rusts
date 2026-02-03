# RusTs PgWire - Architecture

## Overview

The `rusts-pgwire` crate implements PostgreSQL wire protocol support for the RusTs time series database using the [pgwire](https://github.com/sunng87/pgwire) crate. It enables connections from psql, DataGrip, SQLAlchemy, and other PostgreSQL clients.

## Module Structure

```
src/
├── lib.rs        # Public exports, run_postgres_server()
├── backend.rs    # PgWireBackend (Simple + Extended QueryHandler impl)
├── encoder.rs    # QueryResult → DataRow encoding
├── types.rs      # FieldValue → PostgreSQL Type mapping
└── error.rs      # Error types and SQLSTATE code mapping
```

## Server Entry Point (lib.rs)

### run_postgres_server (lib.rs:64-115)

```rust
pub async fn run_postgres_server(
    app_state: Arc<AppState>,
    query_timeout: Duration,
    host: &str,
    port: u16,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
```

**Responsibilities:**
- Bind TCP listener on specified host:port
- Accept incoming connections
- Spawn per-connection handler tasks
- Coordinate graceful shutdown via CancellationToken

### Connection Handling

```
Client → TcpListener::accept() → tokio::spawn
→ process_socket(socket, None, factory)
→ PgWireHandlerFactory creates handlers
→ PgWireBackend handles queries
```

## Backend (backend.rs)

### PgWireBackend (backend.rs:56-71)

```rust
pub struct PgWireBackend {
    app_state: Arc<AppState>,
    query_timeout: Duration,
    query_parser: Arc<RustsQueryParser>,
}
```

**Implements:**
- `NoopStartupHandler` - No-op authentication (accepts all connections)
- `SimpleQueryHandler` - Simple query protocol (text queries)
- `ExtendedQueryHandler` - Extended query protocol (Parse/Bind/Execute)

### SimpleQueryHandler Implementation (backend.rs:250-271)

```rust
#[async_trait]
impl SimpleQueryHandler for PgWireBackend {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &'a str)
        -> PgWireResult<Vec<Response<'a>>>
    {
        // 1. Parse SQL via SqlParser
        // 2. Translate to SqlCommand
        // 3. Execute command
        // 4. Encode result to PostgreSQL format
    }
}
```

### ExtendedQueryHandler Implementation (backend.rs:274-365)

The extended query protocol supports Parse/Bind/Execute flow used by JDBC drivers, DataGrip, DBeaver, and other clients.

```rust
#[async_trait]
impl ExtendedQueryHandler for PgWireBackend {
    type Statement = ParsedStatement;
    type QueryParser = RustsQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser>;
    async fn do_query(&self, client, portal, max_rows) -> PgWireResult<Response>;
    async fn do_describe_statement(&self, client, statement) -> PgWireResult<DescribeStatementResponse>;
    async fn do_describe_portal(&self, client, portal) -> PgWireResult<DescribePortalResponse>;
}
```

**Key Components:**
- `ParsedStatement` - Stores parsed SQL and translated `SqlCommand`
- `RustsQueryParser` - Implements `QueryParser` trait for SQL parsing
- `do_query` - Executes query from Portal (bound statement)
- `do_describe_statement` - Returns parameter types and result schema
- `do_describe_portal` - Returns result schema for bound portal

**Supported pg_catalog/information_schema queries:**
- Parameterized queries work for `pg_catalog.*` and `information_schema.*` tables
- Used by JDBC drivers during connection setup

### Command Execution (backend.rs:85-186)

| Command | Handling |
|---------|----------|
| `ShowTables` | Returns measurements as table list |
| `Explain` | Returns error (not yet supported via pgwire) |
| `Query` | Executes via QueryExecutor with timeout |

### Query Execution Flow

```
SQL Query → SqlParser::parse()
→ SqlTranslator::translate_command()
→ QueryExecutor::execute_with_cancellation()
→ query_result_to_response()
→ Response::Query
```

### PgWireHandlerFactory (backend.rs:368-407)

```rust
impl PgWireServerHandlers for PgWireHandlerFactory {
    type StartupHandler = PgWireBackend;
    type SimpleQueryHandler = PgWireBackend;
    type ExtendedQueryHandler = PgWireBackend;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;
}
```

**Handler Types:**
- `StartupHandler` - Connection startup/authentication
- `SimpleQueryHandler` - Simple query protocol (text queries)
- `ExtendedQueryHandler` - Extended query protocol (Parse/Bind/Execute, no bind parameters)
- `CopyHandler` - No-op (COPY not supported)
- `ErrorHandler` - No-op error handling

## Encoder (encoder.rs)

### Schema Building (encoder.rs:15-80)

```rust
pub fn build_result_schema(result: &QueryResult) -> Arc<Vec<FieldInfo>>
```

**Schema Column Order:**
1. `time` (TIMESTAMPTZ) - if any row has timestamp
2. Tags (TEXT) - sorted alphabetically by key
3. Fields (typed) - sorted alphabetically by key

### RowSchema (encoder.rs:82-118)

```rust
pub struct RowSchema {
    pub has_time: bool,
    pub tag_keys: Vec<String>,
    pub field_keys: Vec<String>,
}
```

Cached metadata for efficient row encoding.

### Row Encoding (encoder.rs:120-167)

```rust
pub fn encode_row(
    row: &ResultRow,
    schema: &Arc<Vec<FieldInfo>>,
    row_schema: &RowSchema,
) -> PgWireResult<pgwire::messages::data::DataRow>
```

**Encoding Order:**
1. Timestamp (if present in schema)
2. Tags in sorted order (NULL for missing)
3. Fields in sorted order (NULL for missing)

### Response Building (encoder.rs:169-188)

```rust
pub fn query_result_to_response(result: QueryResult) -> PgWireResult<Response<'static>>
```

Creates `QueryResponse` with:
- Schema (column metadata)
- Data rows (encoded values)
- Command tag (`SELECT {row_count}`)

### Tables Response (encoder.rs:190-219)

```rust
pub fn tables_to_response(tables: Vec<String>) -> PgWireResult<Response<'static>>
```

Single-column TEXT schema for `SHOW TABLES` results.

## Type Mapping (types.rs)

### FieldValue to PostgreSQL Type (types.rs:14-22)

| RusTs FieldValue | PostgreSQL Type | Notes |
|------------------|-----------------|-------|
| `Float(f64)` | FLOAT8 | Double precision |
| `Integer(i64)` | INT8 | 64-bit signed |
| `UnsignedInteger(u64)` | INT8 | Cast to i64, clamp at MAX |
| `String(String)` | TEXT | Variable length |
| `Boolean(bool)` | BOOL | "t" / "f" text format |

### Timestamp Type (types.rs:24-27)

```rust
pub fn timestamp_pg_type() -> Type {
    Type::TIMESTAMPTZ
}
```

### Field Value Formatting (types.rs:42-75)

| Value Type | Text Format |
|------------|-------------|
| Float (normal) | `v.to_string()` |
| Float (NaN) | `"NaN"` |
| Float (+Inf) | `"Infinity"` |
| Float (-Inf) | `"-Infinity"` |
| Integer | `v.to_string()` |
| UnsignedInteger | Cast to i64, clamp if > i64::MAX |
| String | Clone as-is |
| Boolean | `"t"` or `"f"` |

### Timestamp Formatting (types.rs:77-87)

```rust
pub fn timestamp_to_string(nanos: i64) -> String
```

Converts nanoseconds since Unix epoch to PostgreSQL format:
`YYYY-MM-DD HH:MM:SS.microseconds+00`

### chrono_lite Module (types.rs:89-132)

Minimal timestamp formatting without chrono dependency:
- `timestamp_to_datetime(secs, subsec_nanos)` - Format Unix timestamp
- `days_to_ymd(days)` - Convert days to year/month/day (Howard Hinnant algorithm)

## Error Handling (error.rs)

### PgError Enum (error.rs:25-41)

```rust
pub enum PgError {
    SqlParse(SqlError),           // SQL parsing failed
    Query(QueryError),            // Query execution failed
    Timeout,                      // Query timeout exceeded
    ServiceUnavailable(String),   // Server not ready
    Internal(String),             // Internal error
}
```

### SQLSTATE Code Mapping (error.rs:43-67)

| Error Type | SQLSTATE | Code Name |
|------------|----------|-----------|
| `SqlError::Parse` | 42601 | SYNTAX_ERROR |
| `SqlError::Translation` | 42601 | SYNTAX_ERROR |
| `SqlError::InvalidTimeExpression` | 22023 | INVALID_PARAMETER_VALUE |
| `SqlError::UnsupportedFeature` | 0A000 | FEATURE_NOT_SUPPORTED |
| `SqlError::MissingClause` | 42601 | SYNTAX_ERROR |
| `SqlError::UnknownFunction` | 42P01 | UNDEFINED_TABLE |
| `SqlError::InvalidAggregation` | 22023 | INVALID_PARAMETER_VALUE |
| `SqlError::InvalidInterval` | 22023 | INVALID_PARAMETER_VALUE |
| `QueryError::MeasurementNotFound` | 42P01 | UNDEFINED_TABLE |
| `QueryError::Cancelled` | 57014 | QUERY_CANCELED |
| `PgError::Timeout` | 57014 | QUERY_CANCELED |
| Other | XX000 | INTERNAL_ERROR |

### Error Conversion (error.rs:69-85)

```rust
impl From<PgError> for PgWireError {
    fn from(err: PgError) -> Self {
        let error_info = ErrorInfo::new(
            "ERROR".to_string(),
            err.sqlstate().to_string(),
            err.to_string(),
        );
        PgWireError::UserError(Box::new(error_info))
    }
}
```

## Configuration

### PostgresSettings (rusts-server/src/main.rs)

```rust
pub struct PostgresSettings {
    pub enabled: bool,           // Enable PostgreSQL protocol
    pub host: String,            // Bind address (default: 0.0.0.0)
    pub port: u16,               // Port (default: 5432)
    pub max_connections: u32,    // Max concurrent connections
}
```

### rusts.yml Configuration

```yaml
postgres:
  enabled: true
  host: 0.0.0.0
  port: 5432
  max_connections: 100
```

## Request/Response Flow

### Simple Query Protocol

```
Client → TCP Connection
→ Startup handshake (NoopStartupHandler)
→ SimpleQuery message with SQL
→ PgWireBackend::do_query()
→ Parse → Translate → Execute → Encode
→ RowDescription + DataRow* + CommandComplete
→ ReadyForQuery
```

### Extended Query Protocol

```
Client → TCP Connection
→ Startup handshake (NoopStartupHandler)
→ Parse message (SQL + statement name)
   → RustsQueryParser::parse_sql() → ParsedStatement
   → ParseComplete
→ Bind message (statement + portal + parameters)
   → Portal created with bound parameters
   → BindComplete
→ Describe message (portal)
   → PgWireBackend::do_describe_portal()
   → RowDescription
→ Execute message (portal + max_rows)
   → PgWireBackend::do_query(portal)
   → DataRow* + CommandComplete
→ Sync
   → ReadyForQuery
```

**Note:** While the extended query protocol is fully implemented, bind parameter substitution (`$1`, `$2`) for user queries is not yet supported. Parameterized queries work for `pg_catalog` and `information_schema` introspection queries used by JDBC clients.

### Query with Timeout

```
SQL Query received
→ Acquire query semaphore permit
→ tokio::time::timeout(query_timeout, ...)
→ spawn_blocking(executor.execute_with_cancellation)
→ On timeout: cancel.cancel() → QueryError::Cancelled
→ Encode result or error
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Core types (FieldValue, Tag) |
| `rusts-query` | Query execution (QueryExecutor, QueryResult) |
| `rusts-sql` | SQL parsing and translation |
| `rusts-api` | AppState access |
| `pgwire` | PostgreSQL wire protocol |
| `async-trait` | Async trait implementations |
| `tokio` | Async runtime, TCP listener |
| `tokio-util` | CancellationToken |
| `futures` | Stream utilities |
| `thiserror` | Error derive macro |
| `tracing` | Logging |

## Limitations

- **No Parameterized Queries**: Extended query protocol is supported, but bind variables (`$1`, `$2`) are not
- **No Authentication**: All connections accepted (NoopStartupHandler)
- **No TLS**: Plain TCP connections only
- **No GSSAPI**: PostgreSQL 12+ clients must set `PGGSSENCMODE=disable`
- **No Transactions**: Each query is auto-committed
- **No COPY**: COPY protocol not implemented
- **EXPLAIN**: Not yet supported via PostgreSQL protocol

## Usage

### psql

PostgreSQL 12+ clients attempt GSSAPI encryption by default, which RusTs doesn't support. Disable it with `PGGSSENCMODE=disable`:

```bash
# Single command
PGGSSENCMODE=disable psql -h localhost -p 5432 -c "SHOW TABLES"
PGGSSENCMODE=disable psql -h localhost -p 5432 -c "SELECT * FROM cpu LIMIT 10"
PGGSSENCMODE=disable psql -h localhost -p 5432 -c "SELECT COUNT(*) FROM trips GROUP BY hvfhs_license_num"

# Or set for session
export PGGSSENCMODE=disable
psql -h localhost -p 5432
```

### Python/SQLAlchemy

```python
from sqlalchemy import create_engine, text

# Include gssencmode=disable in connection string for PostgreSQL 12+ clients
engine = create_engine('postgresql://localhost:5432/rusts?gssencmode=disable')
with engine.connect() as conn:
    result = conn.execute(text('SELECT * FROM trips LIMIT 5'))
    for row in result:
        print(row)
```

### DataGrip / DBeaver

1. New Data Source → PostgreSQL
2. Host: localhost, Port: 5432
3. Database: rusts (or any name)
4. Test Connection → Should succeed
5. Run queries in SQL console

## Performance

PostgreSQL wire protocol provides ~5-15x faster query performance compared to HTTP REST due to:
- Binary protocol efficiency
- Persistent connections (no HTTP overhead)
- Connection pooling support (client-side)
- Reduced serialization overhead

## Thread Safety

- `PgWireBackend` is `Send + Sync` via `Arc<AppState>`
- `PgWireHandlerFactory` clones `Arc<PgWireBackend>` for each connection
- Query semaphore limits concurrent query execution
- Each connection handled in separate tokio task
