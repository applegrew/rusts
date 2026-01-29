# RusTs API - Architecture

## Overview

The `rusts-api` crate implements the REST API layer for the RusTs time series database. It provides InfluxDB line protocol support, JSON query API, authentication, and rate limiting.

## Module Structure

```
src/
├── lib.rs              # Public exports
├── router.rs           # Axum HTTP router setup
├── handlers.rs         # HTTP request handlers
├── line_protocol.rs    # InfluxDB line protocol parser
├── auth.rs             # JWT authentication and rate limiting
└── error.rs            # Error types and HTTP responses
```

## Router (router.rs)

### Route Configuration (router.rs:13-34)

```rust
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/write", post(handlers::write))
        .route("/query", post(handlers::query))
        .route("/stats", get(handlers::stats))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::new().allow_origin(Any))
        .with_state(state)
}
```

### Middleware Stack

- `TraceLayer` for HTTP request/response tracing
- `CorsLayer` with permissive settings for cross-origin requests

## Handlers (handlers.rs)

### AppState (handlers.rs:19-44)

```rust
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub series_index: Arc<SeriesIndex>,
    pub tag_index: Arc<TagIndex>,
    pub executor: Arc<QueryExecutor>,
}
```

### Handler Functions

| Handler | Lines | Route | Purpose |
|---------|-------|-------|---------|
| `health` | 55-60 | GET /health | Health check, returns version |
| `ready` | 71-80 | GET /ready | Readiness check |
| `write` | 91-121 | POST /write | Write data via line protocol |
| `query` | 167-260 | POST /query | Query data with filters/aggregation |
| `stats` | 332-351 | GET /stats | Database statistics |

### Query Request (handlers.rs:124-135)

```rust
pub struct QueryRequest {
    pub measurement: String,
    pub time_range: Option<TimeRangeRequest>,
    pub tags: Option<HashMap<String, String>>,
    pub fields: Option<Vec<String>>,
    pub aggregate: Option<AggregateRequest>,
    pub group_by: Option<Vec<String>>,
    pub group_by_time: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
```

### Write Response (handlers.rs:83-88)

```rust
pub struct WriteResponse {
    pub success: bool,
    pub points_written: usize,
    pub errors: Vec<String>,
}
```

### Duration Parsing (handlers.rs:263-305)

Supports: `ns`, `us`, `ms`, `s`, `m`, `h`, `d`, `w` units.

## Line Protocol Parser (line_protocol.rs)

### Parser Methods

| Method | Lines | Purpose |
|--------|-------|---------|
| `parse_line` | 15-61 | Parse single line protocol line |
| `parse_lines` | 64-70 | Parse multiple lines, return results |
| `parse_lines_ok` | 73-90 | Parse lines, separate valid/invalid |

### Line Protocol Format

```
<measurement>,<tag_key>=<tag_value>,... <field_key>=<field_value>,... [timestamp]
```

### Field Type Detection (lines 243-283)

| Pattern | Type |
|---------|------|
| `true`, `t`, `T`, `TRUE`, `false`, `f`, `F`, `FALSE` | Boolean |
| `"quoted string"` | String |
| `123i` | Integer |
| `123u` | Unsigned Integer |
| numeric | Float (default) |

### Helper Functions

| Function | Lines | Purpose |
|----------|-------|---------|
| `split_line` | 92-123 | Split line respecting quoted strings |
| `parse_measurement_tags` | 125-144 | Parse measurement and tag pairs |
| `parse_fields` | 176-209 | Parse field key=value pairs |
| `parse_field_value` | 243-283 | Parse with type detection |
| `unescape` | 285-315 | Unescape special characters |

## Authentication (auth.rs)

### JWT Claims (auth.rs:17-27)

```rust
pub struct Claims {
    pub sub: String,
    pub exp: u64,
    pub iat: u64,
    pub permissions: Vec<String>,
}
```

### API Key (auth.rs:30-37)

```rust
pub struct ApiKey {
    pub id: String,
    pub key_hash: String,
    pub permissions: Vec<String>,
    pub created_at: u64,
    pub last_used: u64,
}
```

### AuthConfig (auth.rs:40-58)

```rust
pub struct AuthConfig {
    pub enabled: bool,
    pub jwt_secret: String,
    pub token_expiration: Duration,
}
```

### AuthState Methods (auth.rs:68-148)

| Method | Lines | Purpose |
|--------|-------|---------|
| `new` | 69-74 | Create new auth state |
| `is_enabled` | 77-79 | Check if auth enabled |
| `generate_token` | 82-101 | Generate JWT token |
| `validate_token` | 104-112 | Validate JWT token |
| `register_api_key` | 115-131 | Register API key |
| `validate_api_key` | 134-142 | Validate API key |
| `has_permission` | 145-147 | Check permission (supports "*" wildcard) |

### Middleware (auth.rs:167-187)

```rust
pub async fn auth_middleware(
    req: Request,
    next: Next,
) -> std::result::Result<Response, StatusCode>
```

Extracts authorization header, validates token if auth enabled.

## Rate Limiter (auth.rs:190-258)

```rust
pub struct RateLimiter {
    requests_per_window: u32,
    window_duration: Duration,
    clients: DashMap<String, (u32, u64)>,
}
```

**Methods:**
- `new(requests_per_window, window_duration)` - Create limiter
- `check(client_id)` - Check if request allowed (returns bool)
- `remaining(client_id)` - Get remaining requests in window

**Algorithm:** Per-client sliding window with automatic reset on window expiration.

## Error Handling (error.rs)

### ApiError (error.rs:12-40)

```rust
pub enum ApiError {
    BadRequest(String),      // 400
    Unauthorized(String),    // 401
    Forbidden(String),       // 403
    NotFound(String),        // 404
    RateLimited,             // 429
    Internal(String),        // 500
    Storage(String),         // 500
    Query(String),           // 400
    Parse(String),           // 400
}
```

### ErrorResponse (error.rs:46-50)

```rust
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}
```

## Request/Response Flow

### Write Path

```
Client → Router (/write) → write() handler
→ LineProtocolParser::parse_lines_ok()
→ Index points (series_index, tag_index)
→ storage.write_batch()
→ WriteResponse
```

### Query Path

```
Client → Router (/query) → query() handler
→ Query::builder()
→ executor.execute()
→ Convert FieldValue to JSON
→ QueryResponse
```

### Auth Flow (if enabled)

```
Client request with Authorization header
→ extract_auth()
→ auth_middleware validates token
→ Claims extracted
→ Handler executes
```

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `rusts-core` | Core types (Point, Series, Field, Tag) |
| `rusts-storage` | Storage engine |
| `rusts-query` | Query builder and executor |
| `rusts-index` | Series and tag indexes |
| `axum` | Web framework |
| `tower-http` | HTTP middleware (CORS, tracing) |
| `jsonwebtoken` | JWT encoding/decoding |
| `dashmap` | Concurrent rate limiter storage |
| `serde` | JSON serialization |
| `thiserror` | Error types |
| `tracing` | Request tracing |

## Stateless Design

- Parser is stateless struct
- All state in `AppState`
- Thread-safe via `Arc<DashMap>` for concurrent access

## Error Recovery

- `parse_lines_ok()` separates successful parses from errors
- Write endpoint accepts partial success (logs errors in response)
