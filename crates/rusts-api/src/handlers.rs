//! HTTP request handlers

use crate::error::ApiError;
use crate::line_protocol::LineProtocolParser;
use axum::{
    body::Bytes,
    extract::State,
    http::header::CONTENT_TYPE,
    http::HeaderMap,
    response::Json,
};
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use parking_lot::RwLock;
use rusts_core::{ParallelConfig, TimeRange};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_query::{AggregateFunction, Query, QueryExecutor, QueryPlanner};
use rusts_sql::{SqlCommand, SqlParser, SqlTranslator};
use rusts_storage::StorageEngine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Server startup phase
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupPhase {
    /// Server is initializing
    Initializing,
    /// WAL recovery in progress
    WalRecovery,
    /// Index rebuilding from partitions
    IndexRebuilding,
    /// Server is ready to serve requests
    Ready,
}

impl std::fmt::Display for StartupPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupPhase::Initializing => write!(f, "initializing"),
            StartupPhase::WalRecovery => write!(f, "wal_recovery"),
            StartupPhase::IndexRebuilding => write!(f, "index_rebuilding"),
            StartupPhase::Ready => write!(f, "ready"),
        }
    }
}

/// Startup state tracking
pub struct StartupState {
    phase: RwLock<StartupPhase>,
    start_time: Instant,
}

impl StartupState {
    pub fn new() -> Self {
        Self {
            phase: RwLock::new(StartupPhase::Initializing),
            start_time: Instant::now(),
        }
    }

    pub fn phase(&self) -> StartupPhase {
        self.phase.read().clone()
    }

    pub fn set_phase(&self, phase: StartupPhase) {
        *self.phase.write() = phase;
    }

    pub fn is_ready(&self) -> bool {
        *self.phase.read() == StartupPhase::Ready
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

impl Default for StartupState {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics handles for recording telemetry from API handlers.
///
/// All fields are OTel instrument handles which are internally `Arc`-ed and
/// lock-free, so cloning and recording is extremely cheap (a few atomics).
#[derive(Clone)]
pub struct ServerMetrics {
    pub points_written: Counter<u64>,
    pub write_latency_ms: Histogram<f64>,
    pub queries_executed: Counter<u64>,
    pub query_latency_ms: Histogram<f64>,
    pub active_queries: Gauge<i64>,
    pub memtable_flushes: Counter<u64>,
    pub wal_syncs: Counter<u64>,
    pub wal_bytes_written: Counter<u64>,
    pub process_cpu_usage: Gauge<f64>,
    pub process_memory_rss: Gauge<i64>,
}

/// Application state shared across handlers
pub struct AppState {
    pub storage: RwLock<Option<Arc<StorageEngine>>>,
    pub series_index: Arc<SeriesIndex>,
    pub tag_index: Arc<TagIndex>,
    pub executor: RwLock<Option<Arc<QueryExecutor>>>,
    pub query_semaphore: Arc<Semaphore>,
    pub query_timeout: Duration,
    pub startup_state: Arc<StartupState>,
    pub parallel_config: ParallelConfig,
    /// Optional telemetry metrics (None when telemetry is disabled).
    pub metrics: Option<ServerMetrics>,
}

impl AppState {
    /// Create a new AppState with storage already initialized (ready state)
    pub fn new(
        storage: Arc<StorageEngine>,
        series_index: Arc<SeriesIndex>,
        tag_index: Arc<TagIndex>,
        query_timeout: Duration,
        max_concurrent_queries: usize,
        parallel_config: ParallelConfig,
    ) -> Self {
        let startup_state = Arc::new(StartupState::new());
        startup_state.set_phase(StartupPhase::Ready);

        let executor = Arc::new(QueryExecutor::with_parallel_config(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
            parallel_config.clone(),
        ));

        Self {
            storage: RwLock::new(Some(storage)),
            series_index,
            tag_index,
            executor: RwLock::new(Some(executor)),
            query_semaphore: Arc::new(Semaphore::new(max_concurrent_queries)),
            query_timeout,
            startup_state,
            parallel_config,
            metrics: None,
        }
    }

    /// Create a new AppState in initializing mode (no storage yet)
    pub fn new_initializing(
        query_timeout: Duration,
        max_concurrent_queries: usize,
        startup_state: Arc<StartupState>,
        parallel_config: ParallelConfig,
    ) -> Self {
        Self {
            storage: RwLock::new(None),
            series_index: Arc::new(SeriesIndex::new()),
            tag_index: Arc::new(TagIndex::new()),
            executor: RwLock::new(None),
            query_semaphore: Arc::new(Semaphore::new(max_concurrent_queries)),
            query_timeout,
            startup_state,
            parallel_config,
            metrics: None,
        }
    }

    /// Set telemetry metrics handles.
    pub fn set_metrics(&mut self, metrics: ServerMetrics) {
        self.metrics = Some(metrics);
    }

    /// Set the storage engine once it's initialized
    pub fn set_storage(&self, storage: Arc<StorageEngine>) {
        let executor = Arc::new(QueryExecutor::with_parallel_config(
            Arc::clone(&storage),
            Arc::clone(&self.series_index),
            Arc::clone(&self.tag_index),
            self.parallel_config.clone(),
        ));
        *self.storage.write() = Some(storage);
        *self.executor.write() = Some(executor);
    }

    /// Check if storage is ready
    pub fn is_storage_ready(&self) -> bool {
        self.storage.read().is_some()
    }

    /// Get storage if ready
    pub fn get_storage(&self) -> Option<Arc<StorageEngine>> {
        self.storage.read().clone()
    }

    /// Get executor if ready
    pub fn get_executor(&self) -> Option<Arc<QueryExecutor>> {
        self.executor.read().clone()
    }
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

/// Health check handler - always responds, even during startup
pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let phase = state.startup_state.phase();
    let is_ready = phase == StartupPhase::Ready;

    Json(HealthResponse {
        status: if is_ready { "healthy".to_string() } else { "unhealthy".to_string() },
        version: env!("CARGO_PKG_VERSION").to_string(),
        phase: if is_ready { None } else { Some(phase.to_string()) },
    })
}

/// Ready check response
#[derive(Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    pub uptime_seconds: u64,
}

/// Ready check handler - always responds, returns ready=false during startup
pub async fn ready(State(state): State<Arc<AppState>>) -> Json<ReadyResponse> {
    let phase = state.startup_state.phase();
    let is_ready = phase == StartupPhase::Ready;

    Json(ReadyResponse {
        ready: is_ready,
        phase: if is_ready { None } else { Some(phase.to_string()) },
        uptime_seconds: state.startup_state.uptime_seconds(),
    })
}

/// Write response
#[derive(Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub points_written: usize,
    pub errors: Vec<String>,
}

/// Write data using line protocol
pub async fn write(
    State(state): State<Arc<AppState>>,
    body: String,
) -> std::result::Result<Json<WriteResponse>, ApiError> {
    let start = Instant::now();

    // Check if storage is ready
    let storage = state.get_storage().ok_or_else(|| {
        let phase = state.startup_state.phase();
        ApiError::ServiceUnavailable(format!("Server is starting up ({})", phase))
    })?;

    let (points, parse_errors) = LineProtocolParser::parse_lines_ok(&body);

    if points.is_empty() && !parse_errors.is_empty() {
        return Err(ApiError::Parse(parse_errors.join("; ")));
    }

    // Index the points
    for point in &points {
        let series_id = point.series_id();
        state.series_index.upsert(
            series_id,
            &point.measurement,
            &point.tags,
            point.timestamp,
        );
        state.tag_index.index_series(series_id, &point.tags);
    }

    let count = points.len();

    // Write to storage
    storage.write_batch(&points)?;

    // Record metrics (no-op when telemetry is disabled)
    if let Some(m) = &state.metrics {
        m.points_written.add(count as u64, &[]);
        m.write_latency_ms.record(start.elapsed().as_secs_f64() * 1000.0, &[]);
    }

    Ok(Json(WriteResponse {
        success: true,
        points_written: count,
        errors: parse_errors,
    }))
}

/// Query request body
#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct TimeRangeRequest {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Deserialize)]
pub struct AggregateRequest {
    pub field: String,
    pub function: String,
    pub alias: Option<String>,
}

/// Query response
#[derive(Serialize)]
pub struct QueryResponse {
    pub measurement: String,
    pub results: Vec<ResultRowResponse>,
    pub total_rows: usize,
    pub execution_time_ms: f64,
}

#[derive(Serialize)]
pub struct ResultRowResponse {
    pub time: Option<i64>,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, serde_json::Value>,
}

/// Query data
pub async fn query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> std::result::Result<Json<QueryResponse>, ApiError> {
    let start = Instant::now();

    // Check if storage/executor is ready
    let executor = state.get_executor().ok_or_else(|| {
        let phase = state.startup_state.phase();
        ApiError::ServiceUnavailable(format!("Server is starting up ({})", phase))
    })?;

    // Acquire semaphore permit (Layer 3: concurrent query limit)
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| ApiError::Internal("Query semaphore closed".to_string()))?;

    // Track active queries
    if let Some(m) = &state.metrics {
        m.active_queries.record(1, &[]);
    }

    // Build query
    let time_range = req.time_range.map(|tr| TimeRange::new(tr.start, tr.end))
        .unwrap_or_default();

    let mut builder = Query::builder(&req.measurement)
        .time_range(time_range.start, time_range.end);

    // Add tag filters
    if let Some(tags) = &req.tags {
        for (key, value) in tags {
            builder = builder.where_tag(key, value);
        }
    }

    // Add field selection or aggregation
    if let Some(agg) = &req.aggregate {
        let function = AggregateFunction::from_str(&agg.function)
            .map_err(|e| ApiError::Query(e.to_string()))?;
        builder = builder.select_aggregate(&agg.field, function, agg.alias.clone());
    } else if let Some(fields) = &req.fields {
        builder = builder.select_fields(fields.clone());
    }

    // Add grouping
    if let Some(group_by) = &req.group_by {
        builder = builder.group_by_tags(group_by.clone());
    }

    if let Some(interval_str) = &req.group_by_time {
        let interval = parse_duration(interval_str)
            .map_err(|e| ApiError::Query(e))?;
        builder = builder.group_by_interval(interval);
    }

    // Add limit/offset
    if let Some(limit) = req.limit {
        builder = builder.limit(limit);
    }
    if let Some(offset) = req.offset {
        builder = builder.offset(offset);
    }

    let query = builder.build().map_err(|e| ApiError::Query(e.to_string()))?;

    // Clone for spawn_blocking
    let timeout = state.query_timeout;
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Execute with timeout and spawn_blocking (Layer 2 + 4)
    let query_result = tokio::time::timeout(timeout, async {
        tokio::task::spawn_blocking(move || {
            executor.execute_with_cancellation(query, cancel_clone)
        }).await
    }).await;

    // Handle results
    let result = match query_result {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(query_err))) => {
            // Query error (including cancellation)
            return Err(ApiError::Query(query_err.to_string()));
        }
        Ok(Err(join_err)) => {
            // spawn_blocking panicked
            return Err(ApiError::Internal(format!("Query task failed: {}", join_err)));
        }
        Err(_timeout) => {
            // Timeout - cancel the query
            cancel.cancel();
            return Err(ApiError::Query("Query timeout exceeded".to_string()));
        }
    };

    // Convert to response
    let results: Vec<ResultRowResponse> = result
        .rows
        .iter()
        .map(|row| {
            let tags: HashMap<String, String> = row
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect();

            let fields: HashMap<String, serde_json::Value> = row
                .fields
                .iter()
                .map(|(k, v)| {
                    let json_value = match v {
                        rusts_core::FieldValue::Float(f) => serde_json::json!(f),
                        rusts_core::FieldValue::Integer(i) => serde_json::json!(i),
                        rusts_core::FieldValue::UnsignedInteger(u) => serde_json::json!(u),
                        rusts_core::FieldValue::String(s) => serde_json::json!(s),
                        rusts_core::FieldValue::Boolean(b) => serde_json::json!(b),
                    };
                    (k.clone(), json_value)
                })
                .collect();

            ResultRowResponse {
                time: row.timestamp,
                tags,
                fields,
            }
        })
        .collect();

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    // Record metrics
    if let Some(m) = &state.metrics {
        m.queries_executed.add(1, &[]);
        m.query_latency_ms.record(elapsed_ms, &[]);
        m.active_queries.record(-1, &[]);
    }

    Ok(Json(QueryResponse {
        measurement: result.measurement,
        results,
        total_rows: result.total_rows,
        execution_time_ms: elapsed_ms,
    }))
}

/// Parse duration string (e.g., "1m", "5s", "1h")
fn parse_duration(s: &str) -> std::result::Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration".to_string());
    }

    let (num_str, unit) = if s.ends_with("ns") {
        (&s[..s.len() - 2], "ns")
    } else if s.ends_with("us") || s.ends_with("µs") {
        (&s[..s.len() - 2], "us")
    } else if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else if s.ends_with('d') {
        (&s[..s.len() - 1], "d")
    } else if s.ends_with('w') {
        (&s[..s.len() - 1], "w")
    } else {
        return Err(format!("Invalid duration unit in: {}", s));
    };

    let num: i64 = num_str.parse()
        .map_err(|_| format!("Invalid number in duration: {}", s))?;

    let nanos = match unit {
        "ns" => num,
        "us" => num * 1_000,
        "ms" => num * 1_000_000,
        "s" => num * 1_000_000_000,
        "m" => num * 60 * 1_000_000_000,
        "h" => num * 3600 * 1_000_000_000,
        "d" => num * 86400 * 1_000_000_000,
        "w" => num * 604800 * 1_000_000_000,
        _ => return Err(format!("Unknown unit: {}", unit)),
    };

    Ok(nanos)
}

/// Stats response
#[derive(Serialize)]
pub struct StatsResponse {
    pub series_count: usize,
    pub measurement_count: usize,
    pub memtable: MemTableStatsResponse,
    pub partitions: PartitionStatsResponse,
}

#[derive(Serialize)]
pub struct MemTableStatsResponse {
    pub active_size_bytes: usize,
    pub active_points: usize,
    pub active_series: usize,
    pub immutable_count: usize,
}

#[derive(Serialize)]
pub struct PartitionStatsResponse {
    pub partition_count: usize,
    pub total_segments: usize,
    pub total_points: usize,
}

/// Get database statistics
pub async fn stats(State(state): State<Arc<AppState>>) -> std::result::Result<Json<StatsResponse>, ApiError> {
    // Check if storage is ready
    let storage = state.get_storage().ok_or_else(|| {
        let phase = state.startup_state.phase();
        ApiError::ServiceUnavailable(format!("Server is starting up ({})", phase))
    })?;

    let memtable = storage.memtable_stats();
    let partitions = storage.partition_stats();

    Ok(Json(StatsResponse {
        series_count: state.series_index.len(),
        measurement_count: state.series_index.measurements().len(),
        memtable: MemTableStatsResponse {
            active_size_bytes: memtable.active_size,
            active_points: memtable.active_points,
            active_series: memtable.active_series,
            immutable_count: memtable.immutable_count,
        },
        partitions: PartitionStatsResponse {
            partition_count: partitions.partition_count,
            total_segments: partitions.total_segments,
            total_points: partitions.total_points,
        },
    }))
}

/// SQL query request body
#[derive(Debug, Deserialize)]
pub struct SqlQueryRequest {
    pub query: String,
}

/// EXPLAIN query response
#[derive(Serialize)]
pub struct ExplainResponse {
    /// Measurement being queried
    pub measurement: String,
    /// Time range of the query
    pub time_range: TimeRangeResponse,
    /// List of optimizations applied
    pub optimizations: Vec<String>,
    /// Filter order with cardinalities
    pub filter_order: Vec<FilterOrderEntry>,
    /// Number of partitions to scan
    pub partitions_to_scan: usize,
    /// Estimated series count
    pub estimated_series: usize,
    /// Estimated point count
    pub estimated_points: u64,
    /// Execution hints
    pub hints: ExecutionHintsResponse,
}

#[derive(Serialize)]
pub struct TimeRangeResponse {
    pub start: i64,
    pub end: i64,
}

#[derive(Serialize)]
pub struct FilterOrderEntry {
    pub filter: String,
    pub cardinality: usize,
}

#[derive(Serialize)]
pub struct ExecutionHintsResponse {
    pub memtable_only: bool,
    pub use_segment_stats: bool,
    pub filters_reordered: bool,
}

/// SQL query response - can be either query results or explain output
#[derive(Serialize)]
#[serde(untagged)]
pub enum SqlQueryResponse {
    /// Normal query results
    QueryResult(QueryResponse),
    /// EXPLAIN output
    Explain(ExplainResponse),
}

/// Parse SQL query from request body based on Content-Type header
fn parse_sql_body(headers: &HeaderMap, body: &Bytes) -> Result<String, ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("application/json") {
        // Parse as JSON
        let req: SqlQueryRequest = serde_json::from_slice(body)
            .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {}", e)))?;
        Ok(req.query)
    } else {
        // Treat as plain text (text/plain or no Content-Type)
        let query = std::str::from_utf8(body)
            .map_err(|e| ApiError::BadRequest(format!("Invalid UTF-8: {}", e)))?
            .trim()
            .to_string();

        if query.is_empty() {
            return Err(ApiError::BadRequest("Empty query".to_string()));
        }
        Ok(query)
    }
}

/// Execute a SQL query
///
/// Accepts either:
/// - `Content-Type: application/json` with body `{"query": "SELECT ..."}`
/// - `Content-Type: text/plain` (or no Content-Type) with body `SELECT ...`
pub async fn sql_query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<SqlQueryResponse>, ApiError> {
    let start = Instant::now();

    // Parse query from body based on Content-Type
    let query_str = parse_sql_body(&headers, &body)?;

    // Parse SQL
    let stmt = SqlParser::parse(&query_str)?;

    // Translate to command
    let command = SqlTranslator::translate_command(&stmt)?;

    match command {
        SqlCommand::ShowTables => {
            // Return list of measurements as tables
            let measurements = state.series_index.measurements();
            let results: Vec<ResultRowResponse> = measurements
                .into_iter()
                .map(|name| {
                    let mut fields = HashMap::new();
                    fields.insert("name".to_string(), serde_json::json!(name));
                    ResultRowResponse {
                        time: None,
                        tags: HashMap::new(),
                        fields,
                    }
                })
                .collect();

            let total_rows = results.len();
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "_tables".to_string(),
                results,
                total_rows,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::Explain(query) => {
            // Generate query plan without executing
            let planner = QueryPlanner::new();

            // Create plan
            let plan = planner.plan(query.clone()).map_err(|e| ApiError::Query(e.to_string()))?;
            let explain = plan.explain();

            // Check if memtable can serve this query
            let memtable_only = if let Some(storage) = state.get_storage() {
                storage.can_serve_from_memtable(&query.time_range)
            } else {
                false
            };

            Ok(Json(SqlQueryResponse::Explain(ExplainResponse {
                measurement: explain.measurement,
                time_range: TimeRangeResponse {
                    start: explain.time_range.start,
                    end: explain.time_range.end,
                },
                optimizations: explain.optimizations,
                filter_order: explain.filter_order.into_iter()
                    .map(|(filter, cardinality)| FilterOrderEntry { filter, cardinality })
                    .collect(),
                partitions_to_scan: explain.partitions_to_scan,
                estimated_series: explain.estimated_series,
                estimated_points: explain.estimated_points,
                hints: ExecutionHintsResponse {
                    memtable_only,
                    use_segment_stats: plan.hints.use_segment_stats,
                    filters_reordered: plan.hints.filters_reordered,
                },
            })))
        }
        SqlCommand::Query(query) => {
            // Check if storage/executor is ready
            let executor = state.get_executor().ok_or_else(|| {
                let phase = state.startup_state.phase();
                ApiError::ServiceUnavailable(format!("Server is starting up ({})", phase))
            })?;

            // Acquire semaphore permit (Layer 3: concurrent query limit)
            let _permit = state.query_semaphore.acquire().await
                .map_err(|_| ApiError::Internal("Query semaphore closed".to_string()))?;

            // Clone for spawn_blocking
            let timeout = state.query_timeout;
            let cancel = CancellationToken::new();
            let cancel_clone = cancel.clone();

            // Execute with timeout and spawn_blocking (Layer 2 + 4)
            let query_result = tokio::time::timeout(timeout, async {
                tokio::task::spawn_blocking(move || {
                    executor.execute_with_cancellation(query, cancel_clone)
                }).await
            }).await;

            // Handle results
            let result = match query_result {
                Ok(Ok(Ok(result))) => result,
                Ok(Ok(Err(query_err))) => {
                    // Query error (including cancellation)
                    return Err(ApiError::Query(query_err.to_string()));
                }
                Ok(Err(join_err)) => {
                    // spawn_blocking panicked
                    return Err(ApiError::Internal(format!("Query task failed: {}", join_err)));
                }
                Err(_timeout) => {
                    // Timeout - cancel the query
                    cancel.cancel();
                    return Err(ApiError::Query("Query timeout exceeded".to_string()));
                }
            };

            // Convert to response (same format as /query endpoint)
            let results: Vec<ResultRowResponse> = result
                .rows
                .iter()
                .map(|row| {
                    let tags: HashMap<String, String> = row
                        .tags
                        .iter()
                        .map(|t| (t.key.clone(), t.value.clone()))
                        .collect();

                    let fields: HashMap<String, serde_json::Value> = row
                        .fields
                        .iter()
                        .map(|(k, v)| {
                            let json_value = match v {
                                rusts_core::FieldValue::Float(f) => serde_json::json!(f),
                                rusts_core::FieldValue::Integer(i) => serde_json::json!(i),
                                rusts_core::FieldValue::UnsignedInteger(u) => serde_json::json!(u),
                                rusts_core::FieldValue::String(s) => serde_json::json!(s),
                                rusts_core::FieldValue::Boolean(b) => serde_json::json!(b),
                            };
                            (k.clone(), json_value)
                        })
                        .collect();

                    ResultRowResponse {
                        time: row.timestamp,
                        tags,
                        fields,
                    }
                })
                .collect();

            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: result.measurement,
                results,
                total_rows: result.total_rows,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::SetVariable(_, _) | SqlCommand::Empty => {
            // SET commands - return empty success response
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "_set".to_string(),
                results: vec![],
                total_rows: 0,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::SystemQuery { column, value } => {
            // System query - return single value
            let mut fields = HashMap::new();
            fields.insert(column, serde_json::json!(value));
            let results = vec![ResultRowResponse {
                time: None,
                tags: HashMap::new(),
                fields,
            }];
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "_system".to_string(),
                results,
                total_rows: 1,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::PgCatalogQuery { table } => {
            // pg_catalog query - return empty or mock response
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: format!("pg_catalog.{}", table),
                results: vec![],
                total_rows: 0,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::InformationSchemaEmpty { table } => {
            // information_schema query for unsupported tables - return empty response
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: format!("information_schema.{}", table),
                results: vec![],
                total_rows: 0,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::InformationSchemaTables => {
            // Return list of measurements as information_schema.tables
            let measurements = state.series_index.measurements();
            let results: Vec<ResultRowResponse> = measurements
                .into_iter()
                .map(|name| {
                    let mut fields = HashMap::new();
                    fields.insert("schema".to_string(), serde_json::json!("public"));
                    fields.insert("name".to_string(), serde_json::json!(name));
                    fields.insert("tabletype".to_string(), serde_json::json!("r"));
                    fields.insert("parenttype".to_string(), serde_json::json!("r"));
                    ResultRowResponse {
                        time: None,
                        tags: HashMap::new(),
                        fields,
                    }
                })
                .collect();

            let total_rows = results.len();
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "information_schema.tables".to_string(),
                results,
                total_rows,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::InformationSchemaViews => {
            // Return empty for views
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "information_schema.views".to_string(),
                results: vec![],
                total_rows: 0,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
        SqlCommand::InformationSchemaColumns => {
            // Return columns for all measurements with actual tag keys
            let measurements = state.series_index.measurements();
            let mut results = Vec::new();

            for measurement in &measurements {
                let mut ordinal = 1;

                // time column
                let mut fields = HashMap::new();
                fields.insert("table_schema".to_string(), serde_json::json!("public"));
                fields.insert("table_name".to_string(), serde_json::json!(measurement));
                fields.insert("column_name".to_string(), serde_json::json!("time"));
                fields.insert("ordinal_position".to_string(), serde_json::json!(ordinal));
                fields.insert("data_type".to_string(), serde_json::json!("timestamp with time zone"));
                fields.insert("is_nullable".to_string(), serde_json::json!("NO"));
                results.push(ResultRowResponse {
                    time: None,
                    tags: HashMap::new(),
                    fields,
                });
                ordinal += 1;

                // Tag columns (actual tag keys)
                let tag_keys = state.series_index.get_tag_keys_for_measurement(measurement);
                for tag_key in tag_keys {
                    let mut fields = HashMap::new();
                    fields.insert("table_schema".to_string(), serde_json::json!("public"));
                    fields.insert("table_name".to_string(), serde_json::json!(measurement));
                    fields.insert("column_name".to_string(), serde_json::json!(tag_key));
                    fields.insert("ordinal_position".to_string(), serde_json::json!(ordinal));
                    fields.insert("data_type".to_string(), serde_json::json!("text"));
                    fields.insert("is_nullable".to_string(), serde_json::json!("YES"));
                    results.push(ResultRowResponse {
                        time: None,
                        tags: HashMap::new(),
                        fields,
                    });
                    ordinal += 1;
                }

                // fields column (dynamic)
                let mut fields = HashMap::new();
                fields.insert("table_schema".to_string(), serde_json::json!("public"));
                fields.insert("table_name".to_string(), serde_json::json!(measurement));
                fields.insert("column_name".to_string(), serde_json::json!("fields"));
                fields.insert("ordinal_position".to_string(), serde_json::json!(ordinal));
                fields.insert("data_type".to_string(), serde_json::json!("jsonb"));
                fields.insert("is_nullable".to_string(), serde_json::json!("YES"));
                results.push(ResultRowResponse {
                    time: None,
                    tags: HashMap::new(),
                    fields,
                });
            }

            let total_rows = results.len();
            Ok(Json(SqlQueryResponse::QueryResult(QueryResponse {
                measurement: "information_schema.columns".to_string(),
                results,
                total_rows,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
    use tempfile::TempDir;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("1s").unwrap(), 1_000_000_000);
        assert_eq!(parse_duration("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_duration("1h").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(parse_duration("100ms").unwrap(), 100_000_000);
        assert_eq!(parse_duration("1d").unwrap(), 86400 * 1_000_000_000);

        assert!(parse_duration("invalid").is_err());
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn test_startup_phase_display() {
        assert_eq!(StartupPhase::Initializing.to_string(), "initializing");
        assert_eq!(StartupPhase::WalRecovery.to_string(), "wal_recovery");
        assert_eq!(StartupPhase::IndexRebuilding.to_string(), "index_rebuilding");
        assert_eq!(StartupPhase::Ready.to_string(), "ready");
    }

    #[test]
    fn test_startup_state_new() {
        let state = StartupState::new();
        assert_eq!(state.phase(), StartupPhase::Initializing);
        assert!(!state.is_ready());
    }

    #[test]
    fn test_startup_state_phase_transitions() {
        let state = StartupState::new();

        // Initially Initializing
        assert_eq!(state.phase(), StartupPhase::Initializing);
        assert!(!state.is_ready());

        // Transition to WalRecovery
        state.set_phase(StartupPhase::WalRecovery);
        assert_eq!(state.phase(), StartupPhase::WalRecovery);
        assert!(!state.is_ready());

        // Transition to IndexRebuilding
        state.set_phase(StartupPhase::IndexRebuilding);
        assert_eq!(state.phase(), StartupPhase::IndexRebuilding);
        assert!(!state.is_ready());

        // Transition to Ready
        state.set_phase(StartupPhase::Ready);
        assert_eq!(state.phase(), StartupPhase::Ready);
        assert!(state.is_ready());
    }

    #[test]
    fn test_startup_state_uptime() {
        let state = StartupState::new();
        // Uptime should be 0 or very small right after creation
        assert!(state.uptime_seconds() < 2);
    }

    #[test]
    fn test_app_state_new_initializing() {
        let startup_state = Arc::new(StartupState::new());
        let app_state = AppState::new_initializing(
            Duration::from_secs(30),
            100,
            Arc::clone(&startup_state),
            ParallelConfig::default(),
        );

        // Storage should not be available
        assert!(!app_state.is_storage_ready());
        assert!(app_state.get_storage().is_none());
        assert!(app_state.get_executor().is_none());

        // Startup state should be Initializing
        assert_eq!(app_state.startup_state.phase(), StartupPhase::Initializing);
    }

    #[test]
    fn test_app_state_new_with_storage() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        let app_state = AppState::new(
            Arc::clone(&storage),
            series_index,
            tag_index,
            Duration::from_secs(30),
            100,
            ParallelConfig::default(),
        );

        // Storage should be available
        assert!(app_state.is_storage_ready());
        assert!(app_state.get_storage().is_some());
        assert!(app_state.get_executor().is_some());

        // Startup state should be Ready
        assert_eq!(app_state.startup_state.phase(), StartupPhase::Ready);
        assert!(app_state.startup_state.is_ready());

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_app_state_set_storage() {
        let startup_state = Arc::new(StartupState::new());
        let app_state = AppState::new_initializing(
            Duration::from_secs(30),
            100,
            Arc::clone(&startup_state),
            ParallelConfig::default(),
        );

        // Initially no storage
        assert!(!app_state.is_storage_ready());
        assert!(app_state.get_storage().is_none());
        assert!(app_state.get_executor().is_none());

        // Create and set storage
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };
        let storage = Arc::new(StorageEngine::new(config).unwrap());

        app_state.set_storage(Arc::clone(&storage));

        // Now storage should be available
        assert!(app_state.is_storage_ready());
        assert!(app_state.get_storage().is_some());
        assert!(app_state.get_executor().is_some());

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_app_state_startup_phase_during_initialization() {
        let startup_state = Arc::new(StartupState::new());
        let app_state = AppState::new_initializing(
            Duration::from_secs(30),
            100,
            Arc::clone(&startup_state),
            ParallelConfig::default(),
        );

        // Simulate startup phases
        startup_state.set_phase(StartupPhase::WalRecovery);
        assert_eq!(app_state.startup_state.phase(), StartupPhase::WalRecovery);

        startup_state.set_phase(StartupPhase::IndexRebuilding);
        assert_eq!(app_state.startup_state.phase(), StartupPhase::IndexRebuilding);

        startup_state.set_phase(StartupPhase::Ready);
        assert_eq!(app_state.startup_state.phase(), StartupPhase::Ready);
        assert!(app_state.startup_state.is_ready());
    }

    #[test]
    fn test_parse_sql_body_json() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let body = Bytes::from(r#"{"query": "SELECT * FROM cpu"}"#);
        let result = parse_sql_body(&headers, &body).unwrap();
        assert_eq!(result, "SELECT * FROM cpu");
    }

    #[test]
    fn test_parse_sql_body_json_charset() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json; charset=utf-8".parse().unwrap());

        let body = Bytes::from(r#"{"query": "SELECT * FROM cpu"}"#);
        let result = parse_sql_body(&headers, &body).unwrap();
        assert_eq!(result, "SELECT * FROM cpu");
    }

    #[test]
    fn test_parse_sql_body_plain_text() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "text/plain".parse().unwrap());

        let body = Bytes::from("SELECT * FROM cpu");
        let result = parse_sql_body(&headers, &body).unwrap();
        assert_eq!(result, "SELECT * FROM cpu");
    }

    #[test]
    fn test_parse_sql_body_plain_text_with_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "text/plain".parse().unwrap());

        let body = Bytes::from("  SELECT * FROM cpu  \n");
        let result = parse_sql_body(&headers, &body).unwrap();
        assert_eq!(result, "SELECT * FROM cpu");
    }

    #[test]
    fn test_parse_sql_body_no_content_type() {
        let headers = HeaderMap::new();
        let body = Bytes::from("SELECT * FROM cpu");
        let result = parse_sql_body(&headers, &body).unwrap();
        assert_eq!(result, "SELECT * FROM cpu");
    }

    #[test]
    fn test_parse_sql_body_empty_plain_text() {
        let headers = HeaderMap::new();
        let body = Bytes::from("");
        let result = parse_sql_body(&headers, &body);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sql_body_invalid_json() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let body = Bytes::from("not valid json");
        let result = parse_sql_body(&headers, &body);
        assert!(result.is_err());
    }
}
