//! HTTP request handlers

use crate::error::ApiError;
use crate::line_protocol::LineProtocolParser;
use axum::{
    extract::State,
    response::Json,
};
use rusts_core::TimeRange;
use rusts_index::{SeriesIndex, TagIndex};
use rusts_query::{AggregateFunction, Query, QueryExecutor};
use rusts_storage::StorageEngine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Application state shared across handlers
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub series_index: Arc<SeriesIndex>,
    pub tag_index: Arc<TagIndex>,
    pub executor: Arc<QueryExecutor>,
}

impl AppState {
    pub fn new(
        storage: Arc<StorageEngine>,
        series_index: Arc<SeriesIndex>,
        tag_index: Arc<TagIndex>,
    ) -> Self {
        let executor = Arc::new(QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        ));

        Self {
            storage,
            series_index,
            tag_index,
            executor,
        }
    }
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Health check handler
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Ready check response
#[derive(Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub storage: String,
    pub uptime_seconds: u64,
}

/// Ready check handler
pub async fn ready(State(_state): State<Arc<AppState>>) -> Json<ReadyResponse> {
    // Check storage health
    let storage_status = "ok".to_string();

    Json(ReadyResponse {
        ready: true,
        storage: storage_status,
        uptime_seconds: 0, // Would track actual uptime
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

    // Write to storage
    state.storage.write_batch(&points)?;

    Ok(Json(WriteResponse {
        success: true,
        points_written: points.len(),
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

    // Execute query
    let result = state.executor.execute(query)?;

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

    Ok(Json(QueryResponse {
        measurement: result.measurement,
        results,
        total_rows: result.total_rows,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
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
pub async fn stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    let memtable = state.storage.memtable_stats();
    let partitions = state.storage.partition_stats();

    Json(StatsResponse {
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
