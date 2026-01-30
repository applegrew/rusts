//! Query executor
//!
//! Uses rayon for parallel query execution across multiple series.

use crate::aggregation::{AggregateFunction, Aggregator, TimeBucketAggregator};
use crate::error::{QueryError, Result};
use crate::model::{FieldSelection, Query, QueryResult, ResultRow, TagFilter};
use crate::planner::{QueryOptimizer, QueryPlanner};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use rayon::prelude::*;
use rusts_core::{FieldValue, SeriesId, Tag};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_storage::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Global regex cache to avoid recompiling patterns
static REGEX_CACHE: Lazy<RwLock<HashMap<String, regex::Regex>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get or compile a regex pattern (cached)
fn get_or_compile_regex(pattern: &str) -> Option<regex::Regex> {
    // Try to get from cache first (read lock)
    {
        let cache = REGEX_CACHE.read();
        if let Some(re) = cache.get(pattern) {
            return Some(re.clone());
        }
    }

    // Compile and cache (write lock)
    match regex::Regex::new(pattern) {
        Ok(re) => {
            let mut cache = REGEX_CACHE.write();
            // Double-check in case another thread compiled it
            if !cache.contains_key(pattern) {
                cache.insert(pattern.to_string(), re.clone());
            }
            Some(re)
        }
        Err(_) => None,
    }
}

/// Query executor for running queries against the storage engine
pub struct QueryExecutor {
    /// Storage engine
    storage: Arc<StorageEngine>,
    /// Series index
    series_index: Arc<SeriesIndex>,
    /// Tag index
    tag_index: Arc<TagIndex>,
    /// Query planner
    planner: QueryPlanner,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(
        storage: Arc<StorageEngine>,
        series_index: Arc<SeriesIndex>,
        tag_index: Arc<TagIndex>,
    ) -> Self {
        Self {
            storage,
            series_index,
            tag_index,
            planner: QueryPlanner::new(),
        }
    }

    /// Execute a query
    pub fn execute(&self, query: Query) -> Result<QueryResult> {
        let start = Instant::now();

        // Validate query
        query.validate()?;

        // Plan the query
        let plan = self.planner.plan(query.clone())?;
        let _plan = QueryOptimizer::optimize(plan);

        // Resolve series IDs from filters
        let series_ids = self.resolve_series_ids(&query, None)?;

        // Execute based on field selection
        let result = match &query.field_selection {
            FieldSelection::All | FieldSelection::Fields(_) => {
                self.execute_select(&query, &series_ids, None)?
            }
            FieldSelection::Aggregate { field, function, alias } => {
                self.execute_aggregate(&query, &series_ids, field, *function, alias.clone(), None)?
            }
        };

        // Apply limit and offset
        let result = self.apply_limit_offset(result, query.limit, query.offset);

        // Set execution time
        let mut result = result;
        result.execution_time_ns = start.elapsed().as_nanos() as u64;

        Ok(result)
    }

    /// Execute a query with cancellation support
    pub fn execute_with_cancellation(
        &self,
        query: Query,
        cancel: CancellationToken,
    ) -> Result<QueryResult> {
        let start = Instant::now();

        // Check cancellation before starting
        if cancel.is_cancelled() {
            return Err(QueryError::Cancelled);
        }

        // Validate query
        query.validate()?;

        // Plan the query
        let plan = self.planner.plan(query.clone())?;
        let _plan = QueryOptimizer::optimize(plan);

        // Check cancellation after planning
        if cancel.is_cancelled() {
            return Err(QueryError::Cancelled);
        }

        // Resolve series IDs from filters
        let series_ids = self.resolve_series_ids(&query, Some(&cancel))?;

        // Execute based on field selection
        let result = match &query.field_selection {
            FieldSelection::All | FieldSelection::Fields(_) => {
                self.execute_select(&query, &series_ids, Some(&cancel))?
            }
            FieldSelection::Aggregate { field, function, alias } => {
                self.execute_aggregate(&query, &series_ids, field, *function, alias.clone(), Some(&cancel))?
            }
        };

        // Apply limit and offset
        let result = self.apply_limit_offset(result, query.limit, query.offset);

        // Set execution time
        let mut result = result;
        result.execution_time_ns = start.elapsed().as_nanos() as u64;

        Ok(result)
    }

    /// Resolve series IDs matching the query filters
    fn resolve_series_ids(
        &self,
        query: &Query,
        cancel: Option<&CancellationToken>,
    ) -> Result<Vec<SeriesId>> {
        // Get series IDs for the measurement
        let measurement_series = self.series_index.get_by_measurement(&query.measurement);

        if measurement_series.is_empty() {
            return Ok(Vec::new());
        }

        if query.tag_filters.is_empty() {
            return Ok(measurement_series);
        }

        // Filter by tags
        let mut result = measurement_series;

        for filter in &query.tag_filters {
            // Check cancellation before each filter
            if let Some(cancel) = cancel {
                if cancel.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
            }

            match filter {
                TagFilter::Equals { key, value } => {
                    let matching = self.tag_index.find_by_tag(key, value);
                    let matching_set: std::collections::HashSet<_> = matching.into_iter().collect();
                    result.retain(|id| matching_set.contains(id));
                }
                TagFilter::NotEquals { key, value } => {
                    let matching = self.tag_index.find_by_tag(key, value);
                    let matching_set: std::collections::HashSet<_> = matching.into_iter().collect();
                    result.retain(|id| !matching_set.contains(id));
                }
                TagFilter::In { key, values } => {
                    let tags: Vec<Tag> = values.iter()
                        .map(|v| Tag::new(key, v))
                        .collect();
                    let matching = self.tag_index.find_by_tags_any(&tags);
                    let matching_set: std::collections::HashSet<_> = matching.into_iter().collect();
                    result.retain(|id| matching_set.contains(id));
                }
                TagFilter::Exists { key } => {
                    // Check which series have this tag
                    result.retain(|id| {
                        if let Some(meta) = self.series_index.get(*id) {
                            meta.tags.iter().any(|t| &t.key == key)
                        } else {
                            false
                        }
                    });
                }
                TagFilter::Regex { key, pattern } => {
                    // Use cached regex to avoid recompilation
                    if let Some(re) = get_or_compile_regex(pattern) {
                        result.retain(|id| {
                            if let Some(meta) = self.series_index.get(*id) {
                                meta.tags.iter().any(|t| &t.key == key && re.is_match(&t.value))
                            } else {
                                false
                            }
                        });
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute a SELECT query (raw data) with parallel series processing
    fn execute_select(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        // Use parallel processing for multiple series (threshold: 4+ series)
        let rows = if series_ids.len() >= 4 && cancel.is_none() {
            // Parallel path using rayon (only when not cancellable for simplicity)
            let results: Vec<Result<Vec<ResultRow>>> = series_ids
                .par_iter()
                .map(|&series_id| {
                    let points = self.storage.query(series_id, &query.time_range)?;
                    let series_meta = self.series_index.get(series_id);
                    let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                    let mut series_rows = Vec::with_capacity(points.len());
                    for point in points {
                        let fields = self.filter_fields(&point.fields, &query.field_selection);
                        series_rows.push(ResultRow {
                            timestamp: Some(point.timestamp),
                            series_id,
                            tags: tags.clone(),
                            fields,
                        });
                    }
                    Ok(series_rows)
                })
                .collect();

            // Collect and flatten results
            let mut all_rows = Vec::new();
            for result in results {
                all_rows.extend(result?);
            }
            all_rows
        } else {
            // Sequential path (for cancellation support or small queries)
            let mut rows = Vec::new();

            for &series_id in series_ids {
                // Check cancellation before each series
                if let Some(cancel) = cancel {
                    if cancel.is_cancelled() {
                        return Err(QueryError::Cancelled);
                    }
                }

                let points = self.storage.query(series_id, &query.time_range)?;

                let series_meta = self.series_index.get(series_id);
                let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                for point in points {
                    let fields = self.filter_fields(&point.fields, &query.field_selection);

                    rows.push(ResultRow {
                        timestamp: Some(point.timestamp),
                        series_id,
                        tags: tags.clone(),
                        fields,
                    });
                }
            }
            rows
        };

        // Sort by timestamp
        let mut rows = rows;
        rows.sort_by_key(|r| r.timestamp);

        let total_rows = rows.len();

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows,
            execution_time_ns: 0,
        })
    }

    /// Execute an aggregate query with parallel series processing
    fn execute_aggregate(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        field: &str,
        function: AggregateFunction,
        alias: Option<String>,
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        let field_name = alias.unwrap_or_else(|| format!("{}_{:?}", field, function).to_lowercase());

        // Group by time if specified
        if let Some(interval) = query.group_by_time {
            return self.execute_aggregate_time_bucket(
                query, series_ids, field, function, &field_name, interval, cancel,
            );
        }

        // Group by tags if specified
        if !query.group_by.is_empty() {
            return self.execute_aggregate_grouped(
                query, series_ids, field, function, &field_name, cancel,
            );
        }

        // Simple aggregation over all data - use parallel processing for many series
        let aggregator = if series_ids.len() >= 4 && cancel.is_none() {
            // Parallel path: each thread gets its own aggregator, then merge
            let field_clone = field.to_string();
            let partial_results: Vec<Result<Aggregator>> = series_ids
                .par_iter()
                .map(|&series_id| {
                    let points = self.storage.query(series_id, &query.time_range)?;
                    let mut local_agg = Aggregator::new(function);

                    for point in points {
                        if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == &field_clone) {
                            local_agg.add(value);
                        }
                    }
                    Ok(local_agg)
                })
                .collect();

            // Merge all partial aggregators
            let mut final_agg = Aggregator::new(function);
            for result in partial_results {
                let partial = result?;
                final_agg.merge(&partial);
            }
            final_agg
        } else {
            // Sequential path
            let mut aggregator = Aggregator::new(function);

            for &series_id in series_ids {
                // Check cancellation before each series
                if let Some(cancel) = cancel {
                    if cancel.is_cancelled() {
                        return Err(QueryError::Cancelled);
                    }
                }

                let points = self.storage.query(series_id, &query.time_range)?;

                for point in points {
                    if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
                        aggregator.add(value);
                    }
                }
            }
            aggregator
        };

        let mut fields = HashMap::new();
        if let Some(value) = aggregator.result() {
            fields.insert(field_name, value);
        }

        let rows = vec![ResultRow {
            timestamp: None,
            series_id: 0,
            tags: Vec::new(),
            fields,
        }];

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows: 1,
            execution_time_ns: 0,
        })
    }

    /// Execute aggregate with time bucketing
    fn execute_aggregate_time_bucket(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        field: &str,
        function: AggregateFunction,
        field_name: &str,
        interval: i64,
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        // Group by series if group_by tags specified, otherwise aggregate all series
        let group_by_series = !query.group_by.is_empty();

        if group_by_series {
            // Each series gets its own time buckets
            let mut rows = Vec::new();

            for &series_id in series_ids {
                // Check cancellation before each series
                if let Some(cancel) = cancel {
                    if cancel.is_cancelled() {
                        return Err(QueryError::Cancelled);
                    }
                }

                let points = self.storage.query(series_id, &query.time_range)?;
                let mut time_agg = TimeBucketAggregator::new(
                    function,
                    query.time_range.start,
                    query.time_range.end,
                    interval,
                );

                for point in points {
                    if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
                        time_agg.add(point.timestamp, value);
                    }
                }

                let series_meta = self.series_index.get(series_id);
                let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                for (bucket_ts, value) in time_agg.results() {
                    if let Some(v) = value {
                        let mut fields = HashMap::new();
                        fields.insert(field_name.to_string(), v);

                        rows.push(ResultRow {
                            timestamp: Some(bucket_ts),
                            series_id,
                            tags: tags.clone(),
                            fields,
                        });
                    }
                }
            }

            rows.sort_by_key(|r| r.timestamp);
            let total_rows = rows.len();

            Ok(QueryResult {
                measurement: query.measurement.clone(),
                rows,
                total_rows,
                execution_time_ns: 0,
            })
        } else {
            // Aggregate all series into single time buckets
            let mut time_agg = TimeBucketAggregator::new(
                function,
                query.time_range.start,
                query.time_range.end,
                interval,
            );

            for &series_id in series_ids {
                // Check cancellation before each series
                if let Some(cancel) = cancel {
                    if cancel.is_cancelled() {
                        return Err(QueryError::Cancelled);
                    }
                }

                let points = self.storage.query(series_id, &query.time_range)?;

                for point in points {
                    if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
                        time_agg.add(point.timestamp, value);
                    }
                }
            }

            let mut rows = Vec::new();
            for (bucket_ts, value) in time_agg.results() {
                if let Some(v) = value {
                    let mut fields = HashMap::new();
                    fields.insert(field_name.to_string(), v);

                    rows.push(ResultRow {
                        timestamp: Some(bucket_ts),
                        series_id: 0,
                        tags: Vec::new(),
                        fields,
                    });
                }
            }

            let total_rows = rows.len();

            Ok(QueryResult {
                measurement: query.measurement.clone(),
                rows,
                total_rows,
                execution_time_ns: 0,
            })
        }
    }

    /// Execute aggregate grouped by tags
    fn execute_aggregate_grouped(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        field: &str,
        function: AggregateFunction,
        field_name: &str,
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        // Group series by the group_by tags
        let mut groups: HashMap<Vec<(String, String)>, Vec<SeriesId>> = HashMap::new();

        for &series_id in series_ids {
            if let Some(meta) = self.series_index.get(series_id) {
                let group_key: Vec<(String, String)> = query
                    .group_by
                    .iter()
                    .filter_map(|key| {
                        meta.tags
                            .iter()
                            .find(|t| &t.key == key)
                            .map(|t| (key.clone(), t.value.clone()))
                    })
                    .collect();

                groups.entry(group_key).or_default().push(series_id);
            }
        }

        let mut rows = Vec::new();

        for (group_key, group_series) in groups {
            // Check cancellation before each group
            if let Some(cancel) = cancel {
                if cancel.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
            }

            let mut aggregator = Aggregator::new(function);

            for &series_id in &group_series {
                let points = self.storage.query(series_id, &query.time_range)?;

                for point in points {
                    if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
                        aggregator.add(value);
                    }
                }
            }

            let mut fields = HashMap::new();
            if let Some(value) = aggregator.result() {
                fields.insert(field_name.to_string(), value);
            }

            let tags: Vec<Tag> = group_key
                .into_iter()
                .map(|(k, v)| Tag::new(k, v))
                .collect();

            rows.push(ResultRow {
                timestamp: None,
                series_id: 0,
                tags,
                fields,
            });
        }

        let total_rows = rows.len();

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows,
            execution_time_ns: 0,
        })
    }

    /// Filter fields based on selection
    fn filter_fields(
        &self,
        fields: &[(String, FieldValue)],
        selection: &FieldSelection,
    ) -> HashMap<String, FieldValue> {
        match selection {
            FieldSelection::All => fields.iter().cloned().collect(),
            FieldSelection::Fields(names) => {
                fields
                    .iter()
                    .filter(|(k, _)| names.contains(k))
                    .cloned()
                    .collect()
            }
            FieldSelection::Aggregate { .. } => HashMap::new(),
        }
    }

    /// Apply limit and offset to results
    fn apply_limit_offset(
        &self,
        mut result: QueryResult,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> QueryResult {
        let offset = offset.unwrap_or(0);

        if offset > 0 {
            if offset >= result.rows.len() {
                result.rows = Vec::new();
            } else {
                result.rows = result.rows.split_off(offset);
            }
        }

        if let Some(limit) = limit {
            result.rows.truncate(limit);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::Point;
    use rusts_storage::{StorageEngineConfig, WalDurability};
    use tempfile::TempDir;

    fn setup_test_env() -> (Arc<StorageEngine>, Arc<SeriesIndex>, Arc<TagIndex>, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        (storage, series_index, tag_index, dir)
    }

    fn create_test_point(host: &str, ts: i64, value: f64) -> Point {
        Point::builder("cpu")
            .timestamp(ts)
            .tag("host", host)
            .field("value", value)
            .build()
            .unwrap()
    }

    #[test]
    fn test_executor_basic_query() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write some data
        for i in 0..10 {
            let point = create_test_point("server01", i * 1000, i as f64);
            storage.write(&point).unwrap();

            let series_id = point.series_id();
            series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
            tag_index.index_series(series_id, &point.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        let query = Query::builder("cpu")
            .time_range(0, 100000)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_executor_with_tag_filter() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write data for two hosts
        for i in 0..10 {
            let p1 = create_test_point("server01", i * 1000, i as f64);
            let p2 = create_test_point("server02", i * 1000, i as f64 * 2.0);

            storage.write(&p1).unwrap();
            storage.write(&p2).unwrap();

            series_index.upsert(p1.series_id(), "cpu", &p1.tags, p1.timestamp);
            series_index.upsert(p2.series_id(), "cpu", &p2.tags, p2.timestamp);

            tag_index.index_series(p1.series_id(), &p1.tags);
            tag_index.index_series(p2.series_id(), &p2.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Query only server01
        let query = Query::builder("cpu")
            .time_range(0, 100000)
            .where_tag("host", "server01")
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);

        // All rows should be from server01
        for row in &result.rows {
            assert!(row.tags.iter().any(|t| t.key == "host" && t.value == "server01"));
        }

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_executor_aggregate() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        for i in 0..10 {
            let point = create_test_point("server01", i * 1000, i as f64);
            storage.write(&point).unwrap();

            let series_id = point.series_id();
            series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
            tag_index.index_series(series_id, &point.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        let query = Query::builder("cpu")
            .time_range(0, 100000)
            .select_aggregate("value", AggregateFunction::Mean, Some("avg_value".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 1);

        // Mean of 0..9 = 4.5
        if let Some(FieldValue::Float(v)) = result.rows[0].fields.get("avg_value") {
            assert!((v - 4.5).abs() < 0.01);
        } else {
            panic!("Expected avg_value field");
        }

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_executor_limit_offset() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        for i in 0..100 {
            let point = create_test_point("server01", i * 1000, i as f64);
            storage.write(&point).unwrap();

            let series_id = point.series_id();
            series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Query with limit
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .limit(10)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.total_rows, 100);

        // Query with offset
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .offset(90)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_wal_recovery_with_index_rebuild_and_query() {
        // Integration test: WAL recovery -> index rebuild -> query execution
        // This simulates server restart scenario
        use rusts_storage::memtable::FlushTrigger;

        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        // Phase 1: Write data and shutdown
        {
            let config = StorageEngineConfig {
                data_dir: data_dir.clone(),
                wal_durability: WalDurability::EveryWrite,
                flush_trigger: FlushTrigger {
                    max_size: 1024 * 1024 * 1024,
                    max_points: 1_000_000_000,
                    max_age_nanos: i64::MAX,
                },
                ..Default::default()
            };

            let storage = Arc::new(StorageEngine::new(config).unwrap());
            let series_index = Arc::new(SeriesIndex::new());
            let tag_index = Arc::new(TagIndex::new());

            // Write data for multiple hosts
            for host_idx in 0..3 {
                for i in 0..10 {
                    let point = Point::builder("cpu")
                        .timestamp(i * 1000)
                        .tag("host", &format!("server{:02}", host_idx))
                        .field("value", (host_idx * 100 + i) as f64)
                        .build()
                        .unwrap();

                    storage.write(&point).unwrap();

                    let series_id = point.series_id();
                    series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
                    tag_index.index_series(series_id, &point.tags);
                }
            }

            // Verify query works before shutdown
            let executor = QueryExecutor::new(
                Arc::clone(&storage),
                Arc::clone(&series_index),
                Arc::clone(&tag_index),
            );

            let query = Query::builder("cpu")
                .time_range(0, 100000)
                .build()
                .unwrap();

            let result = executor.execute(query).unwrap();
            assert_eq!(result.rows.len(), 30, "Expected 30 rows before shutdown");

            storage.shutdown().unwrap();
        }

        // Phase 2: Simulate server restart - create new engine, rebuild indexes, query
        {
            let config = StorageEngineConfig {
                data_dir: data_dir.clone(),
                wal_durability: WalDurability::EveryWrite,
                flush_trigger: FlushTrigger {
                    max_size: 1024 * 1024 * 1024,
                    max_points: 1_000_000_000,
                    max_age_nanos: i64::MAX,
                },
                ..Default::default()
            };

            let storage = Arc::new(StorageEngine::new(config).unwrap());

            // Create fresh indexes (simulating server restart)
            let series_index = Arc::new(SeriesIndex::new());
            let tag_index = Arc::new(TagIndex::new());

            // Rebuild indexes from storage (memtable + partitions)
            // After clean shutdown, data is in partitions, so use get_all_series()
            let recovered_series = storage.get_all_series();
            assert_eq!(recovered_series.len(), 3, "Expected 3 series after restart");

            for (series_id, measurement, tags) in recovered_series {
                series_index.upsert(series_id, &measurement, &tags, 0);
                tag_index.index_series(series_id, &tags);
            }

            // Verify index was rebuilt correctly
            let cpu_series = series_index.get_by_measurement("cpu");
            assert_eq!(cpu_series.len(), 3, "Expected 3 cpu series in index");

            // Create executor and verify queries work
            let executor = QueryExecutor::new(
                Arc::clone(&storage),
                Arc::clone(&series_index),
                Arc::clone(&tag_index),
            );

            // Query all data
            let query = Query::builder("cpu")
                .time_range(0, 100000)
                .build()
                .unwrap();

            let result = executor.execute(query).unwrap();
            assert_eq!(result.rows.len(), 30, "Expected 30 rows after recovery");

            // Query with tag filter
            let query = Query::builder("cpu")
                .time_range(0, 100000)
                .where_tag("host", "server01")
                .build()
                .unwrap();

            let result = executor.execute(query).unwrap();
            assert_eq!(result.rows.len(), 10, "Expected 10 rows for server01 after recovery");

            // Verify all rows have correct host tag
            for row in &result.rows {
                assert!(row.tags.iter().any(|t| t.key == "host" && t.value == "server01"));
            }

            // Query with aggregation
            let query = Query::builder("cpu")
                .time_range(0, 100000)
                .select_aggregate("value", AggregateFunction::Count, Some("count".to_string()))
                .build()
                .unwrap();

            let result = executor.execute(query).unwrap();
            assert_eq!(result.rows.len(), 1);
            if let Some(FieldValue::Integer(count)) = result.rows[0].fields.get("count") {
                assert_eq!(*count, 30, "Expected count of 30 after recovery");
            } else {
                panic!("Expected count field");
            }

            storage.shutdown().unwrap();
        }
    }

    #[test]
    fn test_query_with_default_time_range_no_panic() {
        // Test that queries with default (extreme) time range don't panic
        // This was a bug: i64::MAX - i64::MIN overflowed
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write some data
        for i in 0..10 {
            let point = create_test_point("server01", i * 1000, i as f64);
            storage.write(&point).unwrap();

            let series_id = point.series_id();
            series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
            tag_index.index_series(series_id, &point.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Create query with extreme time range (simulating no time range specified in API)
        let query = Query {
            measurement: "cpu".to_string(),
            time_range: rusts_core::TimeRange::new(i64::MIN, i64::MAX),
            tag_filters: Vec::new(),
            field_selection: crate::model::FieldSelection::All,
            group_by: Vec::new(),
            group_by_time: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        // This should NOT panic (was previously causing "attempt to subtract with overflow")
        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10, "Should return all 10 rows");

        storage.shutdown().unwrap();
    }
}
