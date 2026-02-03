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
use rusts_core::{FieldValue, ParallelConfig, SeriesId, Tag};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_storage::StorageEngine;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Global regex cache to avoid recompiling patterns
static REGEX_CACHE: Lazy<RwLock<HashMap<String, regex::Regex>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// =============================================================================
// TopKCollector: Bounded collection for LIMIT optimization
// =============================================================================

/// Wrapper for ResultRow that implements Ord by timestamp (max-heap for ascending order)
struct TimestampedRow {
    timestamp: i64,
    row: ResultRow,
}

impl PartialEq for TimestampedRow {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for TimestampedRow {}

impl PartialOrd for TimestampedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger timestamps at top (for easy removal when collecting smallest)
        self.timestamp.cmp(&other.timestamp)
    }
}

/// Efficiently collect top K rows by timestamp (ascending order).
/// Uses a max-heap to track the K smallest timestamps seen.
struct TopKCollector {
    heap: BinaryHeap<TimestampedRow>,
    capacity: usize,
    total_seen: usize,
}

impl TopKCollector {
    fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            capacity,
            total_seen: 0,
        }
    }

    /// Add a row, keeping only the K smallest timestamps
    fn push(&mut self, row: ResultRow) {
        self.total_seen += 1;
        let ts = row.timestamp.unwrap_or(i64::MAX);

        if self.heap.len() < self.capacity {
            self.heap.push(TimestampedRow { timestamp: ts, row });
        } else if let Some(max_entry) = self.heap.peek() {
            // Only insert if smaller than current max (which will be evicted)
            if ts < max_entry.timestamp {
                self.heap.pop();
                self.heap.push(TimestampedRow { timestamp: ts, row });
            }
        }
    }

    /// Extract rows sorted by timestamp ascending
    fn into_sorted_vec(self) -> Vec<ResultRow> {
        let mut rows: Vec<_> = self.heap.into_iter().map(|tr| tr.row).collect();
        rows.sort_by_key(|r| r.timestamp);
        rows
    }

    fn total_seen(&self) -> usize {
        self.total_seen
    }
}

/// Wrapper for min-heap (descending order - keeps largest timestamps)
struct TimestampedRowDesc(TimestampedRow);

impl PartialEq for TimestampedRowDesc {
    fn eq(&self, other: &Self) -> bool {
        self.0.timestamp == other.0.timestamp
    }
}

impl Eq for TimestampedRowDesc {}

impl PartialOrd for TimestampedRowDesc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedRowDesc {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: smaller timestamps at top (for easy removal when collecting largest)
        other.0.timestamp.cmp(&self.0.timestamp) // Reversed!
    }
}

/// Collect top K rows by timestamp (descending order).
/// Uses a min-heap to track the K largest timestamps seen.
struct TopKCollectorDesc {
    heap: BinaryHeap<TimestampedRowDesc>,
    capacity: usize,
    total_seen: usize,
}

impl TopKCollectorDesc {
    fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            capacity,
            total_seen: 0,
        }
    }

    /// Add a row, keeping only the K largest timestamps
    fn push(&mut self, row: ResultRow) {
        self.total_seen += 1;
        let ts = row.timestamp.unwrap_or(i64::MIN);

        if self.heap.len() < self.capacity {
            self.heap
                .push(TimestampedRowDesc(TimestampedRow { timestamp: ts, row }));
        } else if let Some(min_entry) = self.heap.peek() {
            // Only insert if larger than current min (which will be evicted)
            if ts > min_entry.0.timestamp {
                self.heap.pop();
                self.heap
                    .push(TimestampedRowDesc(TimestampedRow { timestamp: ts, row }));
            }
        }
    }

    /// Extract rows sorted by timestamp descending
    fn into_sorted_vec(self) -> Vec<ResultRow> {
        let mut rows: Vec<_> = self.heap.into_iter().map(|tr| tr.0.row).collect();
        rows.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)); // Descending
        rows
    }

    fn total_seen(&self) -> usize {
        self.total_seen
    }
}

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
    /// Parallel execution configuration
    parallel_config: ParallelConfig,
}

impl QueryExecutor {
    /// Create a new query executor with default parallel configuration
    pub fn new(
        storage: Arc<StorageEngine>,
        series_index: Arc<SeriesIndex>,
        tag_index: Arc<TagIndex>,
    ) -> Self {
        Self::with_parallel_config(storage, series_index, tag_index, ParallelConfig::default())
    }

    /// Create a new query executor with custom parallel configuration
    pub fn with_parallel_config(
        storage: Arc<StorageEngine>,
        series_index: Arc<SeriesIndex>,
        tag_index: Arc<TagIndex>,
        parallel_config: ParallelConfig,
    ) -> Self {
        Self {
            storage,
            series_index,
            tag_index,
            planner: QueryPlanner::new(),
            parallel_config,
        }
    }

    /// Returns a reference to the parallel configuration
    pub fn parallel_config(&self) -> &ParallelConfig {
        &self.parallel_config
    }

    /// Updates the parallel configuration
    pub fn set_parallel_config(&mut self, config: ParallelConfig) {
        self.parallel_config = config;
    }

    /// Check if query can use hot data routing (memtable-only path).
    ///
    /// Returns true if the query's time range is fully contained within the memtable's
    /// time range, meaning no partition scans are needed.
    fn can_use_hot_data_routing(&self, query: &Query) -> bool {
        self.storage.can_serve_from_memtable(&query.time_range)
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

        // Check if optimized paths can be used
        let uses_optimized_limit = self.can_use_optimized_limit(&query);
        let uses_hot_data = self.can_use_hot_data_routing(&query);

        // Execute based on field selection
        let result = match &query.field_selection {
            FieldSelection::All | FieldSelection::Fields(_) => {
                if uses_hot_data {
                    self.execute_select_memtable_only(&query, &series_ids)?
                } else {
                    self.execute_select(&query, &series_ids, None)?
                }
            }
            FieldSelection::Aggregate { field, function, alias } => {
                self.execute_aggregate(&query, &series_ids, field, *function, alias.clone(), None)?
            }
        };

        // Apply limit and offset (skip for optimized path - already applied)
        let result = if uses_optimized_limit && !uses_hot_data {
            result
        } else {
            self.apply_limit_offset(result, query.limit, query.offset)
        };

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

        // Check if optimized LIMIT path is used (to avoid double limit application)
        let uses_optimized_limit = self.can_use_optimized_limit(&query);

        // Execute based on field selection
        let result = match &query.field_selection {
            FieldSelection::All | FieldSelection::Fields(_) => {
                self.execute_select(&query, &series_ids, Some(&cancel))?
            }
            FieldSelection::Aggregate { field, function, alias } => {
                self.execute_aggregate(&query, &series_ids, field, *function, alias.clone(), Some(&cancel))?
            }
        };

        // Apply limit and offset (skip for optimized path - already applied)
        let result = if uses_optimized_limit {
            result
        } else {
            self.apply_limit_offset(result, query.limit, query.offset)
        };

        // Set execution time
        let mut result = result;
        result.execution_time_ns = start.elapsed().as_nanos() as u64;

        Ok(result)
    }

    /// Order filters by cardinality (most selective first).
    ///
    /// High-selectivity filters (low cardinality) should be applied first
    /// to shrink bitmaps quickly. For example, device_id=X (cardinality ~1)
    /// should come before region=Y (cardinality ~1000).
    fn order_filters_by_cardinality(&self, filters: &[TagFilter]) -> Vec<(TagFilter, usize)> {
        let mut with_cardinality: Vec<(TagFilter, usize)> = filters
            .iter()
            .map(|f| {
                let cardinality = match f {
                    TagFilter::Equals { key, value } => {
                        self.tag_index.get_cardinality(key, value).unwrap_or(1000)
                    }
                    TagFilter::In { key, values } => {
                        // Sum of cardinalities for all values in IN clause
                        values
                            .iter()
                            .filter_map(|v| self.tag_index.get_cardinality(key, v))
                            .sum::<usize>()
                            .max(1)
                    }
                    TagFilter::NotEquals { .. } | TagFilter::NotIn { .. } => {
                        // NotEquals/NotIn are less selective, give high cardinality
                        10000
                    }
                    TagFilter::Regex { .. } | TagFilter::Exists { .. } => {
                        // Default for complex filters - apply last
                        10000
                    }
                };
                (f.clone(), cardinality)
            })
            .collect();

        // Sort by cardinality ascending (most selective first)
        with_cardinality.sort_by_key(|(_, c)| *c);
        with_cardinality
    }

    /// Resolve series IDs matching the query filters
    ///
    /// Uses bitmap operations for efficient filtering when possible:
    /// - Equals filters use bitmap AND operations
    /// - NotEquals filters use bitmap AND NOT operations
    /// - In filters use bitmap OR followed by AND
    ///
    /// Filters are ordered by cardinality (most selective first) to shrink
    /// bitmaps faster and reduce intermediate work.
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

        // Convert measurement series to bitmap for efficient filtering
        let mut result_bitmap = self.tag_index.series_to_bitmap(&measurement_series);

        // Order filters by cardinality (most selective first)
        let ordered_filters = self.order_filters_by_cardinality(&query.tag_filters);

        // Track whether we need to fall back to per-series filtering for some filters
        let mut needs_per_series_filter = Vec::new();

        for (filter, _cardinality) in &ordered_filters {
            // Check cancellation before each filter
            if let Some(cancel) = cancel {
                if cancel.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
            }

            match filter {
                TagFilter::Equals { key, value } => {
                    // Use bitmap AND operation
                    self.tag_index.intersect_with(key, value, &mut result_bitmap);
                    if result_bitmap.is_empty() {
                        return Ok(Vec::new());
                    }
                }
                TagFilter::NotEquals { key, value } => {
                    // Get matching bitmap and subtract from result
                    if let Some(matching_bitmap) = self.tag_index.find_by_tag_bitmap(key, value) {
                        result_bitmap -= &matching_bitmap;
                        if result_bitmap.is_empty() {
                            return Ok(Vec::new());
                        }
                    }
                    // If tag doesn't exist, all series pass the filter (no change needed)
                }
                TagFilter::In { key, values } => {
                    // Build OR bitmap of all matching values, then AND with result
                    let mut or_bitmap = roaring::RoaringBitmap::new();
                    for value in values {
                        self.tag_index.union_with(key, value, &mut or_bitmap);
                    }
                    result_bitmap &= &or_bitmap;
                    if result_bitmap.is_empty() {
                        return Ok(Vec::new());
                    }
                }
                TagFilter::NotIn { key, values } => {
                    // Build OR bitmap of all matching values, then subtract from result
                    let mut or_bitmap = roaring::RoaringBitmap::new();
                    for value in values {
                        self.tag_index.union_with(key, value, &mut or_bitmap);
                    }
                    result_bitmap -= &or_bitmap;
                    if result_bitmap.is_empty() {
                        return Ok(Vec::new());
                    }
                }
                TagFilter::Exists { .. } | TagFilter::Regex { .. } => {
                    // These require per-series evaluation, defer to later
                    needs_per_series_filter.push(filter.clone());
                }
            }
        }

        // Convert bitmap back to series IDs
        let mut result = self.tag_index.bitmap_to_series(&result_bitmap);

        // Apply remaining filters that require per-series evaluation
        for filter in needs_per_series_filter {
            // Check cancellation before each filter
            if let Some(cancel) = cancel {
                if cancel.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
            }

            match &filter {
                TagFilter::Exists { key } => {
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
                _ => unreachable!("Only Exists and Regex filters are deferred"),
            }
        }

        Ok(result)
    }

    /// Check if the query can use the optimized LIMIT path
    fn can_use_optimized_limit(&self, query: &Query) -> bool {
        // Must have a LIMIT
        if query.limit.is_none() {
            return false;
        }

        // Must be a SELECT (not aggregate)
        if !matches!(
            query.field_selection,
            FieldSelection::All | FieldSelection::Fields(_)
        ) {
            return false;
        }

        // Must be default order (time ASC) or explicit time ASC/DESC
        match &query.order_by {
            None => true,                                        // Default: time ASC
            Some((field, _)) if field == "time" => true,         // Explicit time order
            Some(_) => false,                                    // Custom field order - can't optimize
        }
    }

    /// Execute a SELECT query using memtable-only path (hot data routing).
    ///
    /// This is an optimized path for queries where the time range is fully
    /// contained within the memtable, skipping all partition scans.
    fn execute_select_memtable_only(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
    ) -> Result<QueryResult> {
        // Use batch query for efficiency
        let series_data = self.storage.query_memtable_only_multi(series_ids, &query.time_range);

        let mut rows = Vec::new();
        for (series_id, points) in series_data {
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

        // Sort by timestamp
        rows.sort_by_key(|r| r.timestamp);

        let total_rows = rows.len();

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows,
            execution_time_ns: 0,
        })
    }

    /// Execute a SELECT query (raw data) with parallel series processing
    fn execute_select(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        // Use optimized path for LIMIT queries with timestamp ordering
        if self.can_use_optimized_limit(query) {
            return self.execute_select_with_limit(query, series_ids, cancel);
        }

        // Full scan path for queries without LIMIT or with custom ordering
        // Use parallel processing based on config thresholds
        let rows = if self.parallel_config.should_parallelize_series(series_ids.len())
            && cancel.is_none()
        {
            // Parallel path using rayon (only when not cancellable for simplicity)
            let effective_parallelism =
                self.parallel_config.effective_series_parallelism(series_ids.len());

            if effective_parallelism >= series_ids.len() {
                // Full parallelism - process all series in parallel
                let results: Vec<Result<Vec<ResultRow>>> = series_ids
                    .par_iter()
                    .map(|&series_id| {
                        let points = self.storage.query(series_id, &query.time_range)?;
                        let series_meta = self.series_index.get(series_id);
                        let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                        let mut series_rows = Vec::with_capacity(points.len());
                        for point in points {
                            let fields =
                                self.filter_fields(&point.fields, &query.field_selection);
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
                // Limited parallelism - process in chunks
                let mut all_rows = Vec::new();
                for chunk in series_ids.chunks(effective_parallelism) {
                    let results: Vec<Result<Vec<ResultRow>>> = chunk
                        .par_iter()
                        .map(|&series_id| {
                            let points = self.storage.query(series_id, &query.time_range)?;
                            let series_meta = self.series_index.get(series_id);
                            let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                            let mut series_rows = Vec::with_capacity(points.len());
                            for point in points {
                                let fields =
                                    self.filter_fields(&point.fields, &query.field_selection);
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

                    for result in results {
                        all_rows.extend(result?);
                    }
                }
                all_rows
            }
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

    /// Optimized SELECT with LIMIT using bounded collection.
    /// Uses a heap to maintain only the top K rows during collection,
    /// avoiding the O(n log n) sort of the full dataset.
    fn execute_select_with_limit(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        cancel: Option<&CancellationToken>,
    ) -> Result<QueryResult> {
        let limit = query.limit.unwrap();
        let offset = query.offset.unwrap_or(0);
        let need = limit.saturating_add(offset); // Collect enough for offset + limit

        // Determine sort order (default is ascending)
        let descending = query
            .order_by
            .as_ref()
            .map(|(_, asc)| !*asc)
            .unwrap_or(false);

        let (rows, total_seen) = if descending {
            self.collect_top_k_desc(query, series_ids, need, cancel)?
        } else {
            self.collect_top_k_asc(query, series_ids, need, cancel)?
        };

        // Apply offset
        let mut rows = rows;
        if offset > 0 && offset < rows.len() {
            rows = rows.split_off(offset);
        } else if offset >= rows.len() {
            rows = Vec::new();
        }

        // Truncate to limit (should already be at most `need`)
        rows.truncate(limit);

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows: total_seen, // Report total rows scanned
            execution_time_ns: 0,
        })
    }

    /// Collect top K rows by ascending timestamp.
    /// Uses storage-level early termination with partition-ordered queries.
    fn collect_top_k_asc(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        k: usize,
        cancel: Option<&CancellationToken>,
    ) -> Result<(Vec<ResultRow>, usize)> {
        if let Some(cancel) = cancel {
            if cancel.is_cancelled() {
                return Err(QueryError::Cancelled);
            }
        }

        if series_ids.len() == 1 {
            // Single series: use original optimized path
            let series_id = series_ids[0];
            let series_meta = self.series_index.get(series_id);
            let tags = series_meta.map(|m| m.tags).unwrap_or_default();

            let (points, total_seen) = self
                .storage
                .query_with_limit(series_id, &query.time_range, k, true)?;

            let mut collector = TopKCollector::new(k);
            for point in points {
                let fields = self.filter_fields(&point.fields, &query.field_selection);
                collector.push(ResultRow {
                    timestamp: Some(point.timestamp),
                    series_id,
                    tags: tags.clone(),
                    fields,
                });
            }

            let rows = collector.into_sorted_vec();
            Ok((rows, total_seen))
        } else {
            // Multiple series: use partition-ordered query with early termination
            let (series_points, total_seen) = self
                .storage
                .query_multi_series_with_limit(series_ids, &query.time_range, k, true)?;

            let mut collector = TopKCollector::new(k);
            for (series_id, points) in series_points {
                let series_meta = self.series_index.get(series_id);
                let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                for point in points {
                    let fields = self.filter_fields(&point.fields, &query.field_selection);
                    collector.push(ResultRow {
                        timestamp: Some(point.timestamp),
                        series_id,
                        tags: tags.clone(),
                        fields,
                    });
                }
            }

            let rows = collector.into_sorted_vec();
            Ok((rows, total_seen))
        }
    }

    /// Collect top K rows by descending timestamp.
    /// Uses storage-level early termination with partition-ordered queries.
    fn collect_top_k_desc(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        k: usize,
        cancel: Option<&CancellationToken>,
    ) -> Result<(Vec<ResultRow>, usize)> {
        if let Some(cancel) = cancel {
            if cancel.is_cancelled() {
                return Err(QueryError::Cancelled);
            }
        }

        if series_ids.len() == 1 {
            // Single series: use original optimized path
            let series_id = series_ids[0];
            let series_meta = self.series_index.get(series_id);
            let tags = series_meta.map(|m| m.tags).unwrap_or_default();

            let (points, total_seen) = self
                .storage
                .query_with_limit(series_id, &query.time_range, k, false)?;

            let mut collector = TopKCollectorDesc::new(k);
            for point in points {
                let fields = self.filter_fields(&point.fields, &query.field_selection);
                collector.push(ResultRow {
                    timestamp: Some(point.timestamp),
                    series_id,
                    tags: tags.clone(),
                    fields,
                });
            }

            let rows = collector.into_sorted_vec();
            Ok((rows, total_seen))
        } else {
            // Multiple series: use partition-ordered query with early termination
            let (series_points, total_seen) = self
                .storage
                .query_multi_series_with_limit(series_ids, &query.time_range, k, false)?;

            let mut collector = TopKCollectorDesc::new(k);
            for (series_id, points) in series_points {
                let series_meta = self.series_index.get(series_id);
                let tags = series_meta.map(|m| m.tags).unwrap_or_default();

                for point in points {
                    let fields = self.filter_fields(&point.fields, &query.field_selection);
                    collector.push(ResultRow {
                        timestamp: Some(point.timestamp),
                        series_id,
                        tags: tags.clone(),
                        fields,
                    });
                }
            }

            let rows = collector.into_sorted_vec();
            Ok((rows, total_seen))
        }
    }

    /// Check if the query can use segment statistics for aggregation.
    ///
    /// Segment stats can be used when:
    /// - Aggregation is COUNT/SUM/MIN/MAX (not AVG/FIRST/LAST)
    /// - No GROUP BY clause
    /// - No GROUP BY TIME clause
    /// - All segments have stats for the field (or field is "*" for COUNT)
    /// - No data in memtable for the query range (memtable doesn't have stats)
    fn can_use_segment_stats(&self, query: &Query, series_ids: &[SeriesId], field: &str, function: AggregateFunction) -> bool {
        // Must be a simple aggregation without GROUP BY
        if !query.group_by.is_empty() || query.group_by_time.is_some() {
            return false;
        }

        // Only certain aggregation functions can use segment stats
        if !matches!(function, AggregateFunction::Count | AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max) {
            return false;
        }

        // For COUNT(*), we just need segment metadata (point_count)
        if field == "*" && function == AggregateFunction::Count {
            // Check if memtable has data - if so, we can't use stats alone
            let (_, has_memtable_data) = self.storage.get_segment_point_count(series_ids, &query.time_range);
            return !has_memtable_data;
        }

        // For field-specific aggregations, verify segment stats are complete
        // Check if memtable has data - if so, we can't use stats alone
        let (_, has_memtable_data) = self.storage.get_segment_point_count(series_ids, &query.time_range);
        if has_memtable_data {
            return false;
        }

        // Check if all segments have stats for this field
        self.storage.has_complete_segment_stats(series_ids, &query.time_range, field)
    }

    /// Execute aggregation using segment statistics only (no data decompression).
    ///
    /// This is a fast path for simple aggregations that can use pre-computed
    /// statistics from segment metadata.
    fn execute_with_segment_stats(
        &self,
        query: &Query,
        series_ids: &[SeriesId],
        field: &str,
        function: AggregateFunction,
        field_name: &str,
    ) -> Result<QueryResult> {
        let mut fields = HashMap::new();

        // For COUNT(*), use point count from segment metadata
        if field == "*" && function == AggregateFunction::Count {
            let (count, _) = self.storage.get_segment_point_count(series_ids, &query.time_range);
            fields.insert(field_name.to_string(), FieldValue::Integer(count as i64));
        } else {
            // Get aggregated field stats
            if let Some(stats) = self.storage.get_aggregated_field_stats(series_ids, &query.time_range, field) {
                let value = match function {
                    AggregateFunction::Count => FieldValue::Integer(stats.count as i64),
                    AggregateFunction::Sum => FieldValue::Float(stats.sum.unwrap_or(0.0)),
                    AggregateFunction::Min => FieldValue::Float(stats.min.unwrap_or(f64::NAN)),
                    AggregateFunction::Max => FieldValue::Float(stats.max.unwrap_or(f64::NAN)),
                    _ => unreachable!("Only Count/Sum/Min/Max can use segment stats"),
                };
                fields.insert(field_name.to_string(), value);
            }
        }

        let (rows, total_rows) = if fields.is_empty() {
            (Vec::new(), 0)
        } else {
            (vec![ResultRow {
                timestamp: None,
                series_id: 0,
                tags: Vec::new(),
                fields,
            }], 1)
        };

        Ok(QueryResult {
            measurement: query.measurement.clone(),
            rows,
            total_rows,
            execution_time_ns: 0,
        })
    }

    /// Execute an aggregate query with parallel series and partition processing
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

        // Try segment stats optimization for simple aggregations
        if self.can_use_segment_stats(query, series_ids, field, function) {
            return self.execute_with_segment_stats(query, series_ids, field, function, &field_name);
        }

        // Simple aggregation over all data
        // Special case: COUNT(*) counts all rows, not a specific field
        let is_count_star = field == "*" && function == AggregateFunction::Count;

        let aggregator = if self.parallel_config.should_parallelize_series(series_ids.len())
            && cancel.is_none()
        {
            // Use parallel partition scanning for aggregations with configurable parallelism
            let all_series_data = self.storage.query_measurement_parallel_with_config(
                series_ids,
                &query.time_range,
                &self.parallel_config,
            )?;

            // Parallel aggregation across series with configurable limits
            let field_clone = field.to_string();
            let effective_parallelism = self
                .parallel_config
                .effective_series_parallelism(all_series_data.len());

            let partial_results: Vec<Aggregator> = if effective_parallelism >= all_series_data.len()
            {
                // Full parallelism
                all_series_data
                    .par_iter()
                    .map(|(_, points)| {
                        let mut local_agg = Aggregator::new(function);
                        for point in points {
                            if is_count_star {
                                local_agg.add(&FieldValue::Integer(1));
                            } else if let Some((_, value)) =
                                point.fields.iter().find(|(k, _)| k == &field_clone)
                            {
                                local_agg.add(value);
                            }
                        }
                        local_agg
                    })
                    .collect()
            } else {
                // Limited parallelism - process in chunks
                let mut results = Vec::new();
                for chunk in all_series_data.chunks(effective_parallelism) {
                    let chunk_results: Vec<Aggregator> = chunk
                        .par_iter()
                        .map(|(_, points)| {
                            let mut local_agg = Aggregator::new(function);
                            for point in points {
                                if is_count_star {
                                    local_agg.add(&FieldValue::Integer(1));
                                } else if let Some((_, value)) =
                                    point.fields.iter().find(|(k, _)| k == &field_clone)
                                {
                                    local_agg.add(value);
                                }
                            }
                            local_agg
                        })
                        .collect();
                    results.extend(chunk_results);
                }
                results
            };

            // Merge all partial aggregators
            let mut final_agg = Aggregator::new(function);
            for partial in partial_results {
                final_agg.merge(&partial);
            }
            final_agg
        } else {
            // Sequential path for small queries or when cancellation is needed
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
                    if is_count_star {
                        // COUNT(*): count every row
                        aggregator.add(&FieldValue::Integer(1));
                    } else if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
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
        // Special case: COUNT(*) counts all rows, not a specific field
        let is_count_star = field == "*" && function == AggregateFunction::Count;

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
                    if is_count_star {
                        time_agg.add(point.timestamp, &FieldValue::Integer(1));
                    } else if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
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
                    if is_count_star {
                        time_agg.add(point.timestamp, &FieldValue::Integer(1));
                    } else if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
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
        // Special case: COUNT(*) counts all rows, not a specific field
        let is_count_star = field == "*" && function == AggregateFunction::Count;

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
                    if is_count_star {
                        aggregator.add(&FieldValue::Integer(1));
                    } else if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field) {
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
    fn test_executor_count_star() {
        // Test that COUNT(*) counts all rows, not a specific field
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write 10 points
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

        // COUNT(*) should count all rows
        let query = Query::builder("cpu")
            .time_range(0, 100000)
            .select_aggregate("*", AggregateFunction::Count, Some("row_count".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 1);

        if let Some(FieldValue::Integer(count)) = result.rows[0].fields.get("row_count") {
            assert_eq!(*count, 10, "COUNT(*) should return 10");
        } else {
            panic!("Expected row_count field with integer value");
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
                    out_of_order_lag_ms: 0, // No lag for tests
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
                    out_of_order_lag_ms: 0, // No lag for tests
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

    // =========================================================================
    // TopKCollector tests
    // =========================================================================

    fn make_row_with_ts(ts: i64) -> ResultRow {
        ResultRow {
            timestamp: Some(ts),
            series_id: 0,
            tags: Vec::new(),
            fields: HashMap::new(),
        }
    }

    #[test]
    fn test_top_k_collector_basic() {
        let mut collector = TopKCollector::new(3);
        collector.push(make_row_with_ts(100));
        collector.push(make_row_with_ts(50));
        collector.push(make_row_with_ts(200));
        collector.push(make_row_with_ts(25)); // Should evict 200

        assert_eq!(collector.total_seen(), 4);

        let rows = collector.into_sorted_vec();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].timestamp, Some(25));
        assert_eq!(rows[1].timestamp, Some(50));
        assert_eq!(rows[2].timestamp, Some(100));
    }

    #[test]
    fn test_top_k_collector_fewer_than_k() {
        let mut collector = TopKCollector::new(10);
        collector.push(make_row_with_ts(300));
        collector.push(make_row_with_ts(100));

        assert_eq!(collector.total_seen(), 2);

        let rows = collector.into_sorted_vec();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, Some(100));
        assert_eq!(rows[1].timestamp, Some(300));
    }

    #[test]
    fn test_top_k_collector_desc_basic() {
        let mut collector = TopKCollectorDesc::new(3);
        collector.push(make_row_with_ts(100));
        collector.push(make_row_with_ts(50));
        collector.push(make_row_with_ts(200));
        collector.push(make_row_with_ts(25)); // Should NOT evict anything (25 < 50)
        collector.push(make_row_with_ts(300)); // Should evict 50

        assert_eq!(collector.total_seen(), 5);

        let rows = collector.into_sorted_vec();
        assert_eq!(rows.len(), 3);
        // Descending order: 300, 200, 100
        assert_eq!(rows[0].timestamp, Some(300));
        assert_eq!(rows[1].timestamp, Some(200));
        assert_eq!(rows[2].timestamp, Some(100));
    }

    #[test]
    fn test_top_k_collector_empty() {
        let collector = TopKCollector::new(5);
        assert_eq!(collector.total_seen(), 0);
        let rows = collector.into_sorted_vec();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_limit_with_offset_optimized() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write 100 points
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

        // Query with limit and offset
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .limit(10)
            .offset(5)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.total_rows, 100);

        // First row should be at timestamp 5000 (offset 5)
        assert_eq!(result.rows[0].timestamp, Some(5000));
        // Last row should be at timestamp 14000
        assert_eq!(result.rows[9].timestamp, Some(14000));

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_limit_preserves_total_rows_semantic() {
        // Verify that total_rows reports pre-limit count (for pagination UIs)
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        for i in 0..50 {
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

        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .limit(5)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 5);
        assert_eq!(result.total_rows, 50); // Pre-limit count

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_single_series_uses_early_termination() {
        // Single series queries should use storage-level early termination
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write 100 points to a single series
        for i in 0..100 {
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

        // Query with LIMIT and tag filter (single series)
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .where_tag("host", "server01")
            .limit(10)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);

        // Verify ascending order
        for i in 0..10 {
            assert_eq!(result.rows[i].timestamp, Some(i as i64 * 1000));
        }

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_multi_series_limit_returns_correct_results() {
        // Multi-series queries should still return correct results (no early termination)
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write points to two series with interleaved timestamps
        for i in 0..50 {
            // Server01: even timestamps (0, 2000, 4000, ...)
            let p1 = create_test_point("server01", i * 2000, i as f64);
            storage.write(&p1).unwrap();
            series_index.upsert(p1.series_id(), "cpu", &p1.tags, p1.timestamp);
            tag_index.index_series(p1.series_id(), &p1.tags);

            // Server02: odd timestamps (1000, 3000, 5000, ...)
            let p2 = create_test_point("server02", i * 2000 + 1000, (50 + i) as f64);
            storage.write(&p2).unwrap();
            series_index.upsert(p2.series_id(), "cpu", &p2.tags, p2.timestamp);
            tag_index.index_series(p2.series_id(), &p2.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Query without tag filter (both series)
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .limit(10)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.total_rows, 100); // Both series

        // Verify results are interleaved and in ascending order
        // Expected: 0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000
        for i in 0..10 {
            assert_eq!(result.rows[i].timestamp, Some(i as i64 * 1000));
        }

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_limit_descending_order() {
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        for i in 0..100 {
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

        // Query with ORDER BY time DESC
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .order_by("time", false) // descending
            .limit(10)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);

        // Verify descending order (largest timestamps first)
        for i in 0..10 {
            assert_eq!(result.rows[i].timestamp, Some((99 - i) as i64 * 1000));
        }

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_limit_with_same_timestamps_across_series() {
        // Test that multiple points with the same timestamp are correctly returned
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Write points to two series with the SAME timestamps
        for i in 0..10 {
            let p1 = create_test_point("server01", i * 1000, i as f64);
            storage.write(&p1).unwrap();
            series_index.upsert(p1.series_id(), "cpu", &p1.tags, p1.timestamp);
            tag_index.index_series(p1.series_id(), &p1.tags);

            let p2 = create_test_point("server02", i * 1000, (10 + i) as f64); // Same timestamp!
            storage.write(&p2).unwrap();
            series_index.upsert(p2.series_id(), "cpu", &p2.tags, p2.timestamp);
            tag_index.index_series(p2.series_id(), &p2.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        let query = Query::builder("cpu")
            .time_range(0, 100000)
            .limit(10)
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.total_rows, 20);

        // Should have mix of both series (both have timestamps 0, 1000, 2000, ...)
        let server01_count = result.rows.iter()
            .filter(|r| r.tags.iter().any(|t| t.key == "host" && t.value == "server01"))
            .count();
        let server02_count = result.rows.iter()
            .filter(|r| r.tags.iter().any(|t| t.key == "host" && t.value == "server02"))
            .count();

        // With same timestamps, we should get a mix of both
        assert!(server01_count > 0, "Should have some server01 rows");
        assert!(server02_count > 0, "Should have some server02 rows");
        assert_eq!(server01_count + server02_count, 10);

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_filter_ordering_by_cardinality() {
        // Test that filters are applied in cardinality order
        // This doesn't test correctness of results (already tested), but
        // ensures the ordering logic doesn't break queries
        let (storage, series_index, tag_index, _dir) = setup_test_env();

        // Create data with varying cardinalities:
        // - region has 2 values: us-west (5 series), us-east (5 series)
        // - device_id has 10 values: device-0..9 (1 series each)
        for i in 0..10 {
            let region = if i < 5 { "us-west" } else { "us-east" };
            let device_id = format!("device-{}", i);

            let point = Point::builder("metrics")
                .timestamp(i * 1000)
                .tag("region", region)
                .tag("device_id", &device_id)
                .field("value", i as f64)
                .build()
                .unwrap();

            storage.write(&point).unwrap();
            series_index.upsert(point.series_id(), "metrics", &point.tags, point.timestamp);
            tag_index.index_series(point.series_id(), &point.tags);
        }

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Query with both filters - device_id filter (cardinality 1) should be
        // applied before region filter (cardinality 5) for optimal bitmap shrinking
        let query = Query::builder("metrics")
            .time_range(0, 100000)
            .where_tag("region", "us-west")
            .where_tag("device_id", "device-2")
            .build()
            .unwrap();

        let result = executor.execute(query).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].tags.iter().any(|t| t.key == "device_id" && t.value == "device-2"));
        assert!(result.rows[0].tags.iter().any(|t| t.key == "region" && t.value == "us-west"));

        storage.shutdown().unwrap();
    }

    #[test]
    fn test_segment_stats_aggregation_pushdown() {
        // Test that segment stats can be used for COUNT/MIN/MAX/SUM
        // when data is in segments (not memtable)
        use rusts_storage::memtable::FlushTrigger;

        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            flush_trigger: FlushTrigger {
                max_size: 1, // Force immediate flush
                max_points: 1,
                max_age_nanos: 0,
                out_of_order_lag_ms: 0,
            },
            ..Default::default()
        };

        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        // Write data - should flush to segments
        for i in 0..100 {
            let point = create_test_point("server01", (i + 1) * 1000000, (i + 1) as f64);
            storage.write(&point).unwrap();

            let series_id = point.series_id();
            series_index.upsert(series_id, "cpu", &point.tags, point.timestamp);
            tag_index.index_series(series_id, &point.tags);
        }

        // Force flush to ensure data is in segments
        storage.flush().ok();

        // Wait briefly for flush to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        let executor = QueryExecutor::new(
            Arc::clone(&storage),
            Arc::clone(&series_index),
            Arc::clone(&tag_index),
        );

        // Test COUNT aggregation
        let count_query = Query::builder("cpu")
            .time_range(0, 1000000000)
            .select_aggregate("value", AggregateFunction::Count, Some("count".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(count_query).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(FieldValue::Integer(count)) = result.rows[0].fields.get("count") {
            assert_eq!(*count, 100, "COUNT should return 100");
        } else {
            panic!("Expected count field");
        }

        // Test MIN aggregation
        let min_query = Query::builder("cpu")
            .time_range(0, 1000000000)
            .select_aggregate("value", AggregateFunction::Min, Some("min_value".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(min_query).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(FieldValue::Float(min)) = result.rows[0].fields.get("min_value") {
            assert!((min - 1.0).abs() < 0.01, "MIN should be 1.0, got {}", min);
        } else {
            panic!("Expected min_value field");
        }

        // Test MAX aggregation
        let max_query = Query::builder("cpu")
            .time_range(0, 1000000000)
            .select_aggregate("value", AggregateFunction::Max, Some("max_value".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(max_query).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(FieldValue::Float(max)) = result.rows[0].fields.get("max_value") {
            assert!((max - 100.0).abs() < 0.01, "MAX should be 100.0, got {}", max);
        } else {
            panic!("Expected max_value field");
        }

        // Test SUM aggregation
        let sum_query = Query::builder("cpu")
            .time_range(0, 1000000000)
            .select_aggregate("value", AggregateFunction::Sum, Some("sum_value".to_string()))
            .build()
            .unwrap();

        let result = executor.execute(sum_query).unwrap();
        assert_eq!(result.rows.len(), 1);
        if let Some(FieldValue::Float(sum)) = result.rows[0].fields.get("sum_value") {
            // Sum of 1..100 = 100*101/2 = 5050
            assert!((sum - 5050.0).abs() < 0.01, "SUM should be 5050.0, got {}", sum);
        } else {
            panic!("Expected sum_value field");
        }

        storage.shutdown().unwrap();
    }
}
