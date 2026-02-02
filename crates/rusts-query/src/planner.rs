//! Query planner with partition pruning and time-series specific optimizations

use crate::error::Result;
use crate::model::{Query, TagFilter};
use rusts_core::{SeriesId, TimeRange};
use serde::Serialize;

/// Execution hints for time-series specific optimizations.
///
/// These hints are computed during planning and used by the executor
/// to choose optimal execution paths without complex cost models.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutionHints {
    /// Query can be served from memtable alone (hot data routing)
    pub memtable_only: bool,
    /// Aggregation can use segment statistics (no data decompression)
    pub use_segment_stats: bool,
    /// Tag filters were reordered by cardinality for optimal bitmap shrinking
    pub filters_reordered: bool,
    /// Filter order with cardinalities: (filter description, cardinality)
    /// For EXPLAIN output to show which filters are applied first
    pub filter_order: Vec<(String, usize)>,
    /// Number of partitions to scan
    pub partition_count: usize,
    /// Estimated number of series matching filters
    pub estimated_series: usize,
    /// Estimated number of points to scan
    pub estimated_points: u64,
}

impl ExecutionHints {
    /// Create new hints with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create hints indicating memtable-only query
    pub fn memtable_only() -> Self {
        Self {
            memtable_only: true,
            partition_count: 0,
            ..Default::default()
        }
    }

    /// Create hints indicating segment stats can be used
    pub fn with_segment_stats() -> Self {
        Self {
            use_segment_stats: true,
            ..Default::default()
        }
    }
}

/// Query plan describing how to execute a query
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Original query
    pub query: Query,
    /// Partition time ranges to scan
    pub partition_ranges: Vec<TimeRange>,
    /// Series IDs to scan (if known)
    pub series_ids: Option<Vec<SeriesId>>,
    /// Estimated cost (lower is better)
    pub estimated_cost: f64,
    /// Execution hints for time-series specific optimizations
    pub hints: ExecutionHints,
}

impl QueryPlan {
    /// Format the query plan for EXPLAIN output
    pub fn explain(&self) -> ExplainOutput {
        let mut optimizations = Vec::new();

        if self.hints.memtable_only {
            optimizations.push("memtable_only: true (query within memtable time range)".to_string());
        }
        if self.hints.use_segment_stats {
            optimizations.push("segment_stats_pushdown: true (aggregation uses segment statistics)".to_string());
        }
        if self.hints.filters_reordered {
            optimizations.push("filter_reordering: true (filters ordered by cardinality)".to_string());
        }

        ExplainOutput {
            measurement: self.query.measurement.clone(),
            time_range: self.query.time_range,
            optimizations,
            filter_order: self.hints.filter_order.clone(),
            partitions_to_scan: self.hints.partition_count,
            estimated_series: self.hints.estimated_series,
            estimated_points: self.hints.estimated_points,
        }
    }
}

/// Structured EXPLAIN output for JSON serialization
#[derive(Debug, Clone, Serialize)]
pub struct ExplainOutput {
    /// Measurement being queried
    pub measurement: String,
    /// Time range of the query
    pub time_range: TimeRange,
    /// List of optimizations applied
    pub optimizations: Vec<String>,
    /// Filter order with cardinalities
    pub filter_order: Vec<(String, usize)>,
    /// Number of partitions to scan
    pub partitions_to_scan: usize,
    /// Estimated series count
    pub estimated_series: usize,
    /// Estimated point count
    pub estimated_points: u64,
}

/// Query planner for optimizing query execution
pub struct QueryPlanner {
    /// Available partition time ranges
    partition_ranges: Vec<TimeRange>,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            partition_ranges: Vec::new(),
        }
    }

    /// Set available partition ranges
    pub fn set_partitions(&mut self, ranges: Vec<TimeRange>) {
        self.partition_ranges = ranges;
        self.partition_ranges.sort_by_key(|r| r.start);
    }

    /// Plan a query
    pub fn plan(&self, query: Query) -> Result<QueryPlan> {
        // Prune partitions based on time range
        let partition_ranges = self.prune_partitions(&query.time_range);

        // Estimate cost based on:
        // - Number of partitions to scan
        // - Time range width
        // - Whether we have series filters
        let base_cost = partition_ranges.len() as f64 * 100.0;
        // Use saturating_sub to prevent overflow with extreme time ranges (i64::MIN to i64::MAX)
        let time_range_nanos = query.time_range.end.saturating_sub(query.time_range.start);
        let time_cost = time_range_nanos as f64 / 1_000_000_000.0;
        let filter_discount = if query.tag_filters.is_empty() { 1.0 } else { 0.5 };

        let estimated_cost = (base_cost + time_cost) * filter_discount;

        // Initialize hints with partition count
        let hints = ExecutionHints {
            partition_count: partition_ranges.len(),
            ..Default::default()
        };

        Ok(QueryPlan {
            query,
            partition_ranges,
            series_ids: None, // Will be resolved during execution
            estimated_cost,
            hints,
        })
    }

    /// Plan with known series IDs (from index lookup)
    pub fn plan_with_series(&self, query: Query, series_ids: Vec<SeriesId>) -> Result<QueryPlan> {
        let mut plan = self.plan(query)?;

        // Reduce cost estimate based on number of series
        plan.estimated_cost *= series_ids.len() as f64 / 1000.0;
        plan.hints.estimated_series = series_ids.len();
        plan.series_ids = Some(series_ids);

        Ok(plan)
    }

    /// Prune partitions based on query time range
    fn prune_partitions(&self, query_range: &TimeRange) -> Vec<TimeRange> {
        self.partition_ranges
            .iter()
            .filter(|pr| pr.overlaps(query_range))
            .cloned()
            .collect()
    }

    /// Estimate selectivity of tag filters (0.0 - 1.0)
    pub fn estimate_selectivity(&self, filters: &[TagFilter]) -> f64 {
        if filters.is_empty() {
            return 1.0;
        }

        // Simple heuristic: each filter roughly halves the result set
        let mut selectivity = 1.0;
        for filter in filters {
            match filter {
                TagFilter::Equals { .. } => selectivity *= 0.1,
                TagFilter::NotEquals { .. } => selectivity *= 0.9,
                TagFilter::Regex { .. } => selectivity *= 0.3,
                TagFilter::In { values, .. } => selectivity *= (values.len() as f64 * 0.1).min(0.5),
                TagFilter::Exists { .. } => selectivity *= 0.7,
            }
        }

        selectivity.max(0.001)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Query optimizer for rewriting queries
pub struct QueryOptimizer;

impl QueryOptimizer {
    /// Optimize a query plan
    pub fn optimize(plan: QueryPlan) -> QueryPlan {
        // Currently a no-op, but could include:
        // - Filter pushdown
        // - Predicate simplification
        // - Join reordering (for future multi-measurement queries)
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Query;

    #[test]
    fn test_partition_pruning() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![
            TimeRange::new(0, 1000),
            TimeRange::new(1000, 2000),
            TimeRange::new(2000, 3000),
            TimeRange::new(3000, 4000),
        ]);

        let query = Query::builder("cpu")
            .time_range(500, 2500)
            .build()
            .unwrap();

        let plan = planner.plan(query).unwrap();

        // Should include partitions 0-1000, 1000-2000, 2000-3000
        assert_eq!(plan.partition_ranges.len(), 3);
    }

    #[test]
    fn test_partition_pruning_single() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![
            TimeRange::new(0, 1000),
            TimeRange::new(1000, 2000),
            TimeRange::new(2000, 3000),
        ]);

        let query = Query::builder("cpu")
            .time_range(1100, 1900)
            .build()
            .unwrap();

        let plan = planner.plan(query).unwrap();

        // Should only include partition 1000-2000
        assert_eq!(plan.partition_ranges.len(), 1);
        assert_eq!(plan.partition_ranges[0].start, 1000);
    }

    #[test]
    fn test_partition_pruning_none() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![
            TimeRange::new(0, 1000),
            TimeRange::new(1000, 2000),
        ]);

        let query = Query::builder("cpu")
            .time_range(5000, 6000)
            .build()
            .unwrap();

        let plan = planner.plan(query).unwrap();

        assert!(plan.partition_ranges.is_empty());
    }

    #[test]
    fn test_selectivity_estimation() {
        let planner = QueryPlanner::new();

        // No filters
        assert!((planner.estimate_selectivity(&[]) - 1.0).abs() < f64::EPSILON);

        // Equals filter
        let filters = vec![TagFilter::Equals {
            key: "host".to_string(),
            value: "server01".to_string(),
        }];
        assert!(planner.estimate_selectivity(&filters) < 0.2);

        // Multiple filters
        let filters = vec![
            TagFilter::Equals {
                key: "host".to_string(),
                value: "server01".to_string(),
            },
            TagFilter::Equals {
                key: "region".to_string(),
                value: "us-west".to_string(),
            },
        ];
        assert!(planner.estimate_selectivity(&filters) < 0.02);
    }

    #[test]
    fn test_cost_estimation() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![
            TimeRange::new(0, 86400_000_000_000), // 1 day
        ]);

        // Query with no filters
        let q1 = Query::builder("cpu")
            .time_range(0, 3600_000_000_000) // 1 hour
            .build()
            .unwrap();

        // Query with filter
        let q2 = Query::builder("cpu")
            .time_range(0, 3600_000_000_000)
            .where_tag("host", "server01")
            .build()
            .unwrap();

        let plan1 = planner.plan(q1).unwrap();
        let plan2 = planner.plan(q2).unwrap();

        // Filtered query should have lower cost
        assert!(plan2.estimated_cost < plan1.estimated_cost);
    }

    #[test]
    fn test_plan_extreme_time_range_no_panic() {
        // Test that planning with extreme time range (i64::MIN to i64::MAX) does not panic
        // This was previously causing "attempt to subtract with overflow"
        let planner = QueryPlanner::new();

        // Create a query with the default time range (i64::MIN to i64::MAX)
        // We have to manually construct this since Query::builder validates
        let query = Query {
            measurement: "cpu".to_string(),
            time_range: TimeRange::new(i64::MIN, i64::MAX),
            tag_filters: Vec::new(),
            field_selection: crate::model::FieldSelection::All,
            group_by: Vec::new(),
            group_by_time: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        // This should not panic
        let plan = planner.plan(query).unwrap();

        // Cost should be finite and positive
        assert!(plan.estimated_cost.is_finite());
        assert!(plan.estimated_cost >= 0.0);
    }

    #[test]
    fn test_execution_hints_default() {
        let hints = ExecutionHints::default();
        assert!(!hints.memtable_only);
        assert!(!hints.use_segment_stats);
        assert!(!hints.filters_reordered);
        assert!(hints.filter_order.is_empty());
        assert_eq!(hints.partition_count, 0);
        assert_eq!(hints.estimated_series, 0);
        assert_eq!(hints.estimated_points, 0);
    }

    #[test]
    fn test_execution_hints_memtable_only() {
        let hints = ExecutionHints::memtable_only();
        assert!(hints.memtable_only);
        assert_eq!(hints.partition_count, 0);
    }

    #[test]
    fn test_execution_hints_with_segment_stats() {
        let hints = ExecutionHints::with_segment_stats();
        assert!(hints.use_segment_stats);
    }

    #[test]
    fn test_plan_includes_hints() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![
            TimeRange::new(0, 1000),
            TimeRange::new(1000, 2000),
        ]);

        let query = Query::builder("cpu")
            .time_range(0, 1500)
            .build()
            .unwrap();

        let plan = planner.plan(query).unwrap();

        // Should have hints with partition count
        assert_eq!(plan.hints.partition_count, 2);
    }

    #[test]
    fn test_plan_explain() {
        let mut planner = QueryPlanner::new();
        planner.set_partitions(vec![TimeRange::new(0, 1000)]);

        let query = Query::builder("cpu")
            .time_range(0, 500)
            .build()
            .unwrap();

        let mut plan = planner.plan(query).unwrap();
        plan.hints.memtable_only = true;
        plan.hints.estimated_series = 5;
        plan.hints.estimated_points = 1000;

        let explain = plan.explain();
        assert_eq!(explain.measurement, "cpu");
        assert_eq!(explain.partitions_to_scan, 1);
        assert_eq!(explain.estimated_series, 5);
        assert_eq!(explain.estimated_points, 1000);
        assert!(explain.optimizations.iter().any(|o| o.contains("memtable_only")));
    }
}
