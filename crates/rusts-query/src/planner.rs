//! Query planner with partition pruning

use crate::error::Result;
use crate::model::{Query, TagFilter};
use rusts_core::{SeriesId, TimeRange};

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
        let time_cost = (query.time_range.end - query.time_range.start) as f64 / 1_000_000_000.0;
        let filter_discount = if query.tag_filters.is_empty() { 1.0 } else { 0.5 };

        let estimated_cost = (base_cost + time_cost) * filter_discount;

        Ok(QueryPlan {
            query,
            partition_ranges,
            series_ids: None, // Will be resolved during execution
            estimated_cost,
        })
    }

    /// Plan with known series IDs (from index lookup)
    pub fn plan_with_series(&self, query: Query, series_ids: Vec<SeriesId>) -> Result<QueryPlan> {
        let mut plan = self.plan(query)?;

        // Reduce cost estimate based on number of series
        plan.estimated_cost *= series_ids.len() as f64 / 1000.0;
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
}
