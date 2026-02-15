//! Query model - Data structures for queries and results

use crate::aggregation::AggregateFunction;
use crate::error::{QueryError, Result};
use rusts_core::{FieldValue, SeriesId, Tag, TimeRange, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tag filter operators
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TagFilter {
    /// Exact match: tag = value
    Equals { key: String, value: String },
    /// Not equal: tag != value
    NotEquals { key: String, value: String },
    /// Regex match: tag =~ /pattern/
    Regex { key: String, pattern: String },
    /// Any of values: tag IN (v1, v2, ...)
    In { key: String, values: Vec<String> },
    /// None of values: tag NOT IN (v1, v2, ...)
    NotIn { key: String, values: Vec<String> },
    /// Tag exists
    Exists { key: String },
}

impl TagFilter {
    /// Check if a set of tags matches this filter
    pub fn matches(&self, tags: &[Tag]) -> bool {
        match self {
            TagFilter::Equals { key, value } => {
                tags.iter().any(|t| &t.key == key && &t.value == value)
            }
            TagFilter::NotEquals { key, value } => {
                !tags.iter().any(|t| &t.key == key && &t.value == value)
            }
            TagFilter::Regex { key, pattern } => {
                if let Ok(re) = regex::Regex::new(pattern) {
                    tags.iter().any(|t| &t.key == key && re.is_match(&t.value))
                } else {
                    false
                }
            }
            TagFilter::In { key, values } => {
                tags.iter().any(|t| &t.key == key && values.contains(&t.value))
            }
            TagFilter::NotIn { key, values } => {
                // If tag doesn't exist, it matches NOT IN
                // If tag exists, its value must not be in the list
                !tags.iter().any(|t| &t.key == key && values.contains(&t.value))
            }
            TagFilter::Exists { key } => tags.iter().any(|t| &t.key == key),
        }
    }
}

/// A filter expression that supports AND, OR, and NOT composition.
///
/// This allows complex filter logic like:
/// `(host = 'server01' OR host = 'server02') AND region = 'us-west'`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterExpr {
    /// A single tag filter (leaf node)
    Leaf(TagFilter),
    /// All sub-expressions must match (AND)
    And(Vec<FilterExpr>),
    /// At least one sub-expression must match (OR)
    Or(Vec<FilterExpr>),
    /// Negate a sub-expression (NOT)
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Check if a set of tags matches this filter expression
    pub fn matches(&self, tags: &[Tag]) -> bool {
        match self {
            FilterExpr::Leaf(filter) => filter.matches(tags),
            FilterExpr::And(exprs) => exprs.iter().all(|e| e.matches(tags)),
            FilterExpr::Or(exprs) => exprs.iter().any(|e| e.matches(tags)),
            FilterExpr::Not(expr) => !expr.matches(tags),
        }
    }

    /// Create a leaf filter expression
    pub fn leaf(filter: TagFilter) -> Self {
        FilterExpr::Leaf(filter)
    }

    /// Create an AND expression
    pub fn and(exprs: Vec<FilterExpr>) -> Self {
        FilterExpr::And(exprs)
    }

    /// Create an OR expression
    pub fn or(exprs: Vec<FilterExpr>) -> Self {
        FilterExpr::Or(exprs)
    }

    /// Create a NOT expression
    pub fn not(expr: FilterExpr) -> Self {
        FilterExpr::Not(Box::new(expr))
    }

    /// Check if this expression is empty (no actual filters)
    pub fn is_empty(&self) -> bool {
        match self {
            FilterExpr::Leaf(_) => false,
            FilterExpr::And(exprs) | FilterExpr::Or(exprs) => exprs.is_empty(),
            FilterExpr::Not(_) => false,
        }
    }

    /// Collect all leaf TagFilters from this expression tree (flattened)
    pub fn collect_leaf_filters(&self) -> Vec<&TagFilter> {
        match self {
            FilterExpr::Leaf(f) => vec![f],
            FilterExpr::And(exprs) | FilterExpr::Or(exprs) => {
                exprs.iter().flat_map(|e| e.collect_leaf_filters()).collect()
            }
            FilterExpr::Not(expr) => expr.collect_leaf_filters(),
        }
    }
}

impl From<Vec<TagFilter>> for FilterExpr {
    /// Convert a flat list of TagFilters into an AND expression.
    /// This preserves backward compatibility with the old Vec<TagFilter> model.
    fn from(filters: Vec<TagFilter>) -> Self {
        if filters.len() == 1 {
            FilterExpr::Leaf(filters.into_iter().next().unwrap())
        } else {
            FilterExpr::And(filters.into_iter().map(FilterExpr::Leaf).collect())
        }
    }
}

impl From<TagFilter> for FilterExpr {
    fn from(filter: TagFilter) -> Self {
        FilterExpr::Leaf(filter)
    }
}

/// Field selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldSelection {
    /// Select all fields
    All,
    /// Select specific fields
    Fields(Vec<String>),
    /// Select field with aggregation
    Aggregate {
        field: String,
        function: AggregateFunction,
        alias: Option<String>,
    },
}

impl FieldSelection {
    /// Get the list of fields required for this selection.
    ///
    /// Returns:
    /// - `None` for All (read all fields)
    /// - `Some(fields)` for Fields or Aggregate
    /// - `Some([])` for COUNT(*) (no fields needed, just count rows)
    pub fn required_fields(&self) -> Option<Vec<String>> {
        match self {
            FieldSelection::All => None,
            FieldSelection::Fields(fields) => Some(fields.clone()),
            FieldSelection::Aggregate { field, function, .. } => {
                // COUNT(*) doesn't need any specific field
                if field == "*" && *function == AggregateFunction::Count {
                    Some(Vec::new())
                } else if field == "*" {
                    None // Other aggregations on * need all fields
                } else {
                    Some(vec![field.clone()])
                }
            }
        }
    }
}

/// Query definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    /// Measurement to query
    pub measurement: String,
    /// Time range
    pub time_range: TimeRange,
    /// Tag filters (AND) - kept for backward compatibility
    pub tag_filters: Vec<TagFilter>,
    /// Filter expression tree (supports AND, OR, NOT)
    /// When present, this takes precedence over tag_filters
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>,
    /// Field selection
    pub field_selection: FieldSelection,
    /// Group by tags
    pub group_by: Vec<String>,
    /// Group by time interval (nanoseconds)
    pub group_by_time: Option<i64>,
    /// Order by (field, ascending)
    pub order_by: Option<(String, bool)>,
    /// Limit results
    pub limit: Option<usize>,
    /// Offset results
    pub offset: Option<usize>,
}

impl Query {
    /// Create a new query builder
    pub fn builder(measurement: impl Into<String>) -> QueryBuilder {
        QueryBuilder::new(measurement)
    }

    /// Get the effective filter expression for this query.
    ///
    /// If `filter` is set, it takes precedence. Otherwise, `tag_filters` is
    /// converted into a `FilterExpr::And`. Returns `None` if no filters exist.
    pub fn effective_filter(&self) -> Option<FilterExpr> {
        if let Some(ref filter) = self.filter {
            if filter.is_empty() {
                None
            } else {
                Some(filter.clone())
            }
        } else if self.tag_filters.is_empty() {
            None
        } else {
            Some(FilterExpr::from(self.tag_filters.clone()))
        }
    }

    /// Validate the query
    pub fn validate(&self) -> Result<()> {
        if self.measurement.is_empty() {
            return Err(QueryError::InvalidQuery("Empty measurement".to_string()));
        }

        if self.time_range.start >= self.time_range.end {
            return Err(QueryError::InvalidTimeRange {
                start: self.time_range.start,
                end: self.time_range.end,
            });
        }

        Ok(())
    }
}

/// Query builder for fluent API
pub struct QueryBuilder {
    measurement: String,
    time_range: TimeRange,
    tag_filters: Vec<TagFilter>,
    filter: Option<FilterExpr>,
    field_selection: FieldSelection,
    group_by: Vec<String>,
    group_by_time: Option<i64>,
    order_by: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            time_range: TimeRange::default(),
            tag_filters: Vec::new(),
            filter: None,
            field_selection: FieldSelection::All,
            group_by: Vec::new(),
            group_by_time: None,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Set time range
    pub fn time_range(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.time_range = TimeRange::new(start, end);
        self
    }

    /// Add tag equals filter
    pub fn where_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_filters.push(TagFilter::Equals {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add tag not equals filter
    pub fn where_tag_not(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_filters.push(TagFilter::NotEquals {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add tag in filter
    pub fn where_tag_in(mut self, key: impl Into<String>, values: Vec<String>) -> Self {
        self.tag_filters.push(TagFilter::In {
            key: key.into(),
            values,
        });
        self
    }

    /// Add tag not in filter
    pub fn where_tag_not_in(mut self, key: impl Into<String>, values: Vec<String>) -> Self {
        self.tag_filters.push(TagFilter::NotIn {
            key: key.into(),
            values,
        });
        self
    }

    /// Set a filter expression (supports AND, OR, NOT)
    ///
    /// When set, this takes precedence over individual where_tag* filters.
    pub fn filter(mut self, filter: FilterExpr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Select specific fields
    pub fn select_fields(mut self, fields: Vec<String>) -> Self {
        self.field_selection = FieldSelection::Fields(fields);
        self
    }

    /// Select field with aggregation
    pub fn select_aggregate(
        mut self,
        field: impl Into<String>,
        function: AggregateFunction,
        alias: Option<String>,
    ) -> Self {
        self.field_selection = FieldSelection::Aggregate {
            field: field.into(),
            function,
            alias,
        };
        self
    }

    /// Group by tags
    pub fn group_by_tags(mut self, tags: Vec<String>) -> Self {
        self.group_by = tags;
        self
    }

    /// Group by time interval
    pub fn group_by_interval(mut self, interval_nanos: i64) -> Self {
        self.group_by_time = Some(interval_nanos);
        self
    }

    /// Order by field
    pub fn order_by(mut self, field: impl Into<String>, ascending: bool) -> Self {
        self.order_by = Some((field.into(), ascending));
        self
    }

    /// Limit results
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Offset results
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Build the query
    pub fn build(self) -> Result<Query> {
        let query = Query {
            measurement: self.measurement,
            time_range: self.time_range,
            tag_filters: self.tag_filters,
            filter: self.filter,
            field_selection: self.field_selection,
            group_by: self.group_by,
            group_by_time: self.group_by_time,
            order_by: self.order_by,
            limit: self.limit,
            offset: self.offset,
        };

        query.validate()?;
        Ok(query)
    }
}

/// A single row in query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultRow {
    /// Timestamp (if applicable)
    pub timestamp: Option<Timestamp>,
    /// Series ID
    pub series_id: SeriesId,
    /// Tags for this row
    pub tags: Vec<Tag>,
    /// Field values
    pub fields: HashMap<String, FieldValue>,
}

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// The query that produced this result
    pub measurement: String,
    /// Result rows
    pub rows: Vec<ResultRow>,
    /// Total rows before limit/offset
    pub total_rows: usize,
    /// Execution time in nanoseconds
    pub execution_time_ns: u64,
}

impl QueryResult {
    /// Create an empty result
    pub fn empty(measurement: String) -> Self {
        Self {
            measurement,
            rows: Vec::new(),
            total_rows: 0,
            execution_time_ns: 0,
        }
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_builder() {
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .where_tag("host", "server01")
            .select_fields(vec!["value".to_string()])
            .limit(100)
            .build()
            .unwrap();

        assert_eq!(query.measurement, "cpu");
        assert_eq!(query.time_range.start, 0);
        assert_eq!(query.time_range.end, 1000000);
        assert_eq!(query.tag_filters.len(), 1);
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn test_query_validation() {
        // Empty measurement
        let result = Query::builder("")
            .time_range(0, 1000)
            .build();
        assert!(result.is_err());

        // Invalid time range
        let result = Query::builder("cpu")
            .time_range(1000, 0)
            .build();
        assert!(result.is_err());

        // Valid query
        let result = Query::builder("cpu")
            .time_range(0, 1000)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_tag_filter_matches() {
        let tags = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ];

        // Equals
        let filter = TagFilter::Equals {
            key: "host".to_string(),
            value: "server01".to_string(),
        };
        assert!(filter.matches(&tags));

        let filter = TagFilter::Equals {
            key: "host".to_string(),
            value: "server02".to_string(),
        };
        assert!(!filter.matches(&tags));

        // NotEquals
        let filter = TagFilter::NotEquals {
            key: "host".to_string(),
            value: "server02".to_string(),
        };
        assert!(filter.matches(&tags));

        // In
        let filter = TagFilter::In {
            key: "host".to_string(),
            values: vec!["server01".to_string(), "server02".to_string()],
        };
        assert!(filter.matches(&tags));

        // Exists
        let filter = TagFilter::Exists {
            key: "host".to_string(),
        };
        assert!(filter.matches(&tags));

        let filter = TagFilter::Exists {
            key: "nonexistent".to_string(),
        };
        assert!(!filter.matches(&tags));
    }

    #[test]
    fn test_filter_expr_matches_leaf() {
        let tags = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ];

        let expr = FilterExpr::leaf(TagFilter::Equals {
            key: "host".to_string(),
            value: "server01".to_string(),
        });
        assert!(expr.matches(&tags));

        let expr = FilterExpr::leaf(TagFilter::Equals {
            key: "host".to_string(),
            value: "server02".to_string(),
        });
        assert!(!expr.matches(&tags));
    }

    #[test]
    fn test_filter_expr_matches_and() {
        let tags = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ];

        // Both match
        let expr = FilterExpr::and(vec![
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server01".to_string(),
            }),
            FilterExpr::leaf(TagFilter::Equals {
                key: "region".to_string(),
                value: "us-west".to_string(),
            }),
        ]);
        assert!(expr.matches(&tags));

        // One doesn't match
        let expr = FilterExpr::and(vec![
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server01".to_string(),
            }),
            FilterExpr::leaf(TagFilter::Equals {
                key: "region".to_string(),
                value: "us-east".to_string(),
            }),
        ]);
        assert!(!expr.matches(&tags));
    }

    #[test]
    fn test_filter_expr_matches_or() {
        let tags = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ];

        // First matches
        let expr = FilterExpr::or(vec![
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server01".to_string(),
            }),
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server02".to_string(),
            }),
        ]);
        assert!(expr.matches(&tags));

        // Neither matches
        let expr = FilterExpr::or(vec![
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server02".to_string(),
            }),
            FilterExpr::leaf(TagFilter::Equals {
                key: "host".to_string(),
                value: "server03".to_string(),
            }),
        ]);
        assert!(!expr.matches(&tags));
    }

    #[test]
    fn test_filter_expr_matches_not() {
        let tags = vec![Tag::new("host", "server01")];

        let expr = FilterExpr::not(FilterExpr::leaf(TagFilter::Equals {
            key: "host".to_string(),
            value: "server02".to_string(),
        }));
        assert!(expr.matches(&tags));

        let expr = FilterExpr::not(FilterExpr::leaf(TagFilter::Equals {
            key: "host".to_string(),
            value: "server01".to_string(),
        }));
        assert!(!expr.matches(&tags));
    }

    #[test]
    fn test_filter_expr_nested_and_or() {
        // (host = 'server01' OR host = 'server02') AND region = 'us-west'
        let tags_match = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ];
        let tags_wrong_region = vec![
            Tag::new("host", "server01"),
            Tag::new("region", "us-east"),
        ];
        let tags_wrong_host = vec![
            Tag::new("host", "server03"),
            Tag::new("region", "us-west"),
        ];

        let expr = FilterExpr::and(vec![
            FilterExpr::or(vec![
                FilterExpr::leaf(TagFilter::Equals {
                    key: "host".to_string(),
                    value: "server01".to_string(),
                }),
                FilterExpr::leaf(TagFilter::Equals {
                    key: "host".to_string(),
                    value: "server02".to_string(),
                }),
            ]),
            FilterExpr::leaf(TagFilter::Equals {
                key: "region".to_string(),
                value: "us-west".to_string(),
            }),
        ]);

        assert!(expr.matches(&tags_match));
        assert!(!expr.matches(&tags_wrong_region));
        assert!(!expr.matches(&tags_wrong_host));
    }

    #[test]
    fn test_filter_expr_from_vec() {
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
        let expr = FilterExpr::from(filters);
        match &expr {
            FilterExpr::And(children) => assert_eq!(children.len(), 2),
            _ => panic!("Expected And, got {:?}", expr),
        }

        // Single filter should become Leaf, not And
        let filters = vec![TagFilter::Equals {
            key: "host".to_string(),
            value: "server01".to_string(),
        }];
        let expr = FilterExpr::from(filters);
        assert!(matches!(expr, FilterExpr::Leaf(_)));
    }

    #[test]
    fn test_filter_expr_is_empty() {
        assert!(!FilterExpr::leaf(TagFilter::Exists { key: "x".to_string() }).is_empty());
        assert!(FilterExpr::and(vec![]).is_empty());
        assert!(FilterExpr::or(vec![]).is_empty());
        assert!(!FilterExpr::not(FilterExpr::leaf(TagFilter::Exists { key: "x".to_string() })).is_empty());
    }

    #[test]
    fn test_effective_filter_from_tag_filters() {
        let query = Query::builder("cpu")
            .time_range(0, 1000)
            .where_tag("host", "server01")
            .build()
            .unwrap();

        let filter = query.effective_filter().expect("Expected filter");
        match &filter {
            FilterExpr::Leaf(TagFilter::Equals { key, value }) => {
                assert_eq!(key, "host");
                assert_eq!(value, "server01");
            }
            _ => panic!("Expected Leaf(Equals), got {:?}", filter),
        }
    }

    #[test]
    fn test_effective_filter_from_filter_field() {
        let query = Query::builder("cpu")
            .time_range(0, 1000)
            .filter(FilterExpr::or(vec![
                FilterExpr::leaf(TagFilter::Equals {
                    key: "host".to_string(),
                    value: "a".to_string(),
                }),
                FilterExpr::leaf(TagFilter::Equals {
                    key: "host".to_string(),
                    value: "b".to_string(),
                }),
            ]))
            .build()
            .unwrap();

        let filter = query.effective_filter().expect("Expected filter");
        assert!(matches!(filter, FilterExpr::Or(_)));
    }

    #[test]
    fn test_effective_filter_none_when_empty() {
        let query = Query::builder("cpu")
            .time_range(0, 1000)
            .build()
            .unwrap();
        assert!(query.effective_filter().is_none());
    }

    #[test]
    fn test_query_with_aggregation() {
        let query = Query::builder("cpu")
            .time_range(0, 1000000)
            .select_aggregate("value", AggregateFunction::Mean, Some("avg_value".to_string()))
            .group_by_tags(vec!["host".to_string()])
            .group_by_interval(60_000_000_000) // 1 minute
            .build()
            .unwrap();

        assert!(matches!(query.field_selection, FieldSelection::Aggregate { .. }));
        assert_eq!(query.group_by, vec!["host"]);
        assert_eq!(query.group_by_time, Some(60_000_000_000));
    }
}
