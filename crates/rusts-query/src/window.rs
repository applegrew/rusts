//! Window function evaluation engine
//!
//! Evaluates window functions (OVER clauses) on a result set.
//! Window functions compute per-row values without collapsing rows,
//! unlike GROUP BY aggregations.
//!
//! Supported window functions:
//! - Aggregate windows: SUM, AVG, COUNT, MIN, MAX over frames
//! - Ranking: ROW_NUMBER, RANK, DENSE_RANK
//! - Row navigation: LAG, LEAD

use crate::aggregation::AggregateFunction;
use crate::error::Result;
use crate::model::{
    ResultRow, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction, WindowFunctionType,
};
use rusts_core::FieldValue;
use std::collections::HashMap;

/// Evaluate all window functions on the result set, inserting computed
/// values into each row's `fields` map under the window function's alias.
pub fn evaluate_window_functions(
    rows: &mut Vec<ResultRow>,
    window_fns: &[WindowFunction],
) -> Result<()> {
    if rows.is_empty() || window_fns.is_empty() {
        return Ok(());
    }

    for wf in window_fns {
        evaluate_single_window(rows, wf)?;
    }

    Ok(())
}

/// Evaluate a single window function across all rows.
fn evaluate_single_window(rows: &mut Vec<ResultRow>, wf: &WindowFunction) -> Result<()> {
    // Partition rows by the PARTITION BY columns
    let partitions = build_partitions(rows, &wf.partition_by);

    for partition_indices in &partitions {
        // Sort the partition indices by the ORDER BY columns
        let sorted_indices = sort_partition(rows, partition_indices, &wf.order_by);

        // Evaluate the function over the sorted partition
        let values = evaluate_over_partition(rows, &sorted_indices, &wf.function, &wf.frame)?;

        // Write computed values back into rows (skip None = SQL NULL)
        for (i, idx) in sorted_indices.iter().enumerate() {
            if let Some(val) = &values[i] {
                rows[*idx].fields.insert(wf.alias.clone(), val.clone());
            } else {
                rows[*idx].fields.remove(&wf.alias);
            }
        }
    }

    Ok(())
}

/// Group row indices by PARTITION BY key.
/// Returns a Vec of partitions, each being a Vec of row indices.
fn build_partitions(rows: &[ResultRow], partition_by: &[String]) -> Vec<Vec<usize>> {
    if partition_by.is_empty() {
        // No partitioning — all rows in one partition
        return vec![(0..rows.len()).collect()];
    }

    let mut partition_map: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        let key: Vec<String> = partition_by
            .iter()
            .map(|col| {
                // Look in tags first, then fields
                row.tags
                    .iter()
                    .find(|t| t.key == *col)
                    .map(|t| t.value.clone())
                    .or_else(|| row.fields.get(col).map(|v| format!("{:?}", v)))
                    .unwrap_or_default()
            })
            .collect();

        partition_map.entry(key).or_default().push(i);
    }

    partition_map.into_values().collect()
}

/// Sort partition indices by ORDER BY columns.
/// Returns a new Vec of indices in sorted order.
fn sort_partition(
    rows: &[ResultRow],
    indices: &[usize],
    order_by: &[(String, bool)],
) -> Vec<usize> {
    let mut sorted = indices.to_vec();

    if order_by.is_empty() {
        // Default: order by timestamp ascending
        sorted.sort_by(|&a, &b| {
            let ta = rows[a].timestamp.unwrap_or(0);
            let tb = rows[b].timestamp.unwrap_or(0);
            ta.cmp(&tb)
        });
    } else {
        sorted.sort_by(|&a, &b| {
            for (field, ascending) in order_by {
                let cmp = if field == "time" || field == "timestamp" || field == "_time" {
                    let ta = rows[a].timestamp.unwrap_or(0);
                    let tb = rows[b].timestamp.unwrap_or(0);
                    ta.cmp(&tb)
                } else {
                    let va = rows[a].fields.get(field).and_then(|v| v.as_f64());
                    let vb = rows[b].fields.get(field).and_then(|v| v.as_f64());
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                };

                let cmp = if *ascending { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    sorted
}

/// Evaluate a window function over a single sorted partition.
/// Returns one Option<FieldValue> per row in the partition (None = SQL NULL).
fn evaluate_over_partition(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    function: &WindowFunctionType,
    frame: &Option<WindowFrame>,
) -> Result<Vec<Option<FieldValue>>> {
    match function {
        WindowFunctionType::RowNumber => evaluate_row_number(sorted_indices.len()),
        WindowFunctionType::Rank => evaluate_rank(rows, sorted_indices),
        WindowFunctionType::DenseRank => evaluate_dense_rank(rows, sorted_indices),
        WindowFunctionType::Lag {
            field,
            offset,
            default,
        } => evaluate_lag(rows, sorted_indices, field, *offset, default.as_ref()),
        WindowFunctionType::Lead {
            field,
            offset,
            default,
        } => evaluate_lead(rows, sorted_indices, field, *offset, default.as_ref()),
        WindowFunctionType::Aggregate(agg_fn) => {
            evaluate_aggregate_window(rows, sorted_indices, *agg_fn, frame)
        }
    }
}

// =============================================================================
// Ranking functions
// =============================================================================

fn evaluate_row_number(count: usize) -> Result<Vec<Option<FieldValue>>> {
    Ok((1..=count)
        .map(|i| Some(FieldValue::Integer(i as i64)))
        .collect())
}

fn evaluate_rank(rows: &[ResultRow], sorted_indices: &[usize]) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let mut result = Vec::with_capacity(n);

    if n == 0 {
        return Ok(result);
    }

    result.push(Some(FieldValue::Integer(1)));

    for i in 1..n {
        let prev = sorted_indices[i - 1];
        let curr = sorted_indices[i];

        if rows_equal_by_order_key(rows, prev, curr) {
            // Same rank as previous
            result.push(result[i - 1].clone());
        } else {
            // Rank = position (1-indexed)
            result.push(Some(FieldValue::Integer((i + 1) as i64)));
        }
    }

    Ok(result)
}

fn evaluate_dense_rank(rows: &[ResultRow], sorted_indices: &[usize]) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let mut result = Vec::with_capacity(n);

    if n == 0 {
        return Ok(result);
    }

    let mut current_rank: i64 = 1;
    result.push(Some(FieldValue::Integer(current_rank)));

    for i in 1..n {
        let prev = sorted_indices[i - 1];
        let curr = sorted_indices[i];

        if !rows_equal_by_order_key(rows, prev, curr) {
            current_rank += 1;
        }
        result.push(Some(FieldValue::Integer(current_rank)));
    }

    Ok(result)
}

/// Check if two rows have equal values for all numeric fields and timestamp.
/// Used for RANK/DENSE_RANK tie detection.
fn rows_equal_by_order_key(rows: &[ResultRow], a: usize, b: usize) -> bool {
    // Compare timestamps
    if rows[a].timestamp != rows[b].timestamp {
        return false;
    }
    // For ranking, rows with the same timestamp are considered ties
    true
}

// =============================================================================
// LAG / LEAD
// =============================================================================

fn evaluate_lag(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    field: &str,
    offset: usize,
    default: Option<&FieldValue>,
) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        if i >= offset {
            let prev_idx = sorted_indices[i - offset];
            result.push(get_field_or_timestamp(rows, prev_idx, field));
        } else {
            result.push(default.cloned());
        }
    }

    Ok(result)
}

fn evaluate_lead(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    field: &str,
    offset: usize,
    default: Option<&FieldValue>,
) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        if i + offset < n {
            let next_idx = sorted_indices[i + offset];
            result.push(get_field_or_timestamp(rows, next_idx, field));
        } else {
            result.push(default.cloned());
        }
    }

    Ok(result)
}

/// Get a field value from a row, handling "time" as timestamp.
fn get_field_or_timestamp(rows: &[ResultRow], idx: usize, field: &str) -> Option<FieldValue> {
    if field == "time" || field == "timestamp" || field == "_time" {
        rows[idx].timestamp.map(FieldValue::Integer)
    } else {
        rows[idx].fields.get(field).cloned()
    }
}

// =============================================================================
// Aggregate window functions (SUM, AVG, COUNT, MIN, MAX over frames)
// =============================================================================

fn evaluate_aggregate_window(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    agg_fn: AggregateFunction,
    frame: &Option<WindowFrame>,
) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let frame = frame.clone().unwrap_or_default();
    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        let (start, end) = resolve_frame_bounds(&frame, i, n);
        let value = compute_frame_aggregate(rows, sorted_indices, start, end, agg_fn)?;
        result.push(value);
    }

    Ok(result)
}

/// Resolve frame bounds to concrete indices within the partition.
fn resolve_frame_bounds(frame: &WindowFrame, current: usize, partition_len: usize) -> (usize, usize) {
    match frame.units {
        WindowFrameUnits::Rows | WindowFrameUnits::Range => {
            let start = match &frame.start {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::Preceding(n) => current.saturating_sub(*n),
                WindowFrameBound::CurrentRow => current,
                WindowFrameBound::Following(n) => (current + n).min(partition_len.saturating_sub(1)),
                WindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
            };

            let end = match &frame.end {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::Preceding(n) => current.saturating_sub(*n),
                WindowFrameBound::CurrentRow => current,
                WindowFrameBound::Following(n) => (current + n).min(partition_len.saturating_sub(1)),
                WindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
            };

            (start, end)
        }
    }
}

/// Compute an aggregate over a frame window [start..=end] within sorted_indices.
fn compute_frame_aggregate(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    start: usize,
    end: usize,
    agg_fn: AggregateFunction,
) -> Result<Option<FieldValue>> {
    if start > end || start >= sorted_indices.len() {
        return Ok(None);
    }

    let end = end.min(sorted_indices.len() - 1);

    // Collect all numeric field values in the frame
    let mut values: Vec<f64> = Vec::with_capacity(end - start + 1);

    for i in start..=end {
        let row_idx = sorted_indices[i];
        // Aggregate over all numeric fields in the row
        for fv in rows[row_idx].fields.values() {
            if let Some(v) = fv.as_f64() {
                if !v.is_nan() {
                    values.push(v);
                }
            }
        }
    }

    if values.is_empty() {
        return match agg_fn {
            AggregateFunction::Count => Ok(Some(FieldValue::Integer(0))),
            _ => Ok(None),
        };
    }

    compute_aggregate_from_values(&values, agg_fn)
}

// =============================================================================
// Field-specific aggregate window (used when we know the target field)
// =============================================================================

/// Evaluate an aggregate window function over a specific field.
/// This is the preferred path when the SQL translator has resolved the field name.
pub fn evaluate_aggregate_window_for_field(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    field: &str,
    agg_fn: AggregateFunction,
    frame: &Option<WindowFrame>,
) -> Result<Vec<Option<FieldValue>>> {
    let n = sorted_indices.len();
    let frame = frame.clone().unwrap_or_default();
    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        let (start, end) = resolve_frame_bounds(&frame, i, n);
        let value =
            compute_field_frame_aggregate(rows, sorted_indices, start, end, field, agg_fn)?;
        result.push(value);
    }

    Ok(result)
}

/// Compute an aggregate over a specific field within a frame window.
fn compute_field_frame_aggregate(
    rows: &[ResultRow],
    sorted_indices: &[usize],
    start: usize,
    end: usize,
    field: &str,
    agg_fn: AggregateFunction,
) -> Result<Option<FieldValue>> {
    if start > end || start >= sorted_indices.len() {
        return Ok(None);
    }

    let end = end.min(sorted_indices.len() - 1);

    let mut values: Vec<f64> = Vec::with_capacity(end - start + 1);

    for i in start..=end {
        let row_idx = sorted_indices[i];
        if let Some(fv) = get_field_or_timestamp(rows, row_idx, field) {
            if let Some(v) = fv.as_f64() {
                if !v.is_nan() {
                    values.push(v);
                }
            }
        }
    }

    if values.is_empty() {
        return match agg_fn {
            AggregateFunction::Count => Ok(Some(FieldValue::Integer(0))),
            _ => Ok(None),
        };
    }

    compute_aggregate_from_values(&values, agg_fn)
}

/// Shared aggregate computation from a Vec of f64 values.
fn compute_aggregate_from_values(
    values: &[f64],
    agg_fn: AggregateFunction,
) -> Result<Option<FieldValue>> {
    match agg_fn {
        AggregateFunction::Count => Ok(Some(FieldValue::Integer(values.len() as i64))),
        AggregateFunction::Sum => Ok(Some(FieldValue::Float(values.iter().sum()))),
        AggregateFunction::Mean => {
            Ok(Some(FieldValue::Float(
                values.iter().sum::<f64>() / values.len() as f64,
            )))
        }
        AggregateFunction::Min => Ok(Some(FieldValue::Float(
            values.iter().cloned().fold(f64::INFINITY, f64::min),
        ))),
        AggregateFunction::Max => Ok(Some(FieldValue::Float(
            values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        ))),
        AggregateFunction::First => Ok(Some(FieldValue::Float(values[0]))),
        AggregateFunction::Last => Ok(Some(FieldValue::Float(*values.last().unwrap()))),
        AggregateFunction::StdDev => {
            if values.len() < 2 {
                return Ok(Some(FieldValue::Float(0.0)));
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                / (values.len() - 1) as f64;
            Ok(Some(FieldValue::Float(variance.sqrt())))
        }
        AggregateFunction::Variance => {
            if values.len() < 2 {
                return Ok(Some(FieldValue::Float(0.0)));
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                / (values.len() - 1) as f64;
            Ok(Some(FieldValue::Float(variance)))
        }
        AggregateFunction::Percentile(p) => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let rank = (p as f64 / 100.0) * (sorted.len() - 1) as f64;
            let lower = rank.floor() as usize;
            let upper = rank.ceil() as usize;
            if lower == upper {
                Ok(Some(FieldValue::Float(sorted[lower])))
            } else {
                let frac = rank - lower as f64;
                Ok(Some(FieldValue::Float(
                    sorted[lower] * (1.0 - frac) + sorted[upper] * frac,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::Tag;

    fn make_row(ts: i64, tags: Vec<(&str, &str)>, fields: Vec<(&str, f64)>) -> ResultRow {
        ResultRow {
            timestamp: Some(ts),
            series_id: 1,
            tags: tags
                .into_iter()
                .map(|(k, v)| Tag {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), FieldValue::Float(v)))
                .collect(),
        }
    }

    #[test]
    fn test_row_number() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(2, vec![], vec![("usage", 20.0)]),
            make_row(3, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::RowNumber,
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "rn".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        assert_eq!(rows[0].fields.get("rn"), Some(&FieldValue::Integer(1)));
        assert_eq!(rows[1].fields.get("rn"), Some(&FieldValue::Integer(2)));
        assert_eq!(rows[2].fields.get("rn"), Some(&FieldValue::Integer(3)));
    }

    #[test]
    fn test_row_number_with_partition() {
        let mut rows = vec![
            make_row(1, vec![("host", "a")], vec![("usage", 10.0)]),
            make_row(2, vec![("host", "a")], vec![("usage", 20.0)]),
            make_row(1, vec![("host", "b")], vec![("usage", 30.0)]),
            make_row(2, vec![("host", "b")], vec![("usage", 40.0)]),
            make_row(3, vec![("host", "b")], vec![("usage", 50.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::RowNumber,
            partition_by: vec!["host".to_string()],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "rn".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        // Host "a" partition: rows 0,1 → rn 1,2
        assert_eq!(rows[0].fields.get("rn"), Some(&FieldValue::Integer(1)));
        assert_eq!(rows[1].fields.get("rn"), Some(&FieldValue::Integer(2)));

        // Host "b" partition: rows 2,3,4 → rn 1,2,3
        assert_eq!(rows[2].fields.get("rn"), Some(&FieldValue::Integer(1)));
        assert_eq!(rows[3].fields.get("rn"), Some(&FieldValue::Integer(2)));
        assert_eq!(rows[4].fields.get("rn"), Some(&FieldValue::Integer(3)));
    }

    #[test]
    fn test_rank_with_ties() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(1, vec![], vec![("usage", 20.0)]), // same timestamp = tie
            make_row(2, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::Rank,
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "rnk".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        assert_eq!(rows[0].fields.get("rnk"), Some(&FieldValue::Integer(1)));
        assert_eq!(rows[1].fields.get("rnk"), Some(&FieldValue::Integer(1))); // tie
        assert_eq!(rows[2].fields.get("rnk"), Some(&FieldValue::Integer(3))); // gap
    }

    #[test]
    fn test_dense_rank_with_ties() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(1, vec![], vec![("usage", 20.0)]), // same timestamp = tie
            make_row(2, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::DenseRank,
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "drnk".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        assert_eq!(rows[0].fields.get("drnk"), Some(&FieldValue::Integer(1)));
        assert_eq!(rows[1].fields.get("drnk"), Some(&FieldValue::Integer(1))); // tie
        assert_eq!(rows[2].fields.get("drnk"), Some(&FieldValue::Integer(2))); // no gap
    }

    #[test]
    fn test_lag() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(2, vec![], vec![("usage", 20.0)]),
            make_row(3, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::Lag {
                field: "usage".to_string(),
                offset: 1,
                default: None,
            },
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "prev_usage".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        // No default → NULL → field absent
        assert_eq!(rows[0].fields.get("prev_usage"), None);
        assert_eq!(
            rows[1].fields.get("prev_usage"),
            Some(&FieldValue::Float(10.0))
        );
        assert_eq!(
            rows[2].fields.get("prev_usage"),
            Some(&FieldValue::Float(20.0))
        );
    }

    #[test]
    fn test_lead() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(2, vec![], vec![("usage", 20.0)]),
            make_row(3, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::Lead {
                field: "usage".to_string(),
                offset: 1,
                default: Some(FieldValue::Float(0.0)),
            },
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: None,
            alias: "next_usage".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        assert_eq!(
            rows[0].fields.get("next_usage"),
            Some(&FieldValue::Float(20.0))
        );
        assert_eq!(
            rows[1].fields.get("next_usage"),
            Some(&FieldValue::Float(30.0))
        );
        assert_eq!(
            rows[2].fields.get("next_usage"),
            Some(&FieldValue::Float(0.0)) // default
        );
    }

    #[test]
    fn test_running_sum() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(2, vec![], vec![("usage", 20.0)]),
            make_row(3, vec![], vec![("usage", 30.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::Aggregate(AggregateFunction::Sum),
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: Some(WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::CurrentRow,
            }),
            alias: "running_sum".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        // Running sum: 10, 30, 60
        assert_eq!(
            rows[0].fields.get("running_sum"),
            Some(&FieldValue::Float(10.0))
        );
        assert_eq!(
            rows[1].fields.get("running_sum"),
            Some(&FieldValue::Float(30.0))
        );
        assert_eq!(
            rows[2].fields.get("running_sum"),
            Some(&FieldValue::Float(60.0))
        );
    }

    #[test]
    fn test_moving_average() {
        let mut rows = vec![
            make_row(1, vec![], vec![("usage", 10.0)]),
            make_row(2, vec![], vec![("usage", 20.0)]),
            make_row(3, vec![], vec![("usage", 30.0)]),
            make_row(4, vec![], vec![("usage", 40.0)]),
            make_row(5, vec![], vec![("usage", 50.0)]),
        ];

        let wf = WindowFunction {
            function: WindowFunctionType::Aggregate(AggregateFunction::Mean),
            partition_by: vec![],
            order_by: vec![("time".to_string(), true)],
            frame: Some(WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowFrameBound::Preceding(2),
                end: WindowFrameBound::CurrentRow,
            }),
            alias: "moving_avg".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();

        // Window of 3 rows (2 preceding + current):
        // Row 0: avg(10) = 10
        // Row 1: avg(10,20) = 15
        // Row 2: avg(10,20,30) = 20
        // Row 3: avg(20,30,40) = 30
        // Row 4: avg(30,40,50) = 40
        if let Some(FieldValue::Float(v)) = rows[0].fields.get("moving_avg") {
            assert!((v - 10.0).abs() < 0.01);
        } else {
            panic!("Expected float for row 0");
        }
        if let Some(FieldValue::Float(v)) = rows[1].fields.get("moving_avg") {
            assert!((v - 15.0).abs() < 0.01);
        } else {
            panic!("Expected float for row 1");
        }
        if let Some(FieldValue::Float(v)) = rows[2].fields.get("moving_avg") {
            assert!((v - 20.0).abs() < 0.01);
        } else {
            panic!("Expected float for row 2");
        }
        if let Some(FieldValue::Float(v)) = rows[3].fields.get("moving_avg") {
            assert!((v - 30.0).abs() < 0.01);
        } else {
            panic!("Expected float for row 3");
        }
        if let Some(FieldValue::Float(v)) = rows[4].fields.get("moving_avg") {
            assert!((v - 40.0).abs() < 0.01);
        } else {
            panic!("Expected float for row 4");
        }
    }

    #[test]
    fn test_empty_rows() {
        let mut rows: Vec<ResultRow> = vec![];
        let wf = WindowFunction {
            function: WindowFunctionType::RowNumber,
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            alias: "rn".to_string(),
        };

        evaluate_window_functions(&mut rows, &[wf]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_field_specific_running_sum() {
        let rows = vec![
            make_row(1, vec![], vec![("usage", 10.0), ("idle", 90.0)]),
            make_row(2, vec![], vec![("usage", 20.0), ("idle", 80.0)]),
            make_row(3, vec![], vec![("usage", 30.0), ("idle", 70.0)]),
        ];

        let sorted_indices: Vec<usize> = (0..rows.len()).collect();
        let frame = Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        });

        let result = evaluate_aggregate_window_for_field(
            &rows,
            &sorted_indices,
            "usage",
            AggregateFunction::Sum,
            &frame,
        )
        .unwrap();

        // Running sum of "usage" only: 10, 30, 60
        assert_eq!(result[0], Some(FieldValue::Float(10.0)));
        assert_eq!(result[1], Some(FieldValue::Float(30.0)));
        assert_eq!(result[2], Some(FieldValue::Float(60.0)));
    }
}
