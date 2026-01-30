//! Aggregation functions for time series data
//!
//! Uses optimized streaming algorithms where possible:
//! - Welford's algorithm for mean/variance/stddev (single-pass)
//! - Running min/max/sum/count without storing values
//! - Only stores values when necessary (First, Last, Percentile)

use crate::error::{QueryError, Result};
use rusts_core::FieldValue;
use serde::{Deserialize, Serialize};

/// Supported aggregation functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    /// Count of values
    Count,
    /// Sum of values
    Sum,
    /// Mean (average) of values
    Mean,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// First value (by time)
    First,
    /// Last value (by time)
    Last,
    /// Standard deviation
    StdDev,
    /// Variance
    Variance,
    /// Percentile (requires parameter)
    Percentile(u8),
}

impl AggregateFunction {
    /// Parse from string
    pub fn from_str(s: &str) -> Result<Self> {
        let s_lower = s.to_lowercase();
        match s_lower.as_str() {
            "count" => Ok(AggregateFunction::Count),
            "sum" => Ok(AggregateFunction::Sum),
            "mean" | "avg" | "average" => Ok(AggregateFunction::Mean),
            "min" => Ok(AggregateFunction::Min),
            "max" => Ok(AggregateFunction::Max),
            "first" => Ok(AggregateFunction::First),
            "last" => Ok(AggregateFunction::Last),
            "stddev" | "std_dev" => Ok(AggregateFunction::StdDev),
            "variance" | "var" => Ok(AggregateFunction::Variance),
            _ if s_lower.starts_with("percentile_") => {
                let p = s_lower.strip_prefix("percentile_")
                    .and_then(|p| p.parse::<u8>().ok())
                    .filter(|&p| p <= 100)
                    .ok_or_else(|| QueryError::InvalidAggregation(s.to_string()))?;
                Ok(AggregateFunction::Percentile(p))
            }
            _ if s_lower.starts_with("p") && s_lower.len() <= 4 => {
                let p = s_lower.strip_prefix("p")
                    .and_then(|p| p.parse::<u8>().ok())
                    .filter(|&p| p <= 100)
                    .ok_or_else(|| QueryError::InvalidAggregation(s.to_string()))?;
                Ok(AggregateFunction::Percentile(p))
            }
            _ => Err(QueryError::InvalidAggregation(s.to_string())),
        }
    }

    /// Returns true if this aggregation requires storing all values
    #[inline]
    fn requires_stored_values(&self) -> bool {
        matches!(self, AggregateFunction::First | AggregateFunction::Last | AggregateFunction::Percentile(_))
    }
}

/// Streaming state for Welford's algorithm (mean/variance/stddev)
#[derive(Default)]
struct WelfordState {
    count: u64,
    mean: f64,
    m2: f64, // Sum of squared deviations from mean
}

impl WelfordState {
    #[inline]
    fn new() -> Self {
        Self::default()
    }

    /// Add a value using Welford's online algorithm
    #[inline]
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    /// Merge another Welford state into this one (parallel Welford algorithm)
    ///
    /// This allows combining variance computations from parallel streams.
    #[inline]
    fn merge(&mut self, other: &WelfordState) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = WelfordState {
                count: other.count,
                mean: other.mean,
                m2: other.m2,
            };
            return;
        }

        let combined_count = self.count + other.count;
        let delta = other.mean - self.mean;

        // Combined mean
        let combined_mean = self.mean + delta * other.count as f64 / combined_count as f64;

        // Combined M2 using parallel Welford formula
        let combined_m2 = self.m2 + other.m2
            + delta * delta * self.count as f64 * other.count as f64 / combined_count as f64;

        self.count = combined_count;
        self.mean = combined_mean;
        self.m2 = combined_m2;
    }

    #[inline]
    fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / (self.count - 1) as f64
        }
    }

    #[inline]
    fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// Aggregator for computing aggregate values using streaming algorithms
pub struct Aggregator {
    function: AggregateFunction,
    /// Running count
    count: u64,
    /// Running sum
    sum: f64,
    /// Running min
    min: f64,
    /// Running max
    max: f64,
    /// Welford state for variance/stddev
    welford: WelfordState,
    /// Stored values (only used for First, Last, Percentile)
    values: Vec<f64>,
}

impl Aggregator {
    /// Create a new aggregator
    pub fn new(function: AggregateFunction) -> Self {
        Self {
            function,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            welford: WelfordState::new(),
            values: if function.requires_stored_values() {
                Vec::new()
            } else {
                Vec::new() // Won't be used but keeps size predictable
            },
        }
    }

    /// Add a value to the aggregation
    #[inline]
    pub fn add(&mut self, value: &FieldValue) {
        if let Some(v) = value.as_f64() {
            if !v.is_nan() {
                self.add_f64(v);
            }
        }
    }

    /// Add a f64 value directly (internal fast path)
    #[inline]
    fn add_f64(&mut self, v: f64) {
        // Update streaming aggregates
        self.count += 1;
        self.sum += v;

        // Update min/max (branchless would be ideal but this is clear)
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }

        // Update Welford state for variance/stddev
        self.welford.add(v);

        // Store value only if needed
        if self.function.requires_stored_values() {
            self.values.push(v);
        }
    }

    /// Add multiple values
    pub fn add_all(&mut self, values: &[FieldValue]) {
        for v in values {
            self.add(v);
        }
    }

    /// Compute the aggregate result
    pub fn result(&self) -> Option<FieldValue> {
        if self.count == 0 {
            return match self.function {
                AggregateFunction::Count => Some(FieldValue::Integer(0)),
                _ => None,
            };
        }

        match self.function {
            AggregateFunction::Count => Some(FieldValue::Integer(self.count as i64)),

            AggregateFunction::Sum => Some(FieldValue::Float(self.sum)),

            AggregateFunction::Mean => Some(FieldValue::Float(self.sum / self.count as f64)),

            AggregateFunction::Min => Some(FieldValue::Float(self.min)),

            AggregateFunction::Max => Some(FieldValue::Float(self.max)),

            AggregateFunction::First => {
                self.values.first().map(|&v| FieldValue::Float(v))
            }

            AggregateFunction::Last => {
                self.values.last().map(|&v| FieldValue::Float(v))
            }

            AggregateFunction::StdDev => {
                Some(FieldValue::Float(self.welford.stddev()))
            }

            AggregateFunction::Variance => {
                Some(FieldValue::Float(self.welford.variance()))
            }

            AggregateFunction::Percentile(p) => {
                if self.values.is_empty() {
                    return None;
                }

                let mut sorted = self.values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                let rank = (p as f64 / 100.0) * (sorted.len() - 1) as f64;
                let lower = rank.floor() as usize;
                let upper = rank.ceil() as usize;

                if lower == upper {
                    Some(FieldValue::Float(sorted[lower]))
                } else {
                    let frac = rank - lower as f64;
                    let value = sorted[lower] * (1.0 - frac) + sorted[upper] * frac;
                    Some(FieldValue::Float(value))
                }
            }
        }
    }

    /// Merge another aggregator into this one (for parallel aggregation)
    ///
    /// Combines the state from two aggregators that processed different data.
    /// Useful for parallelizing aggregation across multiple series.
    pub fn merge(&mut self, other: &Aggregator) {
        if other.count == 0 {
            return;
        }

        // Merge streaming aggregates
        self.count += other.count;
        self.sum += other.sum;

        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Merge Welford state (for variance/stddev)
        self.welford.merge(&other.welford);

        // Merge stored values (for First, Last, Percentile)
        // Note: For First/Last, order matters - we append other's values
        // which may not preserve global ordering. This is acceptable for
        // parallel aggregation where we don't have a global timestamp order.
        if self.function.requires_stored_values() {
            self.values.extend_from_slice(&other.values);
        }
    }

    /// Reset the aggregator
    pub fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.welford = WelfordState::new();
        self.values.clear();
    }

    /// Get the number of values
    pub fn count(&self) -> usize {
        self.count as usize
    }
}

/// Time-bucketed aggregator for GROUP BY time queries
pub struct TimeBucketAggregator {
    /// Bucket interval in nanoseconds
    interval: i64,
    /// Start time
    start: i64,
    /// Aggregators per bucket
    buckets: Vec<Aggregator>,
}

/// Maximum number of buckets to prevent memory exhaustion
const MAX_BUCKETS: usize = 1_000_000;

impl TimeBucketAggregator {
    /// Create a new time bucket aggregator
    ///
    /// Uses saturating arithmetic to handle extreme time ranges (like i64::MIN to i64::MAX)
    /// and caps the number of buckets to prevent memory exhaustion.
    pub fn new(function: AggregateFunction, start: i64, end: i64, interval: i64) -> Self {
        // Use checked arithmetic to prevent overflow
        // If end - start overflows, use MAX_BUCKETS as the fallback
        let num_buckets = if let Some(range) = end.checked_sub(start) {
            // Calculate (range + interval - 1) / interval with overflow protection
            let adjusted = range.saturating_add(interval.saturating_sub(1));
            let buckets = (adjusted / interval) as usize;
            buckets.min(MAX_BUCKETS)
        } else {
            // Overflow occurred (e.g., i64::MAX - i64::MIN), use max buckets
            MAX_BUCKETS
        };

        let buckets = (0..num_buckets).map(|_| Aggregator::new(function)).collect();

        Self {
            interval,
            start,
            buckets,
        }
    }

    /// Add a value with timestamp
    #[inline]
    pub fn add(&mut self, timestamp: i64, value: &FieldValue) {
        // Use checked subtraction to handle timestamps before start
        if let Some(offset) = timestamp.checked_sub(self.start) {
            if offset >= 0 {
                let bucket_idx = (offset / self.interval) as usize;
                if bucket_idx < self.buckets.len() {
                    self.buckets[bucket_idx].add(value);
                }
            }
        }
    }

    /// Get results as (bucket_start_timestamp, value) pairs
    pub fn results(&self) -> Vec<(i64, Option<FieldValue>)> {
        self.buckets
            .iter()
            .enumerate()
            .map(|(i, agg)| {
                let bucket_start = self.start + (i as i64 * self.interval);
                (bucket_start, agg.result())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_count() {
        let mut agg = Aggregator::new(AggregateFunction::Count);
        agg.add(&FieldValue::Float(1.0));
        agg.add(&FieldValue::Float(2.0));
        agg.add(&FieldValue::Float(3.0));

        assert_eq!(agg.result(), Some(FieldValue::Integer(3)));
    }

    #[test]
    fn test_aggregate_sum() {
        let mut agg = Aggregator::new(AggregateFunction::Sum);
        agg.add(&FieldValue::Float(1.0));
        agg.add(&FieldValue::Float(2.0));
        agg.add(&FieldValue::Float(3.0));

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 6.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_mean() {
        let mut agg = Aggregator::new(AggregateFunction::Mean);
        agg.add(&FieldValue::Float(1.0));
        agg.add(&FieldValue::Float(2.0));
        agg.add(&FieldValue::Float(3.0));

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 2.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_min_max() {
        let values = vec![
            FieldValue::Float(5.0),
            FieldValue::Float(2.0),
            FieldValue::Float(8.0),
            FieldValue::Float(1.0),
        ];

        let mut min_agg = Aggregator::new(AggregateFunction::Min);
        let mut max_agg = Aggregator::new(AggregateFunction::Max);

        for v in &values {
            min_agg.add(v);
            max_agg.add(v);
        }

        if let Some(FieldValue::Float(v)) = min_agg.result() {
            assert!((v - 1.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }

        if let Some(FieldValue::Float(v)) = max_agg.result() {
            assert!((v - 8.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_first_last() {
        let mut first_agg = Aggregator::new(AggregateFunction::First);
        let mut last_agg = Aggregator::new(AggregateFunction::Last);

        for i in 1..=5 {
            first_agg.add(&FieldValue::Float(i as f64));
            last_agg.add(&FieldValue::Float(i as f64));
        }

        if let Some(FieldValue::Float(v)) = first_agg.result() {
            assert!((v - 1.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }

        if let Some(FieldValue::Float(v)) = last_agg.result() {
            assert!((v - 5.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_stddev() {
        let mut agg = Aggregator::new(AggregateFunction::StdDev);
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, StdDev ≈ 2.138
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.add(&FieldValue::Float(v));
        }

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 2.138).abs() < 0.01);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_percentile() {
        let mut agg = Aggregator::new(AggregateFunction::Percentile(50));
        for i in 1..=100 {
            agg.add(&FieldValue::Float(i as f64));
        }

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 50.0).abs() < 1.0);
        } else {
            panic!("Expected float");
        }

        // 90th percentile
        let mut agg = Aggregator::new(AggregateFunction::Percentile(90));
        for i in 1..=100 {
            agg.add(&FieldValue::Float(i as f64));
        }

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 90.0).abs() < 1.0);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_empty() {
        let agg = Aggregator::new(AggregateFunction::Sum);
        assert!(agg.result().is_none());

        let agg = Aggregator::new(AggregateFunction::Count);
        assert_eq!(agg.result(), Some(FieldValue::Integer(0)));
    }

    #[test]
    fn test_aggregate_nan() {
        let mut agg = Aggregator::new(AggregateFunction::Sum);
        agg.add(&FieldValue::Float(1.0));
        agg.add(&FieldValue::Float(f64::NAN));
        agg.add(&FieldValue::Float(2.0));

        // NaN should be ignored
        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 3.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregate_from_integers() {
        let mut agg = Aggregator::new(AggregateFunction::Sum);
        agg.add(&FieldValue::Integer(1));
        agg.add(&FieldValue::Integer(2));
        agg.add(&FieldValue::Integer(3));

        if let Some(FieldValue::Float(v)) = agg.result() {
            assert!((v - 6.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_time_bucket_aggregator() {
        let mut agg = TimeBucketAggregator::new(
            AggregateFunction::Mean,
            0,           // start
            60000,       // end (60 seconds)
            10000,       // 10 second buckets
        );

        // Add values to different buckets
        agg.add(5000, &FieldValue::Float(10.0));   // bucket 0
        agg.add(15000, &FieldValue::Float(20.0));  // bucket 1
        agg.add(16000, &FieldValue::Float(30.0));  // bucket 1
        agg.add(45000, &FieldValue::Float(40.0));  // bucket 4

        let results = agg.results();
        assert_eq!(results.len(), 6); // 6 buckets

        // Check bucket 0
        if let Some(FieldValue::Float(v)) = &results[0].1 {
            assert!((v - 10.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected value in bucket 0");
        }

        // Check bucket 1 (mean of 20 and 30 = 25)
        if let Some(FieldValue::Float(v)) = &results[1].1 {
            assert!((v - 25.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected value in bucket 1");
        }

        // Bucket 2 should be empty
        assert!(results[2].1.is_none());
    }

    #[test]
    fn test_aggregate_function_parse() {
        assert_eq!(AggregateFunction::from_str("count").unwrap(), AggregateFunction::Count);
        assert_eq!(AggregateFunction::from_str("SUM").unwrap(), AggregateFunction::Sum);
        assert_eq!(AggregateFunction::from_str("Mean").unwrap(), AggregateFunction::Mean);
        assert_eq!(AggregateFunction::from_str("avg").unwrap(), AggregateFunction::Mean);
        assert_eq!(AggregateFunction::from_str("p99").unwrap(), AggregateFunction::Percentile(99));
        assert_eq!(AggregateFunction::from_str("percentile_50").unwrap(), AggregateFunction::Percentile(50));

        assert!(AggregateFunction::from_str("invalid").is_err());
    }

    #[test]
    fn test_time_bucket_extreme_range_no_panic() {
        // This should not panic even with extreme time range (i64::MIN to i64::MAX)
        // Previously this would cause "attempt to subtract with overflow"
        let mut agg = TimeBucketAggregator::new(
            AggregateFunction::Count,
            i64::MIN,
            i64::MAX,
            1_000_000_000, // 1 second buckets
        );

        // Should be capped at MAX_BUCKETS
        let results = agg.results();
        assert!(results.len() <= super::MAX_BUCKETS);

        // Adding values should not panic
        agg.add(0, &FieldValue::Float(1.0));
        agg.add(1_000_000_000, &FieldValue::Float(2.0));
    }

    #[test]
    fn test_time_bucket_timestamp_before_start() {
        let mut agg = TimeBucketAggregator::new(
            AggregateFunction::Sum,
            1000,  // start at 1000
            5000,  // end at 5000
            1000,  // 1000 ns buckets
        );

        // Add value before start - should be safely ignored
        agg.add(500, &FieldValue::Float(100.0));

        // Add value within range
        agg.add(1500, &FieldValue::Float(10.0));

        let results = agg.results();
        // First bucket should only have the 10.0 value
        if let Some(FieldValue::Float(v)) = &results[0].1 {
            assert!((v - 10.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected value in bucket 0");
        }
    }

    #[test]
    fn test_time_bucket_negative_timestamps() {
        // Test with negative timestamps (which can occur in some time systems)
        let mut agg = TimeBucketAggregator::new(
            AggregateFunction::Mean,
            -10000,  // start
            10000,   // end
            5000,    // 5000 ns buckets
        );

        agg.add(-8000, &FieldValue::Float(1.0));  // bucket 0
        agg.add(-3000, &FieldValue::Float(2.0));  // bucket 1
        agg.add(2000, &FieldValue::Float(3.0));   // bucket 2
        agg.add(7000, &FieldValue::Float(4.0));   // bucket 3

        let results = agg.results();
        assert_eq!(results.len(), 4);

        // Verify bucket timestamps
        assert_eq!(results[0].0, -10000);
        assert_eq!(results[1].0, -5000);
        assert_eq!(results[2].0, 0);
        assert_eq!(results[3].0, 5000);
    }

    #[test]
    fn test_time_bucket_overflow_start_minus_timestamp() {
        // Test case where timestamp - start could overflow
        let mut agg = TimeBucketAggregator::new(
            AggregateFunction::Sum,
            i64::MIN + 1000,  // start very close to i64::MIN
            i64::MIN + 10000, // small range
            1000,
        );

        // This timestamp would cause (timestamp - start) to overflow if not handled
        // because i64::MIN - (i64::MIN + 1000) would be very negative
        agg.add(i64::MIN, &FieldValue::Float(100.0));

        // Should be safely ignored, not panic
        let results = agg.results();
        // First bucket should be empty since the value was before start
        assert!(results[0].1.is_none());
    }

    #[test]
    fn test_welford_accuracy() {
        // Test that Welford's algorithm gives same results as two-pass
        let values: Vec<f64> = (1..=1000).map(|i| i as f64).collect();

        // Using Aggregator (Welford)
        let mut agg = Aggregator::new(AggregateFunction::StdDev);
        for v in &values {
            agg.add(&FieldValue::Float(*v));
        }
        let welford_stddev = if let Some(FieldValue::Float(v)) = agg.result() { v } else { panic!() };

        // Two-pass calculation
        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let variance: f64 = values.iter()
            .map(|v| (v - mean) * (v - mean))
            .sum::<f64>() / (values.len() - 1) as f64;
        let two_pass_stddev = variance.sqrt();

        // Should be very close (within floating point precision)
        assert!((welford_stddev - two_pass_stddev).abs() < 1e-10);
    }

    #[test]
    fn test_aggregator_merge() {
        // Test merging aggregators (used for parallel aggregation)
        let values1: Vec<f64> = (1..=500).map(|i| i as f64).collect();
        let values2: Vec<f64> = (501..=1000).map(|i| i as f64).collect();

        // Single aggregator with all values
        let mut single = Aggregator::new(AggregateFunction::Sum);
        for v in values1.iter().chain(values2.iter()) {
            single.add(&FieldValue::Float(*v));
        }

        // Two aggregators merged
        let mut agg1 = Aggregator::new(AggregateFunction::Sum);
        let mut agg2 = Aggregator::new(AggregateFunction::Sum);
        for v in &values1 {
            agg1.add(&FieldValue::Float(*v));
        }
        for v in &values2 {
            agg2.add(&FieldValue::Float(*v));
        }
        agg1.merge(&agg2);

        // Results should match
        if let (Some(FieldValue::Float(s)), Some(FieldValue::Float(m))) = (single.result(), agg1.result()) {
            assert!((s - m).abs() < f64::EPSILON, "Sum mismatch: {} vs {}", s, m);
        } else {
            panic!("Expected float results");
        }
    }

    #[test]
    fn test_aggregator_merge_mean() {
        // Test that merged mean is correct
        let mut agg1 = Aggregator::new(AggregateFunction::Mean);
        let mut agg2 = Aggregator::new(AggregateFunction::Mean);

        for v in [1.0, 2.0, 3.0] {
            agg1.add(&FieldValue::Float(v));
        }
        for v in [4.0, 5.0, 6.0] {
            agg2.add(&FieldValue::Float(v));
        }

        agg1.merge(&agg2);

        // Mean of 1,2,3,4,5,6 = 3.5
        if let Some(FieldValue::Float(v)) = agg1.result() {
            assert!((v - 3.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregator_merge_min_max() {
        let mut agg1 = Aggregator::new(AggregateFunction::Min);
        let mut agg2 = Aggregator::new(AggregateFunction::Min);

        agg1.add(&FieldValue::Float(5.0));
        agg1.add(&FieldValue::Float(10.0));
        agg2.add(&FieldValue::Float(3.0));
        agg2.add(&FieldValue::Float(8.0));

        agg1.merge(&agg2);

        if let Some(FieldValue::Float(v)) = agg1.result() {
            assert!((v - 3.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }

        // Test max
        let mut agg1 = Aggregator::new(AggregateFunction::Max);
        let mut agg2 = Aggregator::new(AggregateFunction::Max);

        agg1.add(&FieldValue::Float(5.0));
        agg1.add(&FieldValue::Float(10.0));
        agg2.add(&FieldValue::Float(3.0));
        agg2.add(&FieldValue::Float(15.0));

        agg1.merge(&agg2);

        if let Some(FieldValue::Float(v)) = agg1.result() {
            assert!((v - 15.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_aggregator_merge_stddev() {
        // Verify parallel Welford gives same result as sequential
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();

        // Sequential
        let mut sequential = Aggregator::new(AggregateFunction::StdDev);
        for v in &values {
            sequential.add(&FieldValue::Float(*v));
        }

        // Parallel (split in half)
        let mut agg1 = Aggregator::new(AggregateFunction::StdDev);
        let mut agg2 = Aggregator::new(AggregateFunction::StdDev);
        for v in &values[..50] {
            agg1.add(&FieldValue::Float(*v));
        }
        for v in &values[50..] {
            agg2.add(&FieldValue::Float(*v));
        }
        agg1.merge(&agg2);

        if let (Some(FieldValue::Float(s)), Some(FieldValue::Float(m))) = (sequential.result(), agg1.result()) {
            assert!((s - m).abs() < 1e-10, "StdDev mismatch: {} vs {}", s, m);
        } else {
            panic!("Expected float results");
        }
    }
}
