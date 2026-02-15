//! Time-series function registry
//!
//! Maps SQL function names to RusTs aggregation functions and handles
//! time-series specific functions like now() and time_bucket().

use crate::error::{Result, SqlError};
use rusts_query::AggregateFunction;

/// Registry for SQL functions supported by RusTs
pub struct FunctionRegistry;

impl FunctionRegistry {
    /// Map a SQL function name to an AggregateFunction
    pub fn get_aggregate(name: &str) -> Result<AggregateFunction> {
        let name_lower = name.to_lowercase();
        match name_lower.as_str() {
            "count" => Ok(AggregateFunction::Count),
            "sum" => Ok(AggregateFunction::Sum),
            "avg" | "mean" | "average" => Ok(AggregateFunction::Mean),
            "min" => Ok(AggregateFunction::Min),
            "max" => Ok(AggregateFunction::Max),
            "first" => Ok(AggregateFunction::First),
            "last" => Ok(AggregateFunction::Last),
            "stddev" | "stddev_samp" | "std_dev" => Ok(AggregateFunction::StdDev),
            "variance" | "var_samp" | "var" => Ok(AggregateFunction::Variance),
            _ if name_lower.starts_with("percentile_") => {
                let p = name_lower
                    .strip_prefix("percentile_")
                    .and_then(|p| p.parse::<u8>().ok())
                    .filter(|&p| p <= 100)
                    .ok_or_else(|| SqlError::InvalidAggregation(name.to_string()))?;
                Ok(AggregateFunction::Percentile(p))
            }
            _ => Err(SqlError::UnknownFunction(name.to_string())),
        }
    }

    /// Check if a function name is an aggregate function
    pub fn is_aggregate(name: &str) -> bool {
        Self::get_aggregate(name).is_ok()
    }

    /// Check if a function name is a window-only function (requires OVER clause)
    pub fn is_window_only_function(name: &str) -> bool {
        let name_lower = name.to_lowercase();
        matches!(
            name_lower.as_str(),
            "row_number" | "rank" | "dense_rank" | "lag" | "lead"
        )
    }

    /// Check if a function is a time function
    pub fn is_time_function(name: &str) -> bool {
        let name_lower = name.to_lowercase();
        matches!(name_lower.as_str(), "now" | "time_bucket")
    }

    /// Get the current timestamp in nanoseconds (for now() function)
    pub fn now() -> i64 {
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    }

    /// Parse a duration/interval string into nanoseconds
    ///
    /// Supports formats like:
    /// - '1h', '5m', '30s', '100ms', '1d', '1w'
    /// - '1 hour', '5 minutes', '30 seconds'
    /// - INTERVAL '1' HOUR (parsed from SQL)
    pub fn parse_interval(s: &str) -> Result<i64> {
        let s = s.trim().trim_matches('\'').trim_matches('"');

        if s.is_empty() {
            return Err(SqlError::InvalidInterval("Empty interval".to_string()));
        }

        // Try parsing compact format (1h, 5m, 30s, etc.)
        if let Some(nanos) = Self::parse_compact_interval(s) {
            return Ok(nanos);
        }

        // Try parsing verbose format (1 hour, 5 minutes, etc.)
        if let Some(nanos) = Self::parse_verbose_interval(s) {
            return Ok(nanos);
        }

        Err(SqlError::InvalidInterval(format!(
            "Cannot parse interval: {}",
            s
        )))
    }

    /// Parse compact interval format (1h, 5m, 30s, 100ms, 1d, 1w)
    fn parse_compact_interval(s: &str) -> Option<i64> {
        let s = s.trim();

        // Try each suffix from longest to shortest
        let (num_str, multiplier) = if s.ends_with("ns") {
            (&s[..s.len() - 2], 1i64)
        } else if s.ends_with("us") || s.ends_with("µs") {
            (&s[..s.len() - 2], 1_000i64)
        } else if s.ends_with("ms") {
            (&s[..s.len() - 2], 1_000_000i64)
        } else if s.ends_with('s') && !s.ends_with("us") && !s.ends_with("ns") && !s.ends_with("ms")
        {
            (&s[..s.len() - 1], 1_000_000_000i64)
        } else if s.ends_with('m') {
            (&s[..s.len() - 1], 60 * 1_000_000_000i64)
        } else if s.ends_with('h') {
            (&s[..s.len() - 1], 3600 * 1_000_000_000i64)
        } else if s.ends_with('d') {
            (&s[..s.len() - 1], 86400 * 1_000_000_000i64)
        } else if s.ends_with('w') {
            (&s[..s.len() - 1], 604800 * 1_000_000_000i64)
        } else {
            return None;
        };

        num_str.trim().parse::<i64>().ok().map(|n| n * multiplier)
    }

    /// Parse verbose interval format (1 hour, 5 minutes, 30 seconds)
    fn parse_verbose_interval(s: &str) -> Option<i64> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return None;
        }

        let num: i64 = parts[0].parse().ok()?;
        let unit = parts[1].to_lowercase();

        let multiplier = match unit.as_str() {
            "nanosecond" | "nanoseconds" | "ns" => 1i64,
            "microsecond" | "microseconds" | "us" => 1_000i64,
            "millisecond" | "milliseconds" | "ms" => 1_000_000i64,
            "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000_000i64,
            "minute" | "minutes" | "min" | "mins" | "m" => 60 * 1_000_000_000i64,
            "hour" | "hours" | "hr" | "hrs" | "h" => 3600 * 1_000_000_000i64,
            "day" | "days" | "d" => 86400 * 1_000_000_000i64,
            "week" | "weeks" | "w" => 604800 * 1_000_000_000i64,
            _ => return None,
        };

        Some(num * multiplier)
    }

    /// Parse a timestamp string or expression
    ///
    /// Supports:
    /// - ISO 8601 format: '2024-01-01', '2024-01-01T00:00:00Z'
    /// - now() - returns current timestamp
    /// - now() - interval '1 hour' - relative time expressions
    pub fn parse_timestamp(s: &str) -> Result<i64> {
        let s = s.trim().trim_matches('\'').trim_matches('"');

        // Check for now()
        if s.to_lowercase() == "now()" || s.to_lowercase() == "now" {
            return Ok(Self::now());
        }

        // Try ISO 8601 parsing
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
        }

        // Try date-only format
        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            let dt = date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc();
            return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
        }

        // Try datetime without timezone
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return Ok(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
        }

        // Try datetime with T separator
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return Ok(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
        }

        // Try parsing as nanoseconds directly
        if let Ok(nanos) = s.parse::<i64>() {
            return Ok(nanos);
        }

        Err(SqlError::InvalidTimeExpression(format!(
            "Cannot parse timestamp: {}",
            s
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_aggregate_count() {
        assert!(matches!(
            FunctionRegistry::get_aggregate("count").unwrap(),
            AggregateFunction::Count
        ));
        assert!(matches!(
            FunctionRegistry::get_aggregate("COUNT").unwrap(),
            AggregateFunction::Count
        ));
    }

    #[test]
    fn test_get_aggregate_mean_aliases() {
        assert!(matches!(
            FunctionRegistry::get_aggregate("avg").unwrap(),
            AggregateFunction::Mean
        ));
        assert!(matches!(
            FunctionRegistry::get_aggregate("mean").unwrap(),
            AggregateFunction::Mean
        ));
        assert!(matches!(
            FunctionRegistry::get_aggregate("average").unwrap(),
            AggregateFunction::Mean
        ));
    }

    #[test]
    fn test_get_aggregate_percentile() {
        assert!(matches!(
            FunctionRegistry::get_aggregate("percentile_50").unwrap(),
            AggregateFunction::Percentile(50)
        ));
        assert!(matches!(
            FunctionRegistry::get_aggregate("percentile_99").unwrap(),
            AggregateFunction::Percentile(99)
        ));
    }

    #[test]
    fn test_get_aggregate_unknown() {
        assert!(FunctionRegistry::get_aggregate("unknown_func").is_err());
    }

    #[test]
    fn test_is_time_function() {
        assert!(FunctionRegistry::is_time_function("now"));
        assert!(FunctionRegistry::is_time_function("NOW"));
        assert!(FunctionRegistry::is_time_function("time_bucket"));
        assert!(!FunctionRegistry::is_time_function("count"));
    }

    #[test]
    fn test_parse_compact_interval() {
        assert_eq!(FunctionRegistry::parse_interval("1h").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("30s").unwrap(), 30 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("100ms").unwrap(), 100 * 1_000_000);
        assert_eq!(FunctionRegistry::parse_interval("1d").unwrap(), 86400 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("1w").unwrap(), 604800 * 1_000_000_000);
    }

    #[test]
    fn test_parse_verbose_interval() {
        assert_eq!(FunctionRegistry::parse_interval("1 hour").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("5 minutes").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("30 seconds").unwrap(), 30 * 1_000_000_000);
    }

    #[test]
    fn test_parse_interval_with_quotes() {
        assert_eq!(FunctionRegistry::parse_interval("'1h'").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(FunctionRegistry::parse_interval("\"5m\"").unwrap(), 5 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_timestamp_iso8601() {
        // Test date-only
        let ts = FunctionRegistry::parse_timestamp("2024-01-01").unwrap();
        assert!(ts > 0);

        // Test RFC3339
        let ts = FunctionRegistry::parse_timestamp("2024-01-01T00:00:00Z").unwrap();
        assert!(ts > 0);
    }

    #[test]
    fn test_parse_timestamp_now() {
        let ts = FunctionRegistry::parse_timestamp("now()").unwrap();
        assert!(ts > 0);
    }

    #[test]
    fn test_parse_timestamp_nanoseconds() {
        let ts = FunctionRegistry::parse_timestamp("1704067200000000000").unwrap();
        assert_eq!(ts, 1704067200000000000);
    }

    #[test]
    fn test_now_returns_reasonable_value() {
        let now = FunctionRegistry::now();
        // Should be after 2024-01-01 (1704067200000000000 ns)
        assert!(now > 1704067200000000000);
    }
}
