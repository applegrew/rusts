//! PostgreSQL type mapping for RusTs field values

use pgwire::api::results::FieldFormat;
use pgwire::api::Type;
use rusts_core::FieldValue;

/// PostgreSQL epoch is 2000-01-01 00:00:00 UTC
/// Unix epoch is 1970-01-01 00:00:00 UTC
/// Difference in microseconds: 946684800 seconds * 1_000_000
#[allow(dead_code)]
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Map a RusTs FieldValue to a PostgreSQL Type
pub fn field_value_to_pg_type(value: &FieldValue) -> Type {
    match value {
        FieldValue::Float(_) => Type::FLOAT8,
        FieldValue::Integer(_) => Type::INT8,
        FieldValue::UnsignedInteger(_) => Type::INT8, // Cast to i64
        FieldValue::String(_) => Type::TEXT,
        FieldValue::Boolean(_) => Type::BOOL,
    }
}

/// Get the PostgreSQL type for a timestamp column
pub fn timestamp_pg_type() -> Type {
    Type::TIMESTAMPTZ
}

/// Get the default field format (text for simple query protocol)
pub fn default_field_format() -> FieldFormat {
    FieldFormat::Text
}

/// Convert nanoseconds since Unix epoch to PostgreSQL timestamp (microseconds since 2000-01-01)
#[allow(dead_code)]
pub fn nanos_to_pg_timestamp(nanos: i64) -> i64 {
    // Convert nanos to micros, then adjust for PostgreSQL epoch
    (nanos / 1000) - PG_EPOCH_OFFSET_MICROS
}

/// Format a FieldValue as a string for PostgreSQL text protocol
pub fn field_value_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Float(v) => {
            if v.is_nan() {
                "NaN".to_string()
            } else if v.is_infinite() {
                if *v > 0.0 {
                    "Infinity".to_string()
                } else {
                    "-Infinity".to_string()
                }
            } else {
                v.to_string()
            }
        }
        FieldValue::Integer(v) => v.to_string(),
        FieldValue::UnsignedInteger(v) => {
            // Cast to i64, clamping at i64::MAX if needed
            if *v > i64::MAX as u64 {
                i64::MAX.to_string()
            } else {
                (*v as i64).to_string()
            }
        }
        FieldValue::String(s) => s.clone(),
        FieldValue::Boolean(b) => {
            if *b {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
    }
}

/// Format a timestamp (nanoseconds since Unix epoch) as a PostgreSQL timestamp string
pub fn timestamp_to_string(nanos: i64) -> String {
    // Convert nanos to seconds and subsecond nanos
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000).unsigned_abs() as u32;

    // Use chrono-style formatting but without chrono dependency
    // Format: YYYY-MM-DD HH:MM:SS.microseconds+00
    let datetime = chrono_lite::timestamp_to_datetime(secs, subsec_nanos);
    datetime
}

/// Minimal chrono-like functionality for timestamp formatting
mod chrono_lite {
    /// Convert Unix timestamp to PostgreSQL-formatted datetime string
    pub fn timestamp_to_datetime(secs: i64, subsec_nanos: u32) -> String {
        // Days since Unix epoch (1970-01-01)
        let days_since_epoch = secs.div_euclid(86400);
        let time_of_day = secs.rem_euclid(86400);

        // Convert days to Y-M-D using a simplified algorithm
        let (year, month, day) = days_to_ymd(days_since_epoch as i32 + 719468);

        let hours = time_of_day / 3600;
        let minutes = (time_of_day % 3600) / 60;
        let seconds = time_of_day % 60;
        let micros = subsec_nanos / 1000;

        if micros > 0 {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00",
                year, month, day, hours, minutes, seconds, micros
            )
        } else {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00",
                year, month, day, hours, minutes, seconds
            )
        }
    }

    /// Convert days since 0000-03-01 to (year, month, day)
    /// Algorithm from Howard Hinnant's date algorithms
    fn days_to_ymd(days: i32) -> (i32, u32, u32) {
        let era = if days >= 0 { days } else { days - 146096 } / 146097;
        let doe = (days - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        (y, m, d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_value_to_pg_type() {
        assert_eq!(field_value_to_pg_type(&FieldValue::Float(1.0)), Type::FLOAT8);
        assert_eq!(
            field_value_to_pg_type(&FieldValue::Integer(1)),
            Type::INT8
        );
        assert_eq!(
            field_value_to_pg_type(&FieldValue::UnsignedInteger(1)),
            Type::INT8
        );
        assert_eq!(
            field_value_to_pg_type(&FieldValue::String("test".to_string())),
            Type::TEXT
        );
        assert_eq!(
            field_value_to_pg_type(&FieldValue::Boolean(true)),
            Type::BOOL
        );
    }

    #[test]
    fn test_nanos_to_pg_timestamp() {
        // 2000-01-01 00:00:00 UTC in nanoseconds = 946684800 * 1e9
        let unix_2000 = 946_684_800_000_000_000i64;
        assert_eq!(nanos_to_pg_timestamp(unix_2000), 0);

        // 2000-01-01 00:00:01 UTC
        let unix_2000_plus_1s = unix_2000 + 1_000_000_000;
        assert_eq!(nanos_to_pg_timestamp(unix_2000_plus_1s), 1_000_000);
    }

    #[test]
    fn test_field_value_to_string() {
        assert_eq!(field_value_to_string(&FieldValue::Float(3.14)), "3.14");
        assert_eq!(field_value_to_string(&FieldValue::Integer(-42)), "-42");
        assert_eq!(
            field_value_to_string(&FieldValue::UnsignedInteger(100)),
            "100"
        );
        assert_eq!(
            field_value_to_string(&FieldValue::String("hello".to_string())),
            "hello"
        );
        assert_eq!(field_value_to_string(&FieldValue::Boolean(true)), "t");
        assert_eq!(field_value_to_string(&FieldValue::Boolean(false)), "f");
    }

    #[test]
    fn test_field_value_float_special_values() {
        assert_eq!(field_value_to_string(&FieldValue::Float(f64::NAN)), "NaN");
        assert_eq!(
            field_value_to_string(&FieldValue::Float(f64::INFINITY)),
            "Infinity"
        );
        assert_eq!(
            field_value_to_string(&FieldValue::Float(f64::NEG_INFINITY)),
            "-Infinity"
        );
    }

    #[test]
    fn test_timestamp_to_string() {
        // 2024-01-15 12:30:45.123456 UTC
        // This is approximately 1705322445123456000 nanoseconds since Unix epoch
        let ts = 1705322445_123456000i64;
        let result = timestamp_to_string(ts);
        // The timestamp should produce a valid formatted date with +00 timezone
        assert!(result.ends_with("+00"), "Result was: {}", result);
        // It should contain the expected date components
        assert!(result.contains("2024-01-15"), "Result was: {}", result);
    }
}
