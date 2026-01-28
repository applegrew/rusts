//! InfluxDB Line Protocol Parser
//!
//! Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
//!
//! Example: cpu,host=server01,region=us-west usage=64.5,cores=8i 1609459200000000000

use crate::error::{ApiError, Result};
use rusts_core::{Field, FieldValue, Point, Tag};

/// Line protocol parser
pub struct LineProtocolParser;

impl LineProtocolParser {
    /// Parse a single line
    pub fn parse_line(line: &str) -> Result<Point> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return Err(ApiError::Parse("Empty or comment line".to_string()));
        }

        // Split into parts: measurement,tags fields timestamp
        let parts: Vec<&str> = Self::split_line(line)?;

        if parts.is_empty() || parts.len() > 3 {
            return Err(ApiError::Parse(format!("Invalid line format: {}", line)));
        }

        // Parse measurement and tags
        let (measurement, tags) = Self::parse_measurement_tags(parts[0])?;

        // Parse fields
        if parts.len() < 2 {
            return Err(ApiError::Parse("Missing fields".to_string()));
        }
        let fields = Self::parse_fields(parts[1])?;

        // Parse timestamp (optional)
        let timestamp = if parts.len() == 3 {
            parts[2]
                .parse::<i64>()
                .map_err(|e| ApiError::Parse(format!("Invalid timestamp: {}", e)))?
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64
        };

        // Build point
        let mut builder = Point::builder(measurement).timestamp(timestamp);

        for tag in tags {
            builder = builder.tag(tag.key, tag.value);
        }

        for field in fields {
            builder = builder.field(field.key, field.value);
        }

        builder.build().map_err(|e| ApiError::Parse(e.to_string()))
    }

    /// Parse multiple lines
    pub fn parse_lines(input: &str) -> Vec<Result<Point>> {
        input
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .map(Self::parse_line)
            .collect()
    }

    /// Parse and collect only successful points
    pub fn parse_lines_ok(input: &str) -> (Vec<Point>, Vec<String>) {
        let mut points = Vec::new();
        let mut errors = Vec::new();

        for line in input.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            match Self::parse_line(line) {
                Ok(point) => points.push(point),
                Err(e) => errors.push(format!("{}: {}", line, e)),
            }
        }

        (points, errors)
    }

    fn split_line(line: &str) -> Result<Vec<&str>> {
        // Split by unquoted spaces
        let mut parts = Vec::new();
        let mut current_start = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for (i, c) in line.char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match c {
                '\\' => escape_next = true,
                '"' => in_string = !in_string,
                ' ' if !in_string => {
                    if i > current_start {
                        parts.push(&line[current_start..i]);
                    }
                    current_start = i + 1;
                }
                _ => {}
            }
        }

        if current_start < line.len() {
            parts.push(&line[current_start..]);
        }

        Ok(parts)
    }

    fn parse_measurement_tags(s: &str) -> Result<(String, Vec<Tag>)> {
        let mut parts = s.splitn(2, ',');

        let measurement = parts
            .next()
            .ok_or_else(|| ApiError::Parse("Missing measurement".to_string()))?
            .to_string();

        if measurement.is_empty() {
            return Err(ApiError::Parse("Empty measurement".to_string()));
        }

        let tags = if let Some(tag_str) = parts.next() {
            Self::parse_tags(tag_str)?
        } else {
            Vec::new()
        };

        Ok((measurement, tags))
    }

    fn parse_tags(s: &str) -> Result<Vec<Tag>> {
        if s.is_empty() {
            return Ok(Vec::new());
        }

        let mut tags = Vec::new();

        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            let mut kv = pair.splitn(2, '=');
            let key = kv
                .next()
                .ok_or_else(|| ApiError::Parse(format!("Invalid tag: {}", pair)))?;
            let value = kv
                .next()
                .ok_or_else(|| ApiError::Parse(format!("Invalid tag: {}", pair)))?;

            tags.push(Tag::new(
                Self::unescape(key),
                Self::unescape(value),
            ));
        }

        Ok(tags)
    }

    fn parse_fields(s: &str) -> Result<Vec<Field>> {
        if s.is_empty() {
            return Err(ApiError::Parse("Empty fields".to_string()));
        }

        let mut fields = Vec::new();

        // Handle commas inside quoted strings
        let pairs = Self::split_field_pairs(s);

        for pair in pairs {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            let mut kv = pair.splitn(2, '=');
            let key = kv
                .next()
                .ok_or_else(|| ApiError::Parse(format!("Invalid field: {}", pair)))?;
            let value_str = kv
                .next()
                .ok_or_else(|| ApiError::Parse(format!("Invalid field: {}", pair)))?;

            let value = Self::parse_field_value(value_str)?;
            fields.push(Field::new(Self::unescape(key), value));
        }

        if fields.is_empty() {
            return Err(ApiError::Parse("No fields".to_string()));
        }

        Ok(fields)
    }

    fn split_field_pairs(s: &str) -> Vec<&str> {
        let mut pairs = Vec::new();
        let mut current_start = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for (i, c) in s.char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match c {
                '\\' => escape_next = true,
                '"' => in_string = !in_string,
                ',' if !in_string => {
                    if i > current_start {
                        pairs.push(&s[current_start..i]);
                    }
                    current_start = i + 1;
                }
                _ => {}
            }
        }

        if current_start < s.len() {
            pairs.push(&s[current_start..]);
        }

        pairs
    }

    fn parse_field_value(s: &str) -> Result<FieldValue> {
        let s = s.trim();

        // Boolean
        if s == "true" || s == "t" || s == "T" || s == "TRUE" {
            return Ok(FieldValue::Boolean(true));
        }
        if s == "false" || s == "f" || s == "F" || s == "FALSE" {
            return Ok(FieldValue::Boolean(false));
        }

        // String (quoted)
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            let inner = &s[1..s.len() - 1];
            return Ok(FieldValue::String(Self::unescape(inner)));
        }

        // Integer (ends with 'i')
        if s.ends_with('i') {
            let num_str = &s[..s.len() - 1];
            let value = num_str
                .parse::<i64>()
                .map_err(|e| ApiError::Parse(format!("Invalid integer: {}", e)))?;
            return Ok(FieldValue::Integer(value));
        }

        // Unsigned integer (ends with 'u')
        if s.ends_with('u') {
            let num_str = &s[..s.len() - 1];
            let value = num_str
                .parse::<u64>()
                .map_err(|e| ApiError::Parse(format!("Invalid unsigned: {}", e)))?;
            return Ok(FieldValue::UnsignedInteger(value));
        }

        // Float (default for numbers)
        let value = s
            .parse::<f64>()
            .map_err(|e| ApiError::Parse(format!("Invalid number: {}", e)))?;
        Ok(FieldValue::Float(value))
    }

    fn unescape(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars();

        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(next) = chars.next() {
                    match next {
                        'n' => result.push('\n'),
                        'r' => result.push('\r'),
                        't' => result.push('\t'),
                        '\\' => result.push('\\'),
                        '"' => result.push('"'),
                        ',' => result.push(','),
                        '=' => result.push('='),
                        ' ' => result.push(' '),
                        _ => {
                            result.push('\\');
                            result.push(next);
                        }
                    }
                } else {
                    result.push('\\');
                }
            } else {
                result.push(c);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_line() {
        let line = "cpu,host=server01 value=64.5 1609459200000000000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.timestamp, 1609459200000000000);
        assert_eq!(point.tags.len(), 1);
        assert_eq!(point.tags[0].key, "host");
        assert_eq!(point.tags[0].value, "server01");
        assert_eq!(point.fields.len(), 1);
        assert_eq!(point.fields[0].key, "value");
    }

    #[test]
    fn test_parse_multiple_tags_fields() {
        let line = "cpu,host=server01,region=us-west usage=64.5,cores=8i,active=true 1609459200000000000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        assert_eq!(point.tags.len(), 2);
        assert_eq!(point.fields.len(), 3);

        // Check field types
        assert!(matches!(point.fields[0].value, FieldValue::Float(_)));
        assert!(matches!(point.fields[1].value, FieldValue::Integer(8)));
        assert!(matches!(point.fields[2].value, FieldValue::Boolean(true)));
    }

    #[test]
    fn test_parse_string_field() {
        let line = r#"events,host=server01 message="Hello, World!" 1609459200000000000"#;
        let point = LineProtocolParser::parse_line(line).unwrap();

        if let FieldValue::String(s) = &point.fields[0].value {
            assert_eq!(s, "Hello, World!");
        } else {
            panic!("Expected string field");
        }
    }

    #[test]
    fn test_parse_escaped_characters() {
        let line = r#"events,host=server\ 01 message="Line1\nLine2" 1609459200000000000"#;
        let point = LineProtocolParser::parse_line(line).unwrap();

        assert_eq!(point.tags[0].value, "server 01");

        if let FieldValue::String(s) = &point.fields[0].value {
            assert_eq!(s, "Line1\nLine2");
        } else {
            panic!("Expected string field");
        }
    }

    #[test]
    fn test_parse_no_timestamp() {
        let line = "cpu,host=server01 value=64.5";
        let point = LineProtocolParser::parse_line(line).unwrap();

        // Timestamp should be auto-generated (current time)
        assert!(point.timestamp > 0);
    }

    #[test]
    fn test_parse_no_tags() {
        let line = "cpu value=64.5 1609459200000000000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        assert!(point.tags.is_empty());
    }

    #[test]
    fn test_parse_unsigned_integer() {
        let line = "metrics value=100u 1609459200000000000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        assert!(matches!(point.fields[0].value, FieldValue::UnsignedInteger(100)));
    }

    #[test]
    fn test_parse_boolean_variations() {
        for (line, expected) in [
            ("test value=true 1000", true),
            ("test value=t 1000", true),
            ("test value=T 1000", true),
            ("test value=TRUE 1000", true),
            ("test value=false 1000", false),
            ("test value=f 1000", false),
            ("test value=F 1000", false),
            ("test value=FALSE 1000", false),
        ] {
            let point = LineProtocolParser::parse_line(line).unwrap();
            assert_eq!(point.fields[0].value, FieldValue::Boolean(expected));
        }
    }

    #[test]
    fn test_parse_multiple_lines() {
        let input = r#"
cpu,host=server01 value=64.5 1000
cpu,host=server02 value=70.0 2000
# This is a comment
cpu,host=server03 value=80.0 3000
"#;

        let results = LineProtocolParser::parse_lines(input);
        assert_eq!(results.len(), 3);

        for result in results {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_parse_invalid_lines() {
        // Missing fields
        assert!(LineProtocolParser::parse_line("cpu,host=server01").is_err());

        // Empty measurement
        assert!(LineProtocolParser::parse_line(",host=server01 value=1").is_err());

        // Invalid field value
        assert!(LineProtocolParser::parse_line("cpu value=abc 1000").is_err());

        // Invalid timestamp
        assert!(LineProtocolParser::parse_line("cpu value=1 notanumber").is_err());
    }

    #[test]
    fn test_parse_lines_ok() {
        let input = r#"
cpu,host=server01 value=64.5 1000
invalid_line
cpu,host=server02 value=70.0 2000
"#;

        let (points, errors) = LineProtocolParser::parse_lines_ok(input);

        assert_eq!(points.len(), 2);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_parse_negative_numbers() {
        let line = "temperature,location=outside value=-15.5 1000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        if let FieldValue::Float(v) = point.fields[0].value {
            assert!((v - (-15.5)).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_parse_scientific_notation() {
        let line = "tiny value=1.5e-10 1000";
        let point = LineProtocolParser::parse_line(line).unwrap();

        if let FieldValue::Float(v) = point.fields[0].value {
            assert!((v - 1.5e-10).abs() < f64::EPSILON);
        } else {
            panic!("Expected float");
        }
    }
}
