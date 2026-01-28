//! Core data types for RusTs time series database

use crate::error::{CoreError, Result};
use fxhash::FxHasher;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Nanosecond-precision Unix epoch timestamp
pub type Timestamp = i64;

/// Unique identifier for a time series (hash of measurement + sorted tags)
pub type SeriesId = u64;

/// A tag is a key-value pair used for series identification
/// Tags are indexed and used for filtering queries
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl Tag {
    /// Create a new tag
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Validate the tag
    pub fn validate(&self) -> Result<()> {
        if self.key.is_empty() {
            return Err(CoreError::EmptyTagKey);
        }
        Ok(())
    }
}

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => self.value.cmp(&other.value),
            other => other,
        }
    }
}

/// Field value types supported by the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// 64-bit floating point
    Float(f64),
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit unsigned integer
    UnsignedInteger(u64),
    /// UTF-8 string
    String(String),
    /// Boolean value
    Boolean(bool),
}

impl FieldValue {
    /// Get the type name of this field value
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Float(_) => "float",
            FieldValue::Integer(_) => "integer",
            FieldValue::UnsignedInteger(_) => "unsigned",
            FieldValue::String(_) => "string",
            FieldValue::Boolean(_) => "boolean",
        }
    }

    /// Try to convert to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            FieldValue::Integer(v) => Some(*v as f64),
            FieldValue::UnsignedInteger(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(v) => Some(*v),
            FieldValue::UnsignedInteger(v) => {
                if *v <= i64::MAX as u64 {
                    Some(*v as i64)
                } else {
                    None
                }
            }
            FieldValue::Float(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        FieldValue::Float(v)
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        FieldValue::Integer(v)
    }
}

impl From<u64> for FieldValue {
    fn from(v: u64) -> Self {
        FieldValue::UnsignedInteger(v)
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        FieldValue::String(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        FieldValue::String(v.to_string())
    }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        FieldValue::Boolean(v)
    }
}

/// A field is a named value in a data point
/// Fields are not indexed and store the actual measurements
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: FieldValue,
}

impl Field {
    /// Create a new field
    pub fn new(key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Validate the field
    pub fn validate(&self) -> Result<()> {
        if self.key.is_empty() {
            return Err(CoreError::EmptyFieldKey);
        }
        Ok(())
    }
}

/// A data point represents a single measurement at a specific time
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// The measurement name (like a table name)
    pub measurement: String,
    /// Timestamp in nanoseconds since Unix epoch
    pub timestamp: Timestamp,
    /// Tags for series identification (indexed)
    pub tags: Vec<Tag>,
    /// Field values (not indexed)
    pub fields: Vec<Field>,
}

impl Point {
    /// Create a new point builder
    pub fn builder(measurement: impl Into<String>) -> PointBuilder {
        PointBuilder::new(measurement)
    }

    /// Validate the point
    pub fn validate(&self) -> Result<()> {
        if self.measurement.is_empty() {
            return Err(CoreError::EmptyMeasurement);
        }
        if self.fields.is_empty() {
            return Err(CoreError::NoFields);
        }
        for tag in &self.tags {
            tag.validate()?;
        }
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }

    /// Compute the series ID for this point
    pub fn series_id(&self) -> SeriesId {
        compute_series_id(&self.measurement, &self.tags)
    }

    /// Sort tags alphabetically by key (required for consistent series ID)
    pub fn sort_tags(&mut self) {
        self.tags.sort();
    }

    /// Get a tag value by key
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|t| t.key == key)
            .map(|t| t.value.as_str())
    }

    /// Get a field value by key
    pub fn get_field(&self, key: &str) -> Option<&FieldValue> {
        self.fields.iter().find(|f| f.key == key).map(|f| &f.value)
    }
}

/// Builder for constructing Points
pub struct PointBuilder {
    measurement: String,
    timestamp: Option<Timestamp>,
    tags: Vec<Tag>,
    fields: Vec<Field>,
}

impl PointBuilder {
    /// Create a new point builder
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            timestamp: None,
            tags: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// Set the timestamp
    pub fn timestamp(mut self, ts: Timestamp) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Add a tag
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push(Tag::new(key, value));
        self
    }

    /// Add a field
    pub fn field(mut self, key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.push(Field::new(key, value));
        self
    }

    /// Build the point
    pub fn build(mut self) -> Result<Point> {
        // Sort tags for consistent series ID computation
        self.tags.sort();

        let point = Point {
            measurement: self.measurement,
            timestamp: self.timestamp.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64
            }),
            tags: self.tags,
            fields: self.fields,
        };

        point.validate()?;
        Ok(point)
    }
}

/// Series metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Series {
    /// Unique series identifier
    pub id: SeriesId,
    /// Measurement name
    pub measurement: String,
    /// Tags that identify this series
    pub tags: Vec<Tag>,
}

impl Series {
    /// Create a new series from a point
    pub fn from_point(point: &Point) -> Self {
        let mut tags = point.tags.clone();
        tags.sort();

        Self {
            id: compute_series_id(&point.measurement, &tags),
            measurement: point.measurement.clone(),
            tags,
        }
    }

    /// Create a new series
    pub fn new(measurement: impl Into<String>, tags: Vec<Tag>) -> Self {
        let measurement = measurement.into();
        let mut sorted_tags = tags;
        sorted_tags.sort();
        let id = compute_series_id(&measurement, &sorted_tags);

        Self {
            id,
            measurement,
            tags: sorted_tags,
        }
    }
}

/// Compute a series ID from measurement and tags
/// Uses FxHash for fast hashing
pub fn compute_series_id(measurement: &str, tags: &[Tag]) -> SeriesId {
    let mut hasher = FxHasher::default();
    measurement.hash(&mut hasher);
    for tag in tags {
        tag.key.hash(&mut hasher);
        tag.value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Time range for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: Timestamp,
    /// End timestamp (exclusive)
    pub end: Timestamp,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp falls within this range
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts >= self.start && ts < self.end
    }

    /// Check if this range overlaps with another
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Get the duration of this range in nanoseconds
    pub fn duration_nanos(&self) -> i64 {
        self.end - self.start
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self {
            start: i64::MIN,
            end: i64::MAX,
        }
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database name
    pub name: String,
    /// Data directory path
    pub data_dir: String,
    /// WAL directory path (defaults to data_dir/wal)
    pub wal_dir: Option<String>,
    /// Maximum memory for write buffer (bytes)
    pub max_memtable_size: usize,
    /// Partition duration in nanoseconds
    pub partition_duration: i64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            data_dir: "./data".to_string(),
            wal_dir: None,
            max_memtable_size: 64 * 1024 * 1024, // 64MB
            partition_duration: 24 * 60 * 60 * 1_000_000_000, // 1 day in nanos
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_creation_and_validation() {
        let tag = Tag::new("host", "server01");
        assert_eq!(tag.key, "host");
        assert_eq!(tag.value, "server01");
        assert!(tag.validate().is_ok());

        let empty_key = Tag::new("", "value");
        assert!(empty_key.validate().is_err());
    }

    #[test]
    fn test_tag_ordering() {
        let tag1 = Tag::new("a", "1");
        let tag2 = Tag::new("b", "1");
        let tag3 = Tag::new("a", "2");

        assert!(tag1 < tag2);
        assert!(tag1 < tag3);
        assert!(tag3 < tag2);
    }

    #[test]
    fn test_field_value_conversions() {
        let fv = FieldValue::Float(3.14);
        assert_eq!(fv.as_f64(), Some(3.14));
        assert_eq!(fv.type_name(), "float");

        let fv = FieldValue::Integer(-42);
        assert_eq!(fv.as_i64(), Some(-42));
        assert_eq!(fv.as_f64(), Some(-42.0));

        let fv = FieldValue::UnsignedInteger(100);
        assert_eq!(fv.as_i64(), Some(100));

        let fv = FieldValue::String("hello".to_string());
        assert_eq!(fv.as_str(), Some("hello"));
        assert_eq!(fv.as_f64(), None);

        let fv = FieldValue::Boolean(true);
        assert_eq!(fv.as_bool(), Some(true));
    }

    #[test]
    fn test_field_value_from_impls() {
        let fv: FieldValue = 3.14_f64.into();
        assert!(matches!(fv, FieldValue::Float(_)));

        let fv: FieldValue = 42_i64.into();
        assert!(matches!(fv, FieldValue::Integer(_)));

        let fv: FieldValue = 42_u64.into();
        assert!(matches!(fv, FieldValue::UnsignedInteger(_)));

        let fv: FieldValue = "hello".into();
        assert!(matches!(fv, FieldValue::String(_)));

        let fv: FieldValue = true.into();
        assert!(matches!(fv, FieldValue::Boolean(_)));
    }

    #[test]
    fn test_field_creation_and_validation() {
        let field = Field::new("value", 42.5_f64);
        assert_eq!(field.key, "value");
        assert!(field.validate().is_ok());

        let empty_key = Field::new("", 42_i64);
        assert!(empty_key.validate().is_err());
    }

    #[test]
    fn test_point_builder() {
        let point = Point::builder("cpu")
            .timestamp(1609459200000000000)
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("usage", 64.5_f64)
            .field("cores", 8_i64)
            .build()
            .unwrap();

        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.timestamp, 1609459200000000000);
        assert_eq!(point.tags.len(), 2);
        assert_eq!(point.fields.len(), 2);

        // Tags should be sorted
        assert_eq!(point.tags[0].key, "host");
        assert_eq!(point.tags[1].key, "region");
    }

    #[test]
    fn test_point_validation() {
        // Valid point
        let point = Point::builder("cpu")
            .field("value", 42_i64)
            .build();
        assert!(point.is_ok());

        // Empty measurement
        let point = Point::builder("")
            .field("value", 42_i64)
            .build();
        assert!(point.is_err());

        // No fields
        let point = Point::builder("cpu").build();
        assert!(point.is_err());
    }

    #[test]
    fn test_point_accessors() {
        let point = Point::builder("cpu")
            .timestamp(1000)
            .tag("host", "server01")
            .field("usage", 64.5_f64)
            .build()
            .unwrap();

        assert_eq!(point.get_tag("host"), Some("server01"));
        assert_eq!(point.get_tag("nonexistent"), None);

        assert!(matches!(
            point.get_field("usage"),
            Some(FieldValue::Float(v)) if (*v - 64.5).abs() < f64::EPSILON
        ));
        assert_eq!(point.get_field("nonexistent"), None);
    }

    #[test]
    fn test_series_id_consistency() {
        let point1 = Point::builder("cpu")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 1_i64)
            .build()
            .unwrap();

        let point2 = Point::builder("cpu")
            .tag("region", "us-west")
            .tag("host", "server01")
            .field("value", 2_i64)
            .build()
            .unwrap();

        // Same measurement and tags (different order) should have same series ID
        assert_eq!(point1.series_id(), point2.series_id());

        let point3 = Point::builder("cpu")
            .tag("host", "server02")
            .tag("region", "us-west")
            .field("value", 1_i64)
            .build()
            .unwrap();

        // Different tag value should have different series ID
        assert_ne!(point1.series_id(), point3.series_id());
    }

    #[test]
    fn test_series_creation() {
        let point = Point::builder("cpu")
            .tag("host", "server01")
            .field("value", 1_i64)
            .build()
            .unwrap();

        let series = Series::from_point(&point);
        assert_eq!(series.measurement, "cpu");
        assert_eq!(series.tags.len(), 1);
        assert_eq!(series.id, point.series_id());
    }

    #[test]
    fn test_time_range() {
        let range = TimeRange::new(100, 200);

        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(!range.contains(200)); // end is exclusive
        assert!(!range.contains(50));

        let other = TimeRange::new(150, 250);
        assert!(range.overlaps(&other));

        let non_overlapping = TimeRange::new(200, 300);
        assert!(!range.overlaps(&non_overlapping));

        assert_eq!(range.duration_nanos(), 100);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let point = Point::builder("cpu")
            .timestamp(1609459200000000000)
            .tag("host", "server01")
            .field("usage", 64.5_f64)
            .field("active", true)
            .build()
            .unwrap();

        // Test bincode serialization
        let encoded = bincode::serialize(&point).unwrap();
        let decoded: Point = bincode::deserialize(&encoded).unwrap();
        assert_eq!(point, decoded);

        // Test JSON serialization
        let json = serde_json::to_string(&point).unwrap();
        let decoded: Point = serde_json::from_str(&json).unwrap();
        assert_eq!(point, decoded);
    }

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.name, "default");
        assert_eq!(config.max_memtable_size, 64 * 1024 * 1024);
        assert_eq!(config.partition_duration, 24 * 60 * 60 * 1_000_000_000);
    }
}
