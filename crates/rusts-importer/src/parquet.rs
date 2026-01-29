//! Parquet file reader for importing data into RusTs
//!
//! This module reads Parquet files and converts them to RusTs points.
//!
//! ## Expected Schema
//!
//! The Parquet file should have:
//! - A timestamp column (configurable name, defaults to "timestamp" or "time")
//! - Optional tag columns (string type, specified via CLI)
//! - Field columns (numeric or string types)
//!
//! ## Example
//!
//! ```text
//! | timestamp           | host     | region  | cpu_usage | memory_used |
//! |---------------------|----------|---------|-----------|-------------|
//! | 2024-01-01 00:00:00 | server01 | us-west | 45.2      | 8192        |
//! | 2024-01-01 00:00:01 | server01 | us-west | 46.1      | 8200        |
//! ```

use crate::error::{ImportError, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rusts_core::{FieldValue, Point, Tag, Timestamp};
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

/// Configuration for reading Parquet files
#[derive(Debug, Clone)]
pub struct ParquetReaderConfig {
    /// Measurement name to use for all points
    pub measurement: String,
    /// Column name containing timestamps
    pub timestamp_column: String,
    /// Column names to treat as tags (indexed)
    pub tag_columns: HashSet<String>,
    /// Batch size for reading
    pub batch_size: usize,
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        Self {
            measurement: "imported".to_string(),
            timestamp_column: "timestamp".to_string(),
            tag_columns: HashSet::new(),
            batch_size: 10000,
        }
    }
}

impl ParquetReaderConfig {
    /// Create a new config with the given measurement name
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            ..Default::default()
        }
    }

    /// Set the timestamp column name
    pub fn with_timestamp_column(mut self, column: impl Into<String>) -> Self {
        self.timestamp_column = column.into();
        self
    }

    /// Add tag columns
    pub fn with_tag_columns(mut self, columns: Vec<String>) -> Self {
        self.tag_columns = columns.into_iter().collect();
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// Parquet file reader
pub struct ParquetReader {
    config: ParquetReaderConfig,
}

impl ParquetReader {
    /// Create a new Parquet reader with the given configuration
    pub fn new(config: ParquetReaderConfig) -> Self {
        Self { config }
    }

    /// Read a Parquet file and return an iterator of Points
    pub fn read_file(&self, path: impl AsRef<Path>) -> Result<Vec<Point>> {
        let file = File::open(path.as_ref())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let schema = builder.schema().clone();
        let reader = builder.with_batch_size(self.config.batch_size).build()?;

        // Find timestamp column index
        let ts_idx = schema
            .fields()
            .iter()
            .position(|f| {
                f.name() == &self.config.timestamp_column
                    || f.name() == "time"
                    || f.name() == "ts"
            })
            .ok_or_else(|| {
                ImportError::Schema(format!(
                    "Timestamp column '{}' not found. Available columns: {:?}",
                    self.config.timestamp_column,
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ))
            })?;

        let ts_column_name = schema.field(ts_idx).name().clone();

        // Identify tag and field columns
        let mut tag_indices: Vec<(usize, String)> = Vec::new();
        let mut field_indices: Vec<(usize, String)> = Vec::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            if idx == ts_idx {
                continue;
            }

            let name = field.name().clone();
            if self.config.tag_columns.contains(&name) {
                tag_indices.push((idx, name));
            } else {
                field_indices.push((idx, name));
            }
        }

        // Read all batches and convert to points
        let mut points = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            // Get timestamp array
            let ts_array = batch.column(ts_idx);
            let timestamps = extract_timestamps(ts_array, &ts_column_name)?;

            // Get tag arrays
            let tag_arrays: Vec<_> = tag_indices
                .iter()
                .map(|(idx, name)| (name.clone(), batch.column(*idx).clone()))
                .collect();

            // Get field arrays
            let field_arrays: Vec<_> = field_indices
                .iter()
                .map(|(idx, name)| (name.clone(), batch.column(*idx).clone()))
                .collect();

            // Convert each row to a Point
            for row in 0..num_rows {
                let timestamp = timestamps[row];

                // Extract tags
                let tags: Vec<Tag> = tag_arrays
                    .iter()
                    .filter_map(|(name, array)| {
                        extract_string_value(array, row).map(|value| Tag {
                            key: name.clone(),
                            value,
                        })
                    })
                    .collect();

                // Extract fields
                let fields: Vec<rusts_core::Field> = field_arrays
                    .iter()
                    .filter_map(|(name, array)| {
                        extract_field_value(array, row).map(|value| rusts_core::Field {
                            key: name.clone(),
                            value,
                        })
                    })
                    .collect();

                if !fields.is_empty() {
                    let point = Point {
                        measurement: self.config.measurement.clone(),
                        tags,
                        fields,
                        timestamp,
                    };
                    points.push(point);
                }
            }
        }

        Ok(points)
    }

    /// Read a Parquet file and yield batches of points
    pub fn read_file_batched(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<Vec<Point>>>> {
        let file = File::open(path.as_ref())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let schema = builder.schema().clone();
        let reader = builder.with_batch_size(self.config.batch_size).build()?;

        // Find timestamp column index
        let ts_idx = schema
            .fields()
            .iter()
            .position(|f| {
                f.name() == &self.config.timestamp_column
                    || f.name() == "time"
                    || f.name() == "ts"
            })
            .ok_or_else(|| {
                ImportError::Schema(format!(
                    "Timestamp column '{}' not found",
                    self.config.timestamp_column
                ))
            })?;

        let ts_column_name = schema.field(ts_idx).name().clone();

        // Identify tag and field columns
        let mut tag_indices: Vec<(usize, String)> = Vec::new();
        let mut field_indices: Vec<(usize, String)> = Vec::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            if idx == ts_idx {
                continue;
            }

            let name = field.name().clone();
            if self.config.tag_columns.contains(&name) {
                tag_indices.push((idx, name));
            } else {
                field_indices.push((idx, name));
            }
        }

        let measurement = self.config.measurement.clone();

        Ok(reader.map(move |batch_result| {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            let ts_array = batch.column(ts_idx);
            let timestamps = extract_timestamps(ts_array, &ts_column_name)?;

            let tag_arrays: Vec<_> = tag_indices
                .iter()
                .map(|(idx, name)| (name.clone(), batch.column(*idx).clone()))
                .collect();

            let field_arrays: Vec<_> = field_indices
                .iter()
                .map(|(idx, name)| (name.clone(), batch.column(*idx).clone()))
                .collect();

            let mut points = Vec::with_capacity(num_rows);

            for row in 0..num_rows {
                let timestamp = timestamps[row];

                let tags: Vec<Tag> = tag_arrays
                    .iter()
                    .filter_map(|(name, array)| {
                        extract_string_value(array, row).map(|value| Tag {
                            key: name.clone(),
                            value,
                        })
                    })
                    .collect();

                let fields: Vec<rusts_core::Field> = field_arrays
                    .iter()
                    .filter_map(|(name, array)| {
                        extract_field_value(array, row).map(|value| rusts_core::Field {
                            key: name.clone(),
                            value,
                        })
                    })
                    .collect();

                if !fields.is_empty() {
                    points.push(Point {
                        measurement: measurement.clone(),
                        tags,
                        fields,
                        timestamp,
                    });
                }
            }

            Ok(points)
        }))
    }
}

/// Extract timestamps from an Arrow array
fn extract_timestamps(array: &ArrayRef, column_name: &str) -> Result<Vec<Timestamp>> {
    let len = array.len();
    let mut timestamps = Vec::with_capacity(len);

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast timestamp array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i));
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast timestamp array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i) * 1000); // Convert to nanos
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast timestamp array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i) * 1_000_000); // Convert to nanos
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast timestamp array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i) * 1_000_000_000); // Convert to nanos
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast int64 array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i));
            }
        }
        DataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| ImportError::Conversion("Failed to cast date64 array".into()))?;
            for i in 0..len {
                timestamps.push(arr.value(i) * 1_000_000); // ms to nanos
            }
        }
        other => {
            return Err(ImportError::Schema(format!(
                "Unsupported timestamp type for column '{}': {:?}. Expected Timestamp, Int64, or Date64.",
                column_name, other
            )));
        }
    }

    Ok(timestamps)
}

/// Extract a string value from an Arrow array at the given row
fn extract_string_value(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>()?;
            Some(arr.value(row).to_string())
        }
        DataType::Dictionary(_, _) => {
            // Handle dictionary-encoded strings
            let arr = array.as_any().downcast_ref::<StringArray>();
            arr.map(|a| a.value(row).to_string())
        }
        _ => None,
    }
}

/// Extract a field value from an Arrow array at the given row
fn extract_field_value(array: &ArrayRef, row: usize) -> Option<FieldValue> {
    if array.is_null(row) {
        return None;
    }

    match array.data_type() {
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            Some(FieldValue::Float(arr.value(row)))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>()?;
            Some(FieldValue::Float(arr.value(row) as f64))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            Some(FieldValue::Integer(arr.value(row)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()?;
            Some(FieldValue::Integer(arr.value(row) as i64))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>()?;
            Some(FieldValue::Integer(arr.value(row) as i64))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>()?;
            Some(FieldValue::Integer(arr.value(row) as i64))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>()?;
            Some(FieldValue::UnsignedInteger(arr.value(row)))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>()?;
            Some(FieldValue::UnsignedInteger(arr.value(row) as u64))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>()?;
            Some(FieldValue::UnsignedInteger(arr.value(row) as u64))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>()?;
            Some(FieldValue::UnsignedInteger(arr.value(row) as u64))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            Some(FieldValue::Boolean(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(FieldValue::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>()?;
            Some(FieldValue::String(arr.value(row).to_string()))
        }
        _ => None,
    }
}

/// Get schema information from a Parquet file
pub fn inspect_parquet_schema(path: impl AsRef<Path>) -> Result<Vec<(String, String)>> {
    let file = File::open(path.as_ref())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema();

    let columns: Vec<(String, String)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), format!("{:?}", f.data_type())))
        .collect();

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ParquetReaderConfig::new("metrics")
            .with_timestamp_column("ts")
            .with_tag_columns(vec!["host".to_string(), "region".to_string()])
            .with_batch_size(5000);

        assert_eq!(config.measurement, "metrics");
        assert_eq!(config.timestamp_column, "ts");
        assert!(config.tag_columns.contains("host"));
        assert!(config.tag_columns.contains("region"));
        assert_eq!(config.batch_size, 5000);
    }
}
