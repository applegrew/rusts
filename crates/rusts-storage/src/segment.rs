//! Segment - Columnar storage format
//!
//! Segments store data in a columnar format optimized for time-series queries.
//! Each segment contains data for a specific time range.

use crate::error::{Result, StorageError};
use crate::memtable::MemTablePoint;
use rusts_compression::{
    BlockCompressor, CompressionLevel, DictionaryEncoder,
    GorillaEncoder, GorillaDecoder, TimestampEncoder, TimestampDecoder,
    IntegerEncoder, IntegerDecoder,
};
use rusts_core::{FieldValue, SeriesId, Tag, TimeRange};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Segment file magic number
const SEGMENT_MAGIC: [u8; 4] = [0x52, 0x54, 0x53, 0x53]; // "RTSS"

/// Segment file version
const SEGMENT_VERSION: u16 = 1;

/// Segment metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Segment ID
    pub id: u64,
    /// Series ID this segment belongs to
    pub series_id: SeriesId,
    /// Measurement name
    pub measurement: String,
    /// Tags for this series
    pub tags: Vec<Tag>,
    /// Time range covered by this segment
    pub time_range: TimeRange,
    /// Number of points
    pub point_count: usize,
    /// Field names and their types
    pub fields: Vec<(String, FieldType)>,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Uncompressed size in bytes
    pub uncompressed_size: usize,
}

/// Field type enumeration for schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Float,
    Integer,
    UnsignedInteger,
    String,
    Boolean,
}

impl From<&FieldValue> for FieldType {
    fn from(value: &FieldValue) -> Self {
        match value {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::UnsignedInteger(_) => FieldType::UnsignedInteger,
            FieldValue::String(_) => FieldType::String,
            FieldValue::Boolean(_) => FieldType::Boolean,
        }
    }
}

/// Segment writer for creating segment files
pub struct SegmentWriter {
    /// Compression level to use
    compression: CompressionLevel,
    /// Next segment ID
    next_id: u64,
}

impl SegmentWriter {
    /// Create a new segment writer
    pub fn new(compression: CompressionLevel) -> Self {
        Self {
            compression,
            next_id: 0,
        }
    }

    /// Write a segment from MemTable points
    pub fn write(
        &mut self,
        path: &Path,
        series_id: SeriesId,
        measurement: &str,
        tags: &[Tag],
        points: &[MemTablePoint],
    ) -> Result<SegmentMeta> {
        if points.is_empty() {
            return Err(StorageError::InvalidData("No points to write".to_string()));
        }

        // Sort points by timestamp
        let mut sorted_points = points.to_vec();
        sorted_points.sort_by_key(|p| p.timestamp);

        // Compute time range
        let time_range = TimeRange::new(
            sorted_points.first().unwrap().timestamp,
            sorted_points.last().unwrap().timestamp + 1,
        );

        // Build field schema
        let fields = self.build_field_schema(&sorted_points)?;

        // Compress timestamps
        let timestamp_data = self.compress_timestamps(&sorted_points)?;

        // Compress each field column
        let mut field_data = Vec::new();
        for (field_name, field_type) in &fields {
            let data = self.compress_field(&sorted_points, field_name, *field_type)?;
            field_data.push(data);
        }

        // Calculate sizes
        let uncompressed_size = sorted_points.len() * 8 // timestamps
            + sorted_points.iter().map(|p| {
                p.fields.iter().map(|(_, v)| match v {
                    FieldValue::String(s) => s.len(),
                    _ => 8,
                }).sum::<usize>()
            }).sum::<usize>();

        let compressed_size = timestamp_data.len()
            + field_data.iter().map(|d| d.len()).sum::<usize>();

        // Write to file
        let segment_id = self.next_id;
        self.next_id += 1;

        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(&SEGMENT_MAGIC)?;
        writer.write_all(&SEGMENT_VERSION.to_le_bytes())?;

        // Write metadata
        let meta = SegmentMeta {
            id: segment_id,
            series_id,
            measurement: measurement.to_string(),
            tags: tags.to_vec(),
            time_range,
            point_count: sorted_points.len(),
            fields: fields.clone(),
            compressed_size,
            uncompressed_size,
        };

        let meta_bytes = bincode::serialize(&meta)?;
        writer.write_all(&(meta_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&meta_bytes)?;

        // Write timestamp column
        writer.write_all(&(timestamp_data.len() as u32).to_le_bytes())?;
        writer.write_all(&timestamp_data)?;

        // Write field columns
        for data in &field_data {
            writer.write_all(&(data.len() as u32).to_le_bytes())?;
            writer.write_all(data)?;
        }

        writer.flush()?;

        Ok(meta)
    }

    fn build_field_schema(&self, points: &[MemTablePoint]) -> Result<Vec<(String, FieldType)>> {
        let mut fields: HashMap<String, FieldType> = HashMap::new();

        for point in points {
            for (name, value) in &point.fields {
                let field_type = FieldType::from(value);
                if let Some(existing) = fields.get(name) {
                    if *existing != field_type {
                        return Err(StorageError::InvalidData(format!(
                            "Field '{}' has inconsistent types",
                            name
                        )));
                    }
                } else {
                    fields.insert(name.clone(), field_type);
                }
            }
        }

        let mut field_list: Vec<_> = fields.into_iter().collect();
        field_list.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(field_list)
    }

    fn compress_timestamps(&self, points: &[MemTablePoint]) -> Result<Vec<u8>> {
        let mut encoder = TimestampEncoder::new();
        for point in points {
            encoder.encode(point.timestamp);
        }
        let data = encoder.finish();

        let compressor = BlockCompressor::new(self.compression);
        Ok(compressor.compress(&data)?)
    }

    fn compress_field(
        &self,
        points: &[MemTablePoint],
        field_name: &str,
        field_type: FieldType,
    ) -> Result<Vec<u8>> {
        let compressor = BlockCompressor::new(self.compression);

        match field_type {
            FieldType::Float => {
                let mut encoder = GorillaEncoder::new();
                for point in points {
                    let value = point
                        .fields
                        .iter()
                        .find(|(k, _)| k == field_name)
                        .and_then(|(_, v)| v.as_f64())
                        .unwrap_or(f64::NAN);
                    encoder.encode(value);
                }
                Ok(compressor.compress(&encoder.finish())?)
            }
            FieldType::Integer => {
                let mut encoder = IntegerEncoder::new();
                for point in points {
                    let value = point
                        .fields
                        .iter()
                        .find(|(k, _)| k == field_name)
                        .and_then(|(_, v)| v.as_i64())
                        .unwrap_or(0);
                    encoder.encode(value);
                }
                Ok(compressor.compress(&encoder.finish())?)
            }
            FieldType::UnsignedInteger => {
                let mut encoder = IntegerEncoder::new();
                for point in points {
                    let value = point
                        .fields
                        .iter()
                        .find(|(k, _)| k == field_name)
                        .map(|(_, v)| match v {
                            FieldValue::UnsignedInteger(u) => *u as i64,
                            _ => 0,
                        })
                        .unwrap_or(0);
                    encoder.encode(value);
                }
                Ok(compressor.compress(&encoder.finish())?)
            }
            FieldType::String => {
                let mut encoder = DictionaryEncoder::new();
                for point in points {
                    let value = point
                        .fields
                        .iter()
                        .find(|(k, _)| k == field_name)
                        .and_then(|(_, v)| v.as_str())
                        .unwrap_or("");
                    encoder.encode(value)?;
                }
                Ok(compressor.compress(&encoder.to_bytes())?)
            }
            FieldType::Boolean => {
                // Pack booleans into bytes
                let mut bytes = Vec::with_capacity((points.len() + 7) / 8);
                let mut current_byte = 0u8;
                let mut bit_pos = 0;

                for point in points {
                    let value = point
                        .fields
                        .iter()
                        .find(|(k, _)| k == field_name)
                        .and_then(|(_, v)| v.as_bool())
                        .unwrap_or(false);

                    if value {
                        current_byte |= 1 << bit_pos;
                    }

                    bit_pos += 1;
                    if bit_pos == 8 {
                        bytes.push(current_byte);
                        current_byte = 0;
                        bit_pos = 0;
                    }
                }

                if bit_pos > 0 {
                    bytes.push(current_byte);
                }

                Ok(compressor.compress(&bytes)?)
            }
        }
    }
}

impl Default for SegmentWriter {
    fn default() -> Self {
        Self::new(CompressionLevel::Default)
    }
}

/// Segment reader for reading segment files
pub struct SegmentReader {
    /// Path to segment file
    path: PathBuf,
    /// Segment metadata
    meta: SegmentMeta,
}

impl SegmentReader {
    /// Open a segment file
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        // Read and verify header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != SEGMENT_MAGIC {
            return Err(StorageError::InvalidData("Invalid segment magic".to_string()));
        }

        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != SEGMENT_VERSION {
            return Err(StorageError::InvalidData(format!(
                "Unsupported segment version: {}",
                version
            )));
        }

        // Read metadata
        let mut meta_len_bytes = [0u8; 4];
        reader.read_exact(&mut meta_len_bytes)?;
        let meta_len = u32::from_le_bytes(meta_len_bytes) as usize;

        let mut meta_bytes = vec![0u8; meta_len];
        reader.read_exact(&mut meta_bytes)?;
        let meta: SegmentMeta = bincode::deserialize(&meta_bytes)?;

        Ok(Self { path, meta })
    }

    /// Get segment metadata
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// Read all points from the segment
    pub fn read_all(&self) -> Result<Vec<MemTablePoint>> {
        self.read_range(&self.meta.time_range)
    }

    /// Read points within a time range
    pub fn read_range(&self, time_range: &TimeRange) -> Result<Vec<MemTablePoint>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        // Skip header
        let mut skip_buf = vec![0u8; 4 + 2]; // magic + version
        reader.read_exact(&mut skip_buf)?;

        // Skip metadata
        let mut meta_len_bytes = [0u8; 4];
        reader.read_exact(&mut meta_len_bytes)?;
        let meta_len = u32::from_le_bytes(meta_len_bytes) as usize;
        let mut meta_skip = vec![0u8; meta_len];
        reader.read_exact(&mut meta_skip)?;

        // Read timestamps
        let mut ts_len_bytes = [0u8; 4];
        reader.read_exact(&mut ts_len_bytes)?;
        let ts_len = u32::from_le_bytes(ts_len_bytes) as usize;
        let mut ts_data = vec![0u8; ts_len];
        reader.read_exact(&mut ts_data)?;

        let ts_decompressed = rusts_compression::block::decompress(&ts_data)?;
        let mut ts_decoder = TimestampDecoder::new(&ts_decompressed, self.meta.point_count);
        let timestamps = ts_decoder.decode_all()?;

        // Read field columns
        let mut field_values: Vec<Vec<FieldValue>> = Vec::new();
        for (_, field_type) in &self.meta.fields {
            let mut field_len_bytes = [0u8; 4];
            reader.read_exact(&mut field_len_bytes)?;
            let field_len = u32::from_le_bytes(field_len_bytes) as usize;
            let mut field_data = vec![0u8; field_len];
            reader.read_exact(&mut field_data)?;

            let values = self.decompress_field(&field_data, *field_type)?;
            field_values.push(values);
        }

        // Build points, filtering by time range
        let mut points = Vec::new();
        for (i, ts) in timestamps.iter().enumerate() {
            if !time_range.contains(*ts) {
                continue;
            }

            let fields: Vec<(String, FieldValue)> = self
                .meta
                .fields
                .iter()
                .enumerate()
                .map(|(j, (name, _))| (name.clone(), field_values[j][i].clone()))
                .collect();

            points.push(MemTablePoint {
                timestamp: *ts,
                fields,
            });
        }

        Ok(points)
    }

    fn decompress_field(&self, data: &[u8], field_type: FieldType) -> Result<Vec<FieldValue>> {
        let decompressed = rusts_compression::block::decompress(data)?;

        match field_type {
            FieldType::Float => {
                let mut decoder = GorillaDecoder::new(&decompressed, self.meta.point_count);
                let values = decoder.decode_all()?;
                Ok(values.into_iter().map(FieldValue::Float).collect())
            }
            FieldType::Integer => {
                let mut decoder = IntegerDecoder::new(&decompressed, self.meta.point_count);
                let values = decoder.decode_all()?;
                Ok(values.into_iter().map(FieldValue::Integer).collect())
            }
            FieldType::UnsignedInteger => {
                let mut decoder = IntegerDecoder::new(&decompressed, self.meta.point_count);
                let values = decoder.decode_all()?;
                Ok(values.into_iter().map(|v| FieldValue::UnsignedInteger(v as u64)).collect())
            }
            FieldType::String => {
                let (decoder, encoded) = rusts_compression::DictionaryDecoder::from_bytes(&decompressed)?;
                let mut values = Vec::with_capacity(self.meta.point_count);
                for &id in &encoded {
                    let s = decoder.decode(id)?;
                    values.push(FieldValue::String(s.to_string()));
                }
                Ok(values)
            }
            FieldType::Boolean => {
                let mut values = Vec::with_capacity(self.meta.point_count);
                for i in 0..self.meta.point_count {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    let value = byte_idx < decompressed.len() && (decompressed[byte_idx] >> bit_idx) & 1 == 1;
                    values.push(FieldValue::Boolean(value));
                }
                Ok(values)
            }
        }
    }
}

/// Segment represents an immutable segment file
pub struct Segment {
    reader: SegmentReader,
}

impl Segment {
    /// Open a segment
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: SegmentReader::open(path)?,
        })
    }

    /// Get metadata
    pub fn meta(&self) -> &SegmentMeta {
        self.reader.meta()
    }

    /// Read all points
    pub fn read_all(&self) -> Result<Vec<MemTablePoint>> {
        self.reader.read_all()
    }

    /// Read points in time range
    pub fn read_range(&self, time_range: &TimeRange) -> Result<Vec<MemTablePoint>> {
        self.reader.read_range(time_range)
    }

    /// Check if segment overlaps with time range
    pub fn overlaps(&self, time_range: &TimeRange) -> bool {
        self.reader.meta.time_range.overlaps(time_range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_points(count: usize) -> Vec<MemTablePoint> {
        (0..count)
            .map(|i| MemTablePoint {
                timestamp: i as i64 * 1000,
                fields: vec![
                    ("value".to_string(), FieldValue::Float(i as f64 * 1.5)),
                    ("count".to_string(), FieldValue::Integer(i as i64)),
                    ("active".to_string(), FieldValue::Boolean(i % 2 == 0)),
                ],
            })
            .collect()
    }

    #[test]
    fn test_segment_write_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points = create_test_points(100);
        let tags = vec![Tag::new("host", "server01")];

        let mut writer = SegmentWriter::new(CompressionLevel::Fast);
        let meta = writer
            .write(&path, 12345, "cpu", &tags, &points)
            .unwrap();

        assert_eq!(meta.point_count, 100);
        assert_eq!(meta.series_id, 12345);
        assert_eq!(meta.measurement, "cpu");

        let segment = Segment::open(&path).unwrap();
        let read_points = segment.read_all().unwrap();

        assert_eq!(read_points.len(), 100);

        for (i, point) in read_points.iter().enumerate() {
            assert_eq!(point.timestamp, i as i64 * 1000);

            let value = point.fields.iter().find(|(k, _)| k == "value").unwrap();
            if let FieldValue::Float(f) = &value.1 {
                assert!((f - i as f64 * 1.5).abs() < f64::EPSILON);
            } else {
                panic!("Expected float");
            }
        }
    }

    #[test]
    fn test_segment_time_range_query() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points = create_test_points(100);
        let tags = vec![Tag::new("host", "server01")];

        let mut writer = SegmentWriter::new(CompressionLevel::Default);
        writer.write(&path, 12345, "cpu", &tags, &points).unwrap();

        let segment = Segment::open(&path).unwrap();

        // Query middle range
        let range = TimeRange::new(25000, 75000);
        let results = segment.read_range(&range).unwrap();

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|p| p.timestamp >= 25000 && p.timestamp < 75000));
    }

    #[test]
    fn test_segment_compression_ratio() {
        let dir = TempDir::new().unwrap();

        // Test with different compression levels
        for level in [
            CompressionLevel::None,
            CompressionLevel::Fast,
            CompressionLevel::High,
        ] {
            let path = dir.path().join(format!("test_{:?}.seg", level));
            let points = create_test_points(10000);
            let tags = vec![Tag::new("host", "server01")];

            let mut writer = SegmentWriter::new(level);
            let meta = writer.write(&path, 12345, "cpu", &tags, &points).unwrap();

            // Verify compression is working
            if level != CompressionLevel::None {
                assert!(
                    meta.compressed_size < meta.uncompressed_size,
                    "Expected compression with {:?}",
                    level
                );
            }

            // Verify data integrity
            let segment = Segment::open(&path).unwrap();
            let read_points = segment.read_all().unwrap();
            assert_eq!(read_points.len(), 10000);
        }
    }

    #[test]
    fn test_segment_string_field() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points: Vec<MemTablePoint> = (0..100)
            .map(|i| MemTablePoint {
                timestamp: i * 1000,
                fields: vec![
                    ("status".to_string(), FieldValue::String(format!("status_{}", i % 10))),
                ],
            })
            .collect();

        let mut writer = SegmentWriter::new(CompressionLevel::Fast);
        writer.write(&path, 12345, "events", &[], &points).unwrap();

        let segment = Segment::open(&path).unwrap();
        let read_points = segment.read_all().unwrap();

        for (i, point) in read_points.iter().enumerate() {
            let status = point.fields.iter().find(|(k, _)| k == "status").unwrap();
            assert_eq!(
                status.1.as_str().unwrap(),
                format!("status_{}", i % 10)
            );
        }
    }

    #[test]
    fn test_segment_overlaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points = create_test_points(100); // timestamps 0-99000
        let mut writer = SegmentWriter::new(CompressionLevel::Fast);
        writer.write(&path, 12345, "cpu", &[], &points).unwrap();

        let segment = Segment::open(&path).unwrap();

        assert!(segment.overlaps(&TimeRange::new(0, 100000)));
        assert!(segment.overlaps(&TimeRange::new(50000, 150000)));
        assert!(segment.overlaps(&TimeRange::new(-1000, 1000)));
        assert!(!segment.overlaps(&TimeRange::new(100000, 200000)));
        assert!(!segment.overlaps(&TimeRange::new(-2000, -1000)));
    }
}
