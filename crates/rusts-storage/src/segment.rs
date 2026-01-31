//! Segment - Columnar storage format
//!
//! Segments store data in a columnar format optimized for time-series queries.
//! Each segment contains data for a specific time range.

use crate::error::{Result, StorageError};
use crate::memtable::MemTablePoint;
use memmap2::{Mmap, MmapOptions};
use rusts_compression::{
    BlockCompressor, CompressionLevel, DictionaryEncoder,
    GorillaEncoder, GorillaDecoder, TimestampEncoder, TimestampDecoder,
    IntegerEncoder, IntegerDecoder,
};
use rusts_core::{FieldValue, SeriesId, Tag, TimeRange};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Segment file magic number
const SEGMENT_MAGIC: [u8; 4] = [0x52, 0x54, 0x53, 0x53]; // "RTSS"

/// Segment file version (includes field_offsets and field_stats for column pruning)
const SEGMENT_VERSION: u16 = 2;

/// Field statistics for predicate pushdown.
/// Allows skipping segments when WHERE clause values are outside the segment's range.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldStats {
    /// Minimum value (for numeric fields)
    pub min: Option<f64>,
    /// Maximum value (for numeric fields)
    pub max: Option<f64>,
    /// Sum of all values (for pre-aggregation)
    pub sum: Option<f64>,
    /// Number of non-null values
    pub count: u64,
}

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
    /// Byte offsets for each field column (for column pruning).
    /// Empty for segments written before this feature.
    #[serde(default)]
    pub field_offsets: Vec<u64>,
    /// Statistics for each field (for predicate pushdown).
    /// Keys are field names. Empty for segments written before this feature.
    #[serde(default)]
    pub field_stats: HashMap<String, FieldStats>,
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

/// Comparison operators for field predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A field predicate for filtering
#[derive(Debug, Clone)]
pub struct FieldPredicate {
    pub field: String,
    pub op: FieldOp,
    pub value: f64,
}

impl SegmentMeta {
    /// Check if this segment can be skipped based on field predicates.
    ///
    /// Uses segment statistics to determine if any data could possibly match.
    /// Returns true if the segment definitely has no matching data.
    pub fn can_skip(&self, predicates: &[FieldPredicate]) -> bool {
        for predicate in predicates {
            if let Some(stats) = self.field_stats.get(&predicate.field) {
                let can_skip = match (stats.min, stats.max, predicate.op) {
                    // If looking for values > X and our max <= X, skip
                    (_, Some(max), FieldOp::GreaterThan) => max <= predicate.value,
                    (_, Some(max), FieldOp::GreaterThanOrEqual) => max < predicate.value,

                    // If looking for values < X and our min >= X, skip
                    (Some(min), _, FieldOp::LessThan) => min >= predicate.value,
                    (Some(min), _, FieldOp::LessThanOrEqual) => min > predicate.value,

                    // If looking for exact value and it's outside our range, skip
                    (Some(min), Some(max), FieldOp::Equal) => {
                        predicate.value < min || predicate.value > max
                    }

                    // NotEqual can skip if all values are the exact predicate value
                    (Some(min), Some(max), FieldOp::NotEqual) => {
                        min == max && min == predicate.value
                    }

                    // Can't determine if we can skip
                    _ => false,
                };

                if can_skip {
                    return true;
                }
            }
        }

        false
    }
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

        // Calculate field offsets for column pruning
        // Offsets are relative to the start of the data section (after timestamp column)
        let mut field_offsets = Vec::with_capacity(field_data.len());
        let mut offset = 0u64;
        for data in &field_data {
            field_offsets.push(offset);
            offset += 4 + data.len() as u64; // 4 bytes for length prefix + data
        }

        // Compute field statistics for predicate pushdown
        let field_stats = self.compute_field_stats(&sorted_points, &fields);

        // Write to file
        let segment_id = self.next_id;
        self.next_id += 1;

        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(&SEGMENT_MAGIC)?;
        writer.write_all(&SEGMENT_VERSION.to_le_bytes())?;

        // Create metadata with field offsets and stats
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
            field_offsets: field_offsets.clone(),
            field_stats,
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

    /// Compute statistics for each field (min, max, sum, count).
    /// Used for predicate pushdown to skip segments.
    fn compute_field_stats(
        &self,
        points: &[MemTablePoint],
        fields: &[(String, FieldType)],
    ) -> HashMap<String, FieldStats> {
        let mut stats_map = HashMap::new();

        for (field_name, field_type) in fields {
            // Only compute stats for numeric types
            if !matches!(
                field_type,
                FieldType::Float | FieldType::Integer | FieldType::UnsignedInteger
            ) {
                continue;
            }

            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            let mut sum = 0.0;
            let mut count = 0u64;

            for point in points {
                if let Some((_, value)) = point.fields.iter().find(|(k, _)| k == field_name) {
                    if let Some(v) = value.as_f64() {
                        if !v.is_nan() {
                            if v < min {
                                min = v;
                            }
                            if v > max {
                                max = v;
                            }
                            sum += v;
                            count += 1;
                        }
                    }
                }
            }

            if count > 0 {
                stats_map.insert(
                    field_name.clone(),
                    FieldStats {
                        min: Some(min),
                        max: Some(max),
                        sum: Some(sum),
                        count,
                    },
                );
            }
        }

        stats_map
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
                "Unsupported segment version: {} (expected {})",
                version, SEGMENT_VERSION
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

    /// Read points within a time range, only reading specified fields.
    ///
    /// If `fields` is None, all fields are read.
    /// If `fields` is Some([]), only timestamps are read (useful for COUNT(*)).
    /// This provides 2-5x speedup for queries that only need a subset of fields.
    pub fn read_range_with_fields(
        &self,
        time_range: &TimeRange,
        fields: Option<&[String]>,
    ) -> Result<Vec<MemTablePoint>> {
        // If all fields needed, use the optimized read_range path
        if fields.is_none() {
            return self.read_range(time_range);
        }

        let field_names = fields.unwrap();

        // Determine which field indices to read
        let field_indices: Vec<usize> = self
            .meta
            .fields
            .iter()
            .enumerate()
            .filter_map(|(i, (name, _))| {
                if field_names.contains(name) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        // If no fields needed and we have field_offsets, we can skip all field columns
        let can_skip_fields = field_names.is_empty() && !self.meta.field_offsets.is_empty();

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

        // Read timestamps (always needed for time range filtering)
        let mut ts_len_bytes = [0u8; 4];
        reader.read_exact(&mut ts_len_bytes)?;
        let ts_len = u32::from_le_bytes(ts_len_bytes) as usize;
        let mut ts_data = vec![0u8; ts_len];
        reader.read_exact(&mut ts_data)?;

        let ts_decompressed = rusts_compression::block::decompress(&ts_data)?;
        let mut ts_decoder = TimestampDecoder::new(&ts_decompressed, self.meta.point_count);
        let timestamps = ts_decoder.decode_all()?;

        // If we don't need any fields, just return timestamps
        if can_skip_fields {
            let mut points = Vec::new();
            for ts in timestamps.iter() {
                if time_range.contains(*ts) {
                    points.push(MemTablePoint {
                        timestamp: *ts,
                        fields: Vec::new(),
                    });
                }
            }
            return Ok(points);
        }

        // Read only the needed field columns (use Vec with Option for O(1) access)
        let mut field_values: Vec<Option<Vec<FieldValue>>> = vec![None; self.meta.fields.len()];

        for (current_idx, (_, field_type)) in self.meta.fields.iter().enumerate() {
            let mut field_len_bytes = [0u8; 4];
            reader.read_exact(&mut field_len_bytes)?;
            let field_len = u32::from_le_bytes(field_len_bytes) as usize;

            if field_indices.contains(&current_idx) {
                // Read and decompress this field
                let mut field_data = vec![0u8; field_len];
                reader.read_exact(&mut field_data)?;
                let values = self.decompress_field(&field_data, *field_type)?;
                field_values[current_idx] = Some(values);
            } else {
                // Skip this field column using seek (much faster than reading)
                reader.seek(SeekFrom::Current(field_len as i64))?;
            }
        }

        // Build points, filtering by time range
        let mut points = Vec::new();
        for (i, ts) in timestamps.iter().enumerate() {
            if !time_range.contains(*ts) {
                continue;
            }

            let fields: Vec<(String, FieldValue)> = field_indices
                .iter()
                .filter_map(|&j| {
                    field_values[j]
                        .as_ref()
                        .map(|values| (self.meta.fields[j].0.clone(), values[i].clone()))
                })
                .collect();

            points.push(MemTablePoint {
                timestamp: *ts,
                fields,
            });
        }

        Ok(points)
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

/// Memory-mapped segment reader for zero-copy reads.
///
/// Uses mmap for files above a threshold size (default 4MB), which provides:
/// - Zero-copy reads (no syscall per read)
/// - OS-level page caching
/// - Reduced memory allocation overhead
///
/// For smaller files, the OS's buffer cache is equally effective with BufReader.
pub struct MmapSegmentReader {
    /// Memory-mapped file
    mmap: Mmap,
    /// Segment metadata
    meta: SegmentMeta,
    /// Offset to data section (after header + metadata)
    data_offset: usize,
    /// Timestamp column length
    timestamp_len: usize,
    /// Field column lengths (in order matching meta.fields)
    field_lens: Vec<usize>,
}

impl MmapSegmentReader {
    /// Minimum file size to use mmap (default 4MB)
    pub const MMAP_THRESHOLD: u64 = 4 * 1024 * 1024;

    /// Open a segment file with memory mapping
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // Parse header
        if mmap.len() < 6 {
            return Err(StorageError::InvalidData("File too small".to_string()));
        }

        let magic = &mmap[0..4];
        if magic != SEGMENT_MAGIC {
            return Err(StorageError::InvalidData("Invalid segment magic".to_string()));
        }

        let version = u16::from_le_bytes([mmap[4], mmap[5]]);
        if version != SEGMENT_VERSION {
            return Err(StorageError::InvalidData(format!(
                "Unsupported segment version: {} (expected {})",
                version, SEGMENT_VERSION
            )));
        }

        // Read metadata length and parse metadata
        let meta_len = u32::from_le_bytes([mmap[6], mmap[7], mmap[8], mmap[9]]) as usize;
        let meta_end = 10 + meta_len;

        let meta: SegmentMeta = bincode::deserialize(&mmap[10..meta_end])?;

        // Parse column lengths
        let mut cursor = meta_end;

        // Timestamp column
        let timestamp_len = u32::from_le_bytes([
            mmap[cursor],
            mmap[cursor + 1],
            mmap[cursor + 2],
            mmap[cursor + 3],
        ]) as usize;
        cursor += 4;

        let data_offset = cursor;
        cursor += timestamp_len;

        // Field column lengths
        let mut field_lens = Vec::with_capacity(meta.fields.len());
        for _ in &meta.fields {
            let field_len = u32::from_le_bytes([
                mmap[cursor],
                mmap[cursor + 1],
                mmap[cursor + 2],
                mmap[cursor + 3],
            ]) as usize;
            field_lens.push(field_len);
            cursor += 4 + field_len; // Skip length prefix and data
        }

        Ok(Self {
            mmap,
            meta,
            data_offset,
            timestamp_len,
            field_lens,
        })
    }

    /// Get segment metadata
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// Read all points from the segment
    pub fn read_all(&self) -> Result<Vec<MemTablePoint>> {
        self.read_range(&self.meta.time_range)
    }

    /// Read points within a time range, only reading specified fields
    pub fn read_range_with_fields(
        &self,
        time_range: &TimeRange,
        fields: Option<&[String]>,
    ) -> Result<Vec<MemTablePoint>> {
        // If all fields needed, use the optimized read_range path
        if fields.is_none() {
            return self.read_range(time_range);
        }

        let field_names = fields.unwrap();

        // If no fields needed (COUNT(*)), just read timestamps
        if field_names.is_empty() {
            let ts_data = &self.mmap[self.data_offset..self.data_offset + self.timestamp_len];
            let ts_decompressed = rusts_compression::block::decompress(ts_data)?;
            let mut ts_decoder = TimestampDecoder::new(&ts_decompressed, self.meta.point_count);
            let timestamps = ts_decoder.decode_all()?;

            let mut points = Vec::new();
            for ts in timestamps.iter() {
                if time_range.contains(*ts) {
                    points.push(MemTablePoint {
                        timestamp: *ts,
                        fields: Vec::new(),
                    });
                }
            }
            return Ok(points);
        }

        // Determine which field indices to read
        let field_indices: Vec<usize> = self
            .meta
            .fields
            .iter()
            .enumerate()
            .filter_map(|(i, (name, _))| {
                if field_names.contains(name) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        // Read timestamps directly from mmap
        let ts_data = &self.mmap[self.data_offset..self.data_offset + self.timestamp_len];
        let ts_decompressed = rusts_compression::block::decompress(ts_data)?;
        let mut ts_decoder = TimestampDecoder::new(&ts_decompressed, self.meta.point_count);
        let timestamps = ts_decoder.decode_all()?;

        // Read only needed field columns (use Vec with Option for O(1) access)
        let mut field_offset = self.data_offset + self.timestamp_len;
        let mut field_values: Vec<Option<Vec<FieldValue>>> = vec![None; self.meta.fields.len()];

        for (i, (_, field_type)) in self.meta.fields.iter().enumerate() {
            let data_start = field_offset + 4;
            let data_end = data_start + self.field_lens[i];

            if field_indices.contains(&i) {
                let field_data = &self.mmap[data_start..data_end];
                let values = self.decompress_field(field_data, *field_type)?;
                field_values[i] = Some(values);
            }

            field_offset = data_end;
        }

        // Build points, filtering by time range
        let mut points = Vec::new();
        for (i, ts) in timestamps.iter().enumerate() {
            if !time_range.contains(*ts) {
                continue;
            }

            let fields: Vec<(String, FieldValue)> = field_indices
                .iter()
                .filter_map(|&j| {
                    field_values[j]
                        .as_ref()
                        .map(|values| (self.meta.fields[j].0.clone(), values[i].clone()))
                })
                .collect();

            points.push(MemTablePoint {
                timestamp: *ts,
                fields,
            });
        }

        Ok(points)
    }

    /// Read points within a time range
    pub fn read_range(&self, time_range: &TimeRange) -> Result<Vec<MemTablePoint>> {
        // Read timestamps directly from mmap
        let ts_data = &self.mmap[self.data_offset..self.data_offset + self.timestamp_len];
        let ts_decompressed = rusts_compression::block::decompress(ts_data)?;
        let mut ts_decoder = TimestampDecoder::new(&ts_decompressed, self.meta.point_count);
        let timestamps = ts_decoder.decode_all()?;

        // Calculate field data offsets
        let mut field_offset = self.data_offset + self.timestamp_len;
        let mut field_values: Vec<Vec<FieldValue>> = Vec::with_capacity(self.meta.fields.len());

        for (i, (_, field_type)) in self.meta.fields.iter().enumerate() {
            // Skip length prefix (4 bytes)
            let data_start = field_offset + 4;
            let data_end = data_start + self.field_lens[i];
            let field_data = &self.mmap[data_start..data_end];

            let values = self.decompress_field(field_data, *field_type)?;
            field_values.push(values);

            field_offset = data_end;
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
    /// Standard reader (used for small files)
    reader: Option<SegmentReader>,
    /// Mmap reader (used for large files)
    mmap_reader: Option<MmapSegmentReader>,
}

impl Segment {
    /// Open a segment, using mmap for large files
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file_size = std::fs::metadata(path)?.len();

        if file_size >= MmapSegmentReader::MMAP_THRESHOLD {
            // Use mmap for large files (10-30% faster for large reads)
            Ok(Self {
                reader: None,
                mmap_reader: Some(MmapSegmentReader::open(path)?),
            })
        } else {
            // Use standard reader for small files (OS buffer cache is efficient)
            Ok(Self {
                reader: Some(SegmentReader::open(path)?),
                mmap_reader: None,
            })
        }
    }

    /// Open a segment with forced reader type (for testing)
    pub fn open_with_mmap(path: impl AsRef<Path>, use_mmap: bool) -> Result<Self> {
        let path = path.as_ref();

        if use_mmap {
            Ok(Self {
                reader: None,
                mmap_reader: Some(MmapSegmentReader::open(path)?),
            })
        } else {
            Ok(Self {
                reader: Some(SegmentReader::open(path)?),
                mmap_reader: None,
            })
        }
    }

    /// Get metadata
    pub fn meta(&self) -> &SegmentMeta {
        if let Some(ref mmap_reader) = self.mmap_reader {
            mmap_reader.meta()
        } else if let Some(ref reader) = self.reader {
            reader.meta()
        } else {
            unreachable!("Segment must have either reader or mmap_reader")
        }
    }

    /// Read all points
    pub fn read_all(&self) -> Result<Vec<MemTablePoint>> {
        if let Some(ref mmap_reader) = self.mmap_reader {
            mmap_reader.read_all()
        } else if let Some(ref reader) = self.reader {
            reader.read_all()
        } else {
            unreachable!("Segment must have either reader or mmap_reader")
        }
    }

    /// Read points in time range
    pub fn read_range(&self, time_range: &TimeRange) -> Result<Vec<MemTablePoint>> {
        if let Some(ref mmap_reader) = self.mmap_reader {
            mmap_reader.read_range(time_range)
        } else if let Some(ref reader) = self.reader {
            reader.read_range(time_range)
        } else {
            unreachable!("Segment must have either reader or mmap_reader")
        }
    }

    /// Read points in time range, only reading specified fields.
    ///
    /// If `fields` is None, all fields are read.
    /// If `fields` is Some([]), only timestamps are read (useful for COUNT(*)).
    /// This provides 2-5x speedup for queries that only need a subset of fields.
    pub fn read_range_with_fields(
        &self,
        time_range: &TimeRange,
        fields: Option<&[String]>,
    ) -> Result<Vec<MemTablePoint>> {
        if let Some(ref mmap_reader) = self.mmap_reader {
            mmap_reader.read_range_with_fields(time_range, fields)
        } else if let Some(ref reader) = self.reader {
            reader.read_range_with_fields(time_range, fields)
        } else {
            unreachable!("Segment must have either reader or mmap_reader")
        }
    }

    /// Check if segment overlaps with time range
    pub fn overlaps(&self, time_range: &TimeRange) -> bool {
        self.meta().time_range.overlaps(time_range)
    }

    /// Check if this segment is using mmap
    pub fn is_mmap(&self) -> bool {
        self.mmap_reader.is_some()
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

    #[test]
    fn test_mmap_segment_reader() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points = create_test_points(100);
        let tags = vec![Tag::new("host", "server01")];

        let mut writer = SegmentWriter::new(CompressionLevel::Fast);
        writer.write(&path, 12345, "cpu", &tags, &points).unwrap();

        // Force use of mmap reader
        let segment = Segment::open_with_mmap(&path, true).unwrap();
        assert!(segment.is_mmap());

        // Read all points
        let read_points = segment.read_all().unwrap();
        assert_eq!(read_points.len(), 100);

        // Verify data integrity
        for (i, point) in read_points.iter().enumerate() {
            assert_eq!(point.timestamp, i as i64 * 1000);
            let value = point.fields.iter().find(|(k, _)| k == "value").unwrap();
            if let FieldValue::Float(f) = &value.1 {
                assert!((f - i as f64 * 1.5).abs() < f64::EPSILON);
            } else {
                panic!("Expected float");
            }
        }

        // Test time range query
        let range_points = segment.read_range(&TimeRange::new(25000, 75000)).unwrap();
        assert_eq!(range_points.len(), 50);
    }

    #[test]
    fn test_mmap_vs_standard_reader_consistency() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let points = create_test_points(100);

        let mut writer = SegmentWriter::new(CompressionLevel::Fast);
        writer.write(&path, 12345, "cpu", &[], &points).unwrap();

        // Read with standard reader
        let standard_segment = Segment::open_with_mmap(&path, false).unwrap();
        assert!(!standard_segment.is_mmap());
        let standard_points = standard_segment.read_all().unwrap();

        // Read with mmap reader
        let mmap_segment = Segment::open_with_mmap(&path, true).unwrap();
        assert!(mmap_segment.is_mmap());
        let mmap_points = mmap_segment.read_all().unwrap();

        // Results should be identical
        assert_eq!(standard_points.len(), mmap_points.len());
        for (std_pt, mmap_pt) in standard_points.iter().zip(mmap_points.iter()) {
            assert_eq!(std_pt.timestamp, mmap_pt.timestamp);
            assert_eq!(std_pt.fields.len(), mmap_pt.fields.len());
        }
    }
}
