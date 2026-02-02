//! Partition - Time-based data organization
//!
//! Partitions organize data by time for efficient querying and retention management.

use crate::error::{Result, StorageError};
use crate::memtable::MemTablePoint;
use crate::segment::{Segment, SegmentMeta, SegmentWriter};
use parking_lot::RwLock;
use rusts_compression::CompressionLevel;
use rusts_core::{SeriesId, Tag, TimeRange, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Partition metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// Partition ID
    pub id: u64,
    /// Time range for this partition
    pub time_range: TimeRange,
    /// Creation timestamp
    pub created_at: Timestamp,
    /// Total point count
    pub point_count: usize,
    /// Number of segments
    pub segment_count: usize,
}

/// A partition containing segments for a specific time range
pub struct Partition {
    /// Partition ID
    id: u64,
    /// Base directory for this partition
    dir: PathBuf,
    /// Time range for this partition
    time_range: TimeRange,
    /// Segments in this partition (series_id -> list of segments)
    segments: RwLock<HashMap<SeriesId, Vec<Segment>>>,
    /// Segment metadata
    segment_metas: RwLock<Vec<SegmentMeta>>,
    /// Next segment ID
    next_segment_id: AtomicU64,
    /// Compression level for new segments
    compression: CompressionLevel,
}

impl Partition {
    /// Create a new partition
    pub fn new(id: u64, dir: impl AsRef<Path>, time_range: TimeRange, compression: CompressionLevel) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        Ok(Self {
            id,
            dir,
            time_range,
            segments: RwLock::new(HashMap::new()),
            segment_metas: RwLock::new(Vec::new()),
            next_segment_id: AtomicU64::new(0),
            compression,
        })
    }

    /// Open an existing partition
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();

        // Read partition metadata
        let meta_path = dir.join("partition.meta");
        let meta_bytes = std::fs::read(&meta_path)?;
        let meta: PartitionMeta = bincode::deserialize(&meta_bytes)?;

        let partition = Self {
            id: meta.id,
            dir: dir.clone(),
            time_range: meta.time_range,
            segments: RwLock::new(HashMap::new()),
            segment_metas: RwLock::new(Vec::new()),
            next_segment_id: AtomicU64::new(0),
            compression: CompressionLevel::Default,
        };

        // Load existing segments
        partition.load_segments()?;

        Ok(partition)
    }

    /// Load segments from disk
    fn load_segments(&self) -> Result<()> {
        let mut max_segment_id = 0u64;

        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |e| e == "seg") {
                let segment = Segment::open(&path)?;
                let meta = segment.meta().clone();

                max_segment_id = max_segment_id.max(meta.id);

                let mut segments = self.segments.write();
                segments
                    .entry(meta.series_id)
                    .or_insert_with(Vec::new)
                    .push(segment);

                self.segment_metas.write().push(meta);
            }
        }

        self.next_segment_id.store(max_segment_id + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Write points to a new segment
    pub fn write_segment(
        &self,
        series_id: SeriesId,
        measurement: &str,
        tags: &[Tag],
        points: &[MemTablePoint],
    ) -> Result<SegmentMeta> {
        if points.is_empty() {
            return Err(StorageError::InvalidData("No points to write".to_string()));
        }

        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let segment_path = self.dir.join(format!("segment_{:08}.seg", segment_id));

        let mut writer = SegmentWriter::new(self.compression);
        let meta = writer.write(&segment_path, series_id, measurement, tags, points)?;

        // Load the segment and add to our collection
        let segment = Segment::open(&segment_path)?;

        let mut segments = self.segments.write();
        segments
            .entry(series_id)
            .or_insert_with(Vec::new)
            .push(segment);

        self.segment_metas.write().push(meta.clone());

        // Update partition metadata
        self.save_meta()?;

        Ok(meta)
    }

    /// Query points for a series within a time range
    pub fn query(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
    ) -> Result<Vec<MemTablePoint>> {
        self.query_with_fields(series_id, time_range, None)
    }

    /// Query points for a series with optional field selection (column pruning).
    ///
    /// When `fields` is Some, only the specified fields are read from segments,
    /// which can significantly reduce I/O and decompression time for wide tables.
    pub fn query_with_fields(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
        fields: Option<&[String]>,
    ) -> Result<Vec<MemTablePoint>> {
        let segments = self.segments.read();
        let series_segments = match segments.get(&series_id) {
            Some(segs) => segs,
            None => return Ok(Vec::new()),
        };

        let mut all_points = Vec::new();
        for segment in series_segments {
            if segment.overlaps(time_range) {
                let points = segment.read_range_with_fields(time_range, fields)?;
                all_points.extend(points);
            }
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);
        Ok(all_points)
    }

    /// Query with limit, leveraging sorted segment data.
    /// For ascending order, reads segments oldest-first and stops early when possible.
    /// For descending order, reads segments newest-first.
    ///
    /// Returns (points, total_in_partition) where total_in_partition is an estimate
    /// of total points for this series in overlapping segments.
    pub fn query_limit(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
        limit: usize,
        ascending: bool,
    ) -> Result<(Vec<MemTablePoint>, usize)> {
        let segments = self.segments.read();
        let series_segments = match segments.get(&series_id) {
            Some(segs) => segs,
            None => return Ok((Vec::new(), 0)),
        };

        // Filter and sort segments by time range
        let mut sorted_segments: Vec<_> = series_segments
            .iter()
            .filter(|s| s.overlaps(time_range))
            .collect();

        if sorted_segments.is_empty() {
            return Ok((Vec::new(), 0));
        }

        // Sort segments by their start time
        sorted_segments.sort_by_key(|s| s.meta().time_range.start);

        // Reverse for descending order (newest first)
        if !ascending {
            sorted_segments.reverse();
        }

        // Estimate total points in overlapping segments
        let total_estimate: usize = sorted_segments.iter().map(|s| s.meta().point_count).sum();

        let mut collected_points: Vec<MemTablePoint> = Vec::new();

        for segment in sorted_segments {
            // Early termination check
            if collected_points.len() >= limit {
                if ascending {
                    // For ASC: if we have K points and next segment starts after our max,
                    // we can stop (since segments are time-ordered)
                    if let Some(last) = collected_points.last() {
                        if segment.meta().time_range.start > last.timestamp {
                            break;
                        }
                    }
                } else {
                    // For DESC: if we have K points and next segment ends before our min,
                    // we can stop
                    if let Some(last) = collected_points.last() {
                        if segment.meta().time_range.end <= last.timestamp {
                            break;
                        }
                    }
                }
            }

            // Read segment data (already sorted internally)
            let points = segment.read_range(time_range)?;
            collected_points.extend(points);
        }

        // Sort collected points
        if ascending {
            collected_points.sort_by_key(|p| p.timestamp);
        } else {
            collected_points.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        }

        // Truncate to limit
        collected_points.truncate(limit);

        Ok((collected_points, total_estimate))
    }

    /// Get partition ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get partition time range
    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    /// Check if a timestamp belongs to this partition
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        self.time_range.contains(timestamp)
    }

    /// Check if partition overlaps with time range
    pub fn overlaps(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }

    /// Get partition directory
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Get segment count
    pub fn segment_count(&self) -> usize {
        self.segment_metas.read().len()
    }

    /// Get total point count
    pub fn point_count(&self) -> usize {
        self.segment_metas.read().iter().map(|m| m.point_count).sum()
    }

    /// Get all series IDs in this partition
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.segments.read().keys().copied().collect()
    }

    /// Get all segment metadata for index rebuilding
    /// Returns (series_id, measurement, tags) for each unique series
    pub fn get_series_metadata(&self) -> Vec<(SeriesId, String, Vec<Tag>)> {
        let metas = self.segment_metas.read();
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for meta in metas.iter() {
            if seen.insert(meta.series_id) {
                result.push((meta.series_id, meta.measurement.clone(), meta.tags.clone()));
            }
        }

        result
    }

    /// Get aggregated field statistics for specified series within a time range.
    ///
    /// Returns aggregated FieldStats (min, max, sum, count) across all segments
    /// that overlap with the time range. This enables aggregation pushdown where
    /// COUNT/MIN/MAX/SUM can be computed without reading point data.
    ///
    /// Returns None if no segments have stats for the requested field.
    pub fn get_field_stats(
        &self,
        series_ids: &[SeriesId],
        time_range: &TimeRange,
        field_name: &str,
    ) -> Option<crate::segment::FieldStats> {
        let segments = self.segments.read();
        let mut total_min = f64::INFINITY;
        let mut total_max = f64::NEG_INFINITY;
        let mut total_sum = 0.0;
        let mut total_count = 0u64;
        let mut found_any = false;

        for series_id in series_ids {
            if let Some(series_segments) = segments.get(series_id) {
                for segment in series_segments {
                    // Check if segment overlaps with time range
                    if !segment.overlaps(time_range) {
                        continue;
                    }

                    // Get field stats from segment metadata
                    if let Some(stats) = segment.meta().field_stats.get(field_name) {
                        if let (Some(min), Some(max), Some(sum)) = (stats.min, stats.max, stats.sum) {
                            found_any = true;
                            total_min = total_min.min(min);
                            total_max = total_max.max(max);
                            total_sum += sum;
                            total_count += stats.count;
                        }
                    }
                }
            }
        }

        if found_any {
            Some(crate::segment::FieldStats {
                min: Some(total_min),
                max: Some(total_max),
                sum: Some(total_sum),
                count: total_count,
            })
        } else {
            None
        }
    }

    /// Get total point count for specified series within a time range.
    ///
    /// This is used for COUNT(*) optimization - counts points from segment
    /// metadata without reading actual data.
    pub fn get_point_count_for_series(
        &self,
        series_ids: &[SeriesId],
        time_range: &TimeRange,
    ) -> u64 {
        let segments = self.segments.read();
        let mut total = 0u64;

        for series_id in series_ids {
            if let Some(series_segments) = segments.get(series_id) {
                for segment in series_segments {
                    if segment.overlaps(time_range) {
                        total += segment.meta().point_count as u64;
                    }
                }
            }
        }

        total
    }

    /// Get metadata
    pub fn meta(&self) -> PartitionMeta {
        PartitionMeta {
            id: self.id,
            time_range: self.time_range,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            point_count: self.point_count(),
            segment_count: self.segment_count(),
        }
    }

    /// Save partition metadata
    fn save_meta(&self) -> Result<()> {
        let meta = self.meta();
        let meta_bytes = bincode::serialize(&meta)?;
        let meta_path = self.dir.join("partition.meta");
        std::fs::write(meta_path, meta_bytes)?;
        Ok(())
    }

    /// Delete the partition and all its data
    pub fn delete(self) -> Result<()> {
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

/// Partition manager for handling multiple partitions
pub struct PartitionManager {
    /// Base data directory
    data_dir: PathBuf,
    /// Partition duration in nanoseconds
    partition_duration: i64,
    /// Active partitions
    partitions: RwLock<Vec<Partition>>,
    /// Compression level
    compression: CompressionLevel,
    /// Next partition ID
    next_partition_id: AtomicU64,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(data_dir: impl AsRef<Path>, partition_duration: i64, compression: CompressionLevel) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let manager = Self {
            data_dir,
            partition_duration,
            partitions: RwLock::new(Vec::new()),
            compression,
            next_partition_id: AtomicU64::new(0),
        };

        // Load existing partitions
        manager.load_partitions()?;

        Ok(manager)
    }

    /// Load existing partitions from disk
    fn load_partitions(&self) -> Result<()> {
        let mut max_id = 0u64;

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let meta_path = path.join("partition.meta");
                if meta_path.exists() {
                    let partition = Partition::open(&path)?;
                    max_id = max_id.max(partition.id());
                    self.partitions.write().push(partition);
                }
            }
        }

        self.next_partition_id.store(max_id + 1, Ordering::SeqCst);

        // Sort partitions by time range
        self.partitions.write().sort_by_key(|p| p.time_range().start);

        Ok(())
    }

    /// Get or create partition for a timestamp
    pub fn get_or_create_partition(&self, timestamp: Timestamp) -> Result<&Partition> {
        // Calculate partition boundaries
        let partition_start = (timestamp / self.partition_duration) * self.partition_duration;
        let partition_end = partition_start + self.partition_duration;
        let time_range = TimeRange::new(partition_start, partition_end);

        // Check if partition exists
        {
            let partitions = self.partitions.read();
            for partition in partitions.iter() {
                if partition.contains(timestamp) {
                    // Safety: we're returning a reference to data behind RwLock
                    // This is safe because we never remove partitions during normal operation
                    return Ok(unsafe { &*(partition as *const Partition) });
                }
            }
        }

        // Create new partition
        let partition_id = self.next_partition_id.fetch_add(1, Ordering::SeqCst);
        let partition_dir = self.data_dir.join(format!("partition_{:08}", partition_id));

        let partition = Partition::new(partition_id, &partition_dir, time_range, self.compression)?;
        partition.save_meta()?;

        let mut partitions = self.partitions.write();
        partitions.push(partition);
        partitions.sort_by_key(|p| p.time_range().start);

        // Return reference to the new partition
        for partition in partitions.iter() {
            if partition.contains(timestamp) {
                return Ok(unsafe { &*(partition as *const Partition) });
            }
        }

        Err(StorageError::PartitionNotFound(format!("timestamp: {}", timestamp)))
    }

    /// Get partitions overlapping with a time range
    pub fn get_partitions_for_range(&self, time_range: &TimeRange) -> Vec<&Partition> {
        let partitions = self.partitions.read();
        partitions
            .iter()
            .filter(|p| p.overlaps(time_range))
            .map(|p| unsafe { &*(p as *const Partition) })
            .collect()
    }

    /// Get all partitions
    pub fn partitions(&self) -> Vec<&Partition> {
        let partitions = self.partitions.read();
        partitions
            .iter()
            .map(|p| unsafe { &*(p as *const Partition) })
            .collect()
    }

    /// Delete partitions older than a timestamp
    pub fn delete_partitions_before(&self, timestamp: Timestamp) -> Result<usize> {
        let mut partitions = self.partitions.write();
        let mut deleted = 0;

        partitions.retain(|p| {
            if p.time_range().end <= timestamp {
                if let Err(e) = std::fs::remove_dir_all(p.dir()) {
                    tracing::error!("Failed to delete partition: {}", e);
                } else {
                    deleted += 1;
                }
                false
            } else {
                true
            }
        });

        Ok(deleted)
    }

    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.partitions.read().len()
    }

    /// Get all unique series from all partitions for index rebuilding
    /// Returns (series_id, measurement, tags) for each unique series
    pub fn get_all_series(&self) -> Vec<(SeriesId, String, Vec<Tag>)> {
        let partitions = self.partitions.read();
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for partition in partitions.iter() {
            for (series_id, measurement, tags) in partition.get_series_metadata() {
                if seen.insert(series_id) {
                    result.push((series_id, measurement, tags));
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::FieldValue;
    use tempfile::TempDir;

    fn create_test_points(start_ts: i64, count: usize) -> Vec<MemTablePoint> {
        (0..count)
            .map(|i| MemTablePoint {
                timestamp: start_ts + i as i64 * 1000,
                fields: vec![
                    ("value".to_string(), FieldValue::Float(i as f64)),
                ],
            })
            .collect()
    }

    #[test]
    fn test_partition_create_write() {
        let dir = TempDir::new().unwrap();
        let time_range = TimeRange::new(0, 86400_000_000_000); // 1 day

        let partition = Partition::new(0, dir.path(), time_range, CompressionLevel::Fast).unwrap();

        let points = create_test_points(1000, 100);
        let tags = vec![Tag::new("host", "server01")];

        let meta = partition.write_segment(12345, "cpu", &tags, &points).unwrap();

        assert_eq!(meta.point_count, 100);
        assert_eq!(partition.segment_count(), 1);
    }

    #[test]
    fn test_partition_query() {
        let dir = TempDir::new().unwrap();
        let time_range = TimeRange::new(0, 86400_000_000_000);

        let partition = Partition::new(0, dir.path(), time_range, CompressionLevel::Fast).unwrap();

        let points = create_test_points(0, 100);
        partition.write_segment(12345, "cpu", &[], &points).unwrap();

        // Query all
        let results = partition.query(12345, &TimeRange::new(0, 100000)).unwrap();
        assert_eq!(results.len(), 100);

        // Query range
        let results = partition.query(12345, &TimeRange::new(25000, 75000)).unwrap();
        assert_eq!(results.len(), 50);

        // Query non-existent series
        let results = partition.query(99999, &TimeRange::new(0, 100000)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_partition_manager() {
        let dir = TempDir::new().unwrap();
        let duration = 3600_000_000_000_i64; // 1 hour in nanos

        let manager = PartitionManager::new(dir.path(), duration, CompressionLevel::Fast).unwrap();

        // Create partitions for different times
        let p1 = manager.get_or_create_partition(1000).unwrap();
        let p2 = manager.get_or_create_partition(duration + 1000).unwrap();
        let p3 = manager.get_or_create_partition(2 * duration + 1000).unwrap();

        assert_eq!(manager.partition_count(), 3);
        assert_ne!(p1.id(), p2.id());
        assert_ne!(p2.id(), p3.id());

        // Getting existing partition should return same one
        let p1_again = manager.get_or_create_partition(500).unwrap();
        assert_eq!(p1.id(), p1_again.id());
    }

    #[test]
    fn test_partition_manager_range_query() {
        let dir = TempDir::new().unwrap();
        let duration = 1000_000_000_i64; // 1 second

        let manager = PartitionManager::new(dir.path(), duration, CompressionLevel::Fast).unwrap();

        // Create 5 partitions
        for i in 0..5 {
            manager.get_or_create_partition(i * duration + 100).unwrap();
        }

        // Query range spanning 3 partitions
        let range = TimeRange::new(duration, 4 * duration);
        let partitions = manager.get_partitions_for_range(&range);

        assert_eq!(partitions.len(), 3);
    }

    #[test]
    fn test_partition_persistence() {
        let dir = TempDir::new().unwrap();
        let time_range = TimeRange::new(0, 86400_000_000_000);

        // Create and write
        {
            let partition = Partition::new(42, dir.path(), time_range, CompressionLevel::Fast).unwrap();
            let points = create_test_points(0, 100);
            partition.write_segment(12345, "cpu", &[], &points).unwrap();
        }

        // Reopen and verify
        {
            let partition = Partition::open(dir.path()).unwrap();
            assert_eq!(partition.id(), 42);
            assert_eq!(partition.segment_count(), 1);

            let results = partition.query(12345, &TimeRange::new(0, 100000)).unwrap();
            assert_eq!(results.len(), 100);
        }
    }
}
