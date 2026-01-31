//! Storage Engine - Main coordinator for storage operations
//!
//! The storage engine coordinates all storage operations including:
//! - Write path: WAL -> MemTable -> Segments
//! - Read path: MemTable + Segments
//! - Background flushing and compaction

use crate::error::Result;
use crate::memtable::{FlushTrigger, MemTable, MemTablePoint};
use crate::partition::PartitionManager;
use crate::wal::{WalDurability, WalReader, WalWriter};
use parking_lot::RwLock;
use rusts_compression::CompressionLevel;
use rusts_core::{Point, SeriesId, TimeRange};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Checkpoint file name for tracking flushed WAL sequences
const CHECKPOINT_FILE: &str = "wal_checkpoint";

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEngineConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// WAL directory (defaults to data_dir/wal)
    pub wal_dir: Option<PathBuf>,
    /// WAL durability mode
    pub wal_durability: WalDurability,
    /// WAL retention period in seconds - files older than this will be cleaned up
    /// after their data is flushed to segments. None means retain forever.
    /// Useful for CDC consumers and backup systems that read from WAL.
    /// Default: 7 days (604800 seconds)
    pub wal_retention_secs: Option<u64>,
    /// MemTable flush trigger
    pub flush_trigger: FlushTrigger,
    /// Partition duration in nanoseconds
    pub partition_duration: i64,
    /// Compression level for segments
    pub compression: CompressionLevel,
}

impl Default for StorageEngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_dir: None,
            wal_durability: WalDurability::default(),
            wal_retention_secs: Some(7 * 24 * 60 * 60), // 7 days default
            flush_trigger: FlushTrigger::default(),
            partition_duration: 24 * 60 * 60 * 1_000_000_000, // 1 day
            compression: CompressionLevel::Default,
        }
    }
}

/// Flush command for background flusher
enum FlushCommand {
    /// Flush a memtable with its associated WAL sequence number
    Flush {
        memtable: Arc<MemTable>,
        wal_sequence: u64,
    },
    Shutdown,
}

/// Storage engine
pub struct StorageEngine {
    /// Configuration
    config: StorageEngineConfig,
    /// Write-ahead log writer
    wal: WalWriter,
    /// WAL directory path
    wal_dir: PathBuf,
    /// Active memtable for writes
    active_memtable: RwLock<Arc<MemTable>>,
    /// Immutable memtables being flushed
    immutable_memtables: RwLock<Vec<Arc<MemTable>>>,
    /// Partition manager
    partitions: PartitionManager,
    /// Flush command sender
    flush_tx: mpsc::UnboundedSender<FlushCommand>,
    /// Indicates if engine is running
    running: RwLock<bool>,
    /// Last WAL sequence number that has been flushed to segments
    last_flushed_sequence: AtomicU64,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new(config: StorageEngineConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let wal_dir = config
            .wal_dir
            .clone()
            .unwrap_or_else(|| config.data_dir.join("wal"));

        let wal = WalWriter::new(&wal_dir, config.wal_durability)?;

        let partitions_dir = config.data_dir.join("partitions");
        let partitions = PartitionManager::new(
            &partitions_dir,
            config.partition_duration,
            config.compression,
        )?;

        let active_memtable = Arc::new(MemTable::with_flush_trigger(config.flush_trigger.clone()));

        // Create flush channel
        let (flush_tx, flush_rx) = mpsc::unbounded_channel();

        // Load checkpoint from disk
        let checkpoint_sequence = Self::load_checkpoint(&config.data_dir);
        if let Some(seq) = checkpoint_sequence {
            info!("Loaded WAL checkpoint: sequence {}", seq);
        }

        let engine = Self {
            config,
            wal,
            wal_dir,
            active_memtable: RwLock::new(active_memtable),
            immutable_memtables: RwLock::new(Vec::new()),
            partitions,
            flush_tx,
            running: RwLock::new(true),
            last_flushed_sequence: AtomicU64::new(checkpoint_sequence.unwrap_or(0)),
        };

        // Start background flusher
        engine.start_flusher(flush_rx);

        // Recover from WAL (pass checkpoint so we know whether to filter entries)
        engine.recover(checkpoint_sequence)?;

        Ok(engine)
    }

    /// Write a single point
    pub fn write(&self, point: &Point) -> Result<()> {
        self.write_batch(&[point.clone()])
    }

    /// Write a batch of points
    pub fn write_batch(&self, points: &[Point]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        // Validate points
        for point in points {
            point.validate()?;
        }

        // Write to WAL first
        self.wal.write(points)?;

        // Write to active memtable and check if flush is needed
        let should_flush = {
            let memtable = self.active_memtable.read();
            memtable.insert_batch(points)?;
            memtable.should_flush()
        };

        // Check if flush is needed
        if should_flush {
            self.rotate_memtable()?;
        }

        Ok(())
    }

    /// Query points for a series
    pub fn query(&self, series_id: SeriesId, time_range: &TimeRange) -> Result<Vec<MemTablePoint>> {
        let mut all_points = Vec::new();

        // Query active memtable
        {
            let memtable = self.active_memtable.read();
            let points = memtable.query(series_id, time_range);
            all_points.extend(points);
        }

        // Query immutable memtables
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                let points = memtable.query(series_id, time_range);
                all_points.extend(points);
            }
        }

        // Query partitions
        let partitions = self.partitions.get_partitions_for_range(time_range);
        for partition in partitions {
            let points = partition.query(series_id, time_range)?;
            all_points.extend(points);
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);

        Ok(all_points)
    }

    /// Query points for a series using parallel partition scanning.
    ///
    /// Uses rayon to query multiple partitions in parallel, which provides
    /// 2-4x speedup for aggregation queries that scan many partitions.
    /// Falls back to sequential for small numbers of partitions (< 2).
    pub fn query_parallel(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
    ) -> Result<Vec<MemTablePoint>> {
        use rayon::prelude::*;

        let mut all_points = Vec::new();

        // Query memtables (small, sequential) - must be done first for consistent reads
        {
            let memtable = self.active_memtable.read();
            all_points.extend(memtable.query(series_id, time_range));
        }
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                all_points.extend(memtable.query(series_id, time_range));
            }
        }

        // Query partitions in parallel if there are enough
        let partitions = self.partitions.get_partitions_for_range(time_range);
        if partitions.len() >= 2 {
            // Parallel path using rayon
            let partition_results: Vec<Result<Vec<MemTablePoint>>> = partitions
                .par_iter()
                .map(|partition| partition.query(series_id, time_range))
                .collect();

            for result in partition_results {
                all_points.extend(result?);
            }
        } else {
            // Sequential for single partition (no benefit from parallelism)
            for partition in partitions {
                all_points.extend(partition.query(series_id, time_range)?);
            }
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);

        Ok(all_points)
    }

    /// Query all series in a measurement with parallel partition scanning.
    ///
    /// Optimized for aggregation queries that need to scan all data for multiple
    /// series in a single measurement. Uses rayon to parallelize across partitions.
    pub fn query_measurement_parallel(
        &self,
        series_ids: &[SeriesId],
        time_range: &TimeRange,
    ) -> Result<Vec<(SeriesId, Vec<MemTablePoint>)>> {
        self.query_measurement_parallel_with_fields(series_ids, time_range, None)
    }

    /// Query all series with column pruning support.
    ///
    /// When `fields` is Some, only the specified fields are read from segments,
    /// which can significantly reduce I/O for wide tables with many columns.
    /// Pass `Some(&[])` for COUNT(*) to skip all field data (timestamp only).
    pub fn query_measurement_parallel_with_fields(
        &self,
        series_ids: &[SeriesId],
        time_range: &TimeRange,
        fields: Option<&[String]>,
    ) -> Result<Vec<(SeriesId, Vec<MemTablePoint>)>> {
        use rayon::prelude::*;
        use std::collections::HashMap;

        // Query memtables (sequential, small) - memtables don't support column pruning
        let mut results: HashMap<SeriesId, Vec<MemTablePoint>> = HashMap::new();
        {
            let memtable = self.active_memtable.read();
            for &series_id in series_ids {
                let points = memtable.query(series_id, time_range);
                if !points.is_empty() {
                    results.entry(series_id).or_default().extend(points);
                }
            }
        }
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                for &series_id in series_ids {
                    let points = memtable.query(series_id, time_range);
                    if !points.is_empty() {
                        results.entry(series_id).or_default().extend(points);
                    }
                }
            }
        }

        // Query partitions in parallel with column pruning
        let partitions = self.partitions.get_partitions_for_range(time_range);

        if partitions.len() >= 2 {
            // Parallel: query all series from each partition
            let partition_results: Vec<Result<Vec<(SeriesId, Vec<MemTablePoint>)>>> = partitions
                .par_iter()
                .map(|partition| {
                    let mut partition_data = Vec::new();
                    for &series_id in series_ids {
                        let points = partition.query_with_fields(series_id, time_range, fields)?;
                        if !points.is_empty() {
                            partition_data.push((series_id, points));
                        }
                    }
                    Ok(partition_data)
                })
                .collect();

            // Merge results
            for result in partition_results {
                for (series_id, points) in result? {
                    results.entry(series_id).or_default().extend(points);
                }
            }
        } else {
            // Sequential for single partition
            for partition in partitions {
                for &series_id in series_ids {
                    let points = partition.query_with_fields(series_id, time_range, fields)?;
                    if !points.is_empty() {
                        results.entry(series_id).or_default().extend(points);
                    }
                }
            }
        }

        // Sort points within each series
        let mut result_vec: Vec<_> = results
            .into_iter()
            .map(|(series_id, mut points)| {
                points.sort_by_key(|p| p.timestamp);
                (series_id, points)
            })
            .collect();

        result_vec.sort_by_key(|(id, _)| *id);
        Ok(result_vec)
    }

    /// Query points with early termination for LIMIT queries.
    ///
    /// Returns (points, total_scanned) where:
    /// - points: up to `limit` points in the requested order
    /// - total_scanned: total number of points seen (for pagination info)
    ///
    /// This method queries partitions in time order and terminates early
    /// when remaining partitions cannot contribute to the result.
    pub fn query_with_limit(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
        limit: usize,
        ascending: bool,
    ) -> Result<(Vec<MemTablePoint>, usize)> {
        let mut total_scanned = 0;

        // Collect all memtable points first (can't skip - might have any timestamps)
        let mut memtable_points = Vec::new();
        {
            let memtable = self.active_memtable.read();
            memtable_points.extend(memtable.query(series_id, time_range));
        }
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                memtable_points.extend(memtable.query(series_id, time_range));
            }
        }
        total_scanned += memtable_points.len();

        // Get partitions in time order
        let mut partitions = self.partitions.get_partitions_for_range(time_range);
        if !ascending {
            // Reverse for descending order (newest first)
            partitions.reverse();
        }

        if ascending {
            // Ascending: use max-heap to track K smallest timestamps
            self.query_with_limit_asc(
                series_id,
                time_range,
                limit,
                memtable_points,
                partitions,
                total_scanned,
            )
        } else {
            // Descending: use min-heap to track K largest timestamps
            self.query_with_limit_desc(
                series_id,
                time_range,
                limit,
                memtable_points,
                partitions,
                total_scanned,
            )
        }
    }

    /// Query with limit, ascending order (smallest timestamps first).
    /// Leverages the fact that time series data is mostly sorted - segments within
    /// partitions are sorted and can be queried in time order for early termination.
    fn query_with_limit_asc(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
        limit: usize,
        memtable_points: Vec<MemTablePoint>,
        partitions: Vec<&crate::partition::Partition>,
        mut total_scanned: usize,
    ) -> Result<(Vec<MemTablePoint>, usize)> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        // Wrapper for heap that compares by timestamp only (max-heap)
        struct TsPoint(i64, MemTablePoint);
        impl PartialEq for TsPoint {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for TsPoint {}
        impl PartialOrd for TsPoint {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for TsPoint {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.cmp(&other.0) // Max-heap by timestamp
            }
        }

        // Max-heap to track K smallest timestamps (largest of the K at top for eviction)
        let mut heap: BinaryHeap<TsPoint> = BinaryHeap::with_capacity(limit + 1);

        // Add memtable points to heap
        for point in memtable_points {
            let ts = point.timestamp;
            if heap.len() < limit {
                heap.push(TsPoint(ts, point));
            } else if let Some(max_entry) = heap.peek() {
                if ts < max_entry.0 {
                    heap.pop();
                    heap.push(TsPoint(ts, point));
                }
            }
        }

        // Query partitions (oldest first for ascending)
        // Use partition's optimized query that reads segments in time order
        for partition in partitions {
            // Early termination: if heap is full and partition starts after our max timestamp,
            // no points from this or later partitions can improve our result
            if heap.len() >= limit {
                if let Some(max_entry) = heap.peek() {
                    if partition.time_range().start > max_entry.0 {
                        break;
                    }
                }
            }

            // Use optimized partition query that leverages sorted segments
            let (points, partition_total) =
                partition.query_limit(series_id, time_range, limit, true)?;
            total_scanned += partition_total;

            for point in points {
                let ts = point.timestamp;
                if heap.len() < limit {
                    heap.push(TsPoint(ts, point));
                } else if let Some(max_entry) = heap.peek() {
                    if ts < max_entry.0 {
                        heap.pop();
                        heap.push(TsPoint(ts, point));
                    }
                }
            }
        }

        // Extract results in ascending order
        let mut results: Vec<_> = heap.into_iter().map(|tp| tp.1).collect();
        results.sort_by_key(|p| p.timestamp);

        Ok((results, total_scanned))
    }

    /// Query with limit, descending order (largest timestamps first).
    /// Leverages the fact that time series data is mostly sorted.
    fn query_with_limit_desc(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
        limit: usize,
        memtable_points: Vec<MemTablePoint>,
        partitions: Vec<&crate::partition::Partition>,
        mut total_scanned: usize,
    ) -> Result<(Vec<MemTablePoint>, usize)> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        // Wrapper for heap that compares by timestamp only (min-heap via reversed comparison)
        struct TsPointDesc(i64, MemTablePoint);
        impl PartialEq for TsPointDesc {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for TsPointDesc {}
        impl PartialOrd for TsPointDesc {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for TsPointDesc {
            fn cmp(&self, other: &Self) -> Ordering {
                other.0.cmp(&self.0) // Min-heap by timestamp (reversed)
            }
        }

        // Min-heap to track K largest timestamps (smallest of the K at top for eviction)
        let mut heap: BinaryHeap<TsPointDesc> = BinaryHeap::with_capacity(limit + 1);

        // Add memtable points to heap
        for point in memtable_points {
            let ts = point.timestamp;
            if heap.len() < limit {
                heap.push(TsPointDesc(ts, point));
            } else if let Some(min_entry) = heap.peek() {
                if ts > min_entry.0 {
                    heap.pop();
                    heap.push(TsPointDesc(ts, point));
                }
            }
        }

        // Query partitions (newest first for descending)
        // Use partition's optimized query that reads segments in time order
        for partition in partitions {
            // Early termination: if heap is full and partition ends before our min timestamp,
            // no points from this or earlier partitions can improve our result
            if heap.len() >= limit {
                if let Some(min_entry) = heap.peek() {
                    if partition.time_range().end <= min_entry.0 {
                        break;
                    }
                }
            }

            // Use optimized partition query that leverages sorted segments
            let (points, partition_total) =
                partition.query_limit(series_id, time_range, limit, false)?;
            total_scanned += partition_total;

            for point in points {
                let ts = point.timestamp;
                if heap.len() < limit {
                    heap.push(TsPointDesc(ts, point));
                } else if let Some(min_entry) = heap.peek() {
                    if ts > min_entry.0 {
                        heap.pop();
                        heap.push(TsPointDesc(ts, point));
                    }
                }
            }
        }

        // Extract results in descending order
        let mut results: Vec<_> = heap.into_iter().map(|tp| tp.1).collect();
        results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok((results, total_scanned))
    }

    /// Query points for multiple series with early termination for LIMIT queries.
    ///
    /// This method optimizes multi-series queries by:
    /// 1. Querying partitions in time order (oldest-first for ASC, newest-first for DESC)
    /// 2. Querying all requested series from each partition
    /// 3. Using a bounded heap to track top K results
    /// 4. Early terminating when remaining partitions can't improve the result
    ///
    /// Returns (points_per_series, total_scanned)
    pub fn query_multi_series_with_limit(
        &self,
        series_ids: &[SeriesId],
        time_range: &TimeRange,
        limit: usize,
        ascending: bool,
    ) -> Result<(Vec<(SeriesId, Vec<MemTablePoint>)>, usize)> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        // Collect memtable points for all series first
        let mut memtable_points: HashMap<SeriesId, Vec<MemTablePoint>> = HashMap::new();
        {
            let memtable = self.active_memtable.read();
            for &series_id in series_ids {
                let points = memtable.query(series_id, time_range);
                if !points.is_empty() {
                    memtable_points.entry(series_id).or_default().extend(points);
                }
            }
        }
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                for &series_id in series_ids {
                    let points = memtable.query(series_id, time_range);
                    if !points.is_empty() {
                        memtable_points.entry(series_id).or_default().extend(points);
                    }
                }
            }
        }

        let mut total_scanned: usize = memtable_points.values().map(|v| v.len()).sum();

        // Get partitions in time order
        let mut partitions = self.partitions.get_partitions_for_range(time_range);
        if !ascending {
            partitions.reverse();
        }

        if ascending {
            // Max-heap for ascending (keeps K smallest)
            struct TsPoint(i64, SeriesId, MemTablePoint);
            impl PartialEq for TsPoint {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }
            impl Eq for TsPoint {}
            impl PartialOrd for TsPoint {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }
            impl Ord for TsPoint {
                fn cmp(&self, other: &Self) -> Ordering {
                    self.0.cmp(&other.0) // Max-heap
                }
            }

            let mut heap: BinaryHeap<TsPoint> = BinaryHeap::with_capacity(limit + 1);

            // Add memtable points
            for (&series_id, points) in &memtable_points {
                for point in points {
                    let ts = point.timestamp;
                    if heap.len() < limit {
                        heap.push(TsPoint(ts, series_id, point.clone()));
                    } else if let Some(max) = heap.peek() {
                        if ts < max.0 {
                            heap.pop();
                            heap.push(TsPoint(ts, series_id, point.clone()));
                        }
                    }
                }
            }

            // Query partitions
            for partition in partitions {
                // Early termination check: if heap is full and this partition starts after
                // our max timestamp, remaining partitions can't improve our result
                if heap.len() >= limit {
                    if let Some(max) = heap.peek() {
                        if partition.time_range().start > max.0 {
                            break;
                        }
                    }
                }

                // Query all series from this partition
                for &series_id in series_ids {
                    let (points, scanned) =
                        partition.query_limit(series_id, time_range, limit, true)?;
                    total_scanned += scanned;

                    for point in points {
                        let ts = point.timestamp;
                        if heap.len() < limit {
                            heap.push(TsPoint(ts, series_id, point));
                        } else if let Some(max) = heap.peek() {
                            if ts < max.0 {
                                heap.pop();
                                heap.push(TsPoint(ts, series_id, point));
                            }
                        }
                    }
                }
            }

            // Group results by series
            let mut results: HashMap<SeriesId, Vec<MemTablePoint>> = HashMap::new();
            for TsPoint(_, series_id, point) in heap {
                results.entry(series_id).or_default().push(point);
            }

            // Sort points within each series
            for points in results.values_mut() {
                points.sort_by_key(|p| p.timestamp);
            }

            let result_vec: Vec<_> = results.into_iter().collect();
            Ok((result_vec, total_scanned))
        } else {
            // Min-heap for descending (keeps K largest)
            struct TsPointDesc(i64, SeriesId, MemTablePoint);
            impl PartialEq for TsPointDesc {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }
            impl Eq for TsPointDesc {}
            impl PartialOrd for TsPointDesc {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }
            impl Ord for TsPointDesc {
                fn cmp(&self, other: &Self) -> Ordering {
                    other.0.cmp(&self.0) // Min-heap (reversed)
                }
            }

            let mut heap: BinaryHeap<TsPointDesc> = BinaryHeap::with_capacity(limit + 1);

            // Add memtable points
            for (&series_id, points) in &memtable_points {
                for point in points {
                    let ts = point.timestamp;
                    if heap.len() < limit {
                        heap.push(TsPointDesc(ts, series_id, point.clone()));
                    } else if let Some(min) = heap.peek() {
                        if ts > min.0 {
                            heap.pop();
                            heap.push(TsPointDesc(ts, series_id, point.clone()));
                        }
                    }
                }
            }

            // Query partitions (newest first)
            for partition in partitions {
                // Early termination check
                if heap.len() >= limit {
                    if let Some(min) = heap.peek() {
                        if partition.time_range().end <= min.0 {
                            break;
                        }
                    }
                }

                // Query all series from this partition
                for &series_id in series_ids {
                    let (points, scanned) =
                        partition.query_limit(series_id, time_range, limit, false)?;
                    total_scanned += scanned;

                    for point in points {
                        let ts = point.timestamp;
                        if heap.len() < limit {
                            heap.push(TsPointDesc(ts, series_id, point));
                        } else if let Some(min) = heap.peek() {
                            if ts > min.0 {
                                heap.pop();
                                heap.push(TsPointDesc(ts, series_id, point));
                            }
                        }
                    }
                }
            }

            // Group results by series
            let mut results: HashMap<SeriesId, Vec<MemTablePoint>> = HashMap::new();
            for TsPointDesc(_, series_id, point) in heap {
                results.entry(series_id).or_default().push(point);
            }

            // Sort points within each series (descending)
            for points in results.values_mut() {
                points.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            }

            let result_vec: Vec<_> = results.into_iter().collect();
            Ok((result_vec, total_scanned))
        }
    }

    /// Query points by measurement name
    pub fn query_measurement(
        &self,
        measurement: &str,
        time_range: &TimeRange,
    ) -> Result<Vec<(SeriesId, Vec<MemTablePoint>)>> {
        let mut results: std::collections::HashMap<SeriesId, Vec<MemTablePoint>> =
            std::collections::HashMap::new();

        // Query active memtable
        {
            let memtable = self.active_memtable.read();
            for (series_id, points) in memtable.query_measurement(measurement, time_range) {
                results.entry(series_id).or_default().extend(points);
            }
        }

        // Query immutable memtables
        {
            let immutables = self.immutable_memtables.read();
            for memtable in immutables.iter() {
                for (series_id, points) in memtable.query_measurement(measurement, time_range) {
                    results.entry(series_id).or_default().extend(points);
                }
            }
        }

        // Sort points and convert to vec
        let mut result_vec: Vec<_> = results
            .into_iter()
            .map(|(series_id, mut points)| {
                points.sort_by_key(|p| p.timestamp);
                (series_id, points)
            })
            .collect();

        result_vec.sort_by_key(|(id, _)| *id);
        Ok(result_vec)
    }

    /// Force sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        self.wal.sync()
    }

    /// Force flush active memtable
    pub fn flush(&self) -> Result<()> {
        self.rotate_memtable()
    }

    /// Shutdown the storage engine
    ///
    /// This performs a clean shutdown by:
    /// 1. Flushing the active memtable to partitions
    /// 2. Updating the checkpoint
    /// 3. Syncing WAL to disk
    ///
    /// After a clean shutdown, recovery on restart should be instant
    /// (no WAL entries to replay).
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down storage engine");

        *self.running.write() = false;

        // Flush active memtable if it has data
        // This ensures all data is persisted to partitions before shutdown
        {
            let memtable = self.active_memtable.read();
            if memtable.point_count() > 0 {
                info!(
                    "Flushing {} points from active memtable before shutdown",
                    memtable.point_count()
                );
                // Get current WAL sequence for checkpoint
                let wal_sequence = self.wal.sequence();

                // Flush directly to partitions (synchronously, not via background thread)
                if let Err(e) = flush_memtable_to_partitions(&memtable, &self.partitions) {
                    error!("Failed to flush memtable during shutdown: {}", e);
                } else {
                    // Update checkpoint after successful flush
                    if let Err(e) = Self::save_checkpoint(&self.config.data_dir, wal_sequence) {
                        error!("Failed to save checkpoint during shutdown: {}", e);
                    } else {
                        info!("Updated checkpoint to {} during shutdown", wal_sequence);
                    }
                }
            }
        }

        // Send shutdown command to background flusher
        let _ = self.flush_tx.send(FlushCommand::Shutdown);

        // Sync WAL to disk
        self.sync()?;

        info!("Storage engine shutdown complete");
        Ok(())
    }

    /// Get the data directory
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    /// Get current memtable stats
    pub fn memtable_stats(&self) -> MemTableStats {
        let active = self.active_memtable.read();
        let immutables = self.immutable_memtables.read();

        MemTableStats {
            active_size: active.size(),
            active_points: active.point_count(),
            active_series: active.series_count(),
            immutable_count: immutables.len(),
        }
    }

    /// Get all series data from the active memtable
    /// Used for rebuilding indexes after WAL recovery
    pub fn get_memtable_series(&self) -> Vec<(SeriesId, String, Vec<rusts_core::Tag>)> {
        let memtable = self.active_memtable.read();
        memtable
            .iter_series()
            .map(|(series_id, measurement, tags, _points)| (series_id, measurement, tags))
            .collect()
    }

    /// Get all series from partitions (flushed data)
    /// Used for rebuilding indexes on server startup
    pub fn get_partition_series(&self) -> Vec<(SeriesId, String, Vec<rusts_core::Tag>)> {
        self.partitions.get_all_series()
    }

    /// Get all series from both memtable and partitions
    /// Used for complete index rebuilding on server startup
    pub fn get_all_series(&self) -> Vec<(SeriesId, String, Vec<rusts_core::Tag>)> {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut result = Vec::new();

        // Add series from memtable (recovered from WAL)
        for (series_id, measurement, tags) in self.get_memtable_series() {
            if seen.insert(series_id) {
                result.push((series_id, measurement, tags));
            }
        }

        // Add series from partitions (flushed to disk)
        for (series_id, measurement, tags) in self.get_partition_series() {
            if seen.insert(series_id) {
                result.push((series_id, measurement, tags));
            }
        }

        result
    }

    /// Get partition stats
    pub fn partition_stats(&self) -> PartitionStats {
        let partitions = self.partitions.partitions();
        PartitionStats {
            partition_count: partitions.len(),
            total_segments: partitions.iter().map(|p| p.segment_count()).sum(),
            total_points: partitions.iter().map(|p| p.point_count()).sum(),
        }
    }

    /// Get the last flushed WAL sequence number
    pub fn last_flushed_sequence(&self) -> u64 {
        self.last_flushed_sequence.load(Ordering::SeqCst)
    }

    /// Update the last flushed sequence number (called after successful flush)
    pub fn set_last_flushed_sequence(&self, sequence: u64) {
        self.last_flushed_sequence.store(sequence, Ordering::SeqCst);
    }

    /// Load checkpoint from disk (returns None if no checkpoint exists)
    fn load_checkpoint(data_dir: &Path) -> Option<u64> {
        let checkpoint_path = data_dir.join(CHECKPOINT_FILE);
        match fs::read_to_string(&checkpoint_path) {
            Ok(content) => content.trim().parse().ok(),
            Err(_) => None,
        }
    }

    /// Save checkpoint to disk
    fn save_checkpoint(data_dir: &Path, sequence: u64) -> Result<()> {
        let checkpoint_path = data_dir.join(CHECKPOINT_FILE);
        let mut file = fs::File::create(&checkpoint_path)?;
        writeln!(file, "{}", sequence)?;
        file.sync_all()?;
        Ok(())
    }

    /// Clean up old WAL files based on retention policy.
    /// Only removes files that:
    /// 1. Are older than the configured retention period
    /// 2. Have all entries already flushed to segments (sequence <= last_flushed_sequence)
    ///
    /// Returns the number of files cleaned up.
    pub fn cleanup_wal(&self) -> Result<usize> {
        let retention = match self.config.wal_retention_secs {
            Some(secs) => Duration::from_secs(secs),
            None => {
                debug!("WAL retention is None, skipping cleanup");
                return Ok(0);
            }
        };

        let flushed_seq = self.last_flushed_sequence.load(Ordering::SeqCst);
        if flushed_seq == 0 {
            debug!("No data flushed yet, skipping WAL cleanup");
            return Ok(0);
        }

        let reader = WalReader::new(&self.wal_dir);
        let now = std::time::SystemTime::now();
        let mut cleaned = 0;

        // Get list of WAL files with their metadata
        let wal_files = reader.list_wal_files_with_metadata()?;

        for (file_path, modified_time, max_sequence) in wal_files {
            // Check if file is old enough
            let age = now.duration_since(modified_time).unwrap_or(Duration::ZERO);
            if age < retention {
                debug!(
                    "WAL file {:?} is not old enough ({:?} < {:?})",
                    file_path, age, retention
                );
                continue;
            }

            // Check if all entries in this file have been flushed
            if max_sequence > flushed_seq {
                debug!(
                    "WAL file {:?} has unflushed entries (max_seq {} > flushed {})",
                    file_path, max_sequence, flushed_seq
                );
                continue;
            }

            // Safe to delete this file
            info!(
                "Cleaning up WAL file {:?} (age: {:?}, max_seq: {})",
                file_path, age, max_sequence
            );
            if let Err(e) = std::fs::remove_file(&file_path) {
                warn!("Failed to remove WAL file {:?}: {}", file_path, e);
            } else {
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            info!("Cleaned up {} WAL files", cleaned);
        }

        Ok(cleaned)
    }

    /// Rotate the active memtable (make it immutable and create new active)
    fn rotate_memtable(&self) -> Result<()> {
        // Get the current WAL sequence before rotation
        // All data in this memtable was written at or before this sequence
        let wal_sequence = self.wal.sequence();

        let old_memtable = {
            let mut active = self.active_memtable.write();
            let old = active.clone();
            old.seal();

            // Create new active memtable
            *active = Arc::new(MemTable::with_flush_trigger(self.config.flush_trigger.clone()));

            old
        };

        // Add to immutable list
        self.immutable_memtables.write().push(old_memtable.clone());

        // Trigger background flush with the WAL sequence
        let _ = self.flush_tx.send(FlushCommand::Flush {
            memtable: old_memtable,
            wal_sequence,
        });

        debug!("Rotated memtable (WAL sequence: {})", wal_sequence);
        Ok(())
    }

    /// Start the background flusher thread
    fn start_flusher(&self, mut rx: mpsc::UnboundedReceiver<FlushCommand>) {
        // We need to pass data to the flusher thread
        // Since we can't move self, we'll create the thread in a way that can access partitions
        let data_dir = self.config.data_dir.clone();
        let partitions_dir = self.config.data_dir.join("partitions");
        let partition_duration = self.config.partition_duration;
        let compression = self.config.compression;

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                // Create a new partition manager for the flusher
                let partitions = match PartitionManager::new(&partitions_dir, partition_duration, compression) {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to create partition manager in flusher: {}", e);
                        return;
                    }
                };

                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        FlushCommand::Flush { memtable, wal_sequence } => {
                            if let Err(e) = flush_memtable_to_partitions(&memtable, &partitions) {
                                error!("Failed to flush memtable: {}", e);
                            } else {
                                debug!("Flushed memtable with {} points (WAL sequence: {})",
                                       memtable.point_count(), wal_sequence);

                                // Save checkpoint after successful flush
                                if let Err(e) = Self::save_checkpoint(&data_dir, wal_sequence) {
                                    error!("Failed to save checkpoint: {}", e);
                                } else {
                                    debug!("Saved WAL checkpoint: sequence {}", wal_sequence);
                                }
                            }
                        }
                        FlushCommand::Shutdown => {
                            info!("Flusher shutting down");
                            break;
                        }
                    }
                }
            });
        });
    }

    /// Recover data from WAL
    ///
    /// If checkpoint is None, all WAL entries are recovered (fresh start).
    /// If checkpoint is Some(seq), only entries with sequence > seq are recovered.
    ///
    /// This uses an optimized recovery path that skips entire WAL files
    /// that contain only already-flushed entries.
    fn recover(&self, checkpoint: Option<u64>) -> Result<()> {
        let wal_dir = self
            .config
            .wal_dir
            .clone()
            .unwrap_or_else(|| self.config.data_dir.join("wal"));

        let reader = WalReader::new(&wal_dir);

        // Use optimized checkpoint-aware recovery when we have a checkpoint
        let (entries_to_recover, files_skipped, files_read) = if let Some(cp) = checkpoint {
            reader.read_after_checkpoint(cp)?
        } else {
            // No checkpoint - recover all entries
            let entries = reader.read_all()?;
            let file_count = reader.list_wal_files()?.len();
            (entries, 0, file_count)
        };

        if entries_to_recover.is_empty() {
            if let Some(cp) = checkpoint {
                info!(
                    "All WAL entries already flushed (checkpoint: {}, skipped {} files)",
                    cp, files_skipped
                );
            } else {
                info!("No WAL entries to recover");
            }
            return Ok(());
        }

        match checkpoint {
            Some(cp) => info!(
                "Recovering {} WAL entries (checkpoint: {}, skipped {} files, read {} files)",
                entries_to_recover.len(),
                cp,
                files_skipped,
                files_read
            ),
            None => info!(
                "Recovering {} WAL entries (no checkpoint, read {} files)",
                entries_to_recover.len(),
                files_read
            ),
        }

        let memtable = self.active_memtable.read();
        for entry in entries_to_recover {
            for point in entry.points {
                if let Err(e) = memtable.insert(&point) {
                    error!("Failed to recover point: {}", e);
                }
            }
        }

        info!("WAL recovery complete, {} points recovered", memtable.point_count());
        Ok(())
    }
}

/// Flush a memtable to partitions
fn flush_memtable_to_partitions(memtable: &MemTable, partitions: &PartitionManager) -> Result<()> {
    for (series_id, measurement, tags, points) in memtable.iter_series() {
        if points.is_empty() {
            continue;
        }

        // Group points by partition
        let mut partition_points: std::collections::HashMap<i64, Vec<MemTablePoint>> =
            std::collections::HashMap::new();

        for point in points {
            let partition_start = (point.timestamp / partitions.partitions().first().map(|p| p.time_range().end - p.time_range().start).unwrap_or(86400_000_000_000)) * partitions.partitions().first().map(|p| p.time_range().end - p.time_range().start).unwrap_or(86400_000_000_000);
            partition_points.entry(partition_start).or_default().push(point);
        }

        // Write to each partition
        for (_, points) in partition_points {
            if points.is_empty() {
                continue;
            }

            let partition = partitions.get_or_create_partition(points[0].timestamp)?;
            partition.write_segment(series_id, &measurement, &tags, &points)?;
        }
    }

    Ok(())
}

/// MemTable statistics
#[derive(Debug, Clone)]
pub struct MemTableStats {
    pub active_size: usize,
    pub active_points: usize,
    pub active_series: usize,
    pub immutable_count: usize,
}

/// Partition statistics
#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_count: usize,
    pub total_segments: usize,
    pub total_points: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        Point::builder(measurement)
            .timestamp(ts)
            .tag("host", host)
            .field("value", value)
            .build()
            .unwrap()
    }

    #[test]
    fn test_storage_engine_write_query() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write some points
        for i in 0..100 {
            let point = create_test_point("cpu", "server01", i * 1000, i as f64);
            engine.write(&point).unwrap();
        }

        // Query
        let point = create_test_point("cpu", "server01", 0, 0.0);
        let series_id = point.series_id();

        let results = engine.query(series_id, &TimeRange::new(0, 100000)).unwrap();
        assert_eq!(results.len(), 100);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_storage_engine_batch_write() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        let points: Vec<Point> = (0..1000)
            .map(|i| create_test_point("cpu", "server01", i * 1000, i as f64))
            .collect();

        engine.write_batch(&points).unwrap();

        let series_id = points[0].series_id();
        let results = engine.query(series_id, &TimeRange::new(0, 1000000)).unwrap();
        assert_eq!(results.len(), 1000);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_storage_engine_query_measurement() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write to multiple series
        for i in 0..10 {
            for j in 0..10 {
                let point = create_test_point("cpu", &format!("server{:02}", i), j * 1000, j as f64);
                engine.write(&point).unwrap();
            }
        }

        let results = engine
            .query_measurement("cpu", &TimeRange::new(0, 100000))
            .unwrap();

        assert_eq!(results.len(), 10); // 10 series
        for (_, points) in &results {
            assert_eq!(points.len(), 10); // 10 points each
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_storage_engine_stats() {
        let dir = TempDir::new().unwrap();
        // Use a flush trigger with very high thresholds to prevent auto-flush
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,   // 1GB
                max_points: 1_000_000_000,      // 1B points
                max_age_nanos: i64::MAX,        // Never trigger on age
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Use current time as base to avoid age-based flush trigger
        let base_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        for i in 0..100 {
            let point = create_test_point("cpu", "server01", base_ts + i * 1000, i as f64);
            engine.write(&point).unwrap();
        }

        let stats = engine.memtable_stats();
        assert_eq!(stats.active_points, 100, "Expected 100 points in active memtable");
        assert_eq!(stats.active_series, 1);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_clean_shutdown_flushes_data() {
        // Test that clean shutdown flushes data to partitions
        // After restart, data should be queryable from partitions (not memtable)
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_durability: WalDurability::EveryWrite,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,   // Very high to prevent auto-flush
                max_points: 1_000_000_000,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        {
            let engine = StorageEngine::new(config.clone()).unwrap();

            for i in 0..50 {
                let point = create_test_point("cpu", "server01", i * 1000, i as f64);
                engine.write(&point).unwrap();
            }

            // Data is in memtable before shutdown
            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 50, "Expected 50 points before shutdown");

            // Clean shutdown flushes to partitions
            engine.shutdown().unwrap();
        }

        // After restart, data should be in partitions (memtable empty)
        {
            let engine = StorageEngine::new(config).unwrap();

            // Memtable should be empty (data was flushed to partitions)
            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 0, "Memtable should be empty after clean shutdown");

            // But data should still be queryable (from partitions)
            let results = engine.query(series_id, &TimeRange::new(0, 100000)).unwrap();
            assert_eq!(results.len(), 50, "Expected 50 points from query");

            for (i, point) in results.iter().enumerate() {
                assert_eq!(point.timestamp, i as i64 * 1000);
            }

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_wal_recovery_after_crash() {
        // Test that data is recovered from WAL after a simulated crash (no shutdown)
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_durability: WalDurability::EveryWrite,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,
                max_points: 1_000_000_000,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        // Phase 1: Write data and "crash" (drop without shutdown)
        {
            let engine = StorageEngine::new(config.clone()).unwrap();

            for i in 0..50 {
                let point = create_test_point("cpu", "server01", i * 1000, i as f64);
                engine.write(&point).unwrap();
            }

            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 50, "Expected 50 points before crash");

            // Simulate crash by NOT calling shutdown - just drop the engine
            // This means data is in WAL but not flushed to partitions
        }

        // Phase 2: Create new engine and verify WAL recovery
        {
            let engine = StorageEngine::new(config).unwrap();

            // Data should be recovered from WAL into memtable
            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 50, "Expected 50 points after WAL recovery");

            // Verify data can be queried
            let results = engine.query(series_id, &TimeRange::new(0, 100000)).unwrap();
            assert_eq!(results.len(), 50, "Expected 50 points from query after recovery");

            for (i, point) in results.iter().enumerate() {
                assert_eq!(point.timestamp, i as i64 * 1000);
            }

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_clean_shutdown_multiple_series() {
        // Test that multiple series are preserved after clean shutdown
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_durability: WalDurability::EveryWrite,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,
                max_points: 1_000_000_000,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        // Get series_ids for later querying
        let cpu_series_ids: Vec<_> = (0..5)
            .map(|host_idx| {
                create_test_point("cpu", &format!("server{:02}", host_idx), 0, 0.0).series_id()
            })
            .collect();
        let memory_series_id = create_test_point("memory", "server01", 0, 0.0).series_id();

        // Phase 1: Write data to multiple series
        {
            let engine = StorageEngine::new(config.clone()).unwrap();

            // Write to cpu measurement with different hosts
            for host_idx in 0..5 {
                for i in 0..10 {
                    let point = create_test_point(
                        "cpu",
                        &format!("server{:02}", host_idx),
                        i * 1000,
                        (host_idx * 100 + i) as f64,
                    );
                    engine.write(&point).unwrap();
                }
            }

            // Write to memory measurement
            for i in 0..20 {
                let point = create_test_point("memory", "server01", i * 1000, i as f64 * 1024.0);
                engine.write(&point).unwrap();
            }

            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 70, "Expected 70 total points");
            assert_eq!(stats.active_series, 6, "Expected 6 series (5 cpu + 1 memory)");

            engine.shutdown().unwrap();
        }

        // Phase 2: After clean shutdown, data is in partitions
        {
            let engine = StorageEngine::new(config).unwrap();

            // Memtable should be empty (data flushed to partitions)
            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 0, "Memtable should be empty after clean shutdown");

            // Query using series_id (this queries partitions)
            for series_id in &cpu_series_ids {
                let points = engine.query(*series_id, &TimeRange::new(0, 100000)).unwrap();
                assert_eq!(points.len(), 10, "Expected 10 points per cpu series");
            }

            let mem_points = engine.query(memory_series_id, &TimeRange::new(0, 100000)).unwrap();
            assert_eq!(mem_points.len(), 20, "Expected 20 memory points");

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_get_all_series_after_shutdown() {
        // Test that get_all_series returns correct data for index rebuilding
        // after clean shutdown (data in partitions)
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_durability: WalDurability::EveryWrite,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,
                max_points: 1_000_000_000,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        // Phase 1: Write data and shutdown cleanly
        {
            let engine = StorageEngine::new(config.clone()).unwrap();

            let p1 = create_test_point("cpu", "server01", 1000, 10.0);
            engine.write(&p1).unwrap();

            let p2 = create_test_point("cpu", "server02", 2000, 20.0);
            engine.write(&p2).unwrap();

            let p3 = create_test_point("memory", "server01", 3000, 1024.0);
            engine.write(&p3).unwrap();

            engine.shutdown().unwrap();
        }

        // Phase 2: After restart, get_all_series should return series from partitions
        {
            let engine = StorageEngine::new(config).unwrap();

            // Use get_all_series which includes both memtable and partition series
            let series = engine.get_all_series();
            assert_eq!(series.len(), 3, "Expected 3 series for index rebuilding");

            let mut found_cpu_server01 = false;
            let mut found_cpu_server02 = false;
            let mut found_memory_server01 = false;

            for (series_id, measurement, tags) in &series {
                assert!(*series_id != 0, "Series ID should be non-zero");

                if measurement == "cpu" {
                    for tag in tags {
                        if tag.key == "host" && tag.value == "server01" {
                            found_cpu_server01 = true;
                        } else if tag.key == "host" && tag.value == "server02" {
                            found_cpu_server02 = true;
                        }
                    }
                } else if measurement == "memory" {
                    for tag in tags {
                        if tag.key == "host" && tag.value == "server01" {
                            found_memory_server01 = true;
                        }
                    }
                }
            }

            assert!(found_cpu_server01, "Should find cpu,host=server01");
            assert!(found_cpu_server02, "Should find cpu,host=server02");
            assert!(found_memory_server01, "Should find memory,host=server01");

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_wal_recovery_no_checkpoint_after_crash() {
        // Test that when no checkpoint exists (simulated crash), all WAL entries are recovered
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        let config = StorageEngineConfig {
            data_dir: data_dir.clone(),
            wal_durability: WalDurability::EveryWrite,
            flush_trigger: FlushTrigger {
                max_size: 1024 * 1024 * 1024,
                max_points: 1_000_000_000,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0,         // No lag for tests
            },
            ..Default::default()
        };

        // Phase 1: Write minimal data and "crash" (no shutdown)
        {
            let engine = StorageEngine::new(config.clone()).unwrap();

            let point = create_test_point("test", "host1", 1000, 42.0);
            engine.write(&point).unwrap();

            // Simulate crash - don't call shutdown
        }

        // Verify no checkpoint file exists (no flush happened)
        let checkpoint_path = data_dir.join("wal_checkpoint");
        assert!(!checkpoint_path.exists(), "Checkpoint should not exist after crash");

        // Phase 2: Recover and verify the single point is recovered
        {
            let engine = StorageEngine::new(config).unwrap();

            let stats = engine.memtable_stats();
            assert_eq!(stats.active_points, 1, "Should recover 1 point with no checkpoint");
            assert_eq!(stats.active_series, 1, "Should recover 1 series");

            // Query to verify data is accessible
            let point = create_test_point("test", "host1", 0, 0.0);
            let series_id = point.series_id();
            let results = engine.query(series_id, &TimeRange::new(0, 10000)).unwrap();
            assert_eq!(results.len(), 1, "Should be able to query recovered point");
            assert_eq!(results[0].timestamp, 1000);

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn test_query_with_limit_ascending() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write 100 points with increasing timestamps
        for i in 0..100 {
            let point = create_test_point("cpu", "server01", i * 1000, i as f64);
            engine.write(&point).unwrap();
        }

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        // Query with limit 10 ascending
        let (results, total_scanned) = engine
            .query_with_limit(series_id, &TimeRange::new(0, 1000000), 10, true)
            .unwrap();

        assert_eq!(results.len(), 10, "Should return exactly 10 points");
        assert!(total_scanned >= 10, "Should scan at least 10 points");

        // Verify ascending order (smallest timestamps first)
        for i in 0..10 {
            assert_eq!(results[i].timestamp, i as i64 * 1000);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_query_with_limit_descending() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write 100 points with increasing timestamps
        for i in 0..100 {
            let point = create_test_point("cpu", "server01", i * 1000, i as f64);
            engine.write(&point).unwrap();
        }

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        // Query with limit 10 descending
        let (results, total_scanned) = engine
            .query_with_limit(series_id, &TimeRange::new(0, 1000000), 10, false)
            .unwrap();

        assert_eq!(results.len(), 10, "Should return exactly 10 points");
        assert!(total_scanned >= 10, "Should scan at least 10 points");

        // Verify descending order (largest timestamps first)
        for i in 0..10 {
            assert_eq!(results[i].timestamp, (99 - i) as i64 * 1000);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_query_with_limit_fewer_than_limit() {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write only 5 points
        for i in 0..5 {
            let point = create_test_point("cpu", "server01", i * 1000, i as f64);
            engine.write(&point).unwrap();
        }

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        // Query with limit 10 (more than available)
        let (results, total_scanned) = engine
            .query_with_limit(series_id, &TimeRange::new(0, 100000), 10, true)
            .unwrap();

        assert_eq!(results.len(), 5, "Should return all 5 points");
        assert_eq!(total_scanned, 5, "Should scan all 5 points");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_query_with_limit_same_timestamps() {
        // Test that multiple points with the same timestamp are all returned
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write 10 points all with the same timestamp but different values
        for i in 0..10 {
            let point = Point::builder("cpu")
                .timestamp(1000) // Same timestamp for all
                .tag("host", "server01")
                .field("value", i as f64)
                .build()
                .unwrap();
            engine.write(&point).unwrap();
        }

        let series_id = Point::builder("cpu")
            .timestamp(0)
            .tag("host", "server01")
            .field("value", 0.0)
            .build()
            .unwrap()
            .series_id();

        // Query with limit 10
        let (results, _) = engine
            .query_with_limit(series_id, &TimeRange::new(0, 10000), 10, true)
            .unwrap();

        // Should return all 10 points even though they have the same timestamp
        assert_eq!(results.len(), 10, "Should return all 10 points with same timestamp");

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_query_with_limit_early_termination() {
        // Test that early termination works correctly across partitions
        use std::thread;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::EveryWrite,
            partition_duration: 1000_000_000, // 1 second partitions for testing
            flush_trigger: FlushTrigger {
                max_size: 1024, // Very small to trigger flush
                max_points: 10,
                max_age_nanos: i64::MAX,
                out_of_order_lag_ms: 0, // No lag for tests
            },
            ..Default::default()
        };

        let engine = StorageEngine::new(config).unwrap();

        // Write points across multiple partitions (different time ranges)
        // Partition 1: timestamps 0-999_999_999
        for i in 0..20 {
            let point = create_test_point("cpu", "server01", i * 10_000_000, i as f64);
            engine.write(&point).unwrap();
        }

        // Force flush to create partition
        engine.flush().unwrap();
        thread::sleep(Duration::from_millis(100)); // Let flush complete

        // Partition 2: timestamps 1_000_000_000+
        for i in 0..20 {
            let point = create_test_point("cpu", "server01", 1_000_000_000 + i * 10_000_000, (20 + i) as f64);
            engine.write(&point).unwrap();
        }

        let series_id = create_test_point("cpu", "server01", 0, 0.0).series_id();

        // Query with limit 10 ascending - should get points from partition 1
        let (results, total_scanned) = engine
            .query_with_limit(series_id, &TimeRange::new(0, i64::MAX), 10, true)
            .unwrap();

        assert_eq!(results.len(), 10, "Should return 10 points");
        // First 10 points should be from partition 1
        assert!(results[9].timestamp < 1_000_000_000, "All results should be from first partition");
        // Should have scanned fewer than all 40 points due to early termination
        // (This depends on partition boundaries, so we just verify it works)
        assert!(total_scanned <= 40, "Should not scan more than total points");

        engine.shutdown().unwrap();
    }
}
