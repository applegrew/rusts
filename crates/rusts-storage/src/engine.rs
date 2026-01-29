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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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
    Flush(Arc<MemTable>),
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

        let engine = Self {
            config,
            wal,
            wal_dir,
            active_memtable: RwLock::new(active_memtable),
            immutable_memtables: RwLock::new(Vec::new()),
            partitions,
            flush_tx,
            running: RwLock::new(true),
            last_flushed_sequence: AtomicU64::new(0),
        };

        // Start background flusher
        engine.start_flusher(flush_rx);

        // Recover from WAL
        engine.recover()?;

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

        // Sort by timestamp and deduplicate
        all_points.sort_by_key(|p| p.timestamp);
        all_points.dedup_by_key(|p| p.timestamp);

        Ok(all_points)
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
                points.dedup_by_key(|p| p.timestamp);
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
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down storage engine");

        *self.running.write() = false;

        // Send shutdown command to flusher
        let _ = self.flush_tx.send(FlushCommand::Shutdown);

        // Flush remaining data
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

        // Trigger background flush
        let _ = self.flush_tx.send(FlushCommand::Flush(old_memtable));

        debug!("Rotated memtable");
        Ok(())
    }

    /// Start the background flusher thread
    fn start_flusher(&self, mut rx: mpsc::UnboundedReceiver<FlushCommand>) {
        // We need to pass data to the flusher thread
        // Since we can't move self, we'll create the thread in a way that can access partitions
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
                        FlushCommand::Flush(memtable) => {
                            if let Err(e) = flush_memtable_to_partitions(&memtable, &partitions) {
                                error!("Failed to flush memtable: {}", e);
                            } else {
                                debug!("Flushed memtable with {} points", memtable.point_count());
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
    fn recover(&self) -> Result<()> {
        let wal_dir = self
            .config
            .wal_dir
            .clone()
            .unwrap_or_else(|| self.config.data_dir.join("wal"));

        let reader = WalReader::new(&wal_dir);
        let entries = reader.read_all()?;

        if entries.is_empty() {
            info!("No WAL entries to recover");
            return Ok(());
        }

        info!("Recovering {} WAL entries", entries.len());

        let memtable = self.active_memtable.read();
        for entry in entries {
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
                max_size: 1024 * 1024 * 1024,  // 1GB
                max_points: 1_000_000_000,      // 1B points
                max_age_nanos: i64::MAX,        // Never trigger on age
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
}
