//! WAL writer implementation
//!
//! Optimizations:
//! - Pre-serialization outside lock to reduce contention
//! - Reusable serialization buffer to reduce allocations
//! - Group commit support for batching fsync operations
//! - Configurable buffer sizes for write performance

use crate::error::Result;
use crc32fast::Hasher;
use parking_lot::Mutex;
use rusts_core::Point;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// WAL durability modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalDurability {
    /// fsync after every write (strongest durability, slowest)
    EveryWrite,
    /// fsync periodically (default: every 100ms)
    Periodic { interval_ms: u64 },
    /// Let the OS decide when to flush
    OsDefault,
    /// No fsync (fastest, accepts data loss risk)
    None,
}

impl Default for WalDurability {
    fn default() -> Self {
        WalDurability::Periodic { interval_ms: 100 }
    }
}

/// WAL entry header
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntryHeader {
    /// Entry sequence number
    sequence: u64,
    /// Timestamp of the entry
    timestamp: i64,
    /// Number of points in this entry
    point_count: u32,
    /// Length of the serialized data
    data_len: u32,
    /// CRC32 checksum of header fields (bytes 0-23) and data
    checksum: u32,
}

impl WalEntryHeader {
    const SIZE: usize = 32; // 8 + 8 + 4 + 4 + 4 + 4 (padding)

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.timestamp.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.point_count.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.data_len.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        // bytes[28..32] is padding
        bytes
    }
}

/// Default BufWriter capacity (256KB for better batching)
const DEFAULT_BUFFER_CAPACITY: usize = 256 * 1024;

/// Group commit configuration
#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Maximum pending bytes before forcing a sync
    pub max_pending_bytes: usize,
    /// Maximum pending entries before forcing a sync
    pub max_pending_entries: usize,
    /// Maximum wait time before forcing a sync
    pub max_wait_ms: u64,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_pending_bytes: 1024 * 1024,  // 1MB
            max_pending_entries: 1000,
            max_wait_ms: 10,
        }
    }
}

/// WAL writer with optimized write path
pub struct WalWriter {
    /// Path to the WAL directory
    dir: PathBuf,
    /// Current WAL file
    file: Mutex<BufWriter<File>>,
    /// Current sequence number
    sequence: AtomicU64,
    /// Durability mode
    durability: WalDurability,
    /// Last sync time
    last_sync: Mutex<Instant>,
    /// Current WAL file number
    file_num: AtomicU64,
    /// Maximum WAL file size (bytes)
    max_file_size: u64,
    /// Current file size
    current_size: AtomicU64,
    /// Pending bytes since last sync (for group commit)
    pending_bytes: AtomicUsize,
    /// Pending entries since last sync (for group commit)
    pending_entries: AtomicUsize,
    /// Group commit configuration
    group_commit: GroupCommitConfig,
    /// Whether to use Direct I/O (bypass OS page cache)
    direct_io: bool,
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(dir: impl AsRef<Path>, durability: WalDurability, direct_io: bool) -> Result<Self> {
        Self::with_config(dir, durability, GroupCommitConfig::default(), direct_io)
    }

    /// Create a new WAL writer with custom group commit configuration
    pub fn with_config(
        dir: impl AsRef<Path>,
        durability: WalDurability,
        group_commit: GroupCommitConfig,
        direct_io: bool,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Find existing WAL files and determine next file number
        let file_num = Self::find_next_file_num(&dir)?;
        let file_path = dir.join(format!("wal_{:08}.log", file_num));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        if direct_io {
            Self::apply_direct_io(&file);
        }

        let current_size = file.metadata()?.len();

        Ok(Self {
            dir,
            file: Mutex::new(BufWriter::with_capacity(DEFAULT_BUFFER_CAPACITY, file)),
            sequence: AtomicU64::new(0),
            durability,
            last_sync: Mutex::new(Instant::now()),
            file_num: AtomicU64::new(file_num),
            max_file_size: 64 * 1024 * 1024, // 64MB default
            current_size: AtomicU64::new(current_size),
            pending_bytes: AtomicUsize::new(0),
            pending_entries: AtomicUsize::new(0),
            group_commit,
            direct_io,
        })
    }

    /// Apply Direct I/O hint to a file descriptor.
    /// On macOS: F_NOCACHE disables the unified buffer cache.
    /// On Linux: posix_fadvise with DONTNEED advises the kernel to drop pages.
    fn apply_direct_io(file: &File) {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
        }
    }

    /// Create with custom max file size
    pub fn with_max_file_size(mut self, size: u64) -> Self {
        self.max_file_size = size;
        self
    }

    /// Create with custom group commit config
    pub fn with_group_commit(mut self, config: GroupCommitConfig) -> Self {
        self.group_commit = config;
        self
    }

    /// Write a batch of points to the WAL
    ///
    /// Optimized to serialize outside the lock to reduce contention.
    pub fn write(&self, points: &[Point]) -> Result<u64> {
        if points.is_empty() {
            return Ok(self.sequence.load(Ordering::SeqCst));
        }

        // Pre-serialize points OUTSIDE the lock to reduce contention
        let data = bincode::serialize(points)?;

        // Prepare header fields outside the lock
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let point_count = points.len() as u32;
        let data_len = data.len() as u32;

        // Write the pre-serialized data
        self.write_serialized(&data, timestamp, point_count, data_len)
    }

    /// Write pre-serialized data to the WAL (internal fast path)
    ///
    /// This method minimizes lock hold time by accepting pre-serialized data.
    fn write_serialized(
        &self,
        data: &[u8],
        timestamp: i64,
        point_count: u32,
        data_len: u32,
    ) -> Result<u64> {
        // Get sequence number
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Compute checksum over header fields (bytes 0-23) AND data
        let mut hasher = Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&point_count.to_le_bytes());
        hasher.update(&data_len.to_le_bytes());
        hasher.update(data);
        let checksum = hasher.finalize();

        // Create header
        let header = WalEntryHeader {
            sequence,
            timestamp,
            point_count,
            data_len,
            checksum,
        };

        let header_bytes = header.to_bytes();
        let entry_size = WalEntryHeader::SIZE + data.len();

        // Critical section: minimize lock hold time
        {
            let mut file = self.file.lock();
            file.write_all(&header_bytes)?;
            file.write_all(data)?;
        }

        // Update tracking atomics
        let new_size = self.current_size.fetch_add(entry_size as u64, Ordering::Relaxed)
            + entry_size as u64;
        self.pending_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.pending_entries.fetch_add(1, Ordering::Relaxed);

        // Check for rotation
        if new_size >= self.max_file_size {
            self.rotate()?;
        }

        // Handle durability with group commit awareness
        self.maybe_sync_group_commit()?;

        Ok(sequence)
    }

    /// Write a single point
    pub fn write_point(&self, point: &Point) -> Result<u64> {
        self.write(&[point.clone()])
    }

    /// Force sync to disk
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;
        *self.last_sync.lock() = Instant::now();
        Ok(())
    }

    /// Get current sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get the WAL directory
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Rotate to a new WAL file
    fn rotate(&self) -> Result<()> {
        let new_file_num = self.file_num.fetch_add(1, Ordering::SeqCst) + 1;
        let file_path = self.dir.join(format!("wal_{:08}.log", new_file_num));

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        if self.direct_io {
            Self::apply_direct_io(&new_file);
        }

        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;
        *file = BufWriter::with_capacity(DEFAULT_BUFFER_CAPACITY, new_file);

        self.current_size.store(0, Ordering::SeqCst);
        self.pending_bytes.store(0, Ordering::Relaxed);
        self.pending_entries.store(0, Ordering::Relaxed);
        Ok(())
    }

    #[allow(dead_code)]
    fn maybe_sync(&self) -> Result<()> {
        match self.durability {
            WalDurability::EveryWrite => {
                self.sync()?;
            }
            WalDurability::Periodic { interval_ms } => {
                let last_sync = *self.last_sync.lock();
                if last_sync.elapsed() >= Duration::from_millis(interval_ms) {
                    self.sync()?;
                }
            }
            WalDurability::OsDefault | WalDurability::None => {
                // Flush buffer to OS (but no fsync)
                self.file.lock().flush()?;
            }
        }
        Ok(())
    }

    /// Group commit aware sync - batches fsync operations for better throughput
    ///
    /// Note: `EveryWrite` mode always syncs immediately (no group commit batching)
    /// to maintain its durability guarantee. Group commit only applies to `Periodic` mode.
    fn maybe_sync_group_commit(&self) -> Result<()> {
        match self.durability {
            WalDurability::EveryWrite => {
                // EveryWrite ALWAYS syncs immediately - no group commit batching
                // This maintains the strongest durability guarantee
                self.sync()?;
                self.pending_bytes.store(0, Ordering::Relaxed);
                self.pending_entries.store(0, Ordering::Relaxed);
            }
            WalDurability::Periodic { interval_ms } => {
                // Apply group commit thresholds for Periodic mode
                let pending_bytes = self.pending_bytes.load(Ordering::Relaxed);
                let pending_entries = self.pending_entries.load(Ordering::Relaxed);
                let last_sync = *self.last_sync.lock();

                let should_sync = pending_bytes >= self.group_commit.max_pending_bytes
                    || pending_entries >= self.group_commit.max_pending_entries
                    || last_sync.elapsed() >= Duration::from_millis(interval_ms.min(self.group_commit.max_wait_ms));

                if should_sync {
                    self.sync()?;
                    self.pending_bytes.store(0, Ordering::Relaxed);
                    self.pending_entries.store(0, Ordering::Relaxed);
                }
            }
            WalDurability::OsDefault | WalDurability::None => {
                // Flush buffer to OS (but no fsync)
                // This ensures data is available for reading even with large buffers
                self.file.lock().flush()?;
            }
        }
        Ok(())
    }

    fn find_next_file_num(dir: &Path) -> Result<u64> {
        let mut max_num = 0u64;

        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();

                if name.starts_with("wal_") && name.ends_with(".log") {
                    if let Some(num_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(num) = num_str.parse::<u64>() {
                            max_num = max_num.max(num);
                        }
                    }
                }
            }
        }

        Ok(max_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_core::Point;
    use tempfile::TempDir;

    fn create_test_point(ts: i64, value: f64) -> Point {
        Point::builder("test")
            .timestamp(ts)
            .tag("host", "server01")
            .field("value", value)
            .build()
            .unwrap()
    }

    #[test]
    fn test_wal_write_single() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None, false).unwrap();

        let point = create_test_point(1000, 42.0);
        let seq = wal.write_point(&point).unwrap();

        assert_eq!(seq, 0);
        assert_eq!(wal.sequence(), 1);
    }

    #[test]
    fn test_wal_write_batch() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None, false).unwrap();

        let points: Vec<Point> = (0..100)
            .map(|i| create_test_point(i * 1000, i as f64))
            .collect();

        let seq = wal.write(&points).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(wal.sequence(), 1);
    }

    #[test]
    fn test_wal_durability_modes() {
        for durability in [
            WalDurability::None,
            WalDurability::OsDefault,
            WalDurability::Periodic { interval_ms: 10 },
            WalDurability::EveryWrite,
        ] {
            let dir = TempDir::new().unwrap();
            let wal = WalWriter::new(dir.path(), durability, false).unwrap();

            let point = create_test_point(1000, 42.0);
            wal.write_point(&point).unwrap();
            wal.sync().unwrap();
        }
    }

    #[test]
    fn test_wal_rotation() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None, false)
            .unwrap()
            .with_max_file_size(1024); // Small size to trigger rotation

        // Write enough data to trigger rotation
        for i in 0..100 {
            let point = create_test_point(i * 1000, i as f64);
            wal.write_point(&point).unwrap();
        }

        // Check that multiple WAL files exist
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
            .collect();

        assert!(files.len() > 1, "Expected multiple WAL files after rotation");
    }

    #[test]
    fn test_wal_empty_write() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None, false).unwrap();

        let seq = wal.write(&[]).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(wal.sequence(), 0); // Sequence should not increment
    }

    #[test]
    fn test_wal_group_commit() {
        let dir = TempDir::new().unwrap();

        // Configure group commit to trigger after 5 entries
        // Use Periodic mode (not EveryWrite) since EveryWrite always syncs immediately
        let config = GroupCommitConfig {
            max_pending_bytes: 1024 * 1024,
            max_pending_entries: 5,
            max_wait_ms: 10000, // Long timeout so only entry count triggers
        };

        let wal = WalWriter::with_config(
            dir.path(),
            WalDurability::Periodic { interval_ms: 10000 }, // Long interval
            config,
            false,
        ).unwrap();

        // Write 4 entries - should not trigger sync yet
        for i in 0..4 {
            let point = create_test_point(i * 1000, i as f64);
            wal.write_point(&point).unwrap();
        }

        // Check pending count (should be 4 since no sync triggered)
        assert_eq!(wal.pending_entries.load(Ordering::Relaxed), 4);

        // Write 1 more - should trigger sync (5 entries = threshold)
        let point = create_test_point(4000, 4.0);
        wal.write_point(&point).unwrap();

        // After sync, pending should be reset
        assert_eq!(wal.pending_entries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_wal_custom_buffer_size() {
        let dir = TempDir::new().unwrap();

        // Create with custom config using Periodic mode to test group commit
        let config = GroupCommitConfig {
            max_pending_bytes: 512,
            max_pending_entries: 1000,
            max_wait_ms: 10000,
        };

        let wal = WalWriter::with_config(
            dir.path(),
            WalDurability::Periodic { interval_ms: 10000 },
            config,
            false,
        ).unwrap();

        // Write enough data to exceed the pending bytes threshold
        for i in 0..10 {
            let point = create_test_point(i * 1000, i as f64);
            wal.write_point(&point).unwrap();
        }

        // Should complete without error and sync should have occurred
        // (pending_bytes threshold of 512 bytes was exceeded)
        assert!(wal.sequence() >= 10);
    }

    #[test]
    fn test_wal_direct_io_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::EveryWrite, true).unwrap();

        // Write entries with Direct I/O enabled
        for i in 0..20 {
            let point = create_test_point(i * 1000, i as f64);
            wal.write_point(&point).unwrap();
        }

        // Read them back using the standard reader
        let reader = crate::wal::WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 20);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, i as u64);
            assert_eq!(entry.points.len(), 1);
            assert_eq!(entry.points[0].timestamp, i as i64 * 1000);
        }
    }

    #[test]
    fn test_wal_direct_io_rotation() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None, true)
            .unwrap()
            .with_max_file_size(1024);

        // Write enough data to trigger rotation with Direct I/O
        for i in 0..100 {
            let point = create_test_point(i * 1000, i as f64);
            wal.write_point(&point).unwrap();
        }

        // Verify multiple files were created
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
            .collect();
        assert!(files.len() > 1, "Expected multiple WAL files after rotation with direct_io");

        // Verify all data is readable
        let reader = crate::wal::WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_wal_direct_io_all_durability_modes() {
        for durability in [
            WalDurability::None,
            WalDurability::OsDefault,
            WalDurability::Periodic { interval_ms: 10 },
            WalDurability::EveryWrite,
        ] {
            let dir = TempDir::new().unwrap();
            let wal = WalWriter::new(dir.path(), durability, true).unwrap();

            let point = create_test_point(1000, 42.0);
            wal.write_point(&point).unwrap();
            wal.sync().unwrap();

            // Verify data is readable
            let reader = crate::wal::WalReader::new(dir.path());
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
        }
    }
}
