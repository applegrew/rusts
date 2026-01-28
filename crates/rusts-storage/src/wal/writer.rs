//! WAL writer implementation

use crate::error::{Result, StorageError};
use crc32fast::Hasher;
use parking_lot::Mutex;
use rusts_core::Point;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// CRC32 checksum of the data
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

    fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            sequence: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            timestamp: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            point_count: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            data_len: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            checksum: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        }
    }
}

/// WAL writer
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
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(dir: impl AsRef<Path>, durability: WalDurability) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Find existing WAL files and determine next file number
        let file_num = Self::find_next_file_num(&dir)?;
        let file_path = dir.join(format!("wal_{:08}.log", file_num));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let current_size = file.metadata()?.len();

        Ok(Self {
            dir,
            file: Mutex::new(BufWriter::new(file)),
            sequence: AtomicU64::new(0),
            durability,
            last_sync: Mutex::new(Instant::now()),
            file_num: AtomicU64::new(file_num),
            max_file_size: 64 * 1024 * 1024, // 64MB default
            current_size: AtomicU64::new(current_size),
        })
    }

    /// Create with custom max file size
    pub fn with_max_file_size(mut self, size: u64) -> Self {
        self.max_file_size = size;
        self
    }

    /// Write a batch of points to the WAL
    pub fn write(&self, points: &[Point]) -> Result<u64> {
        if points.is_empty() {
            return Ok(self.sequence.load(Ordering::SeqCst));
        }

        // Serialize points
        let data = bincode::serialize(points)?;

        // Compute checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        // Create header
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let header = WalEntryHeader {
            sequence,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            point_count: points.len() as u32,
            data_len: data.len() as u32,
            checksum,
        };

        // Write to file
        let entry_size = WalEntryHeader::SIZE + data.len();
        {
            let mut file = self.file.lock();
            file.write_all(&header.to_bytes())?;
            file.write_all(&data)?;
        }

        // Update current size and check for rotation
        let new_size = self.current_size.fetch_add(entry_size as u64, Ordering::SeqCst) + entry_size as u64;
        if new_size >= self.max_file_size {
            self.rotate()?;
        }

        // Handle durability
        self.maybe_sync()?;

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

        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;
        *file = BufWriter::new(new_file);

        self.current_size.store(0, Ordering::SeqCst);
        Ok(())
    }

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
            WalDurability::OsDefault => {
                self.file.lock().flush()?;
            }
            WalDurability::None => {
                // Don't flush
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
        let wal = WalWriter::new(dir.path(), WalDurability::None).unwrap();

        let point = create_test_point(1000, 42.0);
        let seq = wal.write_point(&point).unwrap();

        assert_eq!(seq, 0);
        assert_eq!(wal.sequence(), 1);
    }

    #[test]
    fn test_wal_write_batch() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None).unwrap();

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
            let wal = WalWriter::new(dir.path(), durability).unwrap();

            let point = create_test_point(1000, 42.0);
            wal.write_point(&point).unwrap();
            wal.sync().unwrap();
        }
    }

    #[test]
    fn test_wal_rotation() {
        let dir = TempDir::new().unwrap();
        let wal = WalWriter::new(dir.path(), WalDurability::None)
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
        let wal = WalWriter::new(dir.path(), WalDurability::None).unwrap();

        let seq = wal.write(&[]).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(wal.sequence(), 0); // Sequence should not increment
    }
}
