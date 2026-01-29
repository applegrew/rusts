//! WAL reader implementation

use crate::error::{Result, StorageError};
use crc32fast::Hasher;
use rusts_core::Point;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

/// WAL entry header (same as writer)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntryHeader {
    sequence: u64,
    timestamp: i64,
    point_count: u32,
    data_len: u32,
    checksum: u32,
}

impl WalEntryHeader {
    const SIZE: usize = 32;

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

/// WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Entry sequence number
    pub sequence: u64,
    /// Timestamp when entry was written
    pub timestamp: i64,
    /// Points in this entry
    pub points: Vec<Point>,
}

/// WAL reader for recovery
pub struct WalReader {
    dir: PathBuf,
}

impl WalReader {
    /// Create a new WAL reader
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    /// Read all entries from all WAL files
    pub fn read_all(&self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let files = self.list_wal_files()?;

        for file_path in files {
            let file_entries = self.read_file(&file_path)?;
            entries.extend(file_entries);
        }

        // Sort by sequence number
        entries.sort_by_key(|e| e.sequence);
        Ok(entries)
    }

    /// Read entries starting from a specific sequence number
    pub fn read_from(&self, start_sequence: u64) -> Result<Vec<WalEntry>> {
        let entries = self.read_all()?;
        Ok(entries
            .into_iter()
            .filter(|e| e.sequence >= start_sequence)
            .collect())
    }

    /// Read a single WAL file
    pub fn read_file(&self, path: &Path) -> Result<Vec<WalEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read header
            let mut header_bytes = [0u8; WalEntryHeader::SIZE];
            match reader.read_exact(&mut header_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let header = WalEntryHeader::from_bytes(&header_bytes);

            // Read data
            let mut data = vec![0u8; header.data_len as usize];
            match reader.read_exact(&mut data) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("Truncated WAL entry at sequence {}", header.sequence);
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let computed_checksum = hasher.finalize();

            if computed_checksum != header.checksum {
                return Err(StorageError::WalCorrupted(format!(
                    "Checksum mismatch at sequence {}: expected {:x}, got {:x}",
                    header.sequence, header.checksum, computed_checksum
                )));
            }

            // Deserialize points
            let points: Vec<Point> = bincode::deserialize(&data)?;

            entries.push(WalEntry {
                sequence: header.sequence,
                timestamp: header.timestamp,
                points,
            });
        }

        Ok(entries)
    }

    /// List all WAL files in order
    pub fn list_wal_files(&self) -> Result<Vec<PathBuf>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wal_") && name.ends_with(".log") {
                    let num_str = name.strip_prefix("wal_")?.strip_suffix(".log")?;
                    let num = num_str.parse::<u64>().ok()?;
                    Some((num, e.path()))
                } else {
                    None
                }
            })
            .collect();

        files.sort_by_key(|(num, _)| *num);
        Ok(files.into_iter().map(|(_, path)| path).collect())
    }

    /// List WAL files with metadata (path, modified time, max sequence in file)
    /// Returns tuples of (file_path, modified_time, max_sequence)
    pub fn list_wal_files_with_metadata(
        &self,
    ) -> Result<Vec<(PathBuf, std::time::SystemTime, u64)>> {
        let files = self.list_wal_files()?;
        let mut result = Vec::with_capacity(files.len());

        for file_path in files {
            // Get file modified time
            let metadata = std::fs::metadata(&file_path)?;
            let modified = metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);

            // Read entries to find max sequence
            let entries = self.read_file(&file_path)?;
            let max_sequence = entries.iter().map(|e| e.sequence).max().unwrap_or(0);

            result.push((file_path, modified, max_sequence));
        }

        Ok(result)
    }

    /// Delete WAL files up to (and including) the given sequence
    pub fn truncate_to(&self, sequence: u64) -> Result<()> {
        let files = self.list_wal_files()?;

        for file_path in files {
            let entries = self.read_file(&file_path)?;
            if let Some(last) = entries.last() {
                if last.sequence <= sequence {
                    std::fs::remove_file(&file_path)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;
    use crate::WalDurability;
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
    fn test_wal_read_write_roundtrip() {
        let dir = TempDir::new().unwrap();

        // Write some entries
        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();
        for i in 0..10 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        // Read them back
        let reader = WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 10);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, i as u64);
            assert_eq!(entry.points.len(), 1);
            assert_eq!(entry.points[0].timestamp, i as i64 * 1000);
        }
    }

    #[test]
    fn test_wal_batch_read() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();

        // Write batch of 100 points
        let points: Vec<Point> = (0..100)
            .map(|i| create_test_point(i * 1000, i as f64))
            .collect();
        writer.write(&points).unwrap();

        let reader = WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].points.len(), 100);
    }

    #[test]
    fn test_wal_read_from_sequence() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();
        for i in 0..10 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());
        let entries = reader.read_from(5).unwrap();

        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].sequence, 5);
    }

    #[test]
    fn test_wal_read_empty() {
        let dir = TempDir::new().unwrap();

        let reader = WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();

        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_multiple_files() {
        let dir = TempDir::new().unwrap();

        // Use small file size to force rotation
        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite)
            .unwrap()
            .with_max_file_size(512);

        for i in 0..50 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 50);

        // Verify order
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, i as u64);
        }
    }
}
