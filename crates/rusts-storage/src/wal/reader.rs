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

    /// Read entries after a checkpoint sequence efficiently.
    ///
    /// This is optimized for recovery: it uses binary search to find the first
    /// file that might contain entries > checkpoint, then reads only necessary files.
    ///
    /// Returns (entries_to_recover, files_skipped, files_read)
    pub fn read_after_checkpoint(&self, checkpoint: u64) -> Result<(Vec<WalEntry>, usize, usize)> {
        let files = self.list_wal_files()?;

        if files.is_empty() {
            return Ok((Vec::new(), 0, 0));
        }

        // Build index of first sequence per file (fast - only reads headers)
        let mut file_first_seqs: Vec<(usize, u64)> = Vec::with_capacity(files.len());
        for (idx, file_path) in files.iter().enumerate() {
            if let Some(first_seq) = self.peek_first_sequence(file_path)? {
                file_first_seqs.push((idx, first_seq));
            }
        }

        if file_first_seqs.is_empty() {
            return Ok((Vec::new(), 0, 0));
        }

        // Binary search to find the first file that could contain entries > checkpoint
        // We want the last file where first_seq <= checkpoint (it might span the checkpoint)
        let start_idx = match file_first_seqs.binary_search_by_key(&checkpoint, |&(_, seq)| seq) {
            Ok(idx) => idx,        // Exact match - this file starts at checkpoint
            Err(idx) => {
                if idx == 0 {
                    0  // All files start after checkpoint
                } else {
                    idx - 1  // File before this one might contain checkpoint
                }
            }
        };

        let files_skipped = start_idx;
        let mut files_read = 0;
        let mut entries = Vec::new();

        // Read from start_idx onwards
        for (file_idx, _first_seq) in &file_first_seqs[start_idx..] {
            let file_path = &files[*file_idx];
            let file_entries = self.read_file(file_path)?;
            files_read += 1;

            // Filter to entries > checkpoint
            let filtered: Vec<_> = file_entries
                .into_iter()
                .filter(|e| e.sequence > checkpoint)
                .collect();
            entries.extend(filtered);
        }

        // Sort by sequence number (should already be sorted, but ensure it)
        entries.sort_by_key(|e| e.sequence);
        Ok((entries, files_skipped, files_read))
    }

    /// Peek at the first entry's sequence number without reading the whole file
    fn peek_first_sequence(&self, path: &Path) -> Result<Option<u64>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut reader = BufReader::new(file);

        let mut header_bytes = [0u8; WalEntryHeader::SIZE];
        match reader.read_exact(&mut header_bytes) {
            Ok(_) => {
                let header = WalEntryHeader::from_bytes(&header_bytes);
                Ok(Some(header.sequence))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Peek at the last entry's sequence number
    /// This reads through the file but only extracts sequence numbers
    fn peek_last_sequence(&self, path: &Path) -> Result<Option<u64>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut reader = BufReader::new(file);
        let mut last_sequence = None;

        loop {
            let mut header_bytes = [0u8; WalEntryHeader::SIZE];
            match reader.read_exact(&mut header_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let header = WalEntryHeader::from_bytes(&header_bytes);
            last_sequence = Some(header.sequence);

            // Skip the data
            let mut skip_buf = vec![0u8; header.data_len as usize];
            match reader.read_exact(&mut skip_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(last_sequence)
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

            // Verify checksum over header fields (bytes 0-23) AND data
            // This ensures both header and data integrity are protected
            let mut hasher = Hasher::new();
            hasher.update(&header_bytes[0..24]);
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

    #[test]
    fn test_wal_read_after_checkpoint_skips_files() {
        let dir = TempDir::new().unwrap();

        // Use small file size to force multiple files
        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite)
            .unwrap()
            .with_max_file_size(512);

        // Write 50 entries, which should create multiple files
        for i in 0..50 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());

        // Check we have multiple files
        let files = reader.list_wal_files().unwrap();
        assert!(files.len() > 3, "Expected multiple WAL files, got {}", files.len());

        // Read with checkpoint at 40 - should skip most files
        let (entries, files_skipped, files_read) = reader.read_after_checkpoint(40).unwrap();

        // Should only have entries 41-49 (sequence > 40)
        assert_eq!(entries.len(), 9, "Expected 9 entries after checkpoint 40");
        assert_eq!(entries[0].sequence, 41);
        assert_eq!(entries[8].sequence, 49);

        // Should have skipped some files
        assert!(files_skipped > 0, "Expected some files to be skipped");
        assert!(files_read < files.len(), "Expected fewer files read than total");

        // Verify we get the right entries
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, 41 + i as u64);
        }
    }

    #[test]
    fn test_wal_read_after_checkpoint_all_flushed() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();

        // Write 10 entries (sequences 0-9)
        for i in 0..10 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());

        // Checkpoint at 9 - all entries are flushed
        let (entries, files_skipped, files_read) = reader.read_after_checkpoint(9).unwrap();

        // No entries should pass the filter (all <= 9)
        assert!(entries.is_empty(), "Expected no entries after checkpoint 9");
        // With binary search, we still read the file but filter out all entries
        // files_skipped is 0 because binary search starts at the file that might contain checkpoint
        assert_eq!(files_skipped, 0, "Expected 0 files skipped with binary search");
        assert_eq!(files_read, 1, "Expected 1 file read (but all entries filtered out)");
    }

    #[test]
    fn test_wal_read_after_checkpoint_none_flushed() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();

        // Write 10 entries (sequences 0-9)
        for i in 0..10 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());

        // Checkpoint at 0 - most entries need recovery (entries with sequence > 0)
        let (entries, _files_skipped, files_read) = reader.read_after_checkpoint(0).unwrap();

        // Entries 1-9 need recovery (sequence > 0)
        assert_eq!(entries.len(), 9, "Expected 9 entries after checkpoint 0");
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(files_read, 1, "Expected 1 file to be read");
    }

    #[test]
    fn test_wal_peek_first_sequence() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();
        for i in 0..5 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());
        let files = reader.list_wal_files().unwrap();

        // First entry should have sequence 0
        let first_seq = reader.peek_first_sequence(&files[0]).unwrap();
        assert_eq!(first_seq, Some(0));
    }

    #[test]
    fn test_wal_peek_last_sequence() {
        let dir = TempDir::new().unwrap();

        let writer = WalWriter::new(dir.path(), WalDurability::EveryWrite).unwrap();
        for i in 0..5 {
            let point = create_test_point(i * 1000, i as f64);
            writer.write_point(&point).unwrap();
        }

        let reader = WalReader::new(dir.path());
        let files = reader.list_wal_files().unwrap();

        // Last entry should have sequence 4
        let last_seq = reader.peek_last_sequence(&files[0]).unwrap();
        assert_eq!(last_seq, Some(4));
    }
}
