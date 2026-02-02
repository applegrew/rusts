//! RusTs Storage - Storage engine for time series database
//!
//! This crate provides the storage layer:
//! - WAL (Write-Ahead Log) with configurable durability
//! - MemTable (in-memory write buffer)
//! - Segments (columnar storage format)
//! - Partitions (time-based data organization)
//! - Storage Engine (coordinator)

pub mod engine;
pub mod error;
pub mod memtable;
pub mod partition;
pub mod segment;
pub mod wal;

pub use engine::{StorageEngine, StorageEngineConfig};
pub use error::{Result, StorageError};
pub use memtable::{MemTable, MemTablePoint};
pub use partition::{Partition, PartitionManager};
pub use segment::{FieldStats, Segment, SegmentMeta, SegmentReader, SegmentWriter};
pub use wal::{WalDurability, WalReader, WalWriter};
