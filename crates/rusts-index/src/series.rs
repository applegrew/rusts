//! Series Index - SeriesId to metadata mapping
//!
//! Provides fast lookup of series metadata by series ID.

use crate::error::Result;
use dashmap::DashMap;
use rusts_core::{SeriesId, Tag};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Series metadata stored in the index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesMetadata {
    /// Series ID
    pub id: SeriesId,
    /// Measurement name
    pub measurement: String,
    /// Tags for this series
    pub tags: Vec<Tag>,
    /// First seen timestamp
    pub first_seen: i64,
    /// Last seen timestamp
    pub last_seen: i64,
}

/// Series index using DashMap for concurrent access
pub struct SeriesIndex {
    /// SeriesId -> Metadata
    series: DashMap<SeriesId, SeriesMetadata>,
    /// Measurement -> list of SeriesIds
    measurement_index: DashMap<String, Vec<SeriesId>>,
    /// Total series count
    count: AtomicUsize,
}

impl SeriesIndex {
    /// Create a new series index
    pub fn new() -> Self {
        Self {
            series: DashMap::new(),
            measurement_index: DashMap::new(),
            count: AtomicUsize::new(0),
        }
    }

    /// Insert or update a series
    pub fn upsert(
        &self,
        id: SeriesId,
        measurement: &str,
        tags: &[Tag],
        timestamp: i64,
    ) -> bool {
        let mut is_new = false;

        self.series
            .entry(id)
            .and_modify(|meta| {
                meta.last_seen = meta.last_seen.max(timestamp);
                meta.first_seen = meta.first_seen.min(timestamp);
            })
            .or_insert_with(|| {
                is_new = true;
                self.count.fetch_add(1, Ordering::Relaxed);

                // Add to measurement index
                self.measurement_index
                    .entry(measurement.to_string())
                    .or_insert_with(Vec::new)
                    .push(id);

                SeriesMetadata {
                    id,
                    measurement: measurement.to_string(),
                    tags: tags.to_vec(),
                    first_seen: timestamp,
                    last_seen: timestamp,
                }
            });

        is_new
    }

    /// Get series metadata
    pub fn get(&self, id: SeriesId) -> Option<SeriesMetadata> {
        self.series.get(&id).map(|r| r.value().clone())
    }

    /// Check if series exists
    pub fn contains(&self, id: SeriesId) -> bool {
        self.series.contains_key(&id)
    }

    /// Get all series IDs for a measurement
    pub fn get_by_measurement(&self, measurement: &str) -> Vec<SeriesId> {
        self.measurement_index
            .get(measurement)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Get all measurements
    pub fn measurements(&self) -> Vec<String> {
        self.measurement_index
            .iter()
            .map(|r| r.key().clone())
            .collect()
    }

    /// Get all series IDs
    pub fn all_series_ids(&self) -> Vec<SeriesId> {
        self.series.iter().map(|r| *r.key()).collect()
    }

    /// Get series count
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove a series
    pub fn remove(&self, id: SeriesId) -> Option<SeriesMetadata> {
        if let Some((_, meta)) = self.series.remove(&id) {
            self.count.fetch_sub(1, Ordering::Relaxed);

            // Remove from measurement index
            if let Some(mut series_ids) = self.measurement_index.get_mut(&meta.measurement) {
                series_ids.retain(|&sid| sid != id);
            }

            Some(meta)
        } else {
            None
        }
    }

    /// Serialize the index to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let entries: Vec<SeriesMetadata> = self.series.iter().map(|r| r.value().clone()).collect();
        Ok(bincode::serialize(&entries)?)
    }

    /// Deserialize the index from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let entries: Vec<SeriesMetadata> = bincode::deserialize(data)?;
        let index = Self::new();

        for meta in entries {
            index.series.insert(meta.id, meta.clone());
            index
                .measurement_index
                .entry(meta.measurement.clone())
                .or_insert_with(Vec::new)
                .push(meta.id);
            index.count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(index)
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.series.clear();
        self.measurement_index.clear();
        self.count.store(0, Ordering::Relaxed);
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_index_upsert() {
        let index = SeriesIndex::new();

        let tags = vec![Tag::new("host", "server01")];
        let is_new = index.upsert(12345, "cpu", &tags, 1000);

        assert!(is_new);
        assert_eq!(index.len(), 1);

        // Upsert same series
        let is_new = index.upsert(12345, "cpu", &tags, 2000);
        assert!(!is_new);
        assert_eq!(index.len(), 1);

        // Check metadata updated
        let meta = index.get(12345).unwrap();
        assert_eq!(meta.first_seen, 1000);
        assert_eq!(meta.last_seen, 2000);
    }

    #[test]
    fn test_series_index_get() {
        let index = SeriesIndex::new();

        let tags = vec![Tag::new("host", "server01"), Tag::new("region", "us-west")];
        index.upsert(12345, "cpu", &tags, 1000);

        let meta = index.get(12345).unwrap();
        assert_eq!(meta.id, 12345);
        assert_eq!(meta.measurement, "cpu");
        assert_eq!(meta.tags.len(), 2);

        // Non-existent
        assert!(index.get(99999).is_none());
    }

    #[test]
    fn test_series_index_by_measurement() {
        let index = SeriesIndex::new();

        index.upsert(1, "cpu", &[Tag::new("host", "s1")], 1000);
        index.upsert(2, "cpu", &[Tag::new("host", "s2")], 1000);
        index.upsert(3, "mem", &[Tag::new("host", "s1")], 1000);

        let cpu_series = index.get_by_measurement("cpu");
        assert_eq!(cpu_series.len(), 2);
        assert!(cpu_series.contains(&1));
        assert!(cpu_series.contains(&2));

        let mem_series = index.get_by_measurement("mem");
        assert_eq!(mem_series.len(), 1);
        assert!(mem_series.contains(&3));

        let disk_series = index.get_by_measurement("disk");
        assert!(disk_series.is_empty());
    }

    #[test]
    fn test_series_index_remove() {
        let index = SeriesIndex::new();

        index.upsert(1, "cpu", &[], 1000);
        index.upsert(2, "cpu", &[], 1000);

        assert_eq!(index.len(), 2);

        let removed = index.remove(1);
        assert!(removed.is_some());
        assert_eq!(index.len(), 1);
        assert!(!index.contains(1));
        assert!(index.contains(2));

        // Measurement index should be updated
        let cpu_series = index.get_by_measurement("cpu");
        assert_eq!(cpu_series.len(), 1);
        assert!(!cpu_series.contains(&1));
    }

    #[test]
    fn test_series_index_serialization() {
        let index = SeriesIndex::new();

        index.upsert(1, "cpu", &[Tag::new("host", "s1")], 1000);
        index.upsert(2, "mem", &[Tag::new("host", "s1")], 2000);

        let bytes = index.to_bytes().unwrap();
        let restored = SeriesIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 2);
        assert!(restored.contains(1));
        assert!(restored.contains(2));

        let meta = restored.get(1).unwrap();
        assert_eq!(meta.measurement, "cpu");
    }

    #[test]
    fn test_series_index_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let index = Arc::new(SeriesIndex::new());
        let mut handles = vec![];

        for t in 0..4 {
            let idx = Arc::clone(&index);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let id = (t * 1000 + i) as u64;
                    idx.upsert(id, "cpu", &[Tag::new("host", &format!("s{}", t))], i);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(index.len(), 4000);
    }
}
