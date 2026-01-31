//! Tag Index - Inverted index for tag-based queries
//!
//! Uses Roaring bitmaps for efficient set operations.

use crate::error::{IndexError, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use rusts_core::{SeriesId, Tag};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tag key-value pair as index key
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TagKey {
    key: String,
    value: String,
}

impl TagKey {
    fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Tag index using inverted index with Roaring bitmaps
pub struct TagIndex {
    /// Tag (key=value) -> bitmap of series IDs
    /// Note: SeriesId is u64, but Roaring uses u32, so we map them
    index: DashMap<TagKey, RoaringBitmap>,
    /// SeriesId (u64) -> internal ID (u32) for Roaring
    series_to_internal: DashMap<SeriesId, u32>,
    /// Internal ID (u32) -> SeriesId (u64)
    internal_to_series: RwLock<Vec<SeriesId>>,
    /// Tag key -> list of unique values
    tag_values: DashMap<String, Vec<String>>,
}

impl TagIndex {
    /// Create a new tag index
    pub fn new() -> Self {
        Self {
            index: DashMap::new(),
            series_to_internal: DashMap::new(),
            internal_to_series: RwLock::new(Vec::new()),
            tag_values: DashMap::new(),
        }
    }

    /// Index tags for a series
    pub fn index_series(&self, series_id: SeriesId, tags: &[Tag]) {
        let internal_id = self.get_or_create_internal_id(series_id);

        for tag in tags {
            let key = TagKey::new(&tag.key, &tag.value);

            // Add to inverted index
            self.index
                .entry(key)
                .or_insert_with(RoaringBitmap::new)
                .insert(internal_id);

            // Track unique values for each key
            self.tag_values
                .entry(tag.key.clone())
                .or_insert_with(Vec::new)
                .push(tag.value.clone());
        }
    }

    /// Find series matching a single tag
    pub fn find_by_tag(&self, key: &str, value: &str) -> Vec<SeriesId> {
        let tag_key = TagKey::new(key, value);

        self.index
            .get(&tag_key)
            .map(|bitmap| self.bitmap_to_series_ids(&bitmap))
            .unwrap_or_default()
    }

    /// Find series matching all tags (AND)
    pub fn find_by_tags_all(&self, tags: &[Tag]) -> Vec<SeriesId> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut result: Option<RoaringBitmap> = None;

        for tag in tags {
            let tag_key = TagKey::new(&tag.key, &tag.value);

            if let Some(bitmap) = self.index.get(&tag_key) {
                result = match result {
                    Some(r) => Some(&r & bitmap.value()),
                    None => Some(bitmap.value().clone()),
                };
            } else {
                // Tag doesn't exist, no matches
                return Vec::new();
            }
        }

        result
            .map(|bitmap| self.bitmap_to_series_ids(&bitmap))
            .unwrap_or_default()
    }

    /// Find series matching any tag (OR)
    pub fn find_by_tags_any(&self, tags: &[Tag]) -> Vec<SeriesId> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut result = RoaringBitmap::new();

        for tag in tags {
            let tag_key = TagKey::new(&tag.key, &tag.value);

            if let Some(bitmap) = self.index.get(&tag_key) {
                result |= bitmap.value();
            }
        }

        self.bitmap_to_series_ids(&result)
    }

    /// Find series NOT matching a tag
    pub fn find_by_tag_not(&self, key: &str, value: &str, all_series: &[SeriesId]) -> Vec<SeriesId> {
        let matching = self.find_by_tag(key, value);
        let matching_set: std::collections::HashSet<_> = matching.into_iter().collect();

        all_series
            .iter()
            .filter(|id| !matching_set.contains(id))
            .copied()
            .collect()
    }

    /// Get all unique values for a tag key
    pub fn get_tag_values(&self, key: &str) -> Vec<String> {
        self.tag_values
            .get(key)
            .map(|values| {
                let mut unique: Vec<String> = values.clone();
                unique.sort();
                unique.dedup();
                unique
            })
            .unwrap_or_default()
    }

    /// Get all tag keys
    pub fn get_tag_keys(&self) -> Vec<String> {
        self.tag_values.iter().map(|r| r.key().clone()).collect()
    }

    // ==========================================================================
    // Bitmap-native methods for efficient query execution
    // ==========================================================================

    /// Return raw bitmap for a tag (internal IDs, not SeriesIds)
    /// Returns None if the tag doesn't exist
    pub fn find_by_tag_bitmap(&self, key: &str, value: &str) -> Option<RoaringBitmap> {
        let tag_key = TagKey::new(key, value);
        self.index.get(&tag_key).map(|bitmap| bitmap.value().clone())
    }

    /// Intersect an existing bitmap with a tag's bitmap (for AND operations)
    /// Modifies the input bitmap in place
    /// Returns false if the tag doesn't exist (resulting in empty bitmap)
    pub fn intersect_with(&self, key: &str, value: &str, bitmap: &mut RoaringBitmap) -> bool {
        let tag_key = TagKey::new(key, value);
        if let Some(tag_bitmap) = self.index.get(&tag_key) {
            *bitmap &= tag_bitmap.value();
            true
        } else {
            bitmap.clear();
            false
        }
    }

    /// Union an existing bitmap with a tag's bitmap (for OR operations)
    /// Modifies the input bitmap in place
    pub fn union_with(&self, key: &str, value: &str, bitmap: &mut RoaringBitmap) {
        let tag_key = TagKey::new(key, value);
        if let Some(tag_bitmap) = self.index.get(&tag_key) {
            *bitmap |= tag_bitmap.value();
        }
    }

    /// Get all indexed series as a bitmap
    pub fn all_series_bitmap(&self) -> RoaringBitmap {
        let internal_map = self.internal_to_series.read();
        let mut bitmap = RoaringBitmap::new();
        for i in 0..internal_map.len() {
            bitmap.insert(i as u32);
        }
        bitmap
    }

    /// Convert a bitmap of internal IDs to SeriesIds
    pub fn bitmap_to_series(&self, bitmap: &RoaringBitmap) -> Vec<SeriesId> {
        self.bitmap_to_series_ids(bitmap)
    }

    /// Get internal ID for a series (for bitmap operations)
    pub fn get_internal_id(&self, series_id: SeriesId) -> Option<u32> {
        self.series_to_internal.get(&series_id).map(|v| *v)
    }

    /// Convert a slice of series IDs to a bitmap of internal IDs
    pub fn series_to_bitmap(&self, series_ids: &[SeriesId]) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for &series_id in series_ids {
            if let Some(internal_id) = self.series_to_internal.get(&series_id) {
                bitmap.insert(*internal_id);
            }
        }
        bitmap
    }

    /// Remove a series from the index
    pub fn remove_series(&self, series_id: SeriesId) {
        if let Some((_, internal_id)) = self.series_to_internal.remove(&series_id) {
            // Remove from all bitmaps
            for mut entry in self.index.iter_mut() {
                entry.value_mut().remove(internal_id);
            }
        }
    }

    /// Get number of indexed series
    pub fn series_count(&self) -> usize {
        self.series_to_internal.len()
    }

    /// Get number of unique tag combinations
    pub fn tag_count(&self) -> usize {
        self.index.len()
    }

    /// Serialize the index to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // Collect data for serialization
        let index_data: HashMap<String, Vec<u8>> = self
            .index
            .iter()
            .map(|entry| {
                let key = format!("{}={}", entry.key().key, entry.key().value);
                let mut bytes = Vec::new();
                entry.value().serialize_into(&mut bytes).unwrap();
                (key, bytes)
            })
            .collect();

        let series_map: Vec<(SeriesId, u32)> = self
            .series_to_internal
            .iter()
            .map(|r| (*r.key(), *r.value()))
            .collect();

        let internal_map = self.internal_to_series.read().clone();

        #[derive(Serialize, Deserialize)]
        struct IndexData {
            index: HashMap<String, Vec<u8>>,
            series_map: Vec<(SeriesId, u32)>,
            internal_map: Vec<SeriesId>,
        }

        let data = IndexData {
            index: index_data,
            series_map,
            internal_map,
        };

        Ok(bincode::serialize(&data)?)
    }

    /// Deserialize the index from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        #[derive(Serialize, Deserialize)]
        struct IndexData {
            index: HashMap<String, Vec<u8>>,
            series_map: Vec<(SeriesId, u32)>,
            internal_map: Vec<SeriesId>,
        }

        let data: IndexData = bincode::deserialize(data)?;
        let index = Self::new();

        // Restore series mappings
        for (series_id, internal_id) in data.series_map {
            index.series_to_internal.insert(series_id, internal_id);
        }
        *index.internal_to_series.write() = data.internal_map;

        // Restore index
        for (key_str, bitmap_bytes) in data.index {
            let parts: Vec<&str> = key_str.splitn(2, '=').collect();
            if parts.len() == 2 {
                let tag_key = TagKey::new(parts[0], parts[1]);
                let bitmap = RoaringBitmap::deserialize_from(&*bitmap_bytes)
                    .map_err(|e| IndexError::Serialization(e.to_string()))?;
                index.index.insert(tag_key, bitmap);

                // Rebuild tag_values
                index
                    .tag_values
                    .entry(parts[0].to_string())
                    .or_insert_with(Vec::new)
                    .push(parts[1].to_string());
            }
        }

        Ok(index)
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.index.clear();
        self.series_to_internal.clear();
        self.internal_to_series.write().clear();
        self.tag_values.clear();
    }

    fn get_or_create_internal_id(&self, series_id: SeriesId) -> u32 {
        *self.series_to_internal.entry(series_id).or_insert_with(|| {
            let mut internal_map = self.internal_to_series.write();
            let id = internal_map.len() as u32;
            internal_map.push(series_id);
            id
        })
    }

    fn bitmap_to_series_ids(&self, bitmap: &RoaringBitmap) -> Vec<SeriesId> {
        let internal_map = self.internal_to_series.read();
        bitmap
            .iter()
            .filter_map(|id| internal_map.get(id as usize).copied())
            .collect()
    }
}

impl Default for TagIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_index_basic() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01"), Tag::new("region", "us-west")]);
        index.index_series(2, &[Tag::new("host", "server02"), Tag::new("region", "us-west")]);
        index.index_series(3, &[Tag::new("host", "server01"), Tag::new("region", "us-east")]);

        // Find by single tag
        let results = index.find_by_tag("host", "server01");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));

        let results = index.find_by_tag("region", "us-west");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_tag_index_and_query() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01"), Tag::new("region", "us-west")]);
        index.index_series(2, &[Tag::new("host", "server02"), Tag::new("region", "us-west")]);
        index.index_series(3, &[Tag::new("host", "server01"), Tag::new("region", "us-east")]);

        // AND query
        let results = index.find_by_tags_all(&[
            Tag::new("host", "server01"),
            Tag::new("region", "us-west"),
        ]);

        assert_eq!(results.len(), 1);
        assert!(results.contains(&1));
    }

    #[test]
    fn test_tag_index_or_query() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01")]);
        index.index_series(2, &[Tag::new("host", "server02")]);
        index.index_series(3, &[Tag::new("host", "server03")]);

        // OR query
        let results = index.find_by_tags_any(&[
            Tag::new("host", "server01"),
            Tag::new("host", "server02"),
        ]);

        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(!results.contains(&3));
    }

    #[test]
    fn test_tag_index_not_query() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("env", "prod")]);
        index.index_series(2, &[Tag::new("env", "staging")]);
        index.index_series(3, &[Tag::new("env", "prod")]);

        let all = vec![1, 2, 3];
        let results = index.find_by_tag_not("env", "prod", &all);

        assert_eq!(results.len(), 1);
        assert!(results.contains(&2));
    }

    #[test]
    fn test_tag_index_get_values() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01")]);
        index.index_series(2, &[Tag::new("host", "server02")]);
        index.index_series(3, &[Tag::new("host", "server01")]);
        index.index_series(4, &[Tag::new("region", "us-west")]);

        let hosts = index.get_tag_values("host");
        assert_eq!(hosts.len(), 2);
        assert!(hosts.contains(&"server01".to_string()));
        assert!(hosts.contains(&"server02".to_string()));

        let keys = index.get_tag_keys();
        assert!(keys.contains(&"host".to_string()));
        assert!(keys.contains(&"region".to_string()));
    }

    #[test]
    fn test_tag_index_remove() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01")]);
        index.index_series(2, &[Tag::new("host", "server01")]);

        assert_eq!(index.find_by_tag("host", "server01").len(), 2);

        index.remove_series(1);

        let results = index.find_by_tag("host", "server01");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&2));
    }

    #[test]
    fn test_tag_index_serialization() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01"), Tag::new("region", "us-west")]);
        index.index_series(2, &[Tag::new("host", "server02"), Tag::new("region", "us-west")]);

        let bytes = index.to_bytes().unwrap();
        let restored = TagIndex::from_bytes(&bytes).unwrap();

        let results = restored.find_by_tag("region", "us-west");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_tag_index_empty_queries() {
        let index = TagIndex::new();

        index.index_series(1, &[Tag::new("host", "server01")]);

        // Empty tag list
        assert!(index.find_by_tags_all(&[]).is_empty());
        assert!(index.find_by_tags_any(&[]).is_empty());

        // Non-existent tag
        assert!(index.find_by_tag("nonexistent", "value").is_empty());
        assert!(index.find_by_tags_all(&[Tag::new("nonexistent", "value")]).is_empty());
    }

    #[test]
    fn test_tag_index_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let index = Arc::new(TagIndex::new());
        let mut handles = vec![];

        for t in 0..4 {
            let idx = Arc::clone(&index);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let id = (t * 1000 + i) as u64;
                    idx.index_series(id, &[
                        Tag::new("thread", &format!("{}", t)),
                        Tag::new("index", &format!("{}", i)),
                    ]);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(index.series_count(), 4000);

        // Each thread should have 1000 series
        for t in 0..4 {
            let results = index.find_by_tag("thread", &format!("{}", t));
            assert_eq!(results.len(), 1000);
        }
    }
}
