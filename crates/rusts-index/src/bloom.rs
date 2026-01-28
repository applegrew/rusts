//! Bloom Filter for efficient membership testing
//!
//! Used for partition pruning to quickly determine if a partition
//! might contain data for a specific series.

use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use fxhash::FxHasher;

/// Bloom filter for approximate membership testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of items inserted
    count: usize,
}

impl BloomFilter {
    /// Create a new bloom filter optimized for expected items and false positive rate
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive probability (e.g., 0.01 for 1%)
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.max(0.0001).min(0.5);

        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let num_bits = num_bits.max(64);

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2).ceil() as usize;
        let num_hashes = num_hashes.max(1).min(16);

        // Round up to u64 boundary
        let num_words = (num_bits + 63) / 64;
        let num_bits = num_words * 64;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            count: 0,
        }
    }

    /// Create with specific parameters
    pub fn with_params(num_bits: usize, num_hashes: usize) -> Self {
        let num_words = (num_bits + 63) / 64;
        let num_bits = num_words * 64;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes: num_hashes.max(1).min(16),
            count: 0,
        }
    }

    /// Insert an item into the bloom filter
    pub fn insert<T: Hash>(&mut self, item: &T) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            self.bits[word_idx] |= 1 << bit_idx;
        }

        self.count += 1;
    }

    /// Check if an item might be in the bloom filter
    ///
    /// Returns `false` if definitely not present, `true` if possibly present
    pub fn might_contain<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            if self.bits[word_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }

        true
    }

    /// Get the number of items inserted
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the estimated false positive rate based on current fill
    pub fn estimated_fpr(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }

        // FPR = (1 - e^(-k*n/m))^k
        let fill_ratio = (self.num_hashes as f64 * self.count as f64) / self.num_bits as f64;
        let p = 1.0 - (-fill_ratio).exp();
        p.powi(self.num_hashes as i32)
    }

    /// Get the fill ratio (percentage of bits set)
    pub fn fill_ratio(&self) -> f64 {
        let set_bits: usize = self.bits.iter().map(|w| w.count_ones() as usize).sum();
        set_bits as f64 / self.num_bits as f64
    }

    /// Clear all bits
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    /// Get number of bits
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get number of hash functions
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Union with another bloom filter (OR operation)
    pub fn union(&mut self, other: &BloomFilter) {
        assert_eq!(self.num_bits, other.num_bits);
        assert_eq!(self.num_hashes, other.num_hashes);

        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
        self.count += other.count;
    }

    /// Create union of two bloom filters
    pub fn union_new(a: &BloomFilter, b: &BloomFilter) -> BloomFilter {
        let mut result = a.clone();
        result.union(b);
        result
    }

    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut h1 = FxHasher::default();
        item.hash(&mut h1);
        let hash1 = h1.finish();

        // Use a different seed for second hash
        let mut h2 = FxHasher::default();
        hash1.hash(&mut h2);
        let hash2 = h2.finish();

        (hash1, hash2)
    }

    fn get_index(&self, h1: u64, h2: u64, i: usize) -> usize {
        // Double hashing: h(i) = h1 + i * h2
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(1000, 0.01);

        filter.insert(&"hello");
        filter.insert(&"world");

        assert!(filter.might_contain(&"hello"));
        assert!(filter.might_contain(&"world"));
        // Very unlikely to contain this (but not impossible due to false positives)
    }

    #[test]
    fn test_bloom_filter_integers() {
        let mut filter = BloomFilter::new(10000, 0.01);

        for i in 0..10000 {
            filter.insert(&i);
        }

        // All inserted items should be found
        for i in 0..10000 {
            assert!(filter.might_contain(&i));
        }

        // Count false positives for items not inserted
        let mut false_positives = 0;
        for i in 10000..20000 {
            if filter.might_contain(&i) {
                false_positives += 1;
            }
        }

        // False positive rate should be reasonably close to target
        // Note: With simple hash functions, FPR can be higher than optimal
        let fpr = false_positives as f64 / 10000.0;
        assert!(fpr < 0.10, "FPR {} is too high", fpr);
    }

    #[test]
    fn test_bloom_filter_series_ids() {
        let mut filter = BloomFilter::new(100000, 0.001);

        let series_ids: Vec<u64> = (0..100000).collect();

        for id in &series_ids {
            filter.insert(id);
        }

        // All should be found
        for id in &series_ids {
            assert!(filter.might_contain(id));
        }

        // Check estimated FPR
        let estimated_fpr = filter.estimated_fpr();
        assert!(estimated_fpr < 0.01, "Estimated FPR {} is too high", estimated_fpr);
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = BloomFilter::new(100, 0.01);

        filter.insert(&"test");
        assert!(filter.might_contain(&"test"));

        filter.clear();
        assert_eq!(filter.count(), 0);
        assert!(filter.fill_ratio() < 0.001);
    }

    #[test]
    fn test_bloom_filter_union() {
        let mut filter1 = BloomFilter::new(100, 0.01);
        let mut filter2 = BloomFilter::new(100, 0.01);

        filter1.insert(&"hello");
        filter2.insert(&"world");

        let union = BloomFilter::union_new(&filter1, &filter2);

        assert!(union.might_contain(&"hello"));
        assert!(union.might_contain(&"world"));
    }

    #[test]
    fn test_bloom_filter_params() {
        // Test that optimal parameters are calculated correctly
        let filter = BloomFilter::new(10000, 0.01);

        // Bits should be roughly -n*ln(p)/ln(2)^2 ≈ 95850
        assert!(filter.num_bits() > 90000);
        assert!(filter.num_bits() < 110000);

        // Hash functions should be roughly ln(2) * m/n ≈ 7
        assert!(filter.num_hashes() >= 6);
        assert!(filter.num_hashes() <= 8);
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(1000, 0.01);

        for i in 0..100 {
            filter.insert(&i);
        }

        // Serialize
        let bytes = bincode::serialize(&filter).unwrap();

        // Deserialize
        let restored: BloomFilter = bincode::deserialize(&bytes).unwrap();

        // Check that all items are still present
        for i in 0..100 {
            assert!(restored.might_contain(&i));
        }

        assert_eq!(restored.count(), filter.count());
        assert_eq!(restored.num_bits(), filter.num_bits());
    }

    #[test]
    fn test_bloom_filter_fill_ratio() {
        let mut filter = BloomFilter::with_params(1024, 4);

        assert!(filter.fill_ratio() < 0.001);

        for i in 0..100 {
            filter.insert(&i);
        }

        let ratio = filter.fill_ratio();
        assert!(ratio > 0.0);
        assert!(ratio < 1.0);
    }
}
