//! Parallel query execution configuration.
//!
//! This module provides configuration for intra-query parallelism,
//! allowing fine-grained control over how queries are split and
//! executed in parallel.

use serde::{Deserialize, Serialize};

/// Configuration for parallel query execution.
///
/// Controls how queries are split into parallel tasks and limits
/// the degree of parallelism to prevent resource exhaustion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelConfig {
    /// Minimum number of series before enabling parallel series processing.
    /// Default: 4
    pub series_threshold: usize,

    /// Minimum number of partitions before enabling parallel partition scanning.
    /// Default: 2
    pub partition_threshold: usize,

    /// Maximum number of series to process in parallel.
    /// Set to 0 for unlimited (uses all available threads).
    /// Default: 0 (unlimited)
    pub max_parallel_series: usize,

    /// Maximum number of partitions to scan in parallel.
    /// Set to 0 for unlimited (uses all available threads).
    /// Default: 0 (unlimited)
    pub max_parallel_partitions: usize,

    /// Number of threads in the query thread pool.
    /// Set to 0 to use the number of CPU cores.
    /// Default: 0 (CPU cores)
    pub thread_pool_size: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            series_threshold: 4,
            partition_threshold: 2,
            max_parallel_series: 0,
            max_parallel_partitions: 0,
            thread_pool_size: 0,
        }
    }
}

impl ParallelConfig {
    /// Creates a new ParallelConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if parallel series processing should be used.
    pub fn should_parallelize_series(&self, series_count: usize) -> bool {
        series_count >= self.series_threshold
    }

    /// Returns true if parallel partition scanning should be used.
    pub fn should_parallelize_partitions(&self, partition_count: usize) -> bool {
        partition_count >= self.partition_threshold
    }

    /// Returns the effective parallelism for series processing.
    /// Returns the smaller of series_count and max_parallel_series (if set).
    pub fn effective_series_parallelism(&self, series_count: usize) -> usize {
        if self.max_parallel_series == 0 {
            series_count
        } else {
            series_count.min(self.max_parallel_series)
        }
    }

    /// Returns the effective parallelism for partition scanning.
    /// Returns the smaller of partition_count and max_parallel_partitions (if set).
    pub fn effective_partition_parallelism(&self, partition_count: usize) -> usize {
        if self.max_parallel_partitions == 0 {
            partition_count
        } else {
            partition_count.min(self.max_parallel_partitions)
        }
    }

    /// Returns the effective thread pool size.
    /// Returns num_cpus if thread_pool_size is 0.
    pub fn effective_thread_pool_size(&self) -> usize {
        if self.thread_pool_size == 0 {
            num_cpus::get()
        } else {
            self.thread_pool_size
        }
    }

    /// Creates a configuration that disables all parallelism.
    pub fn sequential() -> Self {
        Self {
            series_threshold: usize::MAX,
            partition_threshold: usize::MAX,
            max_parallel_series: 1,
            max_parallel_partitions: 1,
            thread_pool_size: 1,
        }
    }

    /// Builder method to set series threshold.
    pub fn with_series_threshold(mut self, threshold: usize) -> Self {
        self.series_threshold = threshold;
        self
    }

    /// Builder method to set partition threshold.
    pub fn with_partition_threshold(mut self, threshold: usize) -> Self {
        self.partition_threshold = threshold;
        self
    }

    /// Builder method to set max parallel series.
    pub fn with_max_parallel_series(mut self, max: usize) -> Self {
        self.max_parallel_series = max;
        self
    }

    /// Builder method to set max parallel partitions.
    pub fn with_max_parallel_partitions(mut self, max: usize) -> Self {
        self.max_parallel_partitions = max;
        self
    }

    /// Builder method to set thread pool size.
    pub fn with_thread_pool_size(mut self, size: usize) -> Self {
        self.thread_pool_size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParallelConfig::default();
        assert_eq!(config.series_threshold, 4);
        assert_eq!(config.partition_threshold, 2);
        assert_eq!(config.max_parallel_series, 0);
        assert_eq!(config.max_parallel_partitions, 0);
        assert_eq!(config.thread_pool_size, 0);
    }

    #[test]
    fn test_should_parallelize() {
        let config = ParallelConfig::default();

        assert!(!config.should_parallelize_series(3));
        assert!(config.should_parallelize_series(4));
        assert!(config.should_parallelize_series(100));

        assert!(!config.should_parallelize_partitions(1));
        assert!(config.should_parallelize_partitions(2));
        assert!(config.should_parallelize_partitions(10));
    }

    #[test]
    fn test_effective_parallelism() {
        let config = ParallelConfig::default()
            .with_max_parallel_series(8)
            .with_max_parallel_partitions(4);

        // Series: limited to 8
        assert_eq!(config.effective_series_parallelism(5), 5);
        assert_eq!(config.effective_series_parallelism(10), 8);
        assert_eq!(config.effective_series_parallelism(100), 8);

        // Partitions: limited to 4
        assert_eq!(config.effective_partition_parallelism(2), 2);
        assert_eq!(config.effective_partition_parallelism(6), 4);
    }

    #[test]
    fn test_unlimited_parallelism() {
        let config = ParallelConfig::default(); // max values are 0 = unlimited

        assert_eq!(config.effective_series_parallelism(1000), 1000);
        assert_eq!(config.effective_partition_parallelism(100), 100);
    }

    #[test]
    fn test_sequential_config() {
        let config = ParallelConfig::sequential();

        assert!(!config.should_parallelize_series(1000));
        assert!(!config.should_parallelize_partitions(1000));
        assert_eq!(config.effective_series_parallelism(1000), 1);
        assert_eq!(config.effective_partition_parallelism(1000), 1);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ParallelConfig::new()
            .with_series_threshold(10)
            .with_partition_threshold(5)
            .with_max_parallel_series(16)
            .with_max_parallel_partitions(8)
            .with_thread_pool_size(4);

        assert_eq!(config.series_threshold, 10);
        assert_eq!(config.partition_threshold, 5);
        assert_eq!(config.max_parallel_series, 16);
        assert_eq!(config.max_parallel_partitions, 8);
        assert_eq!(config.thread_pool_size, 4);
    }
}
