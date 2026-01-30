//! MemTable - In-memory write buffer
//!
//! Points are first written to the MemTable before being flushed to segments.
//! The MemTable is organized by series ID for efficient querying.

use crate::error::{Result, StorageError};
use dashmap::DashMap;
use parking_lot::RwLock;
use rusts_core::{Point, SeriesId, TimeRange, Timestamp};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A single point stored in the MemTable (without redundant measurement/tags)
#[derive(Debug, Clone)]
pub struct MemTablePoint {
    pub timestamp: Timestamp,
    pub fields: Vec<(String, rusts_core::FieldValue)>,
}

impl MemTablePoint {
    pub fn from_point(point: &Point) -> Self {
        Self {
            timestamp: point.timestamp,
            fields: point
                .fields
                .iter()
                .map(|f| (f.key.clone(), f.value.clone()))
                .collect(),
        }
    }
}

/// Series data in the MemTable
#[derive(Debug)]
struct SeriesData {
    /// Measurement name
    measurement: String,
    /// Tags for this series
    tags: Vec<rusts_core::Tag>,
    /// Points sorted by timestamp
    points: RwLock<Vec<MemTablePoint>>,
}

/// Flush trigger configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FlushTrigger {
    /// Maximum memory size in bytes
    pub max_size: usize,
    /// Maximum number of points
    pub max_points: usize,
    /// Maximum age of oldest point (nanoseconds)
    pub max_age_nanos: i64,
    /// Out-of-order commit lag in milliseconds.
    /// When set, the memtable waits this long after the last write before flushing,
    /// allowing late-arriving out-of-order data to be sorted correctly within segments.
    /// This is similar to QuestDB's cairo.o3.lag.millis setting.
    /// Default: 1000ms (1 second)
    #[serde(default = "default_o3_lag")]
    pub out_of_order_lag_ms: u64,
}

fn default_o3_lag() -> u64 {
    1000 // 1 second default
}

impl Default for FlushTrigger {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024,          // 64MB
            max_points: 1_000_000,               // 1M points
            max_age_nanos: 60 * 1_000_000_000,   // 60 seconds
            out_of_order_lag_ms: default_o3_lag(),
        }
    }
}

/// MemTable - In-memory write buffer
pub struct MemTable {
    /// Series ID -> Series data
    series: DashMap<SeriesId, SeriesData>,
    /// Approximate memory usage
    size: AtomicUsize,
    /// Total point count
    point_count: AtomicUsize,
    /// Flush trigger configuration
    flush_trigger: FlushTrigger,
    /// Oldest timestamp in the MemTable
    oldest_timestamp: RwLock<Option<Timestamp>>,
    /// Is the MemTable sealed (no more writes allowed)
    sealed: RwLock<bool>,
    /// Last write time (wall clock, milliseconds since epoch)
    /// Used for out-of-order lag calculation
    last_write_time_ms: std::sync::atomic::AtomicU64,
}

impl MemTable {
    /// Create a new MemTable with default flush trigger
    pub fn new() -> Self {
        Self::with_flush_trigger(FlushTrigger::default())
    }

    /// Create a new MemTable with custom flush trigger
    pub fn with_flush_trigger(flush_trigger: FlushTrigger) -> Self {
        Self {
            series: DashMap::new(),
            size: AtomicUsize::new(0),
            point_count: AtomicUsize::new(0),
            flush_trigger,
            oldest_timestamp: RwLock::new(None),
            sealed: RwLock::new(false),
            last_write_time_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Insert a point into the MemTable
    pub fn insert(&self, point: &Point) -> Result<()> {
        if *self.sealed.read() {
            return Err(StorageError::MemTableFull);
        }

        let series_id = point.series_id();
        let mem_point = MemTablePoint::from_point(point);

        // Estimate memory size (rough approximation)
        let point_size = std::mem::size_of::<MemTablePoint>()
            + mem_point.fields.iter().map(|(k, v)| {
                k.len() + match v {
                    rusts_core::FieldValue::String(s) => s.len(),
                    _ => 8,
                }
            }).sum::<usize>();

        // Insert into series
        self.series
            .entry(series_id)
            .or_insert_with(|| {
                // New series - add overhead
                let series_size = std::mem::size_of::<SeriesData>()
                    + point.measurement.len()
                    + point.tags.iter().map(|t| t.key.len() + t.value.len()).sum::<usize>();
                self.size.fetch_add(series_size, Ordering::Relaxed);

                SeriesData {
                    measurement: point.measurement.clone(),
                    tags: point.tags.clone(),
                    points: RwLock::new(Vec::new()),
                }
            })
            .points
            .write()
            .push(mem_point);

        self.size.fetch_add(point_size, Ordering::Relaxed);
        self.point_count.fetch_add(1, Ordering::Relaxed);

        // Update oldest timestamp
        {
            let mut oldest = self.oldest_timestamp.write();
            match *oldest {
                None => *oldest = Some(point.timestamp),
                Some(ts) if point.timestamp < ts => *oldest = Some(point.timestamp),
                _ => {}
            }
        }

        // Update last write time for O3 lag calculation
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.last_write_time_ms
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Insert multiple points (optimized batch operation)
    ///
    /// Groups points by series and performs batch updates to reduce
    /// lock contention and atomic operations.
    pub fn insert_batch(&self, points: &[Point]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        if *self.sealed.read() {
            return Err(StorageError::MemTableFull);
        }

        // Group points by series_id for batch processing
        let mut by_series: std::collections::HashMap<SeriesId, Vec<&Point>> =
            std::collections::HashMap::new();
        for point in points {
            by_series
                .entry(point.series_id())
                .or_default()
                .push(point);
        }

        let mut total_size = 0usize;
        let mut total_points = 0usize;
        let mut min_timestamp: Option<Timestamp> = None;

        // Process each series batch
        for (series_id, series_points) in by_series {
            // Convert points and calculate size
            let mem_points: Vec<MemTablePoint> = series_points
                .iter()
                .map(|p| MemTablePoint::from_point(p))
                .collect();

            // Calculate size for all points in this series
            let batch_size: usize = mem_points
                .iter()
                .map(|mp| {
                    std::mem::size_of::<MemTablePoint>()
                        + mp.fields
                            .iter()
                            .map(|(k, v)| {
                                k.len()
                                    + match v {
                                        rusts_core::FieldValue::String(s) => s.len(),
                                        _ => 8,
                                    }
                            })
                            .sum::<usize>()
                })
                .sum();

            total_size += batch_size;
            total_points += mem_points.len();

            // Track minimum timestamp
            for mp in &mem_points {
                match min_timestamp {
                    None => min_timestamp = Some(mp.timestamp),
                    Some(ts) if mp.timestamp < ts => min_timestamp = Some(mp.timestamp),
                    _ => {}
                }
            }

            // Get or create series entry and insert all points at once
            let first_point = series_points[0];
            self.series
                .entry(series_id)
                .or_insert_with(|| {
                    // New series - add overhead (counted separately)
                    let series_size = std::mem::size_of::<SeriesData>()
                        + first_point.measurement.len()
                        + first_point
                            .tags
                            .iter()
                            .map(|t| t.key.len() + t.value.len())
                            .sum::<usize>();
                    total_size += series_size;

                    SeriesData {
                        measurement: first_point.measurement.clone(),
                        tags: first_point.tags.clone(),
                        points: RwLock::new(Vec::new()),
                    }
                })
                .points
                .write()
                .extend(mem_points);
        }

        // Single atomic updates for all points
        self.size.fetch_add(total_size, Ordering::Relaxed);
        self.point_count.fetch_add(total_points, Ordering::Relaxed);

        // Single lock acquisition for oldest timestamp
        if let Some(new_min) = min_timestamp {
            let mut oldest = self.oldest_timestamp.write();
            match *oldest {
                None => *oldest = Some(new_min),
                Some(ts) if new_min < ts => *oldest = Some(new_min),
                _ => {}
            }
        }

        // Update last write time for O3 lag calculation
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.last_write_time_ms
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Query points for a series within a time range
    pub fn query(
        &self,
        series_id: SeriesId,
        time_range: &TimeRange,
    ) -> Vec<MemTablePoint> {
        self.series
            .get(&series_id)
            .map(|series| {
                series
                    .points
                    .read()
                    .iter()
                    .filter(|p| time_range.contains(p.timestamp))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Query points for all series matching a measurement
    pub fn query_measurement(
        &self,
        measurement: &str,
        time_range: &TimeRange,
    ) -> Vec<(SeriesId, Vec<MemTablePoint>)> {
        self.series
            .iter()
            .filter(|entry| entry.value().measurement == measurement)
            .map(|entry| {
                let series_id = *entry.key();
                let points: Vec<MemTablePoint> = entry
                    .value()
                    .points
                    .read()
                    .iter()
                    .filter(|p| time_range.contains(p.timestamp))
                    .cloned()
                    .collect();
                (series_id, points)
            })
            .filter(|(_, points)| !points.is_empty())
            .collect()
    }

    /// Check if flush should be triggered
    ///
    /// Flush is triggered when any threshold is exceeded (size, points, or age)
    /// AND the out-of-order lag period has passed since the last write.
    /// This gives late-arriving data time to be sorted correctly within segments.
    pub fn should_flush(&self) -> bool {
        // First check if any threshold is exceeded
        let threshold_exceeded = self.is_threshold_exceeded();

        if !threshold_exceeded {
            return false;
        }

        // Check if enough time has passed since the last write (O3 lag)
        // This ensures we wait for late-arriving data before committing
        let last_write = self
            .last_write_time_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        // If no writes yet, don't flush
        if last_write == 0 {
            return false;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let elapsed_since_write = now_ms.saturating_sub(last_write);

        // Only flush if O3 lag period has passed since last write
        elapsed_since_write >= self.flush_trigger.out_of_order_lag_ms
    }

    /// Check if any flush threshold is exceeded (without considering O3 lag)
    pub fn is_threshold_exceeded(&self) -> bool {
        if self.size.load(Ordering::Relaxed) >= self.flush_trigger.max_size {
            return true;
        }

        if self.point_count.load(Ordering::Relaxed) >= self.flush_trigger.max_points {
            return true;
        }

        if let Some(oldest) = *self.oldest_timestamp.read() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            if now - oldest >= self.flush_trigger.max_age_nanos {
                return true;
            }
        }

        false
    }

    /// Get the out-of-order lag configuration in milliseconds
    pub fn out_of_order_lag_ms(&self) -> u64 {
        self.flush_trigger.out_of_order_lag_ms
    }

    /// Seal the MemTable (no more writes allowed)
    pub fn seal(&self) {
        *self.sealed.write() = true;
    }

    /// Check if sealed
    pub fn is_sealed(&self) -> bool {
        *self.sealed.read()
    }

    /// Get approximate memory size
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get total point count
    pub fn point_count(&self) -> usize {
        self.point_count.load(Ordering::Relaxed)
    }

    /// Get number of series
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Get all series IDs
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.series.iter().map(|e| *e.key()).collect()
    }

    /// Get series metadata
    pub fn get_series_meta(&self, series_id: SeriesId) -> Option<(String, Vec<rusts_core::Tag>)> {
        self.series.get(&series_id).map(|s| {
            (s.value().measurement.clone(), s.value().tags.clone())
        })
    }

    /// Iterate over all series and their points
    pub fn iter_series(&self) -> impl Iterator<Item = (SeriesId, String, Vec<rusts_core::Tag>, Vec<MemTablePoint>)> + '_ {
        self.series.iter().map(|entry| {
            let series_id = *entry.key();
            let series = entry.value();
            let points = series.points.read().clone();
            (series_id, series.measurement.clone(), series.tags.clone(), points)
        })
    }

    /// Clear all data (used after flush)
    pub fn clear(&self) {
        self.series.clear();
        self.size.store(0, Ordering::Relaxed);
        self.point_count.store(0, Ordering::Relaxed);
        *self.oldest_timestamp.write() = None;
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        Point::builder(measurement)
            .timestamp(ts)
            .tag("host", host)
            .field("value", value)
            .build()
            .unwrap()
    }

    #[test]
    fn test_memtable_insert_query() {
        let memtable = MemTable::new();

        let point = create_test_point("cpu", "server01", 1000, 64.5);
        let series_id = point.series_id();

        memtable.insert(&point).unwrap();

        assert_eq!(memtable.point_count(), 1);
        assert_eq!(memtable.series_count(), 1);

        let range = TimeRange::new(0, 2000);
        let results = memtable.query(series_id, &range);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].timestamp, 1000);
    }

    #[test]
    fn test_memtable_multiple_series() {
        let memtable = MemTable::new();

        let p1 = create_test_point("cpu", "server01", 1000, 64.5);
        let p2 = create_test_point("cpu", "server02", 1000, 70.0);
        let p3 = create_test_point("mem", "server01", 1000, 8000.0);

        memtable.insert(&p1).unwrap();
        memtable.insert(&p2).unwrap();
        memtable.insert(&p3).unwrap();

        assert_eq!(memtable.point_count(), 3);
        assert_eq!(memtable.series_count(), 3);
    }

    #[test]
    fn test_memtable_query_measurement() {
        let memtable = MemTable::new();

        for i in 0..10 {
            let p = create_test_point("cpu", &format!("server{:02}", i), i * 1000, i as f64);
            memtable.insert(&p).unwrap();
        }

        let p = create_test_point("mem", "server01", 5000, 8000.0);
        memtable.insert(&p).unwrap();

        let range = TimeRange::new(0, 100000);
        let results = memtable.query_measurement("cpu", &range);

        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_memtable_time_range_filter() {
        let memtable = MemTable::new();

        let point = create_test_point("cpu", "server01", 1000, 64.5);
        let series_id = point.series_id();

        for i in 0..100 {
            let p = create_test_point("cpu", "server01", i * 1000, i as f64);
            memtable.insert(&p).unwrap();
        }

        // Query only middle range
        let range = TimeRange::new(25000, 75000);
        let results = memtable.query(series_id, &range);

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|p| p.timestamp >= 25000 && p.timestamp < 75000));
    }

    #[test]
    fn test_memtable_flush_trigger_size() {
        let trigger = FlushTrigger {
            max_size: 1000,
            max_points: 1_000_000,
            max_age_nanos: i64::MAX,
            out_of_order_lag_ms: 0, // No lag for test
        };
        let memtable = MemTable::with_flush_trigger(trigger);

        // Insert until we exceed size
        for i in 0..100 {
            let p = create_test_point("cpu", "server01", i * 1000, i as f64);
            memtable.insert(&p).unwrap();
        }

        assert!(memtable.should_flush());
    }

    #[test]
    fn test_memtable_flush_trigger_points() {
        let trigger = FlushTrigger {
            max_size: usize::MAX,
            max_points: 50,
            max_age_nanos: i64::MAX,
            out_of_order_lag_ms: 0, // No lag for test
        };
        let memtable = MemTable::with_flush_trigger(trigger);

        for i in 0..50 {
            let p = create_test_point("cpu", "server01", i * 1000, i as f64);
            memtable.insert(&p).unwrap();
        }

        assert!(memtable.should_flush());
    }

    #[test]
    fn test_memtable_out_of_order_lag() {
        // Test that O3 lag delays flushing to allow late data to arrive
        let trigger = FlushTrigger {
            max_size: 100,          // Very small - will exceed immediately
            max_points: 1,          // Very small - will exceed immediately
            max_age_nanos: i64::MAX,
            out_of_order_lag_ms: 100, // 100ms lag
        };
        let memtable = MemTable::with_flush_trigger(trigger);

        // Insert a point
        let p = create_test_point("cpu", "server01", 1000, 64.5);
        memtable.insert(&p).unwrap();

        // Threshold is exceeded but O3 lag hasn't passed yet
        assert!(memtable.is_threshold_exceeded());
        // should_flush should return false because we're within the lag window
        assert!(!memtable.should_flush());

        // Wait for O3 lag to pass
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Now should_flush should return true
        assert!(memtable.should_flush());
    }

    #[test]
    fn test_memtable_out_of_order_lag_zero() {
        // Test that O3 lag of 0 allows immediate flushing
        let trigger = FlushTrigger {
            max_size: 100,
            max_points: 1,
            max_age_nanos: i64::MAX,
            out_of_order_lag_ms: 0, // No lag
        };
        let memtable = MemTable::with_flush_trigger(trigger);

        let p = create_test_point("cpu", "server01", 1000, 64.5);
        memtable.insert(&p).unwrap();

        // With O3 lag of 0, should_flush should return true immediately
        assert!(memtable.should_flush());
    }

    #[test]
    fn test_memtable_seal() {
        let memtable = MemTable::new();

        let p1 = create_test_point("cpu", "server01", 1000, 64.5);
        memtable.insert(&p1).unwrap();

        memtable.seal();
        assert!(memtable.is_sealed());

        let p2 = create_test_point("cpu", "server01", 2000, 70.0);
        assert!(memtable.insert(&p2).is_err());
    }

    #[test]
    fn test_memtable_clear() {
        let memtable = MemTable::new();

        for i in 0..100 {
            let p = create_test_point("cpu", "server01", i * 1000, i as f64);
            memtable.insert(&p).unwrap();
        }

        assert_eq!(memtable.point_count(), 100);

        memtable.clear();

        assert_eq!(memtable.point_count(), 0);
        assert_eq!(memtable.series_count(), 0);
        assert_eq!(memtable.size(), 0);
    }

    #[test]
    fn test_memtable_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let memtable = Arc::new(MemTable::new());
        let mut handles = vec![];

        for t in 0..4 {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let p = create_test_point(
                        "cpu",
                        &format!("server{}", t),
                        (t * 1000000 + i) as i64,
                        i as f64,
                    );
                    mt.insert(&p).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(memtable.point_count(), 4000);
        assert_eq!(memtable.series_count(), 4);
    }
}
