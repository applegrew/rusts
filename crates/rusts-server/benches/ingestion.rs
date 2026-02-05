//! Ingestion benchmarks

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rusts_api::LineProtocolParser;
use rusts_index::{SeriesIndex, TagIndex};
use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
use std::sync::Arc;
use tempfile::TempDir;

struct BenchEnv {
    _dir: TempDir,
    storage: Arc<StorageEngine>,
    series_index: Arc<SeriesIndex>,
    tag_index: Arc<TagIndex>,
}

impl BenchEnv {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };
        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        Self {
            _dir: dir,
            storage,
            series_index,
            tag_index,
        }
    }
}

impl Drop for BenchEnv {
    fn drop(&mut self) {
        let _ = self.storage.shutdown();
    }
}

fn build_line_protocol(points: usize) -> String {
    let mut s = String::new();
    for i in 0..points {
        let ts = (i as i64) * 1_000;
        let host = format!("h{}", i % 100);
        let region = if i % 2 == 0 { "us" } else { "eu" };
        s.push_str(&format!(
            "cpu,host={},region={} value={} {}\n",
            host,
            region,
            (i % 100) as f64,
            ts
        ));
    }
    s
}

fn bench_ingest_parse_index_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    let sizes = [1_000usize, 10_000usize];

    for size in sizes {
        let input = build_line_protocol(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("parse_index_write_{}", size), |b| {
            b.iter_batched(
                BenchEnv::new,
                |env| {
                    let (points, _errors) = LineProtocolParser::parse_lines_ok(black_box(&input));
                    for point in &points {
                        let series_id = point.series_id();
                        env.series_index.upsert(
                            series_id,
                            &point.measurement,
                            &point.tags,
                            point.timestamp,
                        );
                        env.tag_index.index_series(series_id, &point.tags);
                    }
                    env.storage.write_batch(&points).unwrap();
                    env
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ingest_parse_index_write);
criterion_main!(benches);
