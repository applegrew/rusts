//! Query benchmarks

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_query::{AggregateFunction, Query, QueryExecutor};
use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
use std::sync::Arc;
use tempfile::TempDir;

struct BenchEnv {
    _dir: TempDir,
    storage: Arc<StorageEngine>,
    executor: Arc<QueryExecutor>,
}

impl BenchEnv {
    fn new(series: usize, points_per_series: usize) -> Self {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };
        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        for s in 0..series {
            let host = format!("h{}", s);
            let region = if s % 2 == 0 { "us" } else { "eu" };
            for i in 0..points_per_series {
                let ts = (i as i64) * 1_000 + (s as i64);
                let p = create_point("cpu", &host, region, ts, (i % 100) as f64);
                storage.write(&p).unwrap();
                let series_id = p.series_id();
                series_index.upsert(series_id, "cpu", &p.tags, p.timestamp);
                tag_index.index_series(series_id, &p.tags);
            }
        }

        storage.shutdown().unwrap();

        let storage2 = Arc::new(
            StorageEngine::new(StorageEngineConfig {
                data_dir: dir.path().to_path_buf(),
                wal_durability: WalDurability::None,
                ..Default::default()
            })
            .unwrap(),
        );

        let executor = Arc::new(QueryExecutor::new(
            Arc::clone(&storage2),
            series_index,
            tag_index,
        ));

        Self {
            _dir: dir,
            storage: storage2,
            executor,
        }
    }
}

impl Drop for BenchEnv {
    fn drop(&mut self) {
        let _ = self.storage.shutdown();
    }
}

fn create_point(measurement: &str, host: &str, region: &str, ts: i64, value: f64) -> rusts_core::Point {
    rusts_core::Point::builder(measurement)
        .timestamp(ts)
        .tag("host", host)
        .tag("region", region)
        .field("value", value)
        .build()
        .unwrap()
}

fn bench_select_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_select");
    let env = BenchEnv::new(200, 200);

    let query = Query::builder("cpu")
        .time_range(0, 1_000_000)
        .limit(1000)
        .build()
        .unwrap();

    group.throughput(Throughput::Elements(1000));
    group.bench_function("limit_1000", |b| {
        b.iter(|| black_box(env.executor.execute(black_box(query.clone())).unwrap()))
    });

    drop(env);
    group.finish();
}

fn bench_count_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_agg");

    group.bench_function("count_star", |b| {
        b.iter_batched(
            || BenchEnv::new(200, 200),
            |env| {
                let query = Query::builder("cpu")
                    .time_range(0, 1_000_000)
                    .select_aggregate("*", AggregateFunction::Count, None)
                    .build()
                    .unwrap();
                let _ = black_box(env.executor.execute(black_box(query)).unwrap());
                env
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_select_limit, bench_count_star);
criterion_main!(benches);
