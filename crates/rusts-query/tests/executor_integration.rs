use rusts_core::{FieldValue, Point};
use rusts_index::{SeriesIndex, TagIndex};
use rusts_query::{AggregateFunction, Query, QueryExecutor, TagFilter};
use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn create_test_point(measurement: &str, tags: &[(&str, &str)], ts: i64, value: f64) -> Point {
    let mut builder = Point::builder(measurement).timestamp(ts);
    for (k, v) in tags {
        builder = builder.tag(*k, *v);
    }
    builder.field("value", value).build().unwrap()
}

fn index_point(point: &Point, series_index: &SeriesIndex, tag_index: &TagIndex) {
    let series_id = point.series_id();
    series_index.upsert(series_id, &point.measurement, &point.tags, point.timestamp);
    tag_index.index_series(series_id, &point.tags);
}

struct TestEnv {
    storage: Arc<StorageEngine>,
    series_index: Arc<SeriesIndex>,
    tag_index: Arc<TagIndex>,
    _dir: TempDir,
}

impl TestEnv {
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
            storage,
            series_index,
            tag_index,
            _dir: dir,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = self.storage.shutdown();
    }
}

#[test]
fn out_of_order_ingestion_is_sorted_on_query() {
    let env = TestEnv::new();

    let p1 = create_test_point("cpu", &[("host", "s1")], 3000, 3.0);
    let p2 = create_test_point("cpu", &[("host", "s1")], 1000, 1.0);
    let p3 = create_test_point("cpu", &[("host", "s1")], 2000, 2.0);

    for p in [&p1, &p2, &p3] {
        env.storage.write(p).unwrap();
        index_point(p, &env.series_index, &env.tag_index);
    }

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );
    let query = Query::builder("cpu").time_range(0, 10_000).build().unwrap();
    let result = executor.execute(query).unwrap();

    let timestamps: Vec<i64> = result.rows.iter().map(|r| r.timestamp.unwrap()).collect();
    assert_eq!(timestamps, vec![1000, 2000, 3000]);
}

#[test]
fn tag_filters_match_expected_series() {
    let env = TestEnv::new();

    let p1 = create_test_point("cpu", &[("host", "s1"), ("region", "us")], 1000, 1.0);
    let p2 = create_test_point("cpu", &[("host", "s2"), ("region", "eu")], 1000, 2.0);
    let p3 = create_test_point("cpu", &[("host", "s3")], 1000, 3.0);

    for p in [&p1, &p2, &p3] {
        env.storage.write(p).unwrap();
        index_point(p, &env.series_index, &env.tag_index);
    }

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );

    let query = Query::builder("cpu")
        .time_range(0, 10_000)
        .where_tag_in("host", vec!["s1".to_string(), "s3".to_string()])
        .build()
        .unwrap();
    let result = executor.execute(query).unwrap();
    let series: HashSet<_> = result.rows.iter().map(|r| r.series_id).collect();
    assert_eq!(series.len(), 2);

    let mut query = Query::builder("cpu")
        .time_range(0, 10_000)
        .where_tag_not_in("host", vec!["s2".to_string()])
        .build()
        .unwrap();
    query.tag_filters.push(TagFilter::Exists {
        key: "region".to_string(),
    });

    let result = executor.execute(query).unwrap();
    let series: HashSet<_> = result.rows.iter().map(|r| r.series_id).collect();
    assert_eq!(series.len(), 1);

    let mut query = Query::builder("cpu").time_range(0, 10_000).build().unwrap();
    query.tag_filters.push(TagFilter::Regex {
        key: "host".to_string(),
        pattern: "^s[12]$".to_string(),
    });
    let result = executor.execute(query).unwrap();
    let series: HashSet<_> = result.rows.iter().map(|r| r.series_id).collect();
    assert_eq!(series.len(), 2);
}

#[test]
fn limit_offset_asc_and_desc_are_correct() {
    let env = TestEnv::new();

    let mut all_ts = Vec::new();
    for (host, base) in [("s1", 0), ("s2", 10_000)] {
        for i in 0..10 {
            let ts = base + i * 1000;
            let p = create_test_point("cpu", &[("host", host)], ts, i as f64);
            env.storage.write(&p).unwrap();
            index_point(&p, &env.series_index, &env.tag_index);
            all_ts.push(ts);
        }
    }

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );

    all_ts.sort();

    let limit = 5;
    let offset = 3;

    let query = Query::builder("cpu")
        .time_range(0, 100_000)
        .limit(limit)
        .offset(offset)
        .build()
        .unwrap();

    let result = executor.execute(query).unwrap();
    let got: Vec<i64> = result.rows.iter().map(|r| r.timestamp.unwrap()).collect();
    let expected: Vec<i64> = all_ts.iter().copied().skip(offset).take(limit).collect();
    assert_eq!(got, expected);

    let mut all_ts_desc = all_ts.clone();
    all_ts_desc.sort_by(|a, b| b.cmp(a));

    let query = Query::builder("cpu")
        .time_range(0, 100_000)
        .order_by("time", false)
        .limit(limit)
        .offset(offset)
        .build()
        .unwrap();

    let result = executor.execute(query).unwrap();
    let got: Vec<i64> = result.rows.iter().map(|r| r.timestamp.unwrap()).collect();
    let expected: Vec<i64> = all_ts_desc.iter().copied().skip(offset).take(limit).collect();
    assert_eq!(got, expected);
}

#[test]
fn group_by_time_count_star_counts_per_bucket() {
    let env = TestEnv::new();

    for i in 0..10 {
        let p = create_test_point("cpu", &[("host", "s1")], i * 10, i as f64);
        env.storage.write(&p).unwrap();
        index_point(&p, &env.series_index, &env.tag_index);
    }

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );

    let query = Query::builder("cpu")
        .time_range(0, 100)
        .select_aggregate("*", AggregateFunction::Count, None)
        .group_by_interval(50)
        .build()
        .unwrap();

    let result = executor.execute(query).unwrap();
    assert_eq!(result.rows.len(), 2);

    for row in &result.rows {
        assert!(matches!(row.timestamp, Some(0) | Some(50)));
        assert_eq!(row.fields.len(), 1);
        let v = row.fields.values().next().unwrap();
        assert_eq!(v, &FieldValue::Integer(5));
    }
}

#[test]
fn cancellation_is_respected_before_execution() {
    let env = TestEnv::new();

    let p = create_test_point("cpu", &[("host", "s1")], 1000, 1.0);
    env.storage.write(&p).unwrap();
    index_point(&p, &env.series_index, &env.tag_index);

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );

    let query = Query::builder("cpu").time_range(0, 10_000).build().unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let err = executor
        .execute_with_cancellation(query, cancel)
        .expect_err("expected cancellation");
    assert_eq!(err.to_string(), "Query cancelled");
}

#[test]
fn segment_stats_count_star_is_correct_after_restart() {
    let dir = TempDir::new().unwrap();
    let config = StorageEngineConfig {
        data_dir: dir.path().to_path_buf(),
        wal_durability: WalDurability::None,
        ..Default::default()
    };

    let storage = Arc::new(StorageEngine::new(config).unwrap());
    let series_index = Arc::new(SeriesIndex::new());
    let tag_index = Arc::new(TagIndex::new());

    for i in 0..123 {
        let p = create_test_point("cpu", &[("host", "s1")], i * 10, i as f64);
        storage.write(&p).unwrap();
        index_point(&p, &series_index, &tag_index);
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

    let executor = QueryExecutor::new(Arc::clone(&storage2), series_index, tag_index);
    let query = Query::builder("cpu")
        .time_range(0, 10_000)
        .select_aggregate("*", AggregateFunction::Count, None)
        .build()
        .unwrap();

    let result = executor.execute(query).unwrap();
    assert_eq!(result.rows.len(), 1);

    let v = result.rows[0].fields.values().next().unwrap();
    assert_eq!(v, &FieldValue::Integer(123));

    storage2.shutdown().unwrap();
}

#[test]
#[ignore]
fn large_volume_ingestion_and_query_smoke() {
    let env = TestEnv::new();

    let mut expected = 0usize;
    for series in 0..200u64 {
        let host = format!("h{}", series);
        let region = if series % 2 == 0 { "us" } else { "eu" };

        for i in 0..200u64 {
            let ts = (series * 1_000_000) as i64 + (i as i64);
            let p = create_test_point(
                "cpu",
                &[("host", &host), ("region", region)],
                ts,
                (i % 100) as f64,
            );
            env.storage.write(&p).unwrap();
            index_point(&p, &env.series_index, &env.tag_index);
            if region == "us" {
                expected += 1;
            }
        }
    }

    let executor = QueryExecutor::new(
        Arc::clone(&env.storage),
        Arc::clone(&env.series_index),
        Arc::clone(&env.tag_index),
    );

    let query = Query::builder("cpu")
        .time_range(i64::MIN, i64::MAX)
        .select_aggregate("*", AggregateFunction::Count, None)
        .build()
        .unwrap();

    let mut q = query.clone();
    q.tag_filters = vec![TagFilter::Equals {
        key: "region".to_string(),
        value: "us".to_string(),
    }];

    let result = executor.execute(q).unwrap();
    let v = result.rows[0].fields.values().next().unwrap();
    assert_eq!(v, &FieldValue::Integer(expected as i64));
}
