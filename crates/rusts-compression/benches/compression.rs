//! Compression benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rusts_compression::{
    BlockCompressor, CompressionLevel, GorillaEncoder, GorillaDecoder,
    TimestampEncoder, TimestampDecoder, IntegerEncoder, IntegerDecoder,
};

fn bench_gorilla_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_encode");

    for size in [1000, 10000, 100000].iter() {
        let values: Vec<f64> = (0..*size)
            .map(|i| 100.0 + (i as f64 * 0.01).sin() * 10.0)
            .collect();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_function(format!("encode_{}", size), |b| {
            b.iter(|| {
                let mut encoder = GorillaEncoder::new();
                for &v in &values {
                    encoder.encode(black_box(v));
                }
                black_box(encoder.finish())
            });
        });
    }

    group.finish();
}

fn bench_gorilla_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_decode");

    let values: Vec<f64> = (0..10000)
        .map(|i| 100.0 + (i as f64 * 0.01).sin() * 10.0)
        .collect();

    let mut encoder = GorillaEncoder::new();
    for &v in &values {
        encoder.encode(v);
    }
    let encoded = encoder.finish();

    group.throughput(Throughput::Elements(10000));
    group.bench_function("decode_10000", |b| {
        b.iter(|| {
            let mut decoder = GorillaDecoder::new(black_box(&encoded), 10000);
            black_box(decoder.decode_all().unwrap())
        });
    });

    group.finish();
}

fn bench_timestamp_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_encode");

    // Evenly spaced timestamps (best case)
    let even_timestamps: Vec<i64> = (0..10000)
        .map(|i| i * 1_000_000_000)
        .collect();

    group.throughput(Throughput::Elements(10000));
    group.bench_function("even_10000", |b| {
        b.iter(|| {
            let mut encoder = TimestampEncoder::new();
            for &ts in &even_timestamps {
                encoder.encode(black_box(ts));
            }
            black_box(encoder.finish())
        });
    });

    // Slightly irregular timestamps
    let irregular_timestamps: Vec<i64> = (0..10000)
        .map(|i| i * 1_000_000_000 + (i % 100) * 1000)
        .collect();

    group.bench_function("irregular_10000", |b| {
        b.iter(|| {
            let mut encoder = TimestampEncoder::new();
            for &ts in &irregular_timestamps {
                encoder.encode(black_box(ts));
            }
            black_box(encoder.finish())
        });
    });

    group.finish();
}

fn bench_block_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_compression");

    let data: Vec<u8> = (0..100000)
        .map(|i| ((i % 256) as u8).wrapping_add((i / 1000) as u8))
        .collect();

    group.throughput(Throughput::Bytes(100000));

    for level in [
        CompressionLevel::Fast,
        CompressionLevel::Default,
        CompressionLevel::High,
    ] {
        let compressor = BlockCompressor::new(level);

        group.bench_function(format!("compress_{:?}", level), |b| {
            b.iter(|| {
                black_box(compressor.compress(black_box(&data)).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_gorilla_encode,
    bench_gorilla_decode,
    bench_timestamp_encode,
    bench_block_compression,
);

criterion_main!(benches);
