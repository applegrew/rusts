//! Combined workload orchestration.

use crate::config::{Config, WorkloadMode};
use crate::fleet::{generate_fleet, Device};
use crate::metrics::MetricGenerator;
use crate::queries::{LatencySenders, QueryExecutor, QueryStats};
use crate::report::{LatencyStats, Report};
use crate::writer::{generate_batch, WriteStats, Writer};
use hdrhistogram::Histogram;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Runs the complete workload based on configuration.
pub async fn run_workload(config: Config, mode: WorkloadMode) -> Report {
    info!("Starting workload in {} mode", mode);
    info!(
        "Configuration: {} devices, {:?} duration",
        config.device_count, config.duration
    );

    // Generate the device fleet
    let fleet = Arc::new(generate_fleet(config.device_count));
    info!("Generated fleet of {} devices", fleet.len());

    // Count expected series
    let total_apps: usize = fleet.iter().map(|d| d.applications.len()).sum();
    let estimated_series = fleet.len() * 3 + total_apps; // device_health, network_health, experience_score + app_performance per app
    info!("Estimated series count: ~{}", estimated_series);

    // Create statistics collectors
    let write_stats = Arc::new(WriteStats::new());
    let query_stats = Arc::new(QueryStats::new());

    // Create latency channels
    let (write_latency_tx, write_latency_rx) = mpsc::unbounded_channel();
    let (dashboard_latency_tx, dashboard_latency_rx) = mpsc::unbounded_channel();
    let (alerting_latency_tx, alerting_latency_rx) = mpsc::unbounded_channel();
    let (historical_latency_tx, historical_latency_rx) = mpsc::unbounded_channel();

    // Spawn latency collectors
    let write_latency_handle = tokio::spawn(collect_latencies(write_latency_rx));
    let dashboard_latency_handle = tokio::spawn(collect_latencies(dashboard_latency_rx));
    let alerting_latency_handle = tokio::spawn(collect_latencies(alerting_latency_rx));
    let historical_latency_handle = tokio::spawn(collect_latencies(historical_latency_rx));

    let start_time = Instant::now();

    // Warmup period
    if config.warmup_secs > 0 && mode != WorkloadMode::QueryOnly {
        info!("Starting warmup period ({} seconds)...", config.warmup_secs);
        run_warmup(&config, &fleet, &write_stats, write_latency_tx.clone()).await;
        info!("Warmup complete");
    }

    let benchmark_start = Instant::now();

    // Spawn workload tasks based on mode
    let write_handle = if mode != WorkloadMode::QueryOnly {
        Some(tokio::spawn(run_write_workload(
            config.clone(),
            fleet.clone(),
            write_stats.clone(),
            write_latency_tx,
        )))
    } else {
        drop(write_latency_tx);
        None
    };

    let query_handle = if mode != WorkloadMode::WriteOnly {
        let latency_senders = LatencySenders {
            dashboard: dashboard_latency_tx,
            alerting: alerting_latency_tx,
            historical: historical_latency_tx,
        };
        Some(tokio::spawn(run_query_workload(
            config.clone(),
            fleet.clone(),
            query_stats.clone(),
            latency_senders,
        )))
    } else {
        drop(dashboard_latency_tx);
        drop(alerting_latency_tx);
        drop(historical_latency_tx);
        None
    };

    // Wait for workloads to complete
    if let Some(handle) = write_handle {
        if let Err(e) = handle.await {
            error!("Write workload failed: {}", e);
        }
    }

    if let Some(handle) = query_handle {
        if let Err(e) = handle.await {
            error!("Query workload failed: {}", e);
        }
    }

    let benchmark_duration = benchmark_start.elapsed();
    let total_duration = start_time.elapsed();

    // Collect latency histograms
    let write_histogram = write_latency_handle
        .await
        .unwrap_or_else(|_| empty_histogram());
    let dashboard_histogram = dashboard_latency_handle
        .await
        .unwrap_or_else(|_| empty_histogram());
    let alerting_histogram = alerting_latency_handle
        .await
        .unwrap_or_else(|_| empty_histogram());
    let historical_histogram = historical_latency_handle
        .await
        .unwrap_or_else(|_| empty_histogram());

    // Build report
    Report {
        mode,
        device_count: config.device_count,
        estimated_series,
        duration: benchmark_duration,
        total_duration,
        warmup_secs: config.warmup_secs,
        write_interval: config.write.interval,
        query_rate: config.query.rate_per_sec,

        points_written: write_stats.points_written.load(Ordering::Relaxed),
        batches_written: write_stats.batches_written.load(Ordering::Relaxed),
        bytes_written: write_stats.bytes_written.load(Ordering::Relaxed),
        write_errors: write_stats.errors.load(Ordering::Relaxed),
        write_retries: write_stats.retries.load(Ordering::Relaxed),
        write_latency: compute_latency_stats(&write_histogram),

        dashboard_queries: query_stats.dashboard_count.load(Ordering::Relaxed),
        alerting_queries: query_stats.alerting_count.load(Ordering::Relaxed),
        historical_queries: query_stats.historical_count.load(Ordering::Relaxed),
        query_errors: query_stats.errors.load(Ordering::Relaxed),
        dashboard_latency: compute_latency_stats(&dashboard_histogram),
        alerting_latency: compute_latency_stats(&alerting_histogram),
        historical_latency: compute_latency_stats(&historical_histogram),
    }
}

/// Runs a warmup period to populate data.
async fn run_warmup(
    config: &Config,
    fleet: &[Device],
    stats: &Arc<WriteStats>,
    latency_tx: mpsc::UnboundedSender<Duration>,
) {
    let writer = Writer::new(
        &config.server_url,
        config.write.clone(),
        stats.clone(),
        latency_tx,
    );

    let mut generator = MetricGenerator::new(42);
    let warmup_duration = Duration::from_secs(config.warmup_secs);
    let start = Instant::now();

    // Write data at accelerated rate during warmup
    let warmup_interval = Duration::from_secs(1);
    let mut timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    while start.elapsed() < warmup_duration {
        let batch = generate_batch(fleet, &mut generator, timestamp);
        if let Err(e) = writer.write_batch(&batch).await {
            warn!("Warmup write error: {}", e);
        }

        timestamp += warmup_interval.as_nanos() as i64;
        tokio::time::sleep(warmup_interval).await;
    }
}

/// Runs the write workload for the configured duration.
async fn run_write_workload(
    config: Config,
    fleet: Arc<Vec<Device>>,
    stats: Arc<WriteStats>,
    latency_tx: mpsc::UnboundedSender<Duration>,
) {
    let writer = Writer::new(&config.server_url, config.write.clone(), stats, latency_tx);

    let mut generator = MetricGenerator::new(rand::random());
    let start = Instant::now();
    let mut interval = tokio::time::interval(config.write.interval);

    while start.elapsed() < config.duration {
        interval.tick().await;

        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let batch = generate_batch(&fleet, &mut generator, timestamp);

        if let Err(e) = writer.write_batch(&batch).await {
            warn!("Write error: {}", e);
        }
    }

    info!("Write workload completed");
}

/// Runs the query workload for the configured duration.
async fn run_query_workload(
    config: Config,
    fleet: Arc<Vec<Device>>,
    stats: Arc<QueryStats>,
    latency_senders: LatencySenders,
) {
    let mut executor = QueryExecutor::new(
        &config.server_url,
        config.query.timeout,
        stats,
        latency_senders,
        fleet,
        config.query.dashboard_pct,
        config.query.alerting_pct,
    );

    let start = Instant::now();
    let query_interval = Duration::from_secs_f64(1.0 / config.query.rate_per_sec);
    let mut interval = tokio::time::interval(query_interval);

    while start.elapsed() < config.duration {
        interval.tick().await;

        let query_type = executor.select_query_type();
        if let Err(e) = executor.execute_query(query_type).await {
            // Only log non-timeout errors at warn level
            match &e {
                crate::queries::QueryError::HttpError(req_err) if req_err.is_timeout() => {
                    // Timeouts are expected under load
                }
                _ => warn!("Query error ({}): {}", query_type.as_str(), e),
            }
        }
    }

    info!("Query workload completed");
}

/// Collects latency samples into a histogram.
async fn collect_latencies(mut rx: mpsc::UnboundedReceiver<Duration>) -> Histogram<u64> {
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

    while let Some(duration) = rx.recv().await {
        let micros = duration.as_micros() as u64;
        let _ = histogram.record(micros);
    }

    histogram
}

/// Creates an empty histogram for error cases.
fn empty_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()
}

/// Computes latency statistics from a histogram.
fn compute_latency_stats(histogram: &Histogram<u64>) -> LatencyStats {
    if histogram.is_empty() {
        return LatencyStats::default();
    }

    LatencyStats {
        count: histogram.len(),
        min_us: histogram.min(),
        max_us: histogram.max(),
        mean_us: histogram.mean() as u64,
        p50_us: histogram.value_at_quantile(0.50),
        p95_us: histogram.value_at_quantile(0.95),
        p99_us: histogram.value_at_quantile(0.99),
    }
}
