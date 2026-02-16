//! OpenTelemetry-based telemetry for RusTs server.
//!
//! Design choices for minimal performance impact:
//! - **Background sampler thread**: CPU and memory metrics are collected on a
//!   dedicated OS thread at a configurable interval (default 5 s). The hot
//!   write/query paths never call `getrusage` or read `/proc`.
//! - **Batched OTLP export**: The SDK batches spans and metric points and
//!   exports them asynchronously on the Tokio runtime, so export I/O never
//!   blocks request-serving threads.
//! - **Opt-in**: When `telemetry.enabled` is false (the default), no OTel
//!   providers are installed and the sampler thread is not spawned — zero
//!   overhead.
//! - **Tracing integration**: We add a `tracing-opentelemetry` layer so that
//!   existing `#[instrument]` / `tracing::info_span!` calls automatically
//!   become OTel spans with no code changes in the hot path.

use opentelemetry::metrics::MeterProvider;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use rusts_api::handlers::ServerMetrics;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Telemetry configuration — lives under the `telemetry` key in `rusts.yml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetrySettings {
    /// Master switch.  When false nothing is initialised.
    pub enabled: bool,
    /// OTLP gRPC endpoint (e.g. `http://localhost:4317`).
    pub otlp_endpoint: String,
    /// How often the background thread samples process CPU / memory (seconds).
    pub sample_interval_secs: u64,
    /// OTLP metric export interval (seconds).
    pub export_interval_secs: u64,
    /// Service name reported to the collector.
    pub service_name: String,
}

impl Default for TelemetrySettings {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            sample_interval_secs: 5,
            export_interval_secs: 10,
            service_name: "rusts".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics builder — constructs handlers::ServerMetrics from an OTel Meter
// ---------------------------------------------------------------------------

fn build_metrics(meter: &opentelemetry::metrics::Meter) -> ServerMetrics {
    ServerMetrics {
        points_written: meter
            .u64_counter("rusts.points_written")
            .with_description("Total data points written")
            .build(),
        write_latency_ms: meter
            .f64_histogram("rusts.write_latency_ms")
            .with_description("Write request latency in ms")
            .build(),
        queries_executed: meter
            .u64_counter("rusts.queries_executed")
            .with_description("Total queries executed")
            .build(),
        query_latency_ms: meter
            .f64_histogram("rusts.query_latency_ms")
            .with_description("Query latency in ms")
            .build(),
        active_queries: meter
            .i64_gauge("rusts.active_queries")
            .with_description("Currently executing queries")
            .build(),
        memtable_flushes_total: meter
            .i64_gauge("rusts.memtable_flushes_total")
            .with_description("Cumulative memtable flush count (use 'reason' attribute to filter)")
            .build(),
        wal_syncs: meter
            .u64_counter("rusts.wal_syncs")
            .with_description("Total WAL sync (fsync) operations")
            .build(),
        wal_bytes_written: meter
            .u64_counter("rusts.wal_bytes_written")
            .with_description("Total bytes written to WAL")
            .build(),
        process_cpu_usage: meter
            .f64_gauge("rusts.process.cpu_usage")
            .with_description("Process CPU usage ratio")
            .build(),
        process_memory_rss: meter
            .i64_gauge("rusts.process.memory_rss_bytes")
            .with_description("Process resident set size in bytes")
            .build(),
    }
}

// ---------------------------------------------------------------------------
// Telemetry guard — keeps providers alive and stops the sampler on drop
// ---------------------------------------------------------------------------

/// Returned by [`init`].  Dropping this shuts down the OTel pipeline.
pub struct TelemetryGuard {
    _meter_provider: Option<SdkMeterProvider>,
    _tracer_provider: Option<SdkTracerProvider>,
    sampler_stop: Option<Arc<AtomicBool>>,
    sampler_handle: Option<std::thread::JoinHandle<()>>,
}

impl TelemetryGuard {
    /// Gracefully shut down telemetry (flushes pending exports).
    pub fn shutdown(mut self) {
        // Stop background sampler
        if let Some(stop) = self.sampler_stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(handle) = self.sampler_handle.take() {
            let _ = handle.join();
        }
        // Flush providers
        if let Some(mp) = self._meter_provider.take() {
            if let Err(e) = mp.shutdown() {
                warn!("Meter provider shutdown error: {}", e);
            }
        }
        if let Some(tp) = self._tracer_provider.take() {
            let result: opentelemetry_sdk::error::OTelSdkResult = tp.shutdown();
            if let Err(e) = result {
                warn!("Tracer provider shutdown error: {:?}", e);
            }
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(stop) = self.sampler_stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(handle) = self.sampler_handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------

/// Initialise the OpenTelemetry pipeline (metrics + traces) and the
/// background CPU/memory sampler.  Returns a guard that **must** be held
/// for the lifetime of the server and a [`ServerMetrics`] handle for
/// recording application-level metrics.
///
/// When `settings.enabled` is `false`, this returns no-op metrics and a
/// dummy guard — zero overhead.
pub fn init(
    settings: &TelemetrySettings,
    log_level: tracing::Level,
    log_show_target: bool,
    log_show_thread_ids: bool,
    log_show_location: bool,
) -> anyhow::Result<(TelemetryGuard, Option<ServerMetrics>)> {
    if !settings.enabled {
        // Install a plain fmt subscriber (same as before telemetry existed)
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(log_level)
            .with_target(log_show_target)
            .with_thread_ids(log_show_thread_ids)
            .with_file(log_show_location)
            .with_line_number(log_show_location)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set subscriber");

        return Ok((
            TelemetryGuard {
                _meter_provider: None,
                _tracer_provider: None,
                sampler_stop: None,
                sampler_handle: None,
            },
            None,
        ));
    }

    // -- Resource --------------------------------------------------------
    let resource = Resource::builder()
        .with_service_name(settings.service_name.clone())
        .build();

    // -- Traces ----------------------------------------------------------
    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&settings.otlp_endpoint)
        .build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(trace_exporter)
        .build();

    let tracer = tracer_provider.tracer("rusts");

    // -- Metrics ---------------------------------------------------------
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&settings.otlp_endpoint)
        .build()?;

    let export_interval = Duration::from_secs(settings.export_interval_secs.max(1));
    let metric_reader = PeriodicReader::builder(metric_exporter)
        .with_interval(export_interval)
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(metric_reader)
        .build();

    // Register global providers so libraries can use them
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let meter = meter_provider.meter("rusts");
    let metrics = build_metrics(&meter);

    // -- Tracing subscriber with OTel layer ------------------------------
    let otel_layer = OpenTelemetryLayer::new(tracer);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level.to_string()));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer()
            .with_target(log_show_target)
            .with_thread_ids(log_show_thread_ids)
            .with_file(log_show_location)
            .with_line_number(log_show_location))
        .with(otel_layer)
        .init();

    // -- Background CPU / memory sampler ---------------------------------
    let stop_flag = Arc::new(AtomicBool::new(false));
    let sampler_metrics = metrics.clone();
    let interval = Duration::from_secs(settings.sample_interval_secs.max(1));
    let stop = Arc::clone(&stop_flag);

    let sampler_handle = std::thread::Builder::new()
        .name("rusts-telemetry-sampler".into())
        .spawn(move || {
            run_sampler(stop, interval, sampler_metrics);
        })?;

    Ok((
        TelemetryGuard {
            _meter_provider: Some(meter_provider),
            _tracer_provider: Some(tracer_provider),
            sampler_stop: Some(stop_flag),
            sampler_handle: Some(sampler_handle),
        },
        Some(metrics),
    ))
}

// ---------------------------------------------------------------------------
// Background sampler — runs on its own OS thread, never touches Tokio
// ---------------------------------------------------------------------------

fn run_sampler(stop: Arc<AtomicBool>, interval: Duration, metrics: ServerMetrics) {
    let mut prev_cpu = get_process_cpu_time();
    let mut prev_wall = std::time::Instant::now();
    let num_cpus = num_cpus_available();

    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(interval);
        if stop.load(Ordering::Relaxed) {
            break;
        }

        // CPU usage
        let now_cpu = get_process_cpu_time();
        let now_wall = std::time::Instant::now();
        let wall_elapsed = now_wall.duration_since(prev_wall).as_secs_f64();
        if wall_elapsed > 0.0 {
            let cpu_elapsed = now_cpu - prev_cpu;
            let usage = cpu_elapsed / wall_elapsed; // ratio, e.g. 1.5 = 150% of one core
            metrics.process_cpu_usage.record(usage, &[
                KeyValue::new("cores", num_cpus as i64),
            ]);
        }
        prev_cpu = now_cpu;
        prev_wall = now_wall;

        // RSS
        let rss = get_process_rss_bytes();
        metrics.process_memory_rss.record(rss, &[]);
    }
}

// ---------------------------------------------------------------------------
// Platform-specific helpers
// ---------------------------------------------------------------------------

/// Returns total user+system CPU time consumed by this process, in seconds.
#[cfg(unix)]
fn get_process_cpu_time() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0.0;
    }
    let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 * 1e-6;
    let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 * 1e-6;
    user + sys
}

#[cfg(not(unix))]
fn get_process_cpu_time() -> f64 {
    0.0
}

/// Returns the resident set size of this process in bytes.
#[cfg(target_os = "macos")]
fn get_process_rss_bytes() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    // On macOS ru_maxrss is in bytes
    usage.ru_maxrss as i64
}

#[cfg(target_os = "linux")]
fn get_process_rss_bytes() -> i64 {
    // Read from /proc/self/statm — field 1 is RSS in pages
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(rss_pages) = statm.split_whitespace().nth(1) {
            if let Ok(pages) = rss_pages.parse::<i64>() {
                let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as i64;
                return pages * page_size;
            }
        }
    }
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_process_rss_bytes() -> i64 {
    0
}

/// Number of logical CPUs.
fn num_cpus_available() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
