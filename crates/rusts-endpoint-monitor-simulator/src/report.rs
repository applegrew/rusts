//! Performance report generation.

use crate::config::WorkloadMode;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Latency statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

impl LatencyStats {
    /// Formats latency as a human-readable string.
    pub fn format_ms(&self) -> String {
        if self.count == 0 {
            "N/A".to_string()
        } else {
            format!(
                "p50={:.1}ms p95={:.1}ms p99={:.1}ms",
                self.p50_us as f64 / 1000.0,
                self.p95_us as f64 / 1000.0,
                self.p99_us as f64 / 1000.0
            )
        }
    }
}

/// Complete benchmark report.
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    // Configuration
    pub mode: WorkloadMode,
    pub device_count: usize,
    pub estimated_series: usize,
    pub duration: Duration,
    pub total_duration: Duration,
    pub warmup_secs: u64,
    pub write_interval: Duration,
    pub query_rate: f64,

    // Write statistics
    pub points_written: u64,
    pub batches_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub write_retries: u64,
    pub write_latency: LatencyStats,

    // Query statistics
    pub dashboard_queries: u64,
    pub alerting_queries: u64,
    pub historical_queries: u64,
    pub query_errors: u64,
    pub dashboard_latency: LatencyStats,
    pub alerting_latency: LatencyStats,
    pub historical_latency: LatencyStats,
}

impl Report {
    /// Computes throughput metrics.
    pub fn points_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.points_written as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn queries_per_second(&self) -> f64 {
        let total_queries = self.dashboard_queries + self.alerting_queries + self.historical_queries;
        if self.duration.as_secs_f64() > 0.0 {
            total_queries as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn bytes_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.bytes_written as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Generates a markdown report.
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str("# Endpoint Monitor Simulator Report\n\n");

        // Configuration section
        md.push_str("## Configuration\n\n");
        md.push_str("| Setting | Value |\n");
        md.push_str("|---------|-------|\n");
        md.push_str(&format!("| Mode | {} |\n", self.mode));
        md.push_str(&format!("| Devices | {} |\n", self.device_count));
        md.push_str(&format!("| Estimated Series | ~{} |\n", self.estimated_series));
        md.push_str(&format!("| Duration | {:.1}s |\n", self.duration.as_secs_f64()));
        md.push_str(&format!("| Warmup | {}s |\n", self.warmup_secs));
        md.push_str(&format!(
            "| Write Interval | {:.1}s |\n",
            self.write_interval.as_secs_f64()
        ));
        md.push_str(&format!("| Query Rate | {:.1}/s |\n", self.query_rate));
        md.push('\n');

        // Write statistics
        if self.mode != WorkloadMode::QueryOnly {
            md.push_str("## Write Performance\n\n");
            md.push_str("| Metric | Value |\n");
            md.push_str("|--------|-------|\n");
            md.push_str(&format!("| Points Written | {} |\n", format_number(self.points_written)));
            md.push_str(&format!("| Batches | {} |\n", format_number(self.batches_written)));
            md.push_str(&format!("| Bytes | {} |\n", format_bytes(self.bytes_written)));
            md.push_str(&format!(
                "| Throughput | {:.0} points/s |\n",
                self.points_per_second()
            ));
            md.push_str(&format!(
                "| Bandwidth | {}/s |\n",
                format_bytes(self.bytes_per_second() as u64)
            ));
            md.push_str(&format!("| Errors | {} |\n", self.write_errors));
            md.push_str(&format!("| Retries | {} |\n", self.write_retries));
            md.push('\n');

            md.push_str("### Write Latency\n\n");
            md.push_str("| Percentile | Latency |\n");
            md.push_str("|------------|--------|\n");
            if self.write_latency.count > 0 {
                md.push_str(&format!(
                    "| p50 | {:.2}ms |\n",
                    self.write_latency.p50_us as f64 / 1000.0
                ));
                md.push_str(&format!(
                    "| p95 | {:.2}ms |\n",
                    self.write_latency.p95_us as f64 / 1000.0
                ));
                md.push_str(&format!(
                    "| p99 | {:.2}ms |\n",
                    self.write_latency.p99_us as f64 / 1000.0
                ));
                md.push_str(&format!(
                    "| max | {:.2}ms |\n",
                    self.write_latency.max_us as f64 / 1000.0
                ));
            } else {
                md.push_str("| N/A | No data |\n");
            }
            md.push('\n');
        }

        // Query statistics
        if self.mode != WorkloadMode::WriteOnly {
            let total_queries =
                self.dashboard_queries + self.alerting_queries + self.historical_queries;

            md.push_str("## Query Performance\n\n");
            md.push_str("| Metric | Value |\n");
            md.push_str("|--------|-------|\n");
            md.push_str(&format!("| Total Queries | {} |\n", format_number(total_queries)));
            md.push_str(&format!(
                "| Dashboard Queries | {} |\n",
                format_number(self.dashboard_queries)
            ));
            md.push_str(&format!(
                "| Alerting Queries | {} |\n",
                format_number(self.alerting_queries)
            ));
            md.push_str(&format!(
                "| Historical Queries | {} |\n",
                format_number(self.historical_queries)
            ));
            md.push_str(&format!(
                "| Throughput | {:.1} queries/s |\n",
                self.queries_per_second()
            ));
            md.push_str(&format!("| Errors | {} |\n", self.query_errors));
            md.push('\n');

            md.push_str("### Query Latency by Type\n\n");
            md.push_str("| Type | p50 | p95 | p99 | max |\n");
            md.push_str("|------|-----|-----|-----|-----|\n");

            for (name, stats) in [
                ("Dashboard", &self.dashboard_latency),
                ("Alerting", &self.alerting_latency),
                ("Historical", &self.historical_latency),
            ] {
                if stats.count > 0 {
                    md.push_str(&format!(
                        "| {} | {:.1}ms | {:.1}ms | {:.1}ms | {:.1}ms |\n",
                        name,
                        stats.p50_us as f64 / 1000.0,
                        stats.p95_us as f64 / 1000.0,
                        stats.p99_us as f64 / 1000.0,
                        stats.max_us as f64 / 1000.0
                    ));
                } else {
                    md.push_str(&format!("| {} | N/A | N/A | N/A | N/A |\n", name));
                }
            }
            md.push('\n');
        }

        // Summary
        md.push_str("## Summary\n\n");
        md.push_str(&format!(
            "Total runtime: {:.1}s (including {:.0}s warmup)\n\n",
            self.total_duration.as_secs_f64(),
            self.warmup_secs
        ));

        if self.write_errors > 0 || self.query_errors > 0 {
            md.push_str("⚠️ **Errors detected during benchmark**\n");
        } else {
            md.push_str("✅ **Benchmark completed without errors**\n");
        }

        md
    }

    /// Generates a JSON report.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Prints a summary to stdout.
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("BENCHMARK RESULTS");
        println!("{}", "=".repeat(60));

        println!(
            "\nMode: {} | Devices: {} | Duration: {:.1}s",
            self.mode,
            self.device_count,
            self.duration.as_secs_f64()
        );

        if self.mode != WorkloadMode::QueryOnly {
            println!("\n📝 WRITES:");
            println!(
                "   Points: {} ({:.0}/s)",
                format_number(self.points_written),
                self.points_per_second()
            );
            println!(
                "   Bytes: {} ({}/s)",
                format_bytes(self.bytes_written),
                format_bytes(self.bytes_per_second() as u64)
            );
            println!("   Latency: {}", self.write_latency.format_ms());
            if self.write_errors > 0 {
                println!("   Errors: {} ⚠️", self.write_errors);
            }
        }

        if self.mode != WorkloadMode::WriteOnly {
            let total_queries =
                self.dashboard_queries + self.alerting_queries + self.historical_queries;
            println!("\n🔍 QUERIES:");
            println!(
                "   Total: {} ({:.1}/s)",
                format_number(total_queries),
                self.queries_per_second()
            );
            println!("   Dashboard: {}", self.dashboard_latency.format_ms());
            println!("   Alerting:  {}", self.alerting_latency.format_ms());
            println!("   Historical: {}", self.historical_latency.format_ms());
            if self.query_errors > 0 {
                println!("   Errors: {} ⚠️", self.query_errors);
            }
        }

        println!("\n{}", "=".repeat(60));
    }
}

/// Formats a number with thousand separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Formats bytes in human-readable form.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

// Implement Serialize for WorkloadMode
impl Serialize for WorkloadMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            WorkloadMode::WriteOnly => "write-only",
            WorkloadMode::QueryOnly => "query-only",
            WorkloadMode::Benchmark => "benchmark",
        })
    }
}

impl<'de> Deserialize<'de> for WorkloadMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "write-only" => Ok(WorkloadMode::WriteOnly),
            "query-only" => Ok(WorkloadMode::QueryOnly),
            "benchmark" => Ok(WorkloadMode::Benchmark),
            _ => Err(serde::de::Error::custom(format!("unknown mode: {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(123), "123");
        assert_eq!(format_number(1234), "1,234");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
}
