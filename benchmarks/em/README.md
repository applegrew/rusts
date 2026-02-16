# EM Benchmark Suite for RusTs

Benchmarks RusTs query performance against the EM (Endpoint Management) workload and compares with **DB R** and **DB C** baselines.

## Overview

The EM workload models endpoint monitoring for enterprise devices — CPU, memory, disk, energy, WiFi, installed apps, web apps, and network monitoring metrics. Queries range from simple single-metric time-range scans to multi-field aggregations with GROUP BY.

**Query sources:**
- **Part A** (14 queries): DB R slow queries at 50k device load (perf37)
- **Part B** (40 queries): DB C single-metric queries (perf36)
- **Part C** (16 queries): DB C multi-metric queries (perf36)

**Total: 70 queries** covering raw scans, filtered lookups, TOP-N aggregations, COUNT DISTINCT, multi-field SELECTs, and time-windowed aggregations.

## Quick Start

```bash
# 1. Start RusTs server
cargo run --release -- --config rusts.yml

# 2. Generate and load data (small profile: 100 devices, 2 hours)
./benchmarks/em/load_em_data.sh --profile small

# 3. Run benchmark
python3 benchmarks/em/em_benchmark.py
```

## Files

| File | Description |
|------|-------------|
| `generate_em_data.py` | Generates realistic EM data in InfluxDB line protocol |
| `load_em_data.sh` | Generates data and loads it into RusTs via `/write` |
| `em_queries.sql` | 70 benchmark queries with baseline annotations |
| `em_benchmark.py` | Benchmark runner — executes queries, measures latency, produces report |
| `README.md` | This file |

## Data Generation

The generator creates data across 4 measurements matching the RusTs schema:

| Measurement | Tags | Fields | Description |
|-------------|------|--------|-------------|
| `em_device_metrics` | devicesysid_k, type_k, os_k, city_t, ... | 38 device metrics (CPU, memory, disk, energy, WiFi, ...) | Device health |
| `em_installed_app_metrics` | devicesysid_k, appsysid_k, type_k, appname_t, appversion_t | 11 app metrics (CPU, memory, I/O, crashes, ...) | Installed app performance |
| `em_web_app_metrics` | devicesysid_k, appsysid_k, type_k | 12 web app metrics (availability, response time, ...) | Web app performance |
| `em_network_monitoring_metrics` | devicesysid_k, appsysid_k, type_k | 3 network metrics (packet loss, latency, jitter) | Network monitoring |
| `em_device_metrics_battery` | devicesysid_k, batteryid_k, type_k | 1 battery metric | Battery health |

### Data Profiles

| Profile | Devices | Hours | Approx Lines | Approx Size |
|---------|--------:|------:|-------------:|------------:|
| `small` | 100 | 2 | ~15K | ~5 MB |
| `medium` | 1,000 | 24 | ~1.5M | ~500 MB |
| `large` | 10,000 | 24 | ~15M | ~5 GB |
| `full` | 50,000 | 24 | ~75M | ~25 GB |

```bash
# Generate specific profile
./benchmarks/em/load_em_data.sh --profile medium

# Custom parameters
./benchmarks/em/load_em_data.sh --devices 500 --hours 6 --interval 60

# Use pre-generated data file
python3 benchmarks/em/generate_em_data.py --devices 1000 --hours 24 --output data.lp
./benchmarks/em/load_em_data.sh --data-file data.lp
```

## Running Benchmarks

```bash
# Basic run (5 iterations, 1 warmup)
python3 benchmarks/em/em_benchmark.py

# Custom settings
python3 benchmarks/em/em_benchmark.py \
  --url http://localhost:8086 \
  --iterations 10 \
  --warmup 2 \
  --timeout 60

# Custom output paths
python3 benchmarks/em/em_benchmark.py \
  --output-csv results/em_results.csv \
  --output-md results/em_report.md
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | `http://localhost:8086` | RusTs server URL |
| `--iterations` | `5` | Measured iterations per query |
| `--warmup` | `1` | Warmup iterations (not counted) |
| `--timeout` | `30` | Per-query timeout (seconds) |
| `--queries` | `em_queries.sql` | SQL queries file |
| `--output-csv` | `em_results.csv` | Raw results CSV |
| `--output-md` | `em_report.md` | Markdown comparison report |

## Output

### CSV (`em_results.csv`)

One row per query × iteration with columns:
`query_id, query_name, category, iteration, wall_ms, server_ms, rows, total_rows, status, error, db_r_ms, db_c_ms`

### Markdown Report (`em_report.md`)

Contains:
- **Summary**: total queries, pass/fail counts, geometric mean speedup
- **Category breakdown**: per-category pass/fail/error counts
- **Detailed table**: per-query P50/P95/P99 latency, baseline comparison, speedup ratios
- **Error details**: any queries that failed

### Interpreting Results

- **P95 < 2000ms** = PASS (meets SLA target)
- **vs DB R / vs DB C** = speedup ratio (higher is better)
- **Geometric mean speedup** = overall performance multiplier across all comparable queries

## Query Annotations

Each query in `em_queries.sql` is annotated with:

```sql
-- QA01 | DeviceMet - Page file usage | A | 8776 | 0
```

Format: `-- ID | Name | Category | DB R_ms | DB C_ms`

- `0` means no baseline available for that system
- DB R baselines are from perf37 (50k device load)
- DB C baselines are from perf36

## Known Limitations

1. **ROLLUP queries excluded**: RusTs does not support `CREATE TEMPORARY TABLE`, `DATE_BIN`, or multi-statement transactions. The 2 ROLLUP queries from Part A are omitted.
2. **Parameterized queries**: Queries with `?` placeholders have been substituted with concrete values from the known device/app IDs used in the benchmark data.
3. **DB C single-metric mapping**: DB C stores each metric as a separate measurement. In RusTs, these are fields within `em_device_metrics`. The Part B queries are translated to use the RusTs schema.
4. **COUNT DISTINCT**: Some dashboard queries use `COUNT(DISTINCT tag)` which may have different performance characteristics across systems.
