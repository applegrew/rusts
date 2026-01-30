# RusTs Benchmark Suite

Benchmark queries adapted from [ClickBench](https://github.com/ClickHouse/ClickBench) for NYC TLC trip data.

## Prerequisites

1. RusTs server running with trip data loaded
2. Python 3.6+ (for the Python benchmark runner)

## Loading Test Data

Download NYC TLC HVFHS (High Volume For-Hire Services) trip data:

```bash
# Download sample data (November 2025, ~600MB)
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-11.parquet

# Import into RusTs (direct mode for best performance)
./target/release/rusts-import parquet fhvhv_tripdata_2025-11.parquet -m trips --direct
```

More data available at: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Running Benchmarks

### Python Runner (Recommended)

```bash
# Run all queries with default settings (3 iterations)
python3 benchmarks/benchmark.py

# Custom iterations
python3 benchmarks/benchmark.py --iterations 5

# Custom server URL
python3 benchmarks/benchmark.py --url http://192.168.1.100:8086

# Filter queries (regex)
python3 benchmarks/benchmark.py --filter "COUNT\(\*\)"
python3 benchmarks/benchmark.py --filter "LIMIT"

# Quiet mode (summary only)
python3 benchmarks/benchmark.py --quiet
```

### Bash Runner (Alternative)

Requires: `jq`, `bc`, `curl`

```bash
./benchmarks/run_benchmark.sh [server_url] [iterations]
```

## Query Categories

| Category | Queries | Description |
|----------|---------|-------------|
| Q0 | COUNT(*) | Baseline full scan |
| Q1-Q9 | Filters | WHERE clause performance |
| Q10-Q16 | GROUP BY | Aggregation performance |
| Q17-Q20 | Complex | Multi-condition queries |
| Q21-Q40 | Analytics | Business analytics queries |
| Q41-Q43 | LIMIT | LIMIT push-down performance |

## Output

Results are saved to `benchmarks/benchmark_results.csv` with columns:
- `query_id` - Query number
- `query` - Full SQL query
- `status` - ok/error
- `rows` - Rows returned
- `total_rows` - Total rows scanned (pre-LIMIT)
- `exec_time_ns` - Server execution time (nanoseconds)
- `wall_time_ms` - Wall clock time (milliseconds)
- `iteration` - Iteration number

## Trip Data Schema

NYC TLC HVFHS (Uber/Lyft) trip data columns:

| Column | Type | Description |
|--------|------|-------------|
| hvfhs_license_num | string | License number (HV0003=Uber, HV0005=Lyft) |
| dispatching_base_num | string | TLC base license number |
| PULocationID | int | Pickup location ID |
| DOLocationID | int | Dropoff location ID |
| trip_miles | float | Trip distance in miles |
| trip_time | int | Trip duration in seconds |
| base_passenger_fare | float | Base fare amount |
| driver_pay | float | Driver earnings |
| tips | float | Tip amount |
| tolls | float | Toll charges |
| bcf | float | Black Car Fund |
| sales_tax | float | Sales tax |
| congestion_surcharge | float | Congestion pricing |
| airport_fee | float | Airport fee |
| shared_request_flag | string | Y/N shared ride requested |
| wav_match_flag | string | Y/N wheelchair accessible |
