# RusTs SQL - Architecture

## Overview

The `rusts-sql` crate provides a SQL query interface for RusTs, translating SQL queries into the native Query model using `sqlparser-rs`. This enables users familiar with SQL to query time series data without learning a new query language.

## Module Structure

```
src/
├── lib.rs         # Public API exports
├── error.rs       # SQL-specific error types
├── parser.rs      # SQL parsing wrapper
├── functions.rs   # Function registry (aggregates, time functions)
└── translator.rs  # SQL AST to Query model translation
```

## Query Translation Pipeline

```
SQL String
    │
    ▼
┌─────────────────────────┐
│     SqlParser           │  ← sqlparser-rs (GenericDialect)
│  (parser.rs:12-28)      │
└───────────┬─────────────┘
            │
            ▼
    sqlparser::Statement
            │
            ▼
┌─────────────────────────┐
│    SqlTranslator        │  ← AST to Query conversion
│  (translator.rs:26-143) │
└───────────┬─────────────┘
            │
            ▼
    rusts_query::Query
            │
            ▼
┌─────────────────────────┐
│    QueryExecutor        │  ← Execution (rusts-query crate)
└─────────────────────────┘
```

## Core Components

### SqlParser (parser.rs:9-42)

Thin wrapper around sqlparser-rs providing:
- Single statement parsing (multi-statement rejected)
- Generic SQL dialect support
- Validation for SELECT-only queries

```rust
pub struct SqlParser;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Statement>;
    pub fn parse_select(sql: &str) -> Result<Statement>;
}
```

### SqlTranslator (translator.rs:26-143)

Converts sqlparser AST into RusTs Query model:

```rust
pub struct SqlTranslator;

impl SqlTranslator {
    pub fn translate(stmt: &Statement) -> Result<Query>;
    pub fn translate_command(stmt: &Statement) -> Result<SqlCommand>;
}

pub enum SqlCommand {
    Query(Query),
    ShowTables,
    Explain(Query),  // Returns query plan without executing
}
```

### FunctionRegistry (functions.rs:10-185)

Maps SQL functions to RusTs operations:

**Aggregate functions:**

| SQL Function | RusTs Aggregate |
|--------------|------------------|
| COUNT | AggregateFunction::Count |
| SUM | AggregateFunction::Sum |
| AVG, MEAN, AVERAGE | AggregateFunction::Mean |
| MIN | AggregateFunction::Min |
| MAX | AggregateFunction::Max |
| FIRST | AggregateFunction::First |
| LAST | AggregateFunction::Last |
| STDDEV, STDDEV_SAMP | AggregateFunction::StdDev |
| VARIANCE, VAR_SAMP | AggregateFunction::Variance |
| PERCENTILE_N | AggregateFunction::Percentile(N) |

**Window-only functions** (require OVER clause):

| SQL Function | WindowFunctionType |
|--------------|--------------------|
| ROW_NUMBER() | WindowFunctionType::RowNumber |
| RANK() | WindowFunctionType::Rank |
| DENSE_RANK() | WindowFunctionType::DenseRank |
| LAG(field, offset, default) | WindowFunctionType::Lag |
| LEAD(field, offset, default) | WindowFunctionType::Lead |

Aggregate functions (SUM, AVG, etc.) can also be used as window functions when combined with an OVER clause.

Time functions:
- `now()` - Returns current timestamp in nanoseconds
- `time_bucket('interval', time)` - Time-based grouping

### SqlError (error.rs:7-52)

```rust
pub enum SqlError {
    Parse(String),            // SQL syntax errors
    UnsupportedFeature(String), // Unsupported SQL constructs
    InvalidTimeExpression(String),
    UnknownFunction(String),
    InvalidAggregation(String),
    Translation(String),
    InvalidInterval(String),
    MissingClause(String),
    Query(rusts_query::QueryError),
}
```

## Supported SQL Features

### SELECT Clause

```sql
-- All fields
SELECT * FROM cpu

-- Specific fields
SELECT usage, temperature FROM cpu

-- Aggregations
SELECT AVG(usage), MAX(temperature) FROM cpu

-- With alias
SELECT AVG(usage) AS avg_usage FROM cpu

-- Time bucketing
SELECT time_bucket('1h', time), AVG(usage) FROM cpu GROUP BY 1

-- Window functions
SELECT *, ROW_NUMBER() OVER (ORDER BY time) AS rn FROM cpu
SELECT *, RANK() OVER (PARTITION BY host ORDER BY time DESC) AS rnk FROM cpu
SELECT LAG(usage, 1) OVER (ORDER BY time) AS prev_usage FROM cpu
SELECT SUM(usage) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum FROM cpu
SELECT AVG(usage) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM cpu
```

### FROM Clause

```sql
-- Simple table reference (measurement name)
SELECT * FROM cpu
SELECT * FROM "my-measurement"
```

**Not Supported:** JOINs, subqueries, multiple tables

### WHERE Clause

```sql
-- Tag equality
SELECT * FROM cpu WHERE host = 'server01'

-- Tag not equals
SELECT * FROM cpu WHERE host != 'server01'

-- Tag IN list
SELECT * FROM cpu WHERE region IN ('us-west', 'us-east')

-- Tag existence
SELECT * FROM cpu WHERE host IS NOT NULL

-- Time range
SELECT * FROM cpu WHERE time >= '2024-01-01' AND time < '2024-01-02'

-- Combined filters
SELECT * FROM cpu WHERE host = 'server01' AND region = 'us-west'

-- OR conditions
SELECT * FROM cpu WHERE host = 'server01' OR host = 'server02'

-- Nested AND/OR
SELECT * FROM cpu WHERE (host = 'a' OR host = 'b') AND region = 'us-west'
```

**Not Supported:** Nested subqueries

### GROUP BY Clause

```sql
-- Group by tags
SELECT AVG(usage) FROM cpu GROUP BY host

-- Group by time bucket
SELECT time_bucket('1h', time), AVG(usage) FROM cpu GROUP BY 1

-- Combined grouping
SELECT time_bucket('5m', time), host, AVG(usage) FROM cpu GROUP BY 1, host
```

### ORDER BY, LIMIT, OFFSET

```sql
SELECT * FROM cpu ORDER BY time DESC
SELECT * FROM cpu ORDER BY time ASC LIMIT 100
SELECT * FROM cpu LIMIT 100 OFFSET 50
```

**Default Sort Order:**

When no `ORDER BY` is specified, SQL queries default to `ORDER BY time DESC`. This optimization improves performance for time series queries since the most recent data is typically in the memtable (fast access) while older data requires partition scans.

```sql
-- These are equivalent:
SELECT * FROM cpu LIMIT 100
SELECT * FROM cpu ORDER BY time DESC LIMIT 100
```

Note: The programmatic Query API (not SQL) defaults to ascending order for backward compatibility.

### SHOW TABLES

```sql
SHOW TABLES
```

Returns list of measurements.

### EXPLAIN

```sql
EXPLAIN SELECT COUNT(*) FROM cpu WHERE host = 'server01'
```

Returns query plan with optimization hints:

```json
{
  "measurement": "cpu",
  "time_range": {"start": 0, "end": 9223372036854775807},
  "optimizations": ["segment_stats_pushdown", "filter_reordering"],
  "filter_order": [{"filter": "host = 'server01'", "cardinality": 50}],
  "partitions_to_scan": 3,
  "estimated_series": 50,
  "estimated_points": 5000,
  "hints": {
    "memtable_only": false,
    "use_segment_stats": true,
    "filters_reordered": true
  }
}
```

## Time Expression Parsing

### Timestamp Formats (functions.rs:143-184)

| Format | Example |
|--------|---------|
| ISO 8601 date | `'2024-01-01'` |
| RFC 3339 | `'2024-01-01T00:00:00Z'` |
| DateTime | `'2024-01-01 00:00:00'` |
| Nanoseconds | `1704067200000000000` |
| now() | Current time |

### Interval Formats (functions.rs:60-135)

Compact format:
| Suffix | Unit |
|--------|------|
| ns | Nanoseconds |
| us, µs | Microseconds |
| ms | Milliseconds |
| s | Seconds |
| m | Minutes |
| h | Hours |
| d | Days |
| w | Weeks |

```sql
-- Compact
time_bucket('1h', time)
time_bucket('5m', time)
time_bucket('100ms', time)

-- Verbose
time_bucket('1 hour', time)
time_bucket('5 minutes', time)
```

## Translation Details

### Measurement Extraction (translator.rs:146-170)

FROM clause → measurement name
```sql
SELECT * FROM cpu  →  measurement = "cpu"
```

### Time Range Extraction (translator.rs:182-342)

WHERE clauses with `time`, `timestamp`, or `_time` columns are converted to TimeRange:

```sql
WHERE time >= '2024-01-01' AND time < '2024-02-01'
```
→
```rust
TimeRange { start: 1704067200000000000, end: 1706745600000000000 }
```

### Tag Filter Extraction (translator.rs:276-342)

Non-time WHERE conditions become TagFilters:

```sql
WHERE host = 'server01' AND region != 'us-east'
```
→
```rust
vec![
    TagFilter::Equals { key: "host", value: "server01" },
    TagFilter::NotEquals { key: "region", value: "us-east" },
]
```

### Field Selection (translator.rs:345-449)

SELECT items map to FieldSelection:

| SQL | FieldSelection | Window Functions |
|-----|----------------|------------------|
| `*` | FieldSelection::All | — |
| `field1, field2` | FieldSelection::Fields(vec!["field1", "field2"]) | — |
| `AVG(field)` | FieldSelection::Aggregate { field, function, alias } | — |
| `ROW_NUMBER() OVER (...)` | FieldSelection::All | Vec<WindowFunction> |
| `field, SUM(field) OVER (...)` | FieldSelection::Fields | Vec<WindowFunction> |

## Dependencies

| Dependency | Purpose |
|------------|---------|
| sqlparser | SQL parsing (GenericDialect) |
| rusts-core | Core types (TimeRange) |
| rusts-query | Query model, AggregateFunction |
| chrono | Timestamp parsing |
| thiserror | Error handling |
| tracing | Debug logging |

## Usage Example

```rust
use rusts_sql::{SqlParser, SqlTranslator};

// Parse SQL
let sql = "SELECT AVG(usage) FROM cpu WHERE host = 'server01' GROUP BY time_bucket('1h', time)";
let stmt = SqlParser::parse(sql)?;

// Translate to Query
let query = SqlTranslator::translate(&stmt)?;

// Execute with QueryExecutor (from rusts-query)
let result = executor.execute(query)?;
```

## API Endpoint

The `/sql` endpoint accepts plain text SQL (preferred) or JSON:

```bash
# Plain text (preferred)
curl -X POST http://localhost:8086/sql \
  -d "SELECT AVG(trip_distance) FROM trips WHERE vendor_id = '1' GROUP BY time_bucket('1h', time)"

# JSON (backward compatible)
curl -X POST http://localhost:8086/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu"}'

# EXPLAIN query
curl -X POST http://localhost:8086/sql \
  -d "EXPLAIN SELECT COUNT(*) FROM cpu WHERE host = 'server01'"
```

## Limitations

1. **No JOINs** - Time series data model doesn't support joins
2. **No subqueries** - Flat query structure only
3. **No UNION/INTERSECT/EXCEPT** - Single query only
4. **Single aggregate per query** - First aggregate function used (when not using window functions)
5. **No named windows** - `WINDOW w AS (...)` syntax is not supported
6. **No GROUPS frame unit** - Only ROWS and RANGE frame units are supported

## Testing

```bash
# Run SQL tests
cargo test -p rusts-sql

# Test specific module
cargo test -p rusts-sql translator::tests
cargo test -p rusts-sql parser::tests
cargo test -p rusts-sql functions::tests
```
