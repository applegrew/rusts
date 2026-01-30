#!/bin/bash
#
# RusTs Benchmark Runner
# Runs all queries from trips_queries.sql and measures execution time
#
# Usage: ./benchmarks/run_benchmark.sh [server_url] [iterations]
#
# Requirements:
#   - jq (for JSON parsing)
#   - curl
#   - RusTs server running with trips data loaded

set -e

SERVER_URL="${1:-http://localhost:8086}"
ITERATIONS="${2:-3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERIES_FILE="$SCRIPT_DIR/trips_queries.sql"
RESULTS_FILE="$SCRIPT_DIR/benchmark_results.csv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "RusTs Benchmark Suite"
echo "========================================"
echo "Server: $SERVER_URL"
echo "Iterations: $ITERATIONS"
echo "Queries file: $QUERIES_FILE"
echo ""

# Check server health
echo -n "Checking server health... "
if curl -s "$SERVER_URL/health" > /dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Error: Cannot connect to server at $SERVER_URL"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is required but not installed${NC}"
    exit 1
fi

# Initialize results file
echo "query_id,query,status,rows,total_rows,exec_time_ns,wall_time_ms,iteration" > "$RESULTS_FILE"

# Read queries from file (skip comments and empty lines)
query_id=0
while IFS= read -r line || [[ -n "$line" ]]; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^--.*$ ]] || [[ -z "${line// }" ]]; then
        continue
    fi

    # Skip if not a SELECT statement
    if [[ ! "$line" =~ ^SELECT ]]; then
        continue
    fi

    query="$line"
    query_id=$((query_id + 1))

    echo ""
    echo -e "${YELLOW}Q$query_id:${NC} $query"
    echo "----------------------------------------"

    total_time_ms=0
    success_count=0

    for iter in $(seq 1 $ITERATIONS); do
        # Measure wall clock time
        start_time=$(python3 -c 'import time; print(int(time.time() * 1000))')

        # Execute query
        response=$(curl -s -X POST "$SERVER_URL/sql" \
            -H "Content-Type: text/plain" \
            -d "$query" 2>&1)

        end_time=$(python3 -c 'import time; print(int(time.time() * 1000))')
        wall_time_ms=$((end_time - start_time))

        # Parse response
        if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
            error=$(echo "$response" | jq -r '.error')
            echo -e "  Iter $iter: ${RED}ERROR${NC} - $error"
            echo "$query_id,\"$query\",error,0,0,0,$wall_time_ms,$iter" >> "$RESULTS_FILE"
        else
            rows=$(echo "$response" | jq '.rows | length' 2>/dev/null || echo "0")
            total_rows=$(echo "$response" | jq '.total_rows // 0' 2>/dev/null || echo "0")
            exec_time_ns=$(echo "$response" | jq '.execution_time_ns // 0' 2>/dev/null || echo "0")
            exec_time_ms=$(echo "scale=2; $exec_time_ns / 1000000" | bc)

            echo -e "  Iter $iter: ${GREEN}OK${NC} - rows=$rows, total=$total_rows, exec=${exec_time_ms}ms, wall=${wall_time_ms}ms"
            echo "$query_id,\"$query\",ok,$rows,$total_rows,$exec_time_ns,$wall_time_ms,$iter" >> "$RESULTS_FILE"

            total_time_ms=$((total_time_ms + wall_time_ms))
            success_count=$((success_count + 1))
        fi
    done

    if [ $success_count -gt 0 ]; then
        avg_time_ms=$((total_time_ms / success_count))
        echo -e "  Average: ${GREEN}${avg_time_ms}ms${NC}"
    fi

done < "$QUERIES_FILE"

echo ""
echo "========================================"
echo "Benchmark Complete"
echo "========================================"
echo "Results saved to: $RESULTS_FILE"
echo ""

# Summary statistics
echo "Summary:"
echo "--------"
total_queries=$(grep -c "^[0-9]" "$RESULTS_FILE" || echo "0")
success_queries=$(grep -c ",ok," "$RESULTS_FILE" || echo "0")
error_queries=$(grep -c ",error," "$RESULTS_FILE" || echo "0")
echo "Total query executions: $total_queries"
echo "Successful: $success_queries"
echo "Errors: $error_queries"
