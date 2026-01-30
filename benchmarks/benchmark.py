#!/usr/bin/env python3
"""
RusTs Benchmark Runner

Runs SQL queries against RusTs server and measures performance.

Usage:
    python3 benchmarks/benchmark.py [--url URL] [--iterations N] [--queries FILE]

Example:
    python3 benchmarks/benchmark.py --iterations 5
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class QueryResult:
    query_id: int
    query: str
    success: bool
    error: Optional[str]
    rows: int
    total_rows: int
    exec_time_ns: int
    wall_time_ms: int
    iteration: int


def check_server(url: str) -> bool:
    """Check if server is healthy."""
    try:
        req = urllib.request.Request(f"{url}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def run_query(url: str, query: str) -> Tuple[dict, int]:
    """Execute a query and return (response, wall_time_ms)."""
    start = time.time()

    try:
        # API expects JSON with {"query": "..."}
        payload = json.dumps({"query": query})
        data = payload.encode('utf-8')
        req = urllib.request.Request(
            f"{url}/sql",
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=300) as resp:
            response = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        response = {"error": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        response = {"error": str(e)}

    wall_time_ms = int((time.time() - start) * 1000)
    return response, wall_time_ms


def load_queries(filepath: str) -> List[str]:
    """Load queries from SQL file, skipping comments and empty lines."""
    queries = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if line.startswith('--') or not line:
                continue
            # Only include SELECT statements
            if line.upper().startswith('SELECT'):
                queries.append(line)
    return queries


def format_time(ns: int) -> str:
    """Format nanoseconds as human-readable string."""
    if ns < 1_000:
        return f"{ns}ns"
    elif ns < 1_000_000:
        return f"{ns/1_000:.2f}µs"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f}ms"
    else:
        return f"{ns/1_000_000_000:.2f}s"


def main():
    parser = argparse.ArgumentParser(description='RusTs Benchmark Runner')
    parser.add_argument('--url', default='http://localhost:8086',
                        help='Server URL (default: http://localhost:8086)')
    parser.add_argument('--iterations', '-n', type=int, default=3,
                        help='Number of iterations per query (default: 3)')
    parser.add_argument('--queries', '-q', default=None,
                        help='Path to queries file (default: benchmarks/trips_queries.sql)')
    parser.add_argument('--output', '-o', default=None,
                        help='Output CSV file (default: benchmarks/benchmark_results.csv)')
    parser.add_argument('--filter', '-f', default=None,
                        help='Only run queries matching this pattern')
    parser.add_argument('--quiet', action='store_true',
                        help='Only show summary')

    args = parser.parse_args()

    # Determine paths
    script_dir = Path(__file__).parent
    queries_file = args.queries or script_dir / 'trips_queries.sql'
    output_file = args.output or script_dir / 'benchmark_results.csv'

    print("=" * 60)
    print("RusTs Benchmark Suite")
    print("=" * 60)
    print(f"Server:      {args.url}")
    print(f"Iterations:  {args.iterations}")
    print(f"Queries:     {queries_file}")
    print()

    # Check server
    print("Checking server health... ", end='', flush=True)
    if check_server(args.url):
        print("\033[92mOK\033[0m")
    else:
        print("\033[91mFAILED\033[0m")
        print(f"Error: Cannot connect to server at {args.url}")
        sys.exit(1)

    # Load queries
    queries = load_queries(str(queries_file))
    print(f"Loaded {len(queries)} queries")
    print()

    # Filter queries if requested
    if args.filter:
        pattern = re.compile(args.filter, re.IGNORECASE)
        queries = [q for q in queries if pattern.search(q)]
        print(f"Filtered to {len(queries)} queries matching '{args.filter}'")

    # Run benchmark
    results: List[QueryResult] = []
    total_success = 0
    total_error = 0

    for query_id, query in enumerate(queries, 1):
        if not args.quiet:
            print()
            print(f"\033[93mQ{query_id}:\033[0m {query[:80]}{'...' if len(query) > 80 else ''}")
            print("-" * 60)

        query_times = []

        for iteration in range(1, args.iterations + 1):
            response, wall_time_ms = run_query(args.url, query)

            if 'error' in response:
                result = QueryResult(
                    query_id=query_id,
                    query=query,
                    success=False,
                    error=response['error'],
                    rows=0,
                    total_rows=0,
                    exec_time_ns=0,
                    wall_time_ms=wall_time_ms,
                    iteration=iteration
                )
                if not args.quiet:
                    print(f"  Iter {iteration}: \033[91mERROR\033[0m - {response['error'][:60]}")
                total_error += 1
            else:
                # API returns 'results' for aggregations, 'rows' for selects
                rows = len(response.get('results', response.get('rows', [])))
                total_rows = response.get('total_rows', 0)
                exec_time_ns = response.get('execution_time_ns', 0)

                result = QueryResult(
                    query_id=query_id,
                    query=query,
                    success=True,
                    error=None,
                    rows=rows,
                    total_rows=total_rows,
                    exec_time_ns=exec_time_ns,
                    wall_time_ms=wall_time_ms,
                    iteration=iteration
                )

                query_times.append(wall_time_ms)
                total_success += 1

                if not args.quiet:
                    print(f"  Iter {iteration}: \033[92mOK\033[0m - "
                          f"rows={rows}, total={total_rows}, "
                          f"exec={format_time(exec_time_ns)}, wall={wall_time_ms}ms")

            results.append(result)

        if query_times and not args.quiet:
            avg_ms = sum(query_times) / len(query_times)
            min_ms = min(query_times)
            max_ms = max(query_times)
            print(f"  Stats: avg={avg_ms:.0f}ms, min={min_ms}ms, max={max_ms}ms")

    # Write results to CSV
    with open(output_file, 'w') as f:
        f.write("query_id,query,status,rows,total_rows,exec_time_ns,wall_time_ms,iteration\n")
        for r in results:
            status = "ok" if r.success else "error"
            # Escape quotes in query
            query_escaped = r.query.replace('"', '""')
            f.write(f'{r.query_id},"{query_escaped}",{status},'
                    f'{r.rows},{r.total_rows},{r.exec_time_ns},{r.wall_time_ms},{r.iteration}\n')

    # Print summary
    print()
    print("=" * 60)
    print("Benchmark Complete")
    print("=" * 60)
    print(f"Results saved to: {output_file}")
    print()
    print("Summary:")
    print("-" * 20)
    print(f"Total executions: {len(results)}")
    print(f"Successful:       {total_success}")
    print(f"Errors:           {total_error}")

    # Calculate aggregate stats
    success_results = [r for r in results if r.success]
    if success_results:
        total_wall_time = sum(r.wall_time_ms for r in success_results)
        total_exec_time = sum(r.exec_time_ns for r in success_results)
        avg_wall_time = total_wall_time / len(success_results)
        print()
        print(f"Total wall time:  {total_wall_time}ms ({total_wall_time/1000:.1f}s)")
        print(f"Avg wall time:    {avg_wall_time:.1f}ms")
        print(f"Total exec time:  {format_time(total_exec_time)}")

    return 0 if total_error == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
