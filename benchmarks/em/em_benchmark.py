#!/usr/bin/env python3
"""
EM Benchmark Runner for RusTs

Runs the EM query suite against a RusTs server, collects latency metrics,
and produces a comparison report against known DB R / DB C baselines.

Usage:
  python3 em_benchmark.py [OPTIONS]

  --url URL            RusTs server URL (default: http://localhost:8086)
  --iterations N       Iterations per query (default: 5)
  --queries FILE       SQL queries file (default: em_queries.sql)
  --output-csv FILE    Raw results CSV (default: em_results.csv)
  --output-md FILE     Markdown report (default: em_report.md)
  --warmup N           Warmup iterations (not counted) (default: 1)
  --timeout S          Per-query timeout in seconds (default: 30)
"""

import argparse
import csv
import json
import math
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request

# ── Query parsing ──────────────────────────────────────────────────────────

# Annotation format: -- ID | Name | Category | DB R_ms | DB C_ms
ANNOTATION_RE = re.compile(
    r"^--\s+(\w+)\s+\|\s+(.+?)\s+\|\s+(\w+)\s+\|\s+(\d+)\s+\|\s+(\d+)\s*$"
)


def parse_queries(path: str) -> list:
    """Parse em_queries.sql into a list of query dicts."""
    queries = []
    current_annotation = None

    with open(path, "r") as f:
        for line in f:
            line = line.rstrip("\n")

            # Try to match annotation
            m = ANNOTATION_RE.match(line)
            if m:
                current_annotation = {
                    "id": m.group(1),
                    "name": m.group(2),
                    "category": m.group(3),
                    "db_r_ms": int(m.group(4)),
                    "db_c_ms": int(m.group(5)),
                }
                continue

            # Skip other comments and blank lines
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue

            # This is a SQL line
            if current_annotation:
                queries.append({
                    **current_annotation,
                    "sql": stripped,
                })
                current_annotation = None

    return queries


# ── HTTP helpers ───────────────────────────────────────────────────────────

def check_health(url: str) -> bool:
    """Check if the RusTs server is healthy."""
    try:
        req = urllib.request.Request(f"{url}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def execute_query(url: str, sql: str, timeout: float) -> dict:
    """Execute a SQL query via the /sql endpoint and return timing + result info."""
    start = time.monotonic()
    result = {
        "wall_ms": 0.0,
        "server_ms": 0.0,
        "rows": 0,
        "total_rows": 0,
        "status": "ok",
        "error": "",
    }

    try:
        data = sql.encode("utf-8")
        req = urllib.request.Request(
            f"{url}/sql",
            data=data,
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            result["wall_ms"] = (time.monotonic() - start) * 1000
            result["server_ms"] = body.get("execution_time_ms", 0.0)
            result["rows"] = len(body.get("results", []))
            result["total_rows"] = body.get("total_rows", 0)
    except urllib.error.HTTPError as e:
        result["wall_ms"] = (time.monotonic() - start) * 1000
        result["status"] = "error"
        try:
            err_body = json.loads(e.read().decode("utf-8"))
            result["error"] = err_body.get("error", str(e))
        except Exception:
            result["error"] = str(e)
    except Exception as e:
        result["wall_ms"] = (time.monotonic() - start) * 1000
        result["status"] = "error"
        result["error"] = str(e)

    return result


# ── Statistics ─────────────────────────────────────────────────────────────

def percentile(data: list, p: float) -> float:
    """Calculate the p-th percentile (0-100) of a sorted list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    return data[f] * (c - k) + data[c] * (k - f)


def compute_stats(times: list) -> dict:
    """Compute latency statistics from a list of wall times."""
    if not times:
        return {"min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}
    s = sorted(times)
    return {
        "min": s[0],
        "max": s[-1],
        "avg": statistics.mean(s),
        "p50": percentile(s, 50),
        "p95": percentile(s, 95),
        "p99": percentile(s, 99),
    }


# ── Report generation ─────────────────────────────────────────────────────

def write_csv(results: list, path: str):
    """Write raw results to CSV."""
    fieldnames = [
        "query_id", "query_name", "category", "iteration",
        "wall_ms", "server_ms", "rows", "total_rows",
        "status", "error", "db_r_ms", "db_c_ms",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)


def speedup_str(rusts_ms: float, baseline_ms: int) -> str:
    """Format speedup ratio."""
    if baseline_ms <= 0 or rusts_ms <= 0:
        return "—"
    ratio = baseline_ms / rusts_ms
    return f"{ratio:.1f}x"


def write_markdown_report(queries: list, all_results: list, path: str,
                          iterations: int):
    """Generate a Markdown comparison report."""
    # Group results by query ID
    by_query = {}
    for r in all_results:
        qid = r["query_id"]
        if qid not in by_query:
            by_query[qid] = []
        by_query[qid].append(r)

    # Compute per-query stats
    query_stats = []
    for q in queries:
        qid = q["id"]
        runs = by_query.get(qid, [])
        ok_runs = [r for r in runs if r["status"] == "ok"]
        wall_times = [r["wall_ms"] for r in ok_runs]
        stats = compute_stats(wall_times)
        error_count = len(runs) - len(ok_runs)
        query_stats.append({
            **q,
            **stats,
            "error_count": error_count,
            "ok_count": len(ok_runs),
        })

    # Summary metrics
    total_queries = len(queries)
    passing_p95 = sum(1 for qs in query_stats if qs["p95"] > 0 and qs["p95"] < 2000)
    failing_p95 = sum(1 for qs in query_stats if qs["p95"] >= 2000)
    errored = sum(1 for qs in query_stats if qs["error_count"] > 0)

    # Geometric mean speedup vs DB R (only for queries with DB R baseline)
    db_r_speedups = []
    for qs in query_stats:
        if qs["db_r_ms"] > 0 and qs["p50"] > 0:
            db_r_speedups.append(qs["db_r_ms"] / qs["p50"])
    geo_mean_db_r = (
        math.exp(sum(math.log(s) for s in db_r_speedups) / len(db_r_speedups))
        if db_r_speedups else 0
    )

    # Geometric mean speedup vs DB C
    db_c_speedups = []
    for qs in query_stats:
        if qs["db_c_ms"] > 0 and qs["p50"] > 0:
            db_c_speedups.append(qs["db_c_ms"] / qs["p50"])
    geo_mean_db_c = (
        math.exp(sum(math.log(s) for s in db_c_speedups) / len(db_c_speedups))
        if db_c_speedups else 0
    )

    with open(path, "w") as f:
        f.write("# EM Benchmark Report — RusTs\n\n")
        f.write(f"- **Iterations per query**: {iterations}\n")
        f.write(f"- **Total queries**: {total_queries}\n")
        f.write(f"- **P95 < 2s (pass)**: {passing_p95}\n")
        f.write(f"- **P95 >= 2s (fail)**: {failing_p95}\n")
        f.write(f"- **Queries with errors**: {errored}\n")
        if geo_mean_db_r > 0:
            f.write(f"- **Geometric mean speedup vs DB R**: {geo_mean_db_r:.1f}x\n")
        if geo_mean_db_c > 0:
            f.write(f"- **Geometric mean speedup vs DB C**: {geo_mean_db_c:.1f}x\n")
        f.write("\n")

        # Category breakdown
        categories = {}
        for qs in query_stats:
            cat = qs["category"]
            if cat not in categories:
                categories[cat] = {"count": 0, "pass": 0, "fail": 0, "error": 0}
            categories[cat]["count"] += 1
            if qs["p95"] > 0 and qs["p95"] < 2000:
                categories[cat]["pass"] += 1
            elif qs["p95"] >= 2000:
                categories[cat]["fail"] += 1
            if qs["error_count"] > 0:
                categories[cat]["error"] += 1

        f.write("## Category Summary\n\n")
        f.write("| Category | Queries | P95<2s | P95>=2s | Errors |\n")
        f.write("|----------|--------:|-------:|--------:|-------:|\n")
        for cat in sorted(categories.keys()):
            c = categories[cat]
            f.write(f"| {cat} | {c['count']} | {c['pass']} | {c['fail']} | {c['error']} |\n")
        f.write("\n")

        # Detailed results table
        f.write("## Detailed Results\n\n")
        f.write("| ID | Name | Cat | RusTs P50 | RusTs P95 | RusTs P99 | DB R ms | DB C ms | vs DB R | vs DB C | Pass |\n")
        f.write("|----|------|-----|----------:|----------:|----------:|----------:|----------:|----------:|----------:|:----:|\n")

        for qs in query_stats:
            p50_s = f"{qs['p50']:.0f}" if qs["ok_count"] > 0 else "ERR"
            p95_s = f"{qs['p95']:.0f}" if qs["ok_count"] > 0 else "ERR"
            p99_s = f"{qs['p99']:.0f}" if qs["ok_count"] > 0 else "ERR"
            db_r_s = str(qs["db_r_ms"]) if qs["db_r_ms"] > 0 else "—"
            db_c_s = str(qs["db_c_ms"]) if qs["db_c_ms"] > 0 else "—"
            vs_r = speedup_str(qs["p50"], qs["db_r_ms"])
            vs_c = speedup_str(qs["p50"], qs["db_c_ms"])
            passed = "✅" if qs["ok_count"] > 0 and qs["p95"] < 2000 else "❌"

            f.write(
                f"| {qs['id']} | {qs['name'][:40]} | {qs['category']} "
                f"| {p50_s} | {p95_s} | {p99_s} "
                f"| {db_r_s} | {db_c_s} "
                f"| {vs_r} | {vs_c} | {passed} |\n"
            )

        f.write("\n")

        # Error details
        errored_queries = [qs for qs in query_stats if qs["error_count"] > 0]
        if errored_queries:
            f.write("## Errors\n\n")
            for qs in errored_queries:
                qid = qs["id"]
                errors = [r for r in by_query.get(qid, []) if r["status"] == "error"]
                if errors:
                    f.write(f"### {qid}: {qs['name']}\n")
                    f.write(f"```\n{errors[0]['error']}\n```\n\n")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EM Benchmark Runner for RusTs")
    parser.add_argument("--url", default="http://localhost:8086",
                        help="RusTs server URL")
    parser.add_argument("--iterations", type=int, default=5,
                        help="Iterations per query (default: 5)")
    parser.add_argument("--queries", default=None,
                        help="SQL queries file (default: em_queries.sql)")
    parser.add_argument("--output-csv", default=None,
                        help="Raw results CSV (default: em_results.csv)")
    parser.add_argument("--output-md", default=None,
                        help="Markdown report (default: em_report.md)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="Warmup iterations (default: 1)")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="Per-query timeout in seconds (default: 30)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    queries_file = args.queries or os.path.join(script_dir, "em_queries.sql")
    csv_file = args.output_csv or os.path.join(script_dir, "em_results.csv")
    md_file = args.output_md or os.path.join(script_dir, "em_report.md")

    # Parse queries
    queries = parse_queries(queries_file)
    if not queries:
        print("ERROR: No queries found in", queries_file, file=sys.stderr)
        sys.exit(1)

    print(f"EM Benchmark Runner")
    print(f"  Server:     {args.url}")
    print(f"  Queries:    {len(queries)}")
    print(f"  Iterations: {args.iterations}")
    print(f"  Warmup:     {args.warmup}")
    print(f"  Timeout:    {args.timeout}s")
    print()

    # Health check
    if not check_health(args.url):
        print(f"ERROR: Cannot connect to {args.url}/health", file=sys.stderr)
        sys.exit(1)
    print("Server health: OK")
    print()

    all_results = []

    for qi, q in enumerate(queries, 1):
        print(f"[{qi}/{len(queries)}] {q['id']}: {q['name']}")

        # Warmup
        for w in range(args.warmup):
            execute_query(args.url, q["sql"], args.timeout)

        # Measured iterations
        for it in range(1, args.iterations + 1):
            r = execute_query(args.url, q["sql"], args.timeout)
            status_icon = "✓" if r["status"] == "ok" else "✗"
            print(f"  iter {it}: {status_icon}  wall={r['wall_ms']:.0f}ms  "
                  f"server={r['server_ms']:.1f}ms  rows={r['total_rows']}")

            all_results.append({
                "query_id": q["id"],
                "query_name": q["name"],
                "category": q["category"],
                "iteration": it,
                "wall_ms": round(r["wall_ms"], 2),
                "server_ms": round(r["server_ms"], 2),
                "rows": r["rows"],
                "total_rows": r["total_rows"],
                "status": r["status"],
                "error": r["error"],
                "db_r_ms": q["db_r_ms"],
                "db_c_ms": q["db_c_ms"],
            })

    # Write outputs
    write_csv(all_results, csv_file)
    print(f"\nCSV results written to: {csv_file}")

    write_markdown_report(queries, all_results, md_file, args.iterations)
    print(f"Markdown report written to: {md_file}")

    # Print quick summary
    ok_results = [r for r in all_results if r["status"] == "ok"]
    err_results = [r for r in all_results if r["status"] == "error"]
    print(f"\nSummary: {len(ok_results)} OK, {len(err_results)} errors "
          f"out of {len(all_results)} total executions")


if __name__ == "__main__":
    main()
