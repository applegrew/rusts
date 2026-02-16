#!/bin/bash
#
# EM Benchmark Data Loader for RusTs
#
# Generates EM benchmark data and streams it directly into a running
# RusTs server.  No temporary files are written — the generator pipes
# into the batch uploader so disk usage is near-zero.
#
# Usage:
#   ./benchmarks/em/load_em_data.sh [OPTIONS]
#
# Options:
#   --url URL              RusTs server URL (default: http://localhost:8086)
#   --devices N            Number of devices (default: 100)
#   --apps-per-device N    Installed apps per device (default: 5)
#   --web-apps N           Web apps per device (default: 2)
#   --hours H              Hours of data (default: 2)
#   --interval S           Seconds between points (default: 300)
#   --batch-size N         Lines per /write batch (default: 5000)
#   --seed S               Random seed (default: 42)
#   --data-file FILE       Pre-generated data file (skip generation)
#
# Profiles:
#   --profile small        100 devices, 2 hours   (quick test)
#   --profile medium       1000 devices, 24 hours  (realistic)
#   --profile large        10000 devices, 24 hours (stress test)
#   --profile full         50000 devices, 24 hours (production-scale)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
SERVER_URL="http://localhost:8086"
DEVICES=100
APPS_PER_DEVICE=5
WEB_APPS=2
HOURS=2
INTERVAL=300
BATCH_SIZE=5000
SEED=42
DATA_FILE=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --url) SERVER_URL="$2"; shift 2 ;;
        --devices) DEVICES="$2"; shift 2 ;;
        --apps-per-device) APPS_PER_DEVICE="$2"; shift 2 ;;
        --web-apps) WEB_APPS="$2"; shift 2 ;;
        --hours) HOURS="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --batch-size) BATCH_SIZE="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --data-file) DATA_FILE="$2"; shift 2 ;;
        --profile)
            case $2 in
                small)  DEVICES=100;   HOURS=2;  ;;
                medium) DEVICES=1000;  HOURS=24; ;;
                large)  DEVICES=10000; HOURS=24; ;;
                full)   DEVICES=50000; HOURS=24; ;;
                *)
                    echo -e "${RED}Unknown profile: $2${NC}"
                    echo "Available: small, medium, large, full"
                    exit 1
                    ;;
            esac
            shift 2
            ;;
        -h|--help)
            head -30 "$0" | tail -28
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo "========================================"
echo -e "${CYAN}EM Benchmark Data Loader${NC}"
echo "========================================"
echo "Server:          $SERVER_URL"
echo "Devices:         $DEVICES"
echo "Apps/device:     $APPS_PER_DEVICE"
echo "Web apps/device: $WEB_APPS"
echo "Hours:           $HOURS"
echo "Interval:        ${INTERVAL}s"
echo "Batch size:      $BATCH_SIZE"
echo ""

# Check server health
echo -n "Checking server health... "
if curl -sf "$SERVER_URL/health" > /dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Error: Cannot connect to server at $SERVER_URL"
    exit 1
fi

# Build the data source command — either cat a file or run the generator.
# In both cases the command writes line-protocol to stdout.
if [ -n "$DATA_FILE" ] && [ -f "$DATA_FILE" ]; then
    echo -e "Using pre-generated data: ${CYAN}$DATA_FILE${NC}"
    DATA_CMD="cat '$DATA_FILE'"
else
    echo -e "${YELLOW}Generating + streaming data...${NC}"
    DATA_CMD="python3 '$SCRIPT_DIR/generate_em_data.py' \
        --devices $DEVICES \
        --apps-per-device $APPS_PER_DEVICE \
        --web-apps $WEB_APPS \
        --hours $HOURS \
        --interval $INTERVAL \
        --seed $SEED \
        --output -"
fi

echo ""
echo -e "${YELLOW}Loading data into RusTs...${NC}"

# Pipe the data source into a Python streaming uploader.
# The uploader reads stdin line-by-line, batches BATCH_SIZE lines in
# memory, and POSTs each batch to /write.  Zero temp files, O(n) time,
# constant disk usage.
# Note: we use python3 -c instead of a heredoc so that stdin is free
# for the pipe from the data source.
eval "$DATA_CMD" | python3 -u -c '
import sys, time, urllib.request, urllib.error

batch_size = int(sys.argv[1])
write_url  = f"{sys.argv[2]}/write"

batch_num  = 0
lines_sent = 0
errors     = 0
buf        = []
t_start    = time.monotonic()

def send(payload_bytes):
    """POST payload; retry once on failure.  Returns True on success."""
    req = urllib.request.Request(
        write_url, data=payload_bytes,
        headers={"Content-Type": "text/plain"},
        method="POST",
    )
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                if resp.status in (200, 204):
                    return True
        except urllib.error.HTTPError:
            pass
        except Exception:
            pass
        if attempt == 0:
            time.sleep(1)
    return False

for line in sys.stdin:
    buf.append(line)
    if len(buf) >= batch_size:
        batch_num += 1
        lines_sent += len(buf)
        if send("".join(buf).encode()):
            elapsed = time.monotonic() - t_start
            rate = int(lines_sent / elapsed) if elapsed > 0 else 0
            print(f"\r  Batch {batch_num}: {lines_sent:,} lines loaded  ({rate:,} lines/s)  ", end="", flush=True)
        else:
            errors += 1
            print(f"\n  Batch {batch_num} FAILED", flush=True)
        buf.clear()

if buf:
    batch_num += 1
    lines_sent += len(buf)
    if not send("".join(buf).encode()):
        errors += 1

elapsed = time.monotonic() - t_start
rate = int(lines_sent / elapsed) if elapsed > 0 else 0

print(flush=True)
print(flush=True)
print("========================================", flush=True)
print("Load Complete", flush=True)
print("========================================", flush=True)
print(f"Lines loaded:  {lines_sent:,}", flush=True)
print(f"Batches:       {batch_num:,}", flush=True)
print(f"Errors:        {errors}", flush=True)
print(f"Load time:     {elapsed:.1f}s", flush=True)
print(f"Throughput:    {rate:,} lines/s", flush=True)
' "$BATCH_SIZE" "$SERVER_URL"

echo ""
echo "You can now run the benchmark:"
echo "  python3 $SCRIPT_DIR/em_benchmark.py --url $SERVER_URL"
