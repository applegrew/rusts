#!/bin/bash
#
# EM Benchmark Data Loader for RusTs
#
# Generates EM benchmark data and loads it into a running RusTs server.
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
#   --batch-size N         Lines per /write batch (default: 500)
#   --seed S               Random seed (default: 42)
#   --data-file FILE       Pre-generated data file (skip generation)
#
# Profiles:
#   --profile small        100 devices, 2 hours   (quick test)
#   --profile medium       1000 devices, 24 hours  (realistic)
#   --profile large        10000 devices, 24 hours (stress test)
#   --profile full         50000 devices, 24 hours (production-scale)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
SERVER_URL="http://localhost:8086"
DEVICES=100
APPS_PER_DEVICE=5
WEB_APPS=2
HOURS=2
INTERVAL=300
BATCH_SIZE=500
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

# Generate or use pre-generated data
if [ -n "$DATA_FILE" ] && [ -f "$DATA_FILE" ]; then
    echo -e "Using pre-generated data: ${CYAN}$DATA_FILE${NC}"
    TEMP_FILE="$DATA_FILE"
    CLEANUP_TEMP=false
else
    TEMP_FILE=$(mktemp /tmp/em_data_XXXXXX.lp)
    CLEANUP_TEMP=true

    echo -e "${YELLOW}Generating data...${NC}"
    GEN_START=$(python3 -c 'import time; print(time.time())')

    python3 "$SCRIPT_DIR/generate_em_data.py" \
        --devices "$DEVICES" \
        --apps-per-device "$APPS_PER_DEVICE" \
        --web-apps "$WEB_APPS" \
        --hours "$HOURS" \
        --interval "$INTERVAL" \
        --seed "$SEED" \
        --output "$TEMP_FILE"

    GEN_END=$(python3 -c 'import time; print(time.time())')
    GEN_ELAPSED=$(python3 -c "print(f'{$GEN_END - $GEN_START:.1f}')")
    TOTAL_LINES=$(wc -l < "$TEMP_FILE" | tr -d ' ')
    FILE_SIZE=$(du -h "$TEMP_FILE" | cut -f1)

    echo -e "Generated ${GREEN}$TOTAL_LINES${NC} lines (${FILE_SIZE}) in ${GEN_ELAPSED}s"
fi

TOTAL_LINES=$(wc -l < "$TEMP_FILE" | tr -d ' ')

# Load data in batches
echo ""
echo -e "${YELLOW}Loading data into RusTs...${NC}"
LOAD_START=$(python3 -c 'import time; print(time.time())')

BATCH_NUM=0
LINES_SENT=0
ERRORS=0

while [ "$LINES_SENT" -lt "$TOTAL_LINES" ]; do
    BATCH_NUM=$((BATCH_NUM + 1))
    SKIP=$LINES_SENT
    REMAINING=$((TOTAL_LINES - LINES_SENT))
    TAKE=$((REMAINING < BATCH_SIZE ? REMAINING : BATCH_SIZE))

    # Extract batch using tail+head (portable)
    BATCH=$(tail -n +$((SKIP + 1)) "$TEMP_FILE" | head -n "$TAKE")

    # Send batch
    HTTP_CODE=$(echo "$BATCH" | curl -sf -o /dev/null -w "%{http_code}" \
        -X POST "$SERVER_URL/write" \
        -H "Content-Type: text/plain" \
        --data-binary @- 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ]; then
        LINES_SENT=$((LINES_SENT + TAKE))
        PCT=$((LINES_SENT * 100 / TOTAL_LINES))
        printf "\r  Batch %d: %d/%d lines (%d%%)  " "$BATCH_NUM" "$LINES_SENT" "$TOTAL_LINES" "$PCT"
    else
        ERRORS=$((ERRORS + 1))
        echo -e "\n  ${RED}Batch $BATCH_NUM failed (HTTP $HTTP_CODE)${NC}"
        # Retry once
        sleep 1
        HTTP_CODE=$(echo "$BATCH" | curl -sf -o /dev/null -w "%{http_code}" \
            -X POST "$SERVER_URL/write" \
            -H "Content-Type: text/plain" \
            --data-binary @- 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ]; then
            LINES_SENT=$((LINES_SENT + TAKE))
            echo -e "  ${GREEN}Retry succeeded${NC}"
        else
            echo -e "  ${RED}Retry also failed (HTTP $HTTP_CODE), skipping batch${NC}"
            LINES_SENT=$((LINES_SENT + TAKE))
        fi
    fi
done

echo ""

LOAD_END=$(python3 -c 'import time; print(time.time())')
LOAD_ELAPSED=$(python3 -c "print(f'{$LOAD_END - $LOAD_START:.1f}')")
THROUGHPUT=$(python3 -c "elapsed = $LOAD_END - $LOAD_START; print(f'{$TOTAL_LINES / elapsed:.0f}' if elapsed > 0 else 'N/A')")

# Cleanup
if [ "$CLEANUP_TEMP" = true ]; then
    rm -f "$TEMP_FILE"
fi

echo ""
echo "========================================"
echo -e "${GREEN}Load Complete${NC}"
echo "========================================"
echo "Lines loaded:  $TOTAL_LINES"
echo "Batches:       $BATCH_NUM"
echo "Errors:        $ERRORS"
echo "Load time:     ${LOAD_ELAPSED}s"
echo "Throughput:    ${THROUGHPUT} lines/s"
echo ""
echo "You can now run the benchmark:"
echo "  python3 $SCRIPT_DIR/em_benchmark.py --url $SERVER_URL"
