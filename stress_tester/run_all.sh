#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# run_all.sh — Full benchmark collection + analysis
# ============================================================
# Collects data for all scenarios × all profiles, then runs
# the Python analysis to produce a table and HTML report.
#
# Usage:
#   sudo ./run_all.sh                     # full run (all scenarios)
#   sudo ./run_all.sh baseline_latency    # single scenario
#   sudo ./run_all.sh --duration 10s      # override test duration
#   sudo ./run_all.sh --dry-run           # print commands only
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_BIN="${SCRIPT_DIR}/stress_test"
RESULTS_DIR="${SCRIPT_DIR}/results"
REPORT_HTML="${SCRIPT_DIR}/report.html"

SERVER_ADDR="127.0.0.1:8081"
SERVER_BIN=""  # will be set if we use the Go binary

# Default overrides
DURATION=""
WARMUP=""
DRY_RUN=false
BUILD=false
CLEAN_RESULTS=false

# Parse arguments
CLI_SCENARIOS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)      DURATION="$2";  shift 2 ;;
        --warmup)        WARMUP="$2";    shift 2 ;;
        --dry-run)       DRY_RUN=true;   shift   ;;
        --build)         BUILD=true;     shift   ;;
        --clean)         CLEAN_RESULTS=true; shift ;;
        -h|--help)
            sed -n '3,14p' "$0"
            exit 0 ;;
        *)
            CLI_SCENARIOS+=("$1")
            shift ;;
    esac
done

# Build if requested or if binary doesn't exist
if $BUILD || [ ! -x "$BENCH_BIN" ]; then
    echo ":: building stress_test ..."
    (cd "$SCRIPT_DIR" && go build -o "$BENCH_BIN" .)
fi

if $CLEAN_RESULTS; then
    echo ":: cleaning $RESULTS_DIR ..."
    rm -f "$RESULTS_DIR"/bench_*.json "$RESULTS_DIR"/bench_*.csv
fi

# Scenarios and their default profiles
declare -A SCENARIO_PROFILES
SCENARIO_PROFILES[baseline_latency]="clean,wifi,lte"
SCENARIO_PROFILES[concurrency_stress]="clean,wifi,lte,bad_lte"
SCENARIO_PROFILES[multiplex_stress]="clean,lte,bad_lte"
SCENARIO_PROFILES[loss_sensitivity]="clean,wifi,lte,bad_lte,extreme"
SCENARIO_PROFILES[rtt_scaling]="clean,wifi,lte,bad_lte,extreme"

SCENARIO_ORDER=(baseline_latency concurrency_stress multiplex_stress loss_sensitivity rtt_scaling)

# If specific scenarios were requested, use those
if [ ${#CLI_SCENARIOS[@]} -gt 0 ]; then
    SCENARIO_ORDER=()
    for s in "${CLI_SCENARIOS[@]}"; do
        if [[ -v SCENARIO_PROFILES[$s] ]]; then
            SCENARIO_ORDER+=("$s")
        else
            echo ":: unknown scenario: $s"
            echo "   available: ${!SCENARIO_PROFILES[*]}"
            exit 1
        fi
    done
fi

# Build common args
COMMON_ARGS=()
COMMON_ARGS+=("-addr" "$SERVER_ADDR")
[ -n "$DURATION" ] && COMMON_ARGS+=("-duration" "$DURATION")
[ -n "$WARMUP" ]   && COMMON_ARGS+=("-warmup"   "$WARMUP")

echo "=============================================="
echo " qRPC Full Benchmark Collection"
echo " scenarios : ${SCENARIO_ORDER[*]}"
echo " output    : $RESULTS_DIR"
echo " dry-run   : $DRY_RUN"
echo "=============================================="
echo ""

# Clean netem rules before starting
echo ":: cleaning any leftover netem rules ..."
$DRY_RUN || sudo tc qdisc del dev lo root 2>/dev/null || true

# Start server in background
echo ":: starting benchmark server on $SERVER_ADDR ..."
if ! $DRY_RUN; then
    sudo "$BENCH_BIN" server -addr "$SERVER_ADDR" &
    SERVER_PID=$!
    # Wait for server to be ready
    sleep 2
    # Verify it's running
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo ":: ERROR: server failed to start"
        exit 1
    fi
    echo ":: server PID=$SERVER_PID"
fi

# Ensure server + netem cleanup on exit
cleanup() {
    local rc=$?
    echo ""
    echo ":: cleanup ..."
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    sudo tc qdisc del dev lo root 2>/dev/null || true
    echo ":: done (exit=$rc)"
}
$DRY_RUN || trap cleanup EXIT INT TERM

# Run each scenario
TOTAL=$(( ${#SCENARIO_ORDER[@]} ))
IDX=0

for scenario in "${SCENARIO_ORDER[@]}"; do
    profiles="${SCENARIO_PROFILES[$scenario]}"
    IDX=$(( IDX + 1 ))

    echo ""
    echo "=============================================="
    echo " [$IDX/$TOTAL] scenario: $scenario"
    echo " profiles  : $profiles"
    echo "=============================================="

    RUN_ARGS=("run" "-scenario" "$scenario" "-profile" "$profiles")
    RUN_ARGS+=("${COMMON_ARGS[@]}")
    RUN_ARGS+=("-output" "$RESULTS_DIR")

    # Apply netem for first profile only — the runner applies profiles sequentially
    # and cleans up between them. Just run without pre-applied netem; the tool
    # handles it internally.

    if $DRY_RUN; then
        echo "   >>> sudo $BENCH_BIN ${RUN_ARGS[*]}"
    else
        echo "   >>> $BENCH_BIN ${RUN_ARGS[*]}"
        sudo -E "$BENCH_BIN" "${RUN_ARGS[@]}" 2>&1 | tail -20 || {
            echo "   :: WARNING: scenario '$scenario' failed (exit=$?)"
        }
    fi
done

echo ""
echo "=============================================="
echo " Collection complete — running analysis ..."
echo "=============================================="

if ! $DRY_RUN; then
    python3 "$SCRIPT_DIR/analyze.py" "$RESULTS_DIR" --html "$REPORT_HTML"
    echo ""
    echo " Report     : $REPORT_HTML"
    echo " Results in : $RESULTS_DIR/"
    echo " To view:  xdg-open $REPORT_HTML"
fi
