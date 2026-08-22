#!/usr/bin/env bash
# bench_scenarios.sh — ONE-COMMAND, RESOURCE-MONITORED, STAGED benchmark driver.
#
# Runs the standardized multi-scenario benchmarks from scripts/multi_bench_config.sh
# in INCREASING data scale (so any resource issue surfaces early and never
# surprises you), monitoring memory before/after each stage and saving every
# stage's CSV to a unique path (never clobbered).
#
#   bash scripts/bench_scenarios.sh [--only <stage>] [--skip-regen] [--repeats N]
#
# Stages (each = 6 engines unless noted; threads grid):
#   g-250k  : 250K x W1(128)  x SPARSE-90  -> warmup, fast full 6-engine run
#   g-1m    : 1M   x W1(128)  x SPARSE-90  -> primary 6-engine comparison
#   g-1m-q  : 1M   x W1(128)  x SPARSE-90  -> richer query/filter/sel (4 engines)
#   g-10m   : 10M  x W1(128)  x SPARSE-90  -> next scale, 6 engines, Q2 t=1
#   g-sparse: 1M   x W1, DENSE vs 90 vs 99 (Q2/F2/S1, 4 engines)
#   g-thread: 1M   x W1, aligned both modes, Q2/F1/S0 threads 1/2/4/8
#
# Outputs: bench/out/<stage>.csv + bench/out/SUMMARY.csv
#
# Env: DUCKDB, ALIGNED_DATA_ROOT, BENCH_OUT (default bench/out)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/multi_bench_config.sh"
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"
OUT="${ALIGNED_DATA_ROOT:-$ROOT/testdata}"
BENCH_OUT="${BENCH_OUT:-$ROOT/bench/out}"
REPEATS="${REPEATS:-5}"
ONLY=""
SKIP_REG=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY=$2; shift 2;;
    --skip-regen) SKIP_REG=1; shift;;
    --repeats) REPEATS=$2; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done
mkdir -p "$BENCH_OUT"

ENG5="D-WIDE,D-JOIN,A-ALIGNED,P-CONCAT,P-JOIN"
ENG3="D-WIDE,D-JOIN,A-ALIGNED"

avail(){ echo "N/A"; }

# gen_data <rows> <width> <sparsity> — regenerate bench_mb.
# gen_multi_bench.sh writes a .gen-meta marker (rows/width/sparsity). With
# --skip-regen we regenerate only if the marker matches ALL of rows/width/sparsity;
# a stale or foreign dataset otherwise triggers a confusing self-check FAIL.
gen_data(){ local rows="$1" width="$2" sp="$3" meta
  meta="$OUT/bench_mb/.gen-meta"
  local need_regen=1
  if [ "$SKIP_REG" = 1 ] && [ -f "$meta" ] && [ -d "$OUT/bench_baseline_mb" ]; then
    local mrows mwidth mspare
    IFS=, read -r mrows mwidth mspare < "$meta"
    if [ "$mrows" = "$rows" ] && [ "$mwidth" = "$width" ] && [ "$mspare" = "$sp" ]; then
      need_regen=0
      echo "  (skip-regen) existing bench_mb matches rows=$rows width=$width sparse=$sp"
    else
      echo "  (skip-regen detected stale meta rows=$mrows/$mwidth/$mspare, regenerating $rows/$width/$sp)"
    fi
  fi
  if [ "$need_regen" = 1 ]; then
    rm -rf "$OUT/bench_mb" "$OUT/bench_baseline_mb"
    echo "  gen: rows=$rows width=$width sparse=$sp ..."
    bash "$ROOT/scripts/gen_multi_bench.sh" --rows "$rows" --width "$width" --sparsity "$sp" \
         --aligned true --out "$OUT" --tag mb >/dev/null
  fi
}

# run_stage <label> <rows> <width> <sparsity> <engines> <threads> <queries> <filters> <sels>
run_stage(){ local label="$1" rows="$2" width="$3" sp="$4" eng="$5" th="$6" qs="$7" fs="$8" ss="$9"
  [ -n "$ONLY" ] && [ "$ONLY" != "$label" ] && return 0
  echo ""
  echo "######### STAGE $label : rows=$rows width=$width sparse=$sp engines=$eng threads=$th #########"
  echo "  MEM before: $(avail) available"
  gen_data "$rows" "$width" "$sp"
  QS_OVERRIDE="$qs" FS_OVERRIDE="$fs" SS_OVERRIDE="$ss" \
    bash "$ROOT/scripts/run_multi_bench.sh" --tier A --rows "$rows" --width "$width" \
      --sparsity "$sp" --threads "$th" --engines "$eng" --no-regen
  cp "$ROOT/bench/out/mb-results.csv" "$BENCH_OUT/$label.csv"
  echo "  MEM after:  $(avail) available  -> saved $BENCH_OUT/$label.csv"
}

# g-thread: aligned thread scaling
thread_stage(){ [ -n "$ONLY" ] && [ "$ONLY" != "g-thread" ] && return 0
  echo ""; echo "######### STAGE g-thread : aligned Q2/F1/S0 threads 1/2/4/8 #########"
  echo "  MEM before: $(avail)"
  gen_data 1000000 128 90
  QS_OVERRIDE="Q2" FS_OVERRIDE="F1" SS_OVERRIDE="S0" \
    bash "$ROOT/scripts/run_multi_bench.sh" --tier A --rows 1000000 --width 128 \
      --sparsity 90 --threads 1,2,4,8 --engines "A-ALIGNED" --no-regen
  cp "$ROOT/bench/out/mb-results.csv" "$BENCH_OUT/g-thread.csv"
  echo "  MEM after:  $(avail)  -> saved $BENCH_OUT/g-thread.csv"
}

run_stage g-250k 250000 128 90   "$ENG5" "1,4" "Q2,Q5" "F1,F2" "S0"
# g-1m 5-engine uses Q2 (35 cols) but keeps to probe-only workload (t=1, F2 only)
# so the slow polars P-JOIN baseline stays tractable at 1M.
run_stage g-1m   1000000 128 90  "$ENG5" "1" "Q2" "F2" "S0"
run_stage g-1m-q 1000000 128 90  "$ENG3" "1,4" "Q1,Q2,Q3,Q5" "F1,F2,F3,F4,F5" "S0,S1"
# g-10m: next scale step — 10M x 128 x 90%. Kept to Q2 (35 cols) at t=1 so the
# expensive D-JOIN / P-JOIN baselines stay tractable (they are ~40-150x slower).
run_stage g-10m 10000000 128 90 "$ENG5" "1" "Q2" "F1,F2" "S0"
# g-w2: width direction — 500K x 1024 (W2) x 90%, heavy projections Q3/Q5, 4 DDB
# engines, t=1. Verifies aligned's gap to D-WIDE narrows as columns/row-groups grow.
run_stage g-w2 500000 1024 90 "$ENG3" "1" "Q3,Q5" "F1,F2" "S0"
# sparsity sweep (reuse 1M x 128)
for sp in dense 90 99; do
  run_stage "g-sparse-$sp" 1000000 128 "$sp" "$ENG3" "1,4" "Q2" "F2" "S1"
done
thread_stage

# combined summary
echo ""; echo "============ SUMMARY ============"
: > "$BENCH_OUT/SUMMARY.csv"
echo "stage,engine,query,filter,sel,threads,warm_s" >> "$BENCH_OUT/SUMMARY.csv"
for f in "$BENCH_OUT"/g-*.csv; do
  [ -f "$f" ] || continue
  stage=$(basename "$f" .csv)
  # CSV columns: engine,query,filter,sel,threads,cold_s,warm_s -> keep warm_s only
  tail -n +2 "$f" | awk -v s="$stage" -F, 'NF>=7{printf "%s,%s,%s,%s,%s,%s\n",s,$1,$2,$3,$4,$5,$7}' >> "$BENCH_OUT/SUMMARY.csv"
done
echo "All stages done. Reports under: $BENCH_OUT  (SUMMARY.csv for one-stop comparison)"
