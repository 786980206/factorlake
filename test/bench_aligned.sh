#!/usr/bin/env bash
# bench_aligned.sh 鈥?Linux equivalent of bench_aligned.ps1
# Phase 6 benchmark: aligned_scan vs plain-parquet JOIN vs single wide parquet.
# Dataset: bench_ixday (configurable rows, default 1M) x 127 cols, 4 daily
# partitions, sparse factors. Dimensions: projection 5/25/120 cols,
# scan 25%/100%, threads 1/4/8.
#
# This script also runs a partition-pruning SELF-CHECK before measuring:
# it asserts that a date='2026-09-02' scan (a NON-first partition) returns
# exactly rowsPerDay rows. This guards the pruning/cursor logic that previously
# crashed on any non-first partition (see AGENTS.md "Linux 杩佺Щ + Bug 淇").
#
# Usage: bash test/bench_aligned.sh [TOTAL_ROWS] [DATA_ROOT]
#   TOTAL_ROWS default 1000000 (must be divisible by 4)
#   DATA_ROOT  default $ROOT/testdata
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"
DATA_ROOT="${2:-$ROOT/testdata}"
TOTAL="${1:-1000000}"
THREADS=(1 4 8)
WORKLOADS=(p5 p25 p100 s25 s100)

if ! [[ "$TOTAL" =~ ^[0-9]+$ ]] || [ $((TOTAL % 4)) -ne 0 ]; then
  echo "TOTAL_ROWS must be a positive integer divisible by 4" >&2
  exit 1
fi

# ---- Locate / generate the bench table ---------------------------------------
BENCH="$DATA_ROOT/bench_ixday"
if [ ! -d "$BENCH/index" ]; then
  echo "Generating aligned bench data ($TOTAL rows)..."
  DUCKDB="$DUCKDB" DATA_ROOT="$DATA_ROOT" bash "$ROOT/test/gen_bench.sh" "$TOTAL"
fi

ROWS_PER_DAY=$((TOTAL / 4))
DATE_FILTER="DATE '2026-09-01'"

# ---- Baseline parquet files (wide / join) ------------------------------------
echo "Generating baseline parquet files (wide/join)..."
BASELINE="$DATA_ROOT/bench_baseline"
mkdir -p "$BASELINE"
JOIN_INDEX="$BASELINE/join_index.parquet"
JOIN_ALPHA="$BASELINE/join_alpha.parquet"
JOIN_MA="$BASELINE/join_ma.parquet"
WIDE="$BASELINE/wide.parquet"

index_list="printf('%06d', r + 1) AS symbol, DATE '2026-09-01' + (r // $ROWS_PER_DAY)::INT AS date, CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid"
alpha_list="CAST(r AS BIGINT) AS rowid_alpha"
for n in $(seq 0 99); do
  alpha_list+=", CASE WHEN r % 7 = 0 THEN CAST((r + $((n + 1))) * 0.001 AS DOUBLE) ELSE NULL END AS alpha$(printf '%03d' "$n")"
done
ma_list="CAST(r AS BIGINT) AS rowid_ma"
for n in $(seq 0 19); do
  ma_list+=", CAST((r % $((n + 11))) * 0.0001 AS DOUBLE) AS ma$(printf '%03d' "$n")"
done

gen_baseline() { # file select_list
  local f="$1" sel="$2"
  if [ ! -f "$f" ]; then
    "$DUCKDB" -light-mode -c "COPY (WITH r AS (SELECT range AS r FROM range(0, $TOTAL)) SELECT $sel FROM r) TO '$f' (FORMAT PARQUET, COMPRESSION ZSTD);" >/dev/null
  fi
}
gen_baseline "$JOIN_INDEX" "$index_list"
gen_baseline "$JOIN_ALPHA" "$alpha_list"
gen_baseline "$JOIN_MA" "$ma_list"
gen_baseline "$WIDE" "$index_list, $alpha_list, $ma_list"

# ---- Partition-pruning self-check (guards the bug fixed in aligned_scan.cpp) --
echo "Partition-pruning self-check (non-first partition) ..."
PRUNE_CHECK=$("$DUCKDB" -light-mode -csv -noheader -c "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_scan('bench_ixday') WHERE date = DATE '2026-09-02';" 2>&1 | tail -1)
if [ "$PRUNE_CHECK" != "$ROWS_PER_DAY" ]; then
  echo "FAIL: partition pruning self-check: expected $ROWS_PER_DAY, got '$PRUNE_CHECK'" >&2
  exit 1
fi
echo "PASS: date='2026-09-02' prunes to $PRUNE_CHECK rows (expected $ROWS_PER_DAY)"

# ---- Query templates -----------------------------------------------------------
# Build comma-separated column lists. IMPORTANT: the accumulated value must be
# emitted by a final command that always returns 0 (e.g. `printf`). If the last
# command were the `[ $n -lt N ] && printf ','` conditional, it would fail on the
# last iteration, and under `set -e` a command-substitution of these functions
# would abort the whole script.
alpha5()  { local n out=""; for n in $(seq 0 4);  do out+="alpha$(printf '%03d' "$n")"; [ $n -lt 4 ] && out+=","; done; printf '%s' "$out"; }
alpha25() { local n out=""; for n in $(seq 0 24); do out+="alpha$(printf '%03d' "$n")"; [ $n -lt 24 ] && out+=","; done; printf '%s' "$out"; }
alpha100() { local n out=""; for n in $(seq 0 99); do out+="alpha$(printf '%03d' "$n")"; [ $n -lt 99 ] && out+=","; done; printf '%s' "$out"; }
ma20()    { local n out=""; for n in $(seq 0 19); do out+="ma$(printf '%03d' "$n")"; [ $n -lt 19 ] && out+=","; done; printf '%s' "$out"; }
count_list() { local s="$1" out="" n; IFS=',' read -ra cols <<< "$s"; for i in "${!cols[@]}"; do [ $i -gt 0 ] && out+=", "; out+="count(${cols[$i]})"; done; echo "$out"; }

A5=$(alpha5)
A25=$(alpha25)
A100=$(alpha100)
M20=$(ma20)
AGG5=$(count_list "$A5")
AGG25=$(count_list "$A25")
AGG100=$(count_list "$A100,$M20")

ALIGNED_PRELUDE="SET aligned_data_root='$DATA_ROOT';"
aligned_sql() { # workload
  case "$1" in
    p5)   echo "SELECT $AGG5 FROM (SELECT $A5 FROM aligned_scan('bench_ixday'));" ;;
    p25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM aligned_scan('bench_ixday'));" ;;
    p100) echo "SELECT $AGG100 FROM (SELECT $A100, $M20 FROM aligned_scan('bench_ixday'));" ;;
    s25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM aligned_scan('bench_ixday') WHERE date = $DATE_FILTER);" ;;
    s100) echo "SELECT $AGG25 FROM (SELECT $A25 FROM aligned_scan('bench_ixday'));" ;;
  esac
}
wide_sql() { # workload
  case "$1" in
    p5)   echo "SELECT $AGG5 FROM (SELECT $A5 FROM read_parquet('$WIDE'));" ;;
    p25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$WIDE'));" ;;
    p100) echo "SELECT $AGG100 FROM (SELECT $A100, $M20 FROM read_parquet('$WIDE'));" ;;
    s25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$WIDE') WHERE date = $DATE_FILTER);" ;;
    s100) echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$WIDE'));" ;;
  esac
}
join_sql() { # workload
  local A5a="a.alpha000,a.alpha001,a.alpha002,a.alpha003,a.alpha004"
  case "$1" in
    p5)   echo "SELECT $AGG5 FROM (SELECT $A5a FROM read_parquet('$JOIN_INDEX') i JOIN read_parquet('$JOIN_ALPHA') a ON i.rowid = a.rowid_alpha);" ;;
    p25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$JOIN_INDEX') i JOIN read_parquet('$JOIN_ALPHA') a ON i.rowid = a.rowid_alpha);" ;;
    p100) echo "SELECT $AGG100 FROM (SELECT $A100, $M20 FROM read_parquet('$JOIN_INDEX') i JOIN read_parquet('$JOIN_ALPHA') a ON i.rowid = a.rowid_alpha JOIN read_parquet('$JOIN_MA') m ON i.rowid = m.rowid_ma);" ;;
    s25)  echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$JOIN_INDEX') i JOIN read_parquet('$JOIN_ALPHA') a ON i.rowid = a.rowid_alpha WHERE i.date = $DATE_FILTER);" ;;
    s100) echo "SELECT $AGG25 FROM (SELECT $A25 FROM read_parquet('$JOIN_INDEX') i JOIN read_parquet('$JOIN_ALPHA') a ON i.rowid = a.rowid_alpha);" ;;
  esac
}

# ---- Measurement ----------------------------------------------------------------
# Robust monotonic timer: integer nanoseconds (avoids decimal parsing flakiness
# that once produced a spurious negative reading). elapsed <start_ns> <end_ns>
# echoes seconds as a decimal.
now_ns() { date +%s%N; }
elapsed() { echo "scale=9; ($2 - $1) / 1000000000" | bc -l; }
# measure_sql <engine> <workload> <threads> -> "cold warm" (fresh process, warm=2nd run)
measure_sql() {
  local engine="$1" w="$2" th="$3" sql pre
  if [ "$engine" = "aligned" ]; then
    sql=$(aligned_sql "$w"); pre="$ALIGNED_PRELUDE"
  elif [ "$engine" = "wide" ]; then
    sql=$(wide_sql "$w"); pre=""
  else
    sql=$(join_sql "$w"); pre=""
  fi
  local a b
  # run 1: "cold" (first touch in a fresh process; page cache may still be warm)
  a=$(now_ns)
  "$DUCKDB" -light-mode -csv -noheader -c "SET threads=$th; $pre $sql" >/dev/null 2>&1
  b=$(now_ns)
  local cold=$(elapsed "$a" "$b")
  # run 2: "warm" (second run in a fresh process; OS cache warmed by run 1)
  a=$(now_ns)
  "$DUCKDB" -light-mode -csv -noheader -c "SET threads=$th; $pre $sql" >/dev/null 2>&1
  b=$(now_ns)
  local warm=$(elapsed "$a" "$b")
  printf '%.6f %.6f' "$cold" "$warm"
}

declare -a ALL
for engine in aligned wide join; do
  echo "== engine: $engine =="
  for w in "${WORKLOADS[@]}"; do
    for th in "${THREADS[@]}"; do
      read c tw <<< "$(measure_sql "$engine" "$w" "$th")"
      ALL+=("$engine,$w,$th,$c,$tw")
      printf '  %-5s threads=%d  cold=%.3fs warm=%.3fs\n' "$w" "$th" "$c" "$tw"
    done
  done
done

# ---- Correctness cross-check: p5 counts must agree across engines --------------
echo "Cross-checking p5 counts across engines ..."
expected_a=$("$DUCKDB" -light-mode -csv -noheader -c "SET aligned_data_root='$DATA_ROOT'; SET threads=1; $(aligned_sql p5)" | tr ',' ' ' | awk '{s=0; for(i=1;i<=NF;i++) s+=$i; print s}')
expected_w=$("$DUCKDB" -light-mode -csv -noheader -c "SET threads=1; $(wide_sql p5)" | tr ',' ' ' | awk '{s=0; for(i=1;i<=NF;i++) s+=$i; print s}')
expected_j=$("$DUCKDB" -light-mode -csv -noheader -c "SET threads=1; $(join_sql p5)" | tr ',' ' ' | awk '{s=0; for(i=1;i<=NF;i++) s+=$i; print s}')
if [ "$expected_a" = "$expected_w" ] && [ "$expected_w" = "$expected_j" ]; then
  echo "PASS: aligned/wide/join p5 counts all = $expected_a"
else
  echo "FAIL: p5 counts differ: aligned=$expected_a wide=$expected_w join=$expected_j" >&2
  exit 1
fi

# ---- Output ---------------------------------------------------------------------
CSV="$ROOT/bench/out/bench_output.csv"
mkdir -p "$ROOT/bench/out"
{
  echo "engine,workload,threads,cold_s,warm_s"
  for line in "${ALL[@]}"; do echo "$line"; done
} > "$CSV"

REPORT="$ROOT/docs/BENCHMARK.md"
{
  echo "# AlignedTable Benchmark (Phase 6)"
  echo ""
  echo "Date: $(date +%Y-%m-%d)  Machine: local Linux (see AGENTS.md 搂16.2);"
  echo "aligned engine: $DUCKDB"
  echo "Dataset: **bench_ixday** - $TOTAL rows x 127 columns (index 5 + alpha101 101 + ma 21),"
  echo "4 daily partitions, factors sparse (non-null 1/7). Aligned layout: 3 independent Parquet column groups."
  echo ""
  echo "## Workloads"
  echo ""
  echo "| id | description |"
  echo "|----|-------------|"
  echo "| p5 | project 5 factor columns, full scan |"
  echo "| p25 | project 25 factor columns, full scan |"
  echo "| p100 | project 120 columns (100 alpha + 20 ma), full scan |"
  echo "| s25 | project 25 columns, WHERE date = $DATE_FILTER (25% scan, partition pruning) |"
  echo "| s100 | project 25 columns, full scan |"
  echo ""
  echo "## Engines"
  echo ""
  echo "- **aligned** - \`aligned_scan('bench_ixday')\`: 3 groups assembled into one DataChunk,"
  echo "  no JOIN, projection pushdown, partition pruning, parallel range scan, metadata cache, window carry reuse."
  echo "- **wide** - single wide Parquet ($TOTAL rows), DuckDB \`read_parquet\`."
  echo "- **join** - three separate Parquet files (index/alpha/ma) joined on rowid (keyed layout)."
  echo ""
  echo "## Results (seconds; cold = fresh process first touch, warm = second run after OS cache warmed)"
  echo ""
  echo "| engine | workload | threads | cold | warm |"
  echo "|--------|----------|---------|------|------|"
  for line in "${ALL[@]}"; do
    IFS=',' read -r e w th c tw <<< "$line"
    printf '| %s | %s | %s | %.3f | %.3f |\n' "$e" "$w" "$th" "$c" "$tw"
  done
  echo ""
  echo "## Notes"
  echo ""
  echo "- Warm measurement is a fresh-process second run (OS page cache warmed by run 1)."
  echo "- Partition-pruning self-check passed: \`date='2026-09-02'\` prunes to $ROWS_PER_DAY rows."
} > "$REPORT"

echo ""
echo "Report: $REPORT"
echo "CSV:    $CSV"