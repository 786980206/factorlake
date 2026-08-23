#!/usr/bin/env bash
# gen_bench.sh 鈥?Linux equivalent of gen_bench.ps1
# Generates a larger AlignedTable test dataset for Phase 4 (parallel scan)
# and Phase 6 (benchmark). v5 contract: index mandatory, two-level
# non-index groups, single-level partition (year= / month= / date= 鈥?the SAME
# kind for every group), only the footer is authoritative (no sidecars, no
# commit markers, no _group.json), _tmp ignored.
#
# Layout (mirrors gen_bench.ps1):
#   bench_ixday/
#     index/   date=2026-09-01..04/   1 part per day (rowsPerDay), RGS 32768
#     factor/alpha101/ date=2026-09-01..04/   1 part per day, RGS 65536, 100 sparse cols
#     fieldset/ma/     date=2026-09-01..04/   1 part per day, RGS 65536, 20 cols
#
# Usage: bash test/gen_bench.sh [TOTAL_ROWS]
#   TOTAL_ROWS  default 1000000; must be divisible by 4 (one quarter per day).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="$ROOT/testdata"
TABLE="bench_ixday"
TABLE_DIR="$DATA_ROOT/$TABLE"
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"

TOTAL="${1:-1000000}"
if ! [[ "$TOTAL" =~ ^[0-9]+$ ]] || [ $((TOTAL % 4)) -ne 0 ]; then
  echo "TOTAL_ROWS must be a positive integer divisible by 4" >&2
  exit 1
fi

DAYS=(2026-09-01 2026-09-02 2026-09-03 2026-09-04)
PART_ROWS=$((TOTAL / ${#DAYS[@]}))
RGS_INDEX=32768
RGS_BIG=65536
ROWS_PER_DAY=$PART_ROWS
if [ $ROWS_PER_DAY -le 0 ]; then
  echo "TOTAL_ROWS too small (each day needs >0 rows)" >&2
  exit 1
fi

rj() { # path json  (creates parent dirs)
  mkdir -p "$(dirname "$1")"
  printf '%s\n' "$2" > "$1"
}
# v6 self-describing part name: "{idx:04d}-{rows:10d}"
partn() { printf '%04d-%010d' "$1" "$2"; }
run_duck() { "$DUCKDB" -light-mode -c "$1" >/dev/null; }

rm -rf "$TABLE_DIR"
mkdir -p "$TABLE_DIR"

txid=0
day_start=0
for date in "${DAYS[@]}"; do
  txid=$((txid + 1))
  start=$day_start
  end=$((start + ROWS_PER_DAY))

  # ---- index group: day-level partition, 1 part per day (fully aligned) ----
  index_dir="$TABLE_DIR/index/date=$date"
  mkdir -p "$index_dir"
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT printf('%06d', r+1) AS symbol, DATE '$date' AS date, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid FROM r) TO '$index_dir/$(partn 0 $ROWS_PER_DAY).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"

  # ---- alpha101: 1 part per day, 100 sparse columns --------------------------
  alpha_dir="$TABLE_DIR/factor/alpha101/date=$date"
  mkdir -p "$alpha_dir"
  al="CAST(r AS BIGINT) AS rowid_alpha"
  for n in $(seq 0 99); do
    al+=", CASE WHEN r % 7 = 0 THEN CAST((r + $((n + 1))) * 0.001 AS DOUBLE) ELSE NULL END AS alpha$(printf '%03d' "$n")"
  done
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT $al FROM r) TO '$alpha_dir/$(partn 0 $ROWS_PER_DAY).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  # ---- ma group: day-level partition, 1 part per day (same kind) -------------
  ma_dir="$TABLE_DIR/fieldset/ma/date=$date"
  mkdir -p "$ma_dir"
  ma="CAST(r AS BIGINT) AS rowid_ma"
  for n in $(seq 0 19); do
    ma+=", CAST((r % ($((n + 11)))) * 0.0001 AS DOUBLE) AS ma$(printf '%03d' "$n")"
  done
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT $ma FROM r) TO '$ma_dir/$(partn 0 $ROWS_PER_DAY).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  day_start=$((day_start + ROWS_PER_DAY))
done

echo "Benchmark data generated under $DATA_ROOT/$TABLE"
echo "  rows: $TOTAL, days: ${#DAYS[@]}, cols: index 5 + alpha 101 + ma 21"