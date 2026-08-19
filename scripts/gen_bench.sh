#!/usr/bin/env bash
# gen_bench.sh — Linux equivalent of gen_bench.ps1
# Generates a larger AlignedTable test dataset for Phase 4 (parallel scan)
# and Phase 6 (benchmark). Contract-compliant: index mandatory, two-level
# non-index groups, no sidecars, no commit markers, no _group.json, _tmp
# ignored. Partition names: year= / month= / date= only.
#
# Layout (mirrors gen_bench.ps1):
#   bench_ixday/
#     _table.json                     row_count = TOTAL
#     index/   date=2026-09-01..04/   4 parts per day (PART_ROWS each), RGS 32768
#     factor/alpha101/ year=2026/month=2026-09/date=2026-09-01..04/   1 part per day, RGS 65536, 100 sparse cols
#     fieldset/ma/     year=2026/month=2026-09/                      4 parts (one per day), RGS 65536, 20 cols
#
# Usage: bash scripts/gen_bench.sh [TOTAL_ROWS]
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

PART_ROWS=65536
RGS_INDEX=32768
RGS_BIG=65536
DAYS=(2026-09-01 2026-09-02 2026-09-03 2026-09-04)
ROWS_PER_DAY=$((TOTAL / ${#DAYS[@]}))
if [ $ROWS_PER_DAY -le 0 ]; then
  echo "TOTAL_ROWS too small (each day needs >0 rows)" >&2
  exit 1
fi

rj() { # path json  (creates parent dirs)
  mkdir -p "$(dirname "$1")"
  printf '%s\n' "$2" > "$1"
}
partn() { printf 'part-%06d' "$1"; }
run_duck() { "$DUCKDB" -light-mode -c "$1" >/dev/null; }

rm -rf "$TABLE_DIR"
mkdir -p "$TABLE_DIR"

rj "$TABLE_DIR/_table.json" "{\"name\":\"$TABLE\",\"version\":1,\"schema_version\":1,\"key\":[\"date\",\"symbol\"],\"canonical_order\":\"fixed\",\"row_count\":$TOTAL,\"row_group_size\":131072,\"groups\":[\"index\",\"factor/alpha101\",\"fieldset/ma\"]}"

txid=0
day_start=0
for date in "${DAYS[@]}"; do
  txid=$((txid + 1))
  start=$day_start
  end=$((start + ROWS_PER_DAY))

  # ---- index group: day-level partition, PART_ROWS rows per part ------------
  index_dir="$TABLE_DIR/index/date=$date"
  mkdir -p "$index_dir"
  ps=0
  while [ $ps -lt $ROWS_PER_DAY ]; do
    pc=$((PART_ROWS < ROWS_PER_DAY - ps ? PART_ROWS : ROWS_PER_DAY - ps))
    g_start=$((start + ps))
    pn=$(partn $((ps / PART_ROWS)))
    run_duck "COPY (WITH r AS (SELECT range AS r FROM range($g_start,$((g_start + pc)))) SELECT DATE '$date' AS date, printf('%06d', r+1) AS symbol, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid FROM r) TO '$index_dir/$pn.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"
    ps=$((ps + pc))
  done

  # ---- alpha101: 1 part per day, 100 sparse columns --------------------------
  alpha_dir="$TABLE_DIR/factor/alpha101/year=2026/month=2026-09/date=$date"
  mkdir -p "$alpha_dir"
  al="CAST(r AS BIGINT) AS rowid_alpha"
  for n in $(seq 0 99); do
    al+=", CASE WHEN r % 7 = 0 THEN CAST((r + $((n + 1))) * 0.001 AS DOUBLE) ELSE NULL END AS alpha$(printf '%03d' "$n")"
  done
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT $al FROM r) TO '$alpha_dir/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  # ---- ma group: coarse year/month partition, 1 part per day -----------------
  ma_dir="$TABLE_DIR/fieldset/ma/year=2026/month=2026-09"
  mkdir -p "$ma_dir"
  pn=$(partn $((txid - 1)))
  ma="CAST(r AS BIGINT) AS rowid_ma"
  for n in $(seq 0 19); do
    ma+=", CAST((r % ($((n + 11)))) * 0.0001 AS DOUBLE) AS ma$(printf '%03d' "$n")"
  done
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT $ma FROM r) TO '$ma_dir/$pn.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  day_start=$((day_start + ROWS_PER_DAY))
done

echo "Benchmark data generated under $DATA_ROOT/$TABLE"
echo "  rows: $TOTAL, days: ${#DAYS[@]}, cols: index 5 + alpha 101 + ma 21"