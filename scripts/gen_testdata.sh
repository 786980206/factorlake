#!/usr/bin/env bash
# gen_testdata.sh — Linux equivalent of gen_testdata.ps1
# Generates an AlignedTable test dataset under testdata/ (contract-compliant).
#
# Same layout as the PowerShell generator:
#   cnstk_ixday/
#     _table.json
#     index/  date=2026-08-17/  (2 parts, RGS 2048)   date=2026-08-18/  (2 parts, RGS 2048)
#     factor/alpha101/  year=2026/month=08/day=17/  (1 part, RGS 1000 — irregular RG boundaries)
#                       year=2026/month=08/day=18/  (1 part, RGS 1000, +alpha099: schema evolution)
#     fieldset/ma/      year=2026/month=08/         (2 parts from 2 transactions — coarse partitioning)
#
# Global row space: [0,3000) = 2026-08-17, [3000,6000) = 2026-08-18.
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="$ROOT/testdata"
TABLE="cnstk_ixday"
TABLE_DIR="$DATA_ROOT/$TABLE"
# Same default as lib_aligned.sh: the aligned-enabled build (set DUCKDB to override)
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"

rm -rf "$DATA_ROOT"
mkdir -p "$TABLE_DIR"

# write a JSON file (printf-based, all values are pre-formatted ASCII JSON)
write_json() { # path json
  local path="$1"; shift
  mkdir -p "$(dirname "$path")"
  printf '%s\n' "$1" > "$path"
}

run_duckdb() { # sql
  "$DUCKDB" -light-mode -c "$1" >/dev/null
}

part_name() { printf 'part-%06d' "$1"; }

# ---- _table.json ------------------------------------------------------------
write_json "$TABLE_DIR/_table.json" '{
  "name": "cnstk_ixday",
  "version": 1,
  "schema_version": 1,
  "key": ["date", "symbol"],
  "canonical_order": "fixed",
  "row_count": 6000,
  "row_group_size": 131072,
  "groups": ["index", "factor/alpha101", "fieldset/ma"]
}'

# ---- group manifests --------------------------------------------------------
write_json "$TABLE_DIR/index/_group.json" '{
  "group": "index",
  "row_count": 6000,
  "row_group_size": 2048,
  "partitioning": [{"template": "date=%Y-%m-%d", "source": "date"}]
}'

write_json "$TABLE_DIR/factor/alpha101/_group.json" '{
  "group": "factor/alpha101",
  "row_count": 6000,
  "row_group_size": 1000,
  "partitioning": [
    {"template": "year=%Y", "source": "date"},
    {"template": "month=%m", "source": "date"},
    {"template": "day=%d", "source": "date"}
  ]
}'

write_json "$TABLE_DIR/fieldset/ma/_group.json" '{
  "group": "fieldset/ma",
  "row_count": 6000,
  "row_group_size": 2048,
  "partitioning": [
    {"template": "year=%Y", "source": "date"},
    {"template": "month=%m", "source": "date"}
  ]
}'

# ---- logical partitions -----------------------------------------------------
# date | start | rows | txid
declare -a DATES=(2026-08-17 2026-08-18)

for idx in 0 1; do
  DATE="${DATES[$idx]}"
  START=$(( idx * 3000 ))
  ROWS=3000
  END=$(( START + ROWS ))
  TXID=$(( idx + 1 ))

  # ---- index group: day-level partition, 2 parts per day (RGS 2048) --------
  INDEX_DIR="$TABLE_DIR/index/date=$DATE"
  mkdir -p "$INDEX_DIR"
  for i in 0 1; do
    ps=$(( i * 2048 ))
    pc=$(( ROWS - ps < 2048 ? ROWS - ps : 2048 ))
    gstart=$(( START + ps ))
    gend=$(( gstart + pc ))
    pn=$(part_name "$i")
    run_duckdb "COPY (
      WITH r AS (SELECT range AS r FROM range($gstart, $gend))
      SELECT DATE '$DATE' AS date,
             printf('%06d', r + 1) AS symbol,
             CAST((r + 1) * 0.5 AS DOUBLE) AS close,
             CAST((r + 1) * 100 AS BIGINT) AS volume,
             CAST(r AS BIGINT) AS rowid
      FROM r
    ) TO '$INDEX_DIR/$pn.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
    write_json "$INDEX_DIR/$pn.aligned.json" "{
      \"table\": \"$TABLE\",
      \"group\": \"index\",
      \"part\": \"$pn\",
      \"start_row\": $gstart,
      \"row_count\": $pc,
      \"row_group_size\": 2048,
      \"columns\": [\"date\", \"symbol\", \"close\", \"volume\", \"rowid\"]
    }"
  done
  write_json "$INDEX_DIR/.aligned-commit.json" "{
    \"txid\": $TXID,
    \"parts\": [\"$(part_name 0)\", \"$(part_name 1)\"]
  }"

  # ---- alpha101 group: year/month/day partition, 1 part (RGS 1000) ---------
  ALPHA_DIR="$TABLE_DIR/factor/alpha101/year=2026/month=08/day=$DATE"
  mkdir -p "$ALPHA_DIR"
  # alpha001..alpha010
  ALPHA_SQL=""
  for n in $(seq 1 10); do
    ALPHA_SQL+=", CASE WHEN r % 5 = 0 THEN CAST((r + $(( n + 1 ))) * 0.01 AS DOUBLE) ELSE NULL END AS alpha$(printf '%03d' $n)"
  done
  EXTRA=""
  if [ "$DATE" = "2026-08-18" ]; then
    EXTRA=", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha099"
  fi
  run_duckdb "COPY (
    WITH r AS (SELECT range AS r FROM range($START, $END))
    SELECT CAST(r AS BIGINT) AS rowid_alpha$ALPHA_SQL,
           CAST((r + 1) * 0.25 AS DOUBLE) AS close,
           CAST((r + 1) * 0.125 AS DOUBLE) AS vwap$EXTRA
    FROM r
  ) TO '$ALPHA_DIR/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000, COMPRESSION ZSTD);"
  ALPHA_COLS="[\"rowid_alpha\""
  for n in $(seq 1 10); do ALPHA_COLS+=", \"alpha$(printf '%03d' $n)\""; done
  ALPHA_COLS+=", \"close\", \"vwap\""
  [[ "$DATE" = "2026-08-18" ]] && ALPHA_COLS+=", \"alpha099\""
  ALPHA_COLS+="]"
  write_json "$ALPHA_DIR/part-000000.aligned.json" "{
    \"table\": \"$TABLE\",
    \"group\": \"factor/alpha101\",
    \"part\": \"part-000000\",
    \"start_row\": $START,
    \"row_count\": $ROWS,
    \"row_group_size\": 1000,
    \"columns\": $ALPHA_COLS
  }"
  write_json "$ALPHA_DIR/.aligned-commit.json" "{
    \"txid\": $TXID,
    \"parts\": [\"part-000000\"]
  }"

  # ---- ma group: COARSE year/month partition (both days share one dir) ------
  MA_DIR="$TABLE_DIR/fieldset/ma/year=2026/month=08"
  mkdir -p "$MA_DIR"
  PART_IDX=$(( TXID - 1 ))
  PN=$(part_name "$PART_IDX")
  run_duckdb "COPY (
    WITH r AS (SELECT range AS r FROM range($START, $END))
    SELECT CAST(r AS BIGINT) AS rowid_ma,
           CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
           CAST((r % 30) * 0.05 AS DOUBLE) AS ma10,
           CAST((r % 60) * 0.025 AS DOUBLE) AS ma20,
           CAST((r + 1) * 0.0625 AS DOUBLE) AS close,
           CAST((r + 1) * 0.03125 AS DOUBLE) AS vwap
    FROM r
  ) TO '$MA_DIR/$PN.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
  write_json "$MA_DIR/$PN.aligned.json" "{
    \"table\": \"$TABLE\",
    \"group\": \"fieldset/ma\",
    \"part\": \"$PN\",
    \"start_row\": $START,
    \"row_count\": $ROWS,
    \"row_group_size\": 2048,
    \"columns\": [\"rowid_ma\", \"ma5\", \"ma10\", \"ma20\", \"close\", \"vwap\"]
  }"
  # append part to the shared marker (contract v1.1)
  MARKER_PARTS="[\"part-000000\""
  for k in $(seq 1 $PART_IDX); do MARKER_PARTS+=", \"$(part_name $k)\""; done
  MARKER_PARTS+="]"
  write_json "$MA_DIR/.aligned-commit.json" "{
    \"txid\": $TXID,
    \"parts\": $MARKER_PARTS
  }"
done

# ---- ignored directory (contract 搂2.1d): a _tmp directory with stray parts --
TMP_DIR="$TABLE_DIR/_tmp/transaction-999/index/date=2026-08-17"
mkdir -p "$TMP_DIR"
run_duckdb "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 100))
  SELECT DATE '2026-08-17' AS date, printf('%06d', r + 1) AS symbol, CAST(r AS DOUBLE) AS close
  FROM r
) TO '$TMP_DIR/part-000000.parquet' (FORMAT PARQUET);"

echo "Test data generated under $DATA_ROOT"
echo "  table: $TABLE, rows: 6000, groups: index / factor/alpha101 / fieldset/ma"
