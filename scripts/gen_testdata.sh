#!/usr/bin/env bash
# gen_testdata.sh — Linux equivalent of gen_testdata.ps1 (new contract, no sidecars).
# Generates an AlignedTable test dataset under testdata/.
#
# Layout exercised by this script:
#   cnstk_ixday/
#     _table.json                              (includes optional part_rows override)
#     index/        date=2026-08-17/  (2 parts, RGS 2048)
#                   date=2026-08-18/  (2 parts, RGS 2048)
#     factor/alpha101/ year=2026/month=08/date=2026-08-17/  (1 part, RGS 1000)
#                      year=2026/month=08/date=2026-08-18/  (1 part, RGS 1000, +alpha099 schema evolution)
#     fieldset/ma/  year=2026/month=08/           (2 parts from 2 transactions — coarse partitioning)
#
# Global row space: [0,3000) = 2026-08-17, [3000,6000) = 2026-08-18.
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.
#
# New contract (alpha branch, 2026-08):
#   * Metadata is read from _table.json + per-directory layout + Parquet footer.
#     No *.aligned.json sidecars. No .aligned-commit.json markers.
#   * Partition names must be one of: year=, month=, date= (no day=).
#   * PART_ROWS defaults to 4_194_304; optional override in _table.json's part_rows.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="$ROOT/testdata"
TABLE="cnstk_ixday"
TABLE_DIR="$DATA_ROOT/$TABLE"
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
# The only manifest. Group metadata (row counts, partitioning) is derived from
# the directory layout + Parquet footers; no _group.json files exist. The
# explicit partitioning map is omitted on purpose so the reader's directory
# derivation path is exercised.
write_json "$TABLE_DIR/_table.json" '{
  "name": "cnstk_ixday",
  "version": 1,
  "schema_version": 1,
  "key": ["date", "symbol"],
  "canonical_order": "fixed",
  "row_count": 6000,
  "row_group_size": 131072,
  "part_rows": 4194304,
  "groups": ["index", "factor/alpha101", "fieldset/ma"]
}'

# ---- logical partitions -----------------------------------------------------
declare -a DATES=(2026-08-17 2026-08-18)

for idx in 0 1; do
  DATE="${DATES[$idx]}"
  START=$(( idx * 3000 ))
  ROWS=3000
  END=$(( START + ROWS ))

  # ---- index group: date-level partition, 2 parts per day (RGS 2048) --------
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
  done

  # ---- alpha101 group: year/month/date partition, 1 part (RGS 1000) ---------
  ALPHA_DIR="$TABLE_DIR/factor/alpha101/year=2026/month=08/date=$DATE"
  mkdir -p "$ALPHA_DIR"
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

  # ---- ma group: COARSE year/month partition (both days share one dir) ------
  MA_DIR="$TABLE_DIR/fieldset/ma/year=2026/month=08"
  mkdir -p "$MA_DIR"
  PART_IDX=$idx
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
done

# ---- ignored directory (contract §2.1d): a _tmp directory with stray parts --
TMP_DIR="$TABLE_DIR/_tmp/transaction-999/index/date=2026-08-17"
mkdir -p "$TMP_DIR"
run_duckdb "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 100))
  SELECT DATE '2026-08-17' AS date, printf('%06d', r + 1) AS symbol, CAST(r AS DOUBLE) AS close
  FROM r
) TO '$TMP_DIR/part-000000.parquet' (FORMAT PARQUET);"

echo "Test data generated under $DATA_ROOT"
echo "  table: $TABLE, rows: 6000, groups: index / factor/alpha101 / fieldset/ma"
