#!/usr/bin/env bash
# gen_testdata.sh — Linux equivalent of gen_testdata.ps1 (v6 contract).
# Generates an AlignedTable test dataset under testdata/.
#
# Layout exercised by this script (partition-aligned, single-level month=
# partition — every group uses the SAME partition kind; self-describing part
# names "{idx:04d}-{rows:10d}.parquet"):
#   cnstk_ixday/
#     _table.json                              (groups field present but NEVER read)
#     index/        month=2026-07/  0000-0000002000.parquet (1 part, 2000 rows)
#                   month=2026-08/  0000-0000002000.parquet
#                                   0001-0000002000.parquet (2 parts, 2000+2000)
#     factor/alpha101/ month=2026-07/  0000-0000002000.parquet (1 part, 2000 rows)
#                      month=2026-08/  0000-0000002000.parquet
#                                      0002-0000002000.parquet  <-- index 0001 SKIPPED
#                                                              (deletion: gaps allowed
#                                                               in non-index groups)
#                                                              (0002 part adds alpha099
#                                                               schema evolution)
#     fieldset/ma/  month=2026-08/  0000-0000002000.parquet
#                                   0001-0000002000.parquet  <-- MISSING month=2026-07
#
# Global row space: [0,2000) = 2026-07, [2000,6000) = 2026-08. Row counts and
# start rows come from the FILE NAMES (no footer reads at plan time). The index
# group's indexes are consecutive from 0000; alpha101 SKIPS index 0001 in
# month=2026-08 (legal — the index is only a group-local label; the shared
# partition's TOTAL row count still matches the index: 2000+2000=4000, and the
# shared index 0000 agrees on 2000 rows). The ma group omits 2026-07 entirely:
# rows [0,2000) read as NULL for its columns.
#
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.
#
# v6 contract (2026-08):
#   * Single-level partition (year= / month= / date=); index and every group
#     use the same kind. Group partition keys must be a subset of the index's.
#   * Part files are named "{idx:04d}-{rows:10d}.parquet"; row counts and start
#     rows are derived from the names (zero footer reads at plan time; ONE
#     footer per group for the schema / index date-field contract).
#   * Index group indexes are consecutive from 0000; non-index groups may skip
#     indexes (deletion). Shared partitions must agree on the TOTAL row count
#     and every SHARED index's row count (fail-fast).
#   * The index schema's first two columns must contain a DATE/TIMESTAMP field
#     (the partition source column; here 'date').
#   * Missing partitions read as NULL (row space stays index-defined).
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

# v6 self-describing part name: "{idx:04d}-{rows:10d}"
part_name() { printf '%04d-%010d' "$1" "$2"; }

# ---- _table.json ------------------------------------------------------------
# The only manifest. Group metadata (row counts, partitioning) is derived from
# the directory layout + Parquet footers; no _group.json files exist. The
# explicit partitioning map is omitted on purpose so the reader's directory
# derivation path is exercised. The groups field is legacy (never read).
write_json "$TABLE_DIR/_table.json" '{
  "name": "cnstk_ixday",
  "version": 1,
  "part_rows": 4194304,
  "groups": ["index", "factor/alpha101", "fieldset/ma"]
}'

# ---- logical partitions -----------------------------------------------------
# per-part row count is 2000 everywhere (row bookkeeping is from names).
PER=2000
# month, start, date, index indexes, alpha indexes, ma indexes
MONTHS=("2026-07" "2026-08")
STARTS=(0 2000)
DATES=("2026-07-01" "2026-08-01")
IX_0=(0)          IX_1=(0 1)
AL_0=(0)          AL_1=(0 2)
MA_0=()           MA_1=(0 1)

for idx in 0 1; do
  MONTH="${MONTHS[$idx]}"
  START="${STARTS[$idx]}"
  DATE="${DATES[$idx]}"
  eval "IX_PARTS=( \"\${IX_$idx[@]}\" )"
  eval "AL_PARTS=( \"\${AL_$idx[@]}\" )"
  eval "MA_PARTS=( \"\${MA_$idx[@]}\" )"

  # ---- index group: month-level partition, consecutive indexes from 0000 ----
  INDEX_DIR="$TABLE_DIR/index/month=$MONTH"
  mkdir -p "$INDEX_DIR"
  for p in "${IX_PARTS[@]}"; do
    S=$(( START + p * PER ))
    E=$(( S + PER ))
    PN=$(part_name "$p" "$PER")
    run_duckdb "COPY (
      WITH r AS (SELECT range AS r FROM range($S, $E))
      SELECT DATE '$DATE' AS date,
             printf('%06d', r + 1) AS symbol,
             CAST((r + 1) * 0.5 AS DOUBLE) AS close,
             CAST((r + 1) * 100 AS BIGINT) AS volume,
             CAST(r AS BIGINT) AS rowid
      FROM r
    ) TO '$INDEX_DIR/$PN.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
  done

  # ---- alpha101 group: month-level partition. month=2026-08 skips index 0001
  # (deletion). The LAST part (0002) carries the alpha099 schema-evolution
  # column. NOTE: the DATA range of a part follows its position within the
  # partition (0-based, ignoring gaps) — part 0002 is the partition's 2nd part
  # and holds rows [4000,6000), even though its file-name index is 2.
  ALPHA_DIR="$TABLE_DIR/factor/alpha101/month=$MONTH"
  mkdir -p "$ALPHA_DIR"
  POS=0
  for p in "${AL_PARTS[@]}"; do
    S=$(( START + POS * PER ))
    E=$(( S + PER ))
    PN=$(part_name "$p" "$PER")
    ALPHA_SQL=""
    for n in $(seq 1 10); do
      ALPHA_SQL+=", CASE WHEN r % 5 = 0 THEN CAST((r + $(( n + 1 ))) * 0.01 AS DOUBLE) ELSE NULL END AS alpha$(printf '%03d' $n)"
    done
    EXTRA=""
    if [ "$MONTH" = "2026-08" ] && [ "$p" = "2" ]; then
      EXTRA=", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha099"
    fi
    run_duckdb "COPY (
      WITH r AS (SELECT range AS r FROM range($S, $E))
      SELECT CAST(r AS BIGINT) AS rowid_alpha$ALPHA_SQL,
             CAST((r + 1) * 0.25 AS DOUBLE) AS close,
             CAST((r + 1) * 0.125 AS DOUBLE) AS vwap$EXTRA
      FROM r
    ) TO '$ALPHA_DIR/$PN.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000, COMPRESSION ZSTD);"
    POS=$((POS + 1))
  done

  # ---- ma group: month-level partition, matching the index's indexes. The
  # 2026-07 partition is SKIPPED on purpose (missing partition -> NULL fill).
  MA_DIR="$TABLE_DIR/fieldset/ma/month=$MONTH"
  mkdir -p "$MA_DIR"
  POS=0
  for p in "${MA_PARTS[@]}"; do
    S=$(( START + POS * PER ))
    E=$(( S + PER ))
    PN=$(part_name "$p" "$PER")
    run_duckdb "COPY (
      WITH r AS (SELECT range AS r FROM range($S, $E))
      SELECT CAST(r AS BIGINT) AS rowid_ma,
             CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
             CAST((r % 30) * 0.05 AS DOUBLE) AS ma10,
             CAST((r % 60) * 0.025 AS DOUBLE) AS ma20,
             CAST((r + 1) * 0.0625 AS DOUBLE) AS close,
             CAST((r + 1) * 0.03125 AS DOUBLE) AS vwap
      FROM r
    ) TO '$MA_DIR/$PN.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
    POS=$((POS + 1))
  done
done

# ---- ignored directory (contract §2.1d): a _tmp directory with stray parts --
TMP_DIR="$TABLE_DIR/_tmp/transaction-999/index/month=2026-07"
mkdir -p "$TMP_DIR"
run_duckdb "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 100))
  SELECT DATE '2026-07-01' AS date, printf('%06d', r + 1) AS symbol, CAST(r AS DOUBLE) AS close
  FROM r
) TO '$TMP_DIR/part-000000.parquet' (FORMAT PARQUET);"

echo "Test data generated under $DATA_ROOT"
echo "  table: $TABLE, rows: 6000, groups: index / factor/alpha101 / fieldset/ma (ma missing month=2026-07; alpha101 month=2026-08 skips index 0001)"