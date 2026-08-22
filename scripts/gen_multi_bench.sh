#!/usr/bin/env bash
# gen_multi_bench.sh — parameterized dataset generator for the multi-scenario
# benchmark. Produces a single benchmark table at a given (rows, width,
# sparsity, aligned) along with the matching wide/join baselines.
#
# All engines must observe the SAME logical table (same key order, same NULL
# distribution), so every file is derived from the SAME source row generator
# over r in [0, rows). The logical schema is:
#   date (partition key) | symbol | close | volume | rowid |
#   index_x00..          | alpha000..alpha<A-1> | fieldset000..fieldset<B-1>
#
# Column counts by width (index fixed 20; alpha = 3/4 of rest; fieldset = rest):
#   W1(128):  20 + 81 + 27
#   W2(1024): 20 + 753 + 251
#   W3(10240):20 + 7665 + 2555
#
# Usage:
#   gen_multi_bench.sh --rows N --width N --sparsity dense|90|99
#                      [--out DIR] [--tag NAME]
# env: DUCKDB (default build/duckdb)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"

ROWS=1000000
WIDTH=128
SPARSITY=dense
ALIGNED=true
OUT="$ROOT/testdata"
TAG=bench

while [ $# -gt 0 ]; do
  case "$1" in
    --rows) ROWS=$2; shift 2;;
    --width) WIDTH=$2; shift 2;;
    --sparsity) SPARSITY=$2; shift 2;;
    --aligned) ALIGNED=$2; shift 2;; # accepted for CLI compat; reader now ignores this flag
    --out) OUT=$2; shift 2;;
    --tag) TAG=$2; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

case "$SPARSITY" in
  dense|0) NULL_PCT=0;;
  90) NULL_PCT=90;;
  99) NULL_PCT=99;;
  *) echo "sparsity must be dense|90|99" >&2; exit 1;;
esac
if [ $((ROWS % 4)) -ne 0 ]; then echo "rows must be divisible by 4" >&2; exit 1; fi

# column layout
INDEX_COLS=20
ALPHA_COLS=$(( (WIDTH - INDEX_COLS) * 3 / 4 ))
FIELDSET_COLS=$(( WIDTH - INDEX_COLS - ALPHA_COLS ))
TOTAL_COLS=$(( INDEX_COLS + ALPHA_COLS + FIELDSET_COLS ))
if [ $TOTAL_COLS -ne "$WIDTH" ]; then echo "width split mismatch ($TOTAL_COLS != $WIDTH)" >&2; exit 1; fi

TABLE="bench_${TAG}"
tableDir="$OUT/$TABLE"
PART_ROWS=$((ROWS / 4))
RGS_INDEX=32768
RGS_BIG=65536
Days=(2026-09-01 2026-09-02 2026-09-03 2026-09-04)
rowsPerDay=$((ROWS / ${#Days[@]}))

rj(){ mkdir -p "$(dirname "$1")"; printf '%s\n' "$2" > "$1"; }
partn(){ printf '%04d-%010d' "$1" "$2"; }
# Run a (potentially huge) SQL statement via a temp file on stdin, so very wide
# datasets (W3, thousands of columns) that overflow ARG_MAX with `-c` still work.
run_duck(){ local tmp="$OUT/.gen_bench.sql"; printf '%s\n' "$1" > "$tmp"; "/d/proj/factorlake/duckdb/build/duckdb_aligned.exe" -light-mode < "$tmp" >/dev/null; }

echo "== gen $TABLE: rows=$ROWS width=$WIDTH (idx=$INDEX_COLS alpha=$ALPHA_COLS fs=$FIELDSET_COLS) sparse=${NULL_PCT}% aligned=$ALIGNED =="

# ---------------- aligned table manifest ----------------
rm -rf "$tableDir"; mkdir -p "$tableDir"
rj "$tableDir/_table.json" "{\"groups\":[\"index\",\"factor/alpha\",\"fieldset/fs\"]}"

# ---------------- column definition helpers ----------------
# index_cols returns a SQL select list for the given global row range [g0,g1)
# of the index group (date derived from dayStart..): we generate per-day parts,
# so index cols are constant per part.
# factor value: sparse NULL with rate NULL_PCT; when present value = (r+n)*w.
# NOTE: each helper accumulates into a local string and emits it with a final
# `printf` that ALWAYS returns 0. If the last command were a `[ ... ] && printf`
# conditional it would return non-zero on the last iteration and abort the
# script under `set -e` inside a command substitution.
alpha_col_list() { local n out=""; for n in $(seq 0 $((ALPHA_COLS-1))); do
    if [ "$NULL_PCT" = 0 ]; then
      out+="$(printf 'CAST((r + %d) * 0.001 AS DOUBLE) AS alpha%03d' "$((n+1))" "$n")"
    else
      out+="$(printf 'CASE WHEN r %% %d = 0 THEN CAST((r + %d) * 0.001 AS DOUBLE) ELSE NULL END AS alpha%03d' "$((100 - NULL_PCT))" "$((n+1))" "$n")"
    fi
    [ $n -lt $((ALPHA_COLS-1)) ] && out+=", "; done; printf '%s' "$out"; }
fieldset_col_list() { local n out=""; for n in $(seq 0 $((FIELDSET_COLS-1))); do
    out+="$(printf 'CAST((r %% %d) * 0.0001 AS DOUBLE) AS fs%03d' "$((n+11))" "$n")"
    [ $n -lt $((FIELDSET_COLS-1)) ] && out+=", "; done; printf '%s' "$out"; }
extra_index_cols() { local n out=""; for n in $(seq 1 $((INDEX_COLS-5))); do
    out+="$(printf 'CAST((r + %d) * 0.5 AS DOUBLE) AS ix%03d' "$n" "$n")"
    [ $n -lt $((INDEX_COLS-5)) ] && out+=", "; done; printf '%s' "$out"; }

ALPHA_SEL=$(alpha_col_list)
FS_SEL=$(fieldset_col_list)
IX_SEL=$(extra_index_cols)

# ---------------- per-day parts -----------------
txid=0; dayStart=0
for date in "${Days[@]}"; do
  txid=$((txid+1)); start=$dayStart; end=$((start+rowsPerDay))

#   index group: day-level partition, 1 part per day (same kind for all
#   groups — v5 single-level partition contract), RGS 32768
  indexDir="$tableDir/index/date=$date"; mkdir -p "$indexDir"
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT DATE '$date' AS date, printf('%06d', r+1) AS symbol, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid $([ -n "$IX_SEL" ] && echo ", $IX_SEL") FROM r) TO '$indexDir/$(partn 0 $rowsPerDay).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"

  # alpha group: day-level partition, 1 part, ALPHA_COLS sparse cols
  alphaDir="$tableDir/factor/alpha/date=$date"; mkdir -p "$alphaDir"
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT CAST(r AS BIGINT) AS rowid_alpha $([ -n "$ALPHA_SEL" ] && echo ", $ALPHA_SEL") FROM r) TO '$alphaDir/$(partn 0 $rowsPerDay).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  # fieldset group: day-level partition, 1 part (same kind as index/alpha)
  fsDir="$tableDir/fieldset/fs/date=$date"; mkdir -p "$fsDir"
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT CAST(r AS BIGINT) AS rowid_fs $([ -n "$FS_SEL" ] && echo ", $FS_SEL") FROM r) TO '$fsDir/$(partn 0 $rowsPerDay).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"

  dayStart=$((dayStart+rowsPerDay))
done

# ---------------- wide + join baselines (same logical table) ----------------
# index base columns repeated (per-day date derived from global r)
base_sel="DATE '2026-09-01' + (r // $rowsPerDay)::INT AS date, printf('%06d', r + 1) AS symbol, CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid"
[ -n "$IX_SEL" ] && base_sel+=", $IX_SEL"
full_sel="$base_sel, CAST(r AS BIGINT) AS rowid_alpha, $ALPHA_SEL, CAST(r AS BIGINT) AS rowid_fs, $FS_SEL"
alpha_only="CAST(r AS BIGINT) AS rowid_alpha, $ALPHA_SEL"
fs_only="CAST(r AS BIGINT) AS rowid_fs, $FS_SEL"

baseDir="$OUT/bench_baseline_${TAG}"
mkdir -p "$baseDir"
wide="$baseDir/wide.parquet"; joinIndex="$baseDir/join_index.parquet"
joinAl="$baseDir/join_alpha.parquet"; joinFs="$baseDir/join_fs.parquet"
run_duck "COPY (WITH r AS (SELECT range AS r FROM range(0,$ROWS)) SELECT $full_sel FROM r) TO '$wide' (FORMAT PARQUET, COMPRESSION ZSTD);"
run_duck "COPY (WITH r AS (SELECT range AS r FROM range(0,$ROWS)) SELECT $base_sel FROM r) TO '$joinIndex' (FORMAT PARQUET, COMPRESSION ZSTD);"
run_duck "COPY (WITH r AS (SELECT range AS r FROM range(0,$ROWS)) SELECT $alpha_only FROM r) TO '$joinAl' (FORMAT PARQUET, COMPRESSION ZSTD);"
run_duck "COPY (WITH r AS (SELECT range AS r FROM range(0,$ROWS)) SELECT $fs_only FROM r) TO '$joinFs' (FORMAT PARQUET, COMPRESSION ZSTD);"

# record generation marker so staged drivers can verify (rows,width,sparsity)
printf '%s,%s,%s\n' "$ROWS" "$WIDTH" "$SPARSITY" > "$tableDir/.gen-meta"

echo "Generated $TABLE under $OUT"
echo "  rows=$ROWS cols=$WIDTH sparse=${NULL_PCT}% aligned=$ALIGNED"
echo "  baselines -> $baseDir (wide.parquet, join_index.parquet, join_alpha.parquet, join_fs.parquet)"
