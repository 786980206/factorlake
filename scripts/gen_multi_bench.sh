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
#                      --aligned true|false [--out DIR] [--tag NAME]
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
    --aligned) ALIGNED=$2; shift 2;;
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
[ "$ALIGNED" = true ] || [ "$ALIGNED" = false ] || { echo "aligned must be true|false" >&2; exit 1; }

# column layout
INDEX_COLS=20
ALPHA_COLS=$(( (WIDTH - INDEX_COLS) * 3 / 4 ))
FIELDSET_COLS=$(( WIDTH - INDEX_COLS - ALPHA_COLS ))
TOTAL_COLS=$(( INDEX_COLS + ALPHA_COLS + FIELDSET_COLS ))
if [ $TOTAL_COLS -ne "$WIDTH" ]; then echo "width split mismatch ($TOTAL_COLS != $WIDTH)" >&2; exit 1; fi

TABLE="bench_${TAG}"
ALTER="bench_alter_${TAG}"
tableDir="$OUT/$TABLE"
PART_ROWS=65536
RGS_INDEX=32768
RGS_BIG=65536
Days=(2026-09-01 2026-09-02 2026-09-03 2026-09-04)
rowsPerDay=$((ROWS / ${#Days[@]}))

rj(){ mkdir -p "$(dirname "$1")"; printf '%s\n' "$2" > "$1"; }
partn(){ printf 'part-%06d' "$1"; }
run_duck(){ "$DUCKDB" -light-mode -c "$1" >/dev/null; }

echo "== gen $TABLE: rows=$ROWS width=$WIDTH (idx=$INDEX_COLS alpha=$ALPHA_COLS fs=$FIELDSET_COLS) sparse=${NULL_PCT}% aligned=$ALIGNED =="

# ---------------- aligned table manifests ----------------
rm -rf "$tableDir"; mkdir -p "$tableDir"
aligned_txt=$([ "$ALIGNED" = true ] && echo true || echo false)
rj "$tableDir/_table.json" "{\"name\":\"$TABLE\",\"version\":1,\"schema_version\":1,\"key\":[\"date\",\"symbol\"],\"canonical_order\":\"fixed\",\"row_count\":$ROWS,\"row_group_size\":131072,\"aligned\":$aligned_txt,\"groups\":[\"index\",\"factor/alpha\",\"fieldset/fs\"]}"
rj "$tableDir/index/_group.json" "{\"group\":\"index\",\"row_count\":$ROWS,\"row_group_size\":$RGS_INDEX,\"partitioning\":[{\"template\":\"date=%Y-%m-%d\",\"source\":\"date\"}]}"
rj "$tableDir/factor/alpha/_group.json" "{\"group\":\"factor/alpha\",\"row_count\":$ROWS,\"row_group_size\":$RGS_BIG,\"partitioning\":[{\"template\":\"year=%Y\",\"source\":\"date\"},{\"template\":\"month=%m\",\"source\":\"date\"},{\"template\":\"day=%d\",\"source\":\"date\"}]}"
rj "$tableDir/fieldset/fs/_group.json" "{\"group\":\"fieldset/fs\",\"row_count\":$ROWS,\"row_group_size\":$RGS_BIG,\"partitioning\":[{\"template\":\"year=%Y\",\"source\":\"date\"},{\"template\":\"month=%m\",\"source\":\"date\"}]}"

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

# alpha and fieldset column name JSON arrays
alpha_names='["rowid_alpha"'; for n in $(seq 0 $((ALPHA_COLS-1))); do alpha_names+=", \"alpha$(printf '%03d' "$n")\""; done; alpha_names+="]"
fs_names='["rowid_fs"'; for n in $(seq 0 $((FIELDSET_COLS-1))); do fs_names+=", \"fs$(printf '%03d' "$n")\""; done; fs_names+="]"

# ---------------- per-day parts -----------------
txid=0; dayStart=0
for date in "${Days[@]}"; do
  txid=$((txid+1)); start=$dayStart; end=$((start+rowsPerDay))

  # index group: day-level partition, parts of PART_ROWS, RGS 32768
  indexDir="$tableDir/index/date=$date"; mkdir -p "$indexDir"
  ps=0; markers=()
  while [ $ps -lt $rowsPerDay ]; do
    pc=$((PART_ROWS < rowsPerDay-ps ? PART_ROWS : rowsPerDay-ps))
    gStart=$((start+ps)); pn=$(partn ${#markers[@]})
    local_sel="CAST(r AS DOUBLE) AS rowid"
    run_duck "COPY (WITH r AS (SELECT range AS r FROM range($gStart,$((gStart+pc)))) SELECT DATE '$date' AS date, printf('%06d', r+1) AS symbol, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid $([ -n "$IX_SEL" ] && echo ", $IX_SEL") FROM r) TO '$indexDir/$pn.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"
    ixcols='["date","symbol","close","volume","rowid"'
    for n in $(seq 1 $((INDEX_COLS-5))); do ixcols+=", \"ix$(printf '%03d' "$n")\""; done
    ixcols+="]"
    rj "$indexDir/$pn.aligned.json" "{\"table\":\"$TABLE\",\"group\":\"index\",\"part\":\"$pn\",\"start_row\":$gStart,\"row_count\":$pc,\"row_group_size\":$RGS_INDEX,\"columns\":$ixcols}"
    markers+=("$pn"); ps=$((ps+pc))
  done
  mp="["; for i in "${!markers[@]}"; do [ $i -gt 0 ] && mp+=","; mp+="\"${markers[$i]}\""; done; mp+="]"
  rj "$indexDir/.aligned-commit.json" "{\"txid\":$txid,\"parts\":$mp}"

  # alpha group: year/month/day, 1 part, ALPHA_COLS sparse cols
  alphaDir="$tableDir/factor/alpha/year=2026/month=09/day=$date"; mkdir -p "$alphaDir"
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT CAST(r AS BIGINT) AS rowid_alpha $([ -n "$ALPHA_SEL" ] && echo ", $ALPHA_SEL") FROM r) TO '$alphaDir/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
  rj "$alphaDir/part-000000.aligned.json" "{\"table\":\"$TABLE\",\"group\":\"factor/alpha\",\"part\":\"part-000000\",\"start_row\":$start,\"row_count\":$rowsPerDay,\"row_group_size\":$RGS_BIG,\"columns\":$alpha_names}"
  rj "$alphaDir/.aligned-commit.json" "{\"txid\":$txid,\"parts\":[\"part-000000\"]}"

  # fieldset group: coarse year/month, 1 part per day
  fsDir="$tableDir/fieldset/fs/year=2026/month=09"; mkdir -p "$fsDir"
  pn=$(partn $((txid-1)))
  run_duck "COPY (WITH r AS (SELECT range AS r FROM range($start,$end)) SELECT CAST(r AS BIGINT) AS rowid_fs $([ -n "$FS_SEL" ] && echo ", $FS_SEL") FROM r) TO '$fsDir/$pn.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
  rj "$fsDir/$pn.aligned.json" "{\"table\":\"$TABLE\",\"group\":\"fieldset/fs\",\"part\":\"$pn\",\"start_row\":$start,\"row_count\":$rowsPerDay,\"row_group_size\":$RGS_BIG,\"columns\":$fs_names}"
  mparts="["; for k in $(seq 0 $((txid-1))); do [ $k -gt 0 ] && mparts+=","; mparts+="\"$(partn $k)\""; done; mparts+="]"
  rj "$fsDir/.aligned-commit.json" "{\"txid\":$txid,\"parts\":$mparts}"

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

echo "Generated $TABLE under $OUT"
echo "  rows=$ROWS cols=$WIDTH sparse=${NULL_PCT}% aligned=$ALIGNED"
echo "  baselines -> $baseDir (wide.parquet, join_index.parquet, join_alpha.parquet, join_fs.parquet)"
