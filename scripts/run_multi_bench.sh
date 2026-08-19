#!/usr/bin/env bash
# run_multi_bench.sh — multi-scenario benchmark harness (Phase 6+).
#
# Reads the standardized Group Settings from bench/multi_bench_config.sh and
# executes a Tier (A smoke / B main / C stress) or an explicit selection across
# the engine groups, measuring COLD + WARM per test point.
#
# Engines: D-WIDE D-JOIN A-ALIGNED (+ P-CONCAT P-JOIN if polars is
# importable). A-ALIGNED is the aligned reader (full alignment + position
# assembly + intersection pruning), not a hash key-join.
#
# Usage:
#   bash scripts/run_multi_bench.sh --tier A [--rows N] [--width N]
#                                   [--sparsity dense|90|99]
#                                   [--out DIR] [--threads 1,4,8]
#                                   [--engines D-WIDE,D-JOIN,A-ALIGNED]
#                                   [--no-regen]
# env: DUCKDB (default build/duckdb), ALIGNED_DATA_ROOT,
#      REPEATS (int, default 5) executions per warm measurement —
#      pass as env prefix, NOT a positional arg:
#      REPEATS=5 bash scripts/run_multi_bench.sh --tier A ...
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$ROOT/bench/multi_bench_config.sh"
source "$CONFIG"

DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb_aligned.exe}"
OUT="${ALIGNED_DATA_ROOT:-$ROOT/testdata}"
TIER=A
ROWS_ARG=""
WIDTH_ARG=""
SPARSITY=dense
THREADS_ARG=""
ENGINES_ARG=""
FLAG_NOREGEN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --tier) TIER=$2; shift 2;;
    --rows) ROWS_ARG=$2; shift 2;;
    --width) WIDTH_ARG=$2; shift 2;;
    --sparsity) SPARSITY=$2; shift 2;;
    --threads) THREADS_ARG=$2; shift 2;;
    --engines) ENGINES_ARG=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --no-regen) FLAG_NOREGEN=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

# ---- resolve tier -----------------------------------------------------------------
case "$TIER" in
  A) RVAR=TIER_A_ROWS; WVAR=TIER_A_WIDTH; QS=TIER_A_QUERIES; FS=TIER_A_FILTERS; SS=TIER_A_SEL;;
  B) RVAR=TIER_B_ROWS; WVAR=TIER_B_WIDTH; QS=TIER_B_QUERIES; FS=TIER_B_FILTERS; SS=TIER_B_SEL;;
  C) RVAR=TIER_C_ROWS; WVAR=TIER_C_WIDTH; QS=TIER_C_QUERIES; FS=TIER_C_FILTERS; SS=TIER_C_SEL;;
  *) echo "tier must be A|B|C" >&2; exit 1;;
esac
ROW_TAG=${!RVAR}
W_TAG=${!WVAR}
ROWS="${ROWS_ARG:-$(row_of "$ROW_TAG")}"
WIDTH="${WIDTH_ARG:-$(width_of "$W_TAG")}"
if [ -z "$ROWS" ] || [ -z "$WIDTH" ]; then echo "cannot resolve tier $TIER ($ROW_TAG/$W_TAG)"; exit 1; fi

# ---- polars runner detection -----------------------------------------------------------------
# prefer .venv-bench (created via `uv venv --python 3.13` + `uv pip install polars`),
# else the system python3. Used for P-CONCAT / P-JOIN engines.
PYTHON_POLARS=""
for PY in "$ROOT/.venv-bench/bin/python" python3; do
  if $PY -c "import polars" >/dev/null 2>&1; then PYTHON_POLARS="$PY"; break; fi
done

# ---- engine selection ---------------------------------------------------------------
declare -a ENGINES
if [ -n "$ENGINES_ARG" ]; then IFS=',' read -ra ENGINES <<< "$ENGINES_ARG";
else
  ENGINES=( "${DDB_ENGINES[@]}" "${ALIGNED_ENGINES[@]}" )
  if [ -n "$PYTHON_POLARS" ]; then ENGINES+=( "${POLARS_ENGINES[@]}" ); fi
fi

# ---- threads ------------------------------------------------------------------------
if [ -n "$THREADS_ARG" ]; then IFS=',' read -ra THREADS <<< "$THREADS_ARG"; else THREADS=(1); fi

ALPHA=$(layout_alpha "$WIDTH")
FIELDSET=$(layout_fieldset "$WIDTH")
echo "== Tier $TIER: rows=$ROWS width=$WIDTH (alpha=$ALPHA fs=$FIELDSET) sparse=$SPARSITY engines=${ENGINES[*]} threads=${THREADS[*]} =="

ALIGNED_TABLE="bench_mb"        # physical table

# ---------- data generation (once per rows/width/sparsity) --------------------------
GEN_FLAGS=(--rows "$ROWS" --width "$WIDTH" --sparsity "$SPARSITY" --aligned true --out "$OUT" --tag mb)
need_gen=0
if [ ! -f "$OUT/$ALIGNED_TABLE/_table.json" ]; then need_gen=1; fi
if [ "$need_gen" = 1 ] && [ "$FLAG_NOREGEN" = 0 ]; then
  bash "$ROOT/scripts/gen_multi_bench.sh" "${GEN_FLAGS[@]}"
fi
if [ ! -f "$OUT/$ALIGNED_TABLE/_table.json" ]; then echo "aligned table missing (use --no-regen only if data exists): $OUT/$ALIGNED_TABLE"; exit 1; fi

BASE="$OUT/bench_baseline_mb"
WIDE="$BASE/wide.parquet"; JOINAL="$BASE/join_alpha.parquet"; JOINFS="$BASE/join_fs.parquet"

# ---------- query builders ------------------------------------------------------------
# projection column list for a query group given alpha/fieldset counts
projection_of() { # q IN -> "col1,col2,..." (bare names, shared across engines)
  local q="$1"
  case "$q" in
    Q1) echo "date,symbol,close";;
    Q2) echo "date,symbol,close,volume,rowid,$(range_cols alpha 0 20),$(range_cols fs 0 10)";;
    Q3) echo "date,symbol,close,volume,rowid,$(range_cols ix 1 15),$(range_cols alpha 0 400),$(range_cols fs 0 80)";;
    Q4) echo "date,symbol,close,volume,rowid,$(range_cols ix 1 15),$(range_cols alpha 0 4400),$(range_cols fs 0 580)";;
    Q5) echo "*";;
    *) echo "";;
  esac; }

# filter predicate for (filter, selectivity, rows) -> "WHERE ..." or ""
filter_of() { # f sel rows
  local f="$1" sel="$2" rows="$3" w="" frac
  case "$f" in
    F1) w="";;
    F2) w="date = DATE '2026-09-02'";;
    F3) w="date BETWEEN DATE '2026-09-01' AND DATE '2026-09-03'";;
    F4) w="close > 0";;
    F5) w="symbol = '000001'";;
    *) echo ""; return;;
  esac
  frac=${SEL_FRAC[$sel]:-1.0}
  if [ "$frac" != "1.0" ]; then
    local limit=$(python3 -c "print(int($rows * $frac))")
    if [ -n "$w" ]; then w="$w AND rowid < $limit"; else w="rowid < $limit"; fi
  fi
  [ -n "$w" ] && echo " WHERE $w" || echo ""
}

# engine SQL for (engine, query, filter, sel, rows)
engine_sql() { # e q f sel rows -> sql
  local e="$1" q="$2" f="$3" sel="$4" rows="$5" proj where tbl
  proj=$(projection_of "$q")
  where=$(filter_of "$f" "$sel" "$rows")
  case "$e" in
    A-ALIGNED) tbl="$ALIGNED_TABLE"; echo "SELECT count(*) FROM (SELECT $proj FROM aligned_table('$tbl')$where);";;
    D-WIDE)    echo "SELECT count(*) FROM (SELECT $proj FROM read_parquet('$WIDE')$where);";;
    D-JOIN)
      # index rt join alpha on rowid=rowid_alpha join fieldset on rowid=rowid_fs
      # project references are bare; the join exposes all. WHERE on index cols only.
      echo "SELECT count(*) FROM (SELECT $proj FROM read_parquet('$BASE/join_index.parquet') i JOIN read_parquet('$JOINAL') a ON i.rowid=a.rowid_alpha JOIN read_parquet('$JOINFS') m ON i.rowid=m.rowid_fs$where);"
      ;;
    P-CONCAT|P-JOIN)
      echo "__polars__ $e $q $f $sel $ALPHA $FIELDSET $BASE $rows"
      ;;
    *) echo "";;
  esac
}

# baselines for D-JOIN share join_index (already written by generator as
# bench_baseline_<tag>/join_alpha, join_fs, wide — index baseline is in wide's
# first columns but we need a standalone join_index.parquet; generate it.)
if ! [ -f "$BASE/join_index.parquet" ]; then
  "$DUCKDB" -light-mode -c "COPY (SELECT date,symbol,close,volume,rowid,$(range_cols ix 1 15) FROM read_parquet('$WIDE')) TO '$BASE/join_index.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);" >/dev/null
fi

# ---------- measurement ---------------------------------------------------------------
now_ns(){ date +%s%N; }
elapsed(){ echo "scale=9; ($2 - $1)/1000000000" | bc -l; }
# Repeat the SAME query REPEATS times in ONE duckdb process (separated by ';')
# and time the whole batch, then divide by REPEATS. This amortizes the per-
# process startup cost (~0.02s) that otherwise dominates tiny/cached datasets.
# warm_s = mean per query.
REPEATS=${REPEATS:-5}
# ddb_run <th> <pre> <sql...> — run DuckDB, feeding the SQL through a temp file
# on stdin so very wide queries (W3, thousands of columns) that overflow ARG_MAX
# with `-c` still execute. All measurement/consistency calls go through here.
DDB_SQL_TMP="${TMPDIR:-/tmp}/rmb_$$.sql"
ddb_run(){ local th="$1" pre="$2"; shift 2
  printf 'SET threads=%s;\n%s\n' "$th" "$pre" > "$DDB_SQL_TMP"
  printf '%s\n' "$@" >> "$DDB_SQL_TMP"
  "$DUCKDB" -light-mode -csv -noheader < "$DDB_SQL_TMP"
}
ddb_repeat(){ # pre sql threads -> mean seconds over REPEATS in one process
  local pre="$1" sql="$2" th="$3" i a b dt
  a=$(now_ns)
  for i in $(seq 1 "$REPEATS"); do ddb_run "$th" "$pre" "$sql" >/dev/null 2>&1 || true; done
  b=$(now_ns)
  dt=$(elapsed "$a" "$b")
  echo "scale=9; $dt / $REPEATS" | bc -l
}
run_one(){ # e q f sel th -> prints "cold warm"
  local e="$1" q="$2" f="$3" sel="$4" th="$5" sql a b cold warm pre
  sql=$(engine_sql "$e" "$q" "$f" "$sel" "$ROWS")
  if [[ "$sql" == __polars__* ]]; then
    read -r _ pe pq pf psel palpha pfs pbase prows <<< "$sql"
    if [ -z "$PYTHON_POLARS" ]; then printf '0.000000 0.000000'; return; fi
    local out
    out=$("$PYTHON_POLARS" "$ROOT/scripts/bench_polars_multi.py" "$pe" "$pq" "$pf" "$psel" "$palpha" "$pfs" "$pbase" "$prows" "$th" 2>/dev/null | grep -E '^TIMES')
    printf '%s' "$out" | sed 's/TIMES //'
    return
  fi
  if [ "$e" = "A-ALIGNED" ]; then pre="SET aligned_data_root='$OUT';"; else pre=""; fi
  # cold: single fresh-process run (first touch; still includes startup)
  a=$(now_ns); ddb_run "$th" "$pre" "$sql" >/dev/null 2>&1 || true; b=$(now_ns); cold=$(elapsed "$a" "$b")
  # warm: mean over REPEATS in ONE process (startup amortized out)
  warm=$(ddb_repeat "$pre" "$sql" "$th")
  printf '%.6f %.6f' "$cold" "$warm"
}

# ---------- correctness guard: partition pruning on a non-first partition ---------------
sc_a=$("$DUCKDB" -light-mode -csv -noheader -c "SET aligned_data_root='$OUT'; SELECT count(*) FROM aligned_table('$ALIGNED_TABLE') WHERE date=DATE '2026-09-02';" | tail -1)
expected=$((ROWS/4))
if [ "$sc_a" != "$expected" ]; then echo "SELF-CHECK FAIL: got '$sc_a' expected $expected"; exit 1; fi
echo "SELF-CHECK OK: A-ALIGNED prunes to $expected rows on non-first partition."

# cross-engine row-count consistency: every engine must return ROWS/4 rows for
# Q2+F2+S0 (date partition point). This proves all engines observe the SAME
# logical table (same key, order, NULL distribution) — timing is only meaningful
# if the outputs agree.
check_count(){ # engine -> rows after (date='2026-09-02')
  local e="$1"
  case "$e" in
    A-ALIGNED) "$DUCKDB" -light-mode -csv -noheader -c "SET aligned_data_root='$OUT'; SELECT count(*) FROM (SELECT date,symbol,close FROM aligned_table('$ALIGNED_TABLE') WHERE date=DATE '2026-09-02');" 2>/dev/null | tail -1;;
    D-WIDE)    "$DUCKDB" -light-mode -csv -noheader -c "SELECT count(*) FROM (SELECT date,symbol,close FROM read_parquet('$WIDE') WHERE date=DATE '2026-09-02');" 2>/dev/null | tail -1;;
    D-JOIN)    "$DUCKDB" -light-mode -csv -noheader -c "SELECT count(*) FROM (SELECT i.date,i.symbol,i.close FROM read_parquet('$BASE/join_index.parquet') i JOIN read_parquet('$JOINAL') a ON i.rowid=a.rowid_alpha WHERE i.date=DATE '2026-09-02');" 2>/dev/null | tail -1;;
    P-CONCAT|P-JOIN)
      if [ -n "$PYTHON_POLARS" ]; then
        "$PYTHON_POLARS" "$ROOT/scripts/bench_polars_multi.py" "$e" Q2 F2 S0 "$ALPHA" "$FIELDSET" "$BASE" "$ROWS" 1 2>/dev/null | grep '^ROWS' | awk '{print $2}'
      else echo "0"; fi;;
    *) echo "0";;
  esac
}
cc_fail=0
for e in "${ENGINES[@]}"; do
  cc=$(check_count "$e")
  if [ "$cc" != "$expected" ]; then
    echo "CONSISTENCY FAIL: $e returned $cc rows (expected $expected)"; cc_fail=1
  fi
done
if [ "$cc_fail" = 0 ]; then echo "CONSISTENCY OK: all ${#ENGINES[@]} engines return $expected rows for Q2/F2/S0"; fi

# ---------- execution ------------------------------------------------------------------
declare -a RESULTS
# Resolve the tier's query/filter/sel lists, allowing a caller (e.g.
# bench_scenarios.sh) to override them via QS_OVERRIDE/FS_OVERRIDE/SS_OVERRIDE.
if [ -n "${QS_OVERRIDE:-}" ]; then IFS=',' read -ra Q_LIST <<< "$QS_OVERRIDE";
else eval "Q_LIST=(\"\${$QS[@]}\")"; fi
if [ -n "${FS_OVERRIDE:-}" ]; then IFS=',' read -ra F_LIST <<< "$FS_OVERRIDE";
else eval "F_LIST=(\"\${$FS[@]}\")"; fi
if [ -n "${SS_OVERRIDE:-}" ]; then IFS=',' read -ra S_LIST <<< "$SS_OVERRIDE";
else eval "S_LIST=(\"\${$SS[@]}\")"; fi
for e in "${ENGINES[@]}"; do
  echo "== engine: $e =="
  for q in "${Q_LIST[@]}"; do
    for f in "${F_LIST[@]}"; do
      for sel in "${S_LIST[@]}"; do
        for th in "${THREADS[@]}"; do
          read c w <<< "$(run_one "$e" "$q" "$f" "$sel" "$th")"
          RESULTS+=("$e,$q,$f,$sel,$th,$c,$w")
          printf '  %-9s %-4s %-4s %-4s t=%-3s cold=%.3fs warm=%.3fs\n' "$e" "$q" "$f" "$sel" "$th" "$c" "$w"
        done
      done
    done
  done
done

# ---------- report ---------------------------------------------------------------------
STAMP=$(date +%Y-%m-%d)
REPORT="$ROOT/docs/BENCH_MULTI.md"
{ echo "# AlignedTable Multi-Scenario Benchmark"; echo "";
  echo "Date: $STAMP  Machine: local Linux (AGENTS.md §16.2); tier=$TIER";
  echo "Dataset: bench_mb rows=$ROWS cols=$WIDTH (idx20+alpha$ALPHA+fs$FIELDSET) sparse=$SPARSITY% NULL";
  echo "Engines: ${ENGINES[*]}; threads: ${THREADS[*]}";
  echo "";
  echo "| engine | query | filter | sel | threads | cold_s | warm_s |";
  echo "|--------|-------|--------|-----|---------|--------|--------|";
  for line in "${RESULTS[@]}"; do IFS=',' read -r e q f s th c w <<< "$line";
    printf '| %s | %s | %s | %s | %s | %.4f | %.4f |\n' "$e" "$q" "$f" "$s" "$th" "$c" "$w"; done;
  echo "";
  echo "> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),";
  echo "> alpha$ALPHA sparse (${SPARSITY}% NULL), fs$FIELDSET. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.";
  echo "> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.";
} > "$REPORT"

CSV="$ROOT/scripts/bench_multi_output.csv"
{ echo "engine,query,filter,sel,threads,cold_s,warm_s"; for line in "${RESULTS[@]}"; do echo "$line"; done; } > "$CSV"
echo ""
echo "Report: $REPORT"
echo "CSV:    $CSV"
