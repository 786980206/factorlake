#!/usr/bin/env bash
# test_aligned.sh — Linux equivalent of test_aligned.ps1 (Phase 1-4 acceptance).
# Requires: scripts/gen_testdata.sh has been run, $ROOT/duckdb/build/duckdb built.
# Usage: bash scripts/test_aligned.sh   (env: DUCKDB, ALIGNED_DATA_ROOT)
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib_aligned.sh"

# For SQL whose identifiers contain quotes/backticks that -c would mangle,
# pipe via a temp file (DuckDB 1.5.4 rejects backtick identifiers; use double quotes).
run_duckdb_file() {
  local tmp
  tmp="$(mktemp)"
  printf '%s\n' "$1" > "$tmp"
  "$DUCKDB" -light-mode -csv -noheader < "$tmp" 2>&1
  local rc=$?
  rm -f "$tmp"
  return $rc
}
first_val() { printf '%s\n' "$1" | grep -E '^[0-9]' | head -n1; }

# --- counts + cross-group alignment -----------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; WITH t AS (SELECT * FROM aligned_table('cnstk_ixday')) SELECT count(*) AS c, count(alpha001) AS a1, count(alpha099) AS a99, sum(CASE WHEN rowid != rowid_alpha OR rowid != rowid_ma THEN 1 ELSE 0 END) AS mis FROM t;")
vals=$(first_val "$out" | tr ',' ' ')
expect_equal 'total rows' "$(echo "$vals" | awk '{print $1}')" "6000"
expect_equal 'alpha001 non-null (r%5==0)' "$(echo "$vals" | awk '{print $2}')" "1200"
expect_equal 'alpha099 non-null (day18 only, r%3==0)' "$(echo "$vals" | awk '{print $3}')" "1000"
expect_equal 'misaligned rows' "$(echo "$vals" | awk '{print $4}')" "0"

# --- schema evolution --------------------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 0;")
if printf '%s' "$out" | grep -qE '^(NULL|[[:space:]]*)$'; then
  echo 'PASS: alpha099 NULL in old partition'
else
  echo "FAIL: alpha099 in old partition ($out)"; failures=$((failures+1))
fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 3000;")
if printf '%s' "$out" | grep -q '3\.001'; then echo 'PASS: alpha099 value in new partition'; else echo "FAIL: alpha099 value ($out)"; failures=$((failures+1)); fi

# --- boundary rows (part boundary 2047/2048, RG boundary 4095/4096) ----------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT symbol FROM aligned_table('cnstk_ixday') WHERE rowid IN (2047, 2048) ORDER BY rowid;")
if printf '%s' "$out" | grep -q '002048' && printf '%s' "$out" | grep -q '002049'; then
  echo 'PASS: part boundary rows'
else echo "FAIL: part boundary ($out)"; failures=$((failures+1)); fi

# --- aligned_scan variant ----------------------------------------------------
out=$(run_duckdb "SELECT count(*) FROM aligned_scan('$DATA_ROOT', 'cnstk_ixday');")
if printf '%s' "$out" | grep -qE '^6000$'; then echo 'PASS: aligned_scan(root, name) variant'; else echo "FAIL: aligned_scan variant ($out)"; failures=$((failures+1)); fi

# --- aggregates / repeat queries ---------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT max(close), count(*) FROM aligned_table('cnstk_ixday'); SELECT count(*) FROM aligned_table('cnstk_ixday');")
counts=$(printf '%s\n' "$out" | grep -cE '^6000$')
if printf '%s\n' "$out" | grep -qE '^3000\.0,6000' && [ "$counts" -ge 1 ]; then
  echo 'PASS: aggregates + repeat queries'
else echo "FAIL: aggregates/repeat ($out)"; failures=$((failures+1)); fi

# --- projection pushdown (Phase 2) -------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha001, ma20, date FROM aligned_table('cnstk_ixday') WHERE rowid IN (0, 4095) ORDER BY rowid;")
if printf '%s' "$out" | grep -q '0\.02' && printf '%s' "$out" | grep -q '40\.97' \
  && printf '%s' "$out" | grep -q '2026-08-17' && printf '%s' "$out" | grep -q '2026-08-18'; then
  echo 'PASS: projected multi-group query values'
else echo "FAIL: projected values ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(rowid_ma) FROM aligned_table('cnstk_ixday');")
if printf '%s' "$out" | grep -qE '^6000$'; then echo 'PASS: single-group projection'; else echo "FAIL: single-group projection ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 3000;")
if printf '%s' "$out" | grep -qE '^3\.001$'; then echo 'PASS: projection + schema evolution'; else echo "FAIL: projection + evolution ($out)"; failures=$((failures+1)); fi

# --- parallel scan (Phase 4) -------------------------------------------------
out1=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=1; SELECT count(*), count(alpha001), count(alpha099), sum(rowid) FROM aligned_table('cnstk_ixday');")
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(*), count(alpha001), count(alpha099), sum(rowid) FROM aligned_table('cnstk_ixday');")
if [ "$out1" = "$out8" ] && printf '%s' "$out1" | grep -qE '^6000,1200,1000,17997000$'; then
  echo 'PASS: parallel scan aggregates (threads 1 == 8)'
else echo "FAIL: parallel aggregates ($out1 / $out8)"; failures=$((failures+1)); fi
out1=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=1; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;")
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;")
if [ "$out1" = "$out8" ] && printf '%s' "$out1" | grep -qE '^11$'; then
  echo 'PASS: parallel scan filters (threads 1 == 8)'
else echo "FAIL: parallel filters ($out1 / $out8)"; failures=$((failures+1)); fi
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(rowid_ma), count(alpha099) FROM aligned_table('cnstk_ixday');")
if printf '%s' "$out8" | grep -qE '^6000,1000$'; then echo 'PASS: parallel projection + schema evolution'; else echo "FAIL: parallel projection ($out8)"; failures=$((failures+1)); fi

# --- metadata cache (Phase 4) -------------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT current_setting('parquet_metadata_cache');")
if printf '%s' "$out" | grep -qE '^true$'; then echo 'PASS: parquet metadata cache default on'; else echo "FAIL: metadata cache default ($out)"; failures=$((failures+1)); fi

# --- partition pruning correctness (Phase 3/6) ---------------------------------
# The generator splits the 6000 rows across two daily partitions:
#   date=2026-08-17 -> rows [0,3000), date=2026-08-18 -> rows [3000,6000).
# A date equality filter MUST prune the scan so only the matching partition's
# rows are reachable. This guards the pruning path that Phase 6 thrust depends
# on (s25 must not collapse to a full scan).
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*), min(rowid), max(rowid) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-18';")
if printf '%s' "$out" | grep -qE '^3000,3000,5999$'; then echo 'PASS: partition pruning day18 -> rows [3000,6000) only'; else echo "FAIL: partition pruning day18 ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-18' AND rowid = 100;")
if printf '%s' "$out" | grep -qE '^0$'; then echo 'PASS: pruned-then-rowfilter row in wrong partition returns 0'; else echo "FAIL: pruned+rowfilter ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-18' AND rowid = 4000;")
if printf '%s' "$out" | grep -qE '^1$'; then echo 'PASS: pruned+rowfilter row in matching partition returns 1'; else echo "FAIL: pruned+rowfilter match ($out)"; failures=$((failures+1)); fi

# --- column-name rules (contract 搂2.2e) --------------------------------------
# e1: columns duplicated with index resolve to the index copy (authoritative)
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT close FROM aligned_table('cnstk_ixday') WHERE rowid = 100;")
if printf '%s' "$out" | grep -qE '^50\.5$'; then echo 'PASS: e1 bare close = index (authoritative)'; else echo "FAIL: e1 bare close ($out)"; failures=$((failures+1)); fi
# e1: the index-duplicated copy is ignored in the non-index group (qualified ref must fail)
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT \"factor.alpha101.close\" FROM aligned_table('cnstk_ixday') LIMIT 1;" "not found"; then
  echo 'PASS: e1 qualified alpha101.close rejected'
else echo 'FAIL: e1 qualified alpha101.close'; failures=$((failures+1)); fi
# e2: bare name of a cross-group duplicate must fail
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT vwap FROM aligned_table('cnstk_ixday') LIMIT 1;" "not found"; then
  echo 'PASS: e2 bare vwap rejected'
else echo 'FAIL: e2 bare vwap'; failures=$((failures+1)); fi
# e2: qualified names resolve per group
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT \"factor.alpha101.vwap\" AS a, \"fieldset.ma.vwap\" AS m FROM aligned_table('cnstk_ixday') WHERE rowid = 100;")
if printf '%s' "$out" | grep -qE '^12\.625,3\.15625$'; then echo 'PASS: e2 qualified vwap per group'; else echo "FAIL: e2 qualified vwap ($out)"; failures=$((failures+1)); fi
# e3: bare names of non-duplicated columns work
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT rowid_alpha, ma5 FROM aligned_table('cnstk_ixday') WHERE rowid = 100;")
if printf '%s' "$out" | grep -qE '^100,0\.0$'; then echo 'PASS: e3 bare non-duplicated columns'; else echo "FAIL: e3 bare columns ($out)"; failures=$((failures+1)); fi

# --- directory rules (contract 搂2.1b/c) --------------------------------------
# 搂2.1d: '_tmp' stray parts are ignored (proven by total rows = 6000 above)
# 搂2.1b: a table without the mandatory index group must fail
BADIDX="$DATA_ROOT/badidx/_table.json"
mkdir -p "$(dirname "$BADIDX")"
printf '%s\n' '{"name":"badidx","version":1,"groups":["factor/alpha101"]}' > "$BADIDX"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badidx');" "mandatory group 'index'"; then
  echo 'PASS: 搂2.1b missing index group rejected'
else echo 'FAIL: 搂2.1b missing index'; failures=$((failures+1)); fi
rm -rf "$DATA_ROOT/badidx"
# 搂2.1c: a one-level non-index group must fail
BADLVL="$DATA_ROOT/badlvl/_table.json"
mkdir -p "$(dirname "$BADLVL")"
printf '%s\n' '{"name":"badlvl","version":1,"groups":["single","index"]}' > "$BADLVL"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badlvl');" "two-level path"; then
  echo 'PASS: 搂2.1c one-level group rejected'
else echo 'FAIL: 搂2.1c one-level group'; failures=$((failures+1)); fi
rm -rf "$DATA_ROOT/badlvl"

# --- optional _table.json + full-alignment contract ---------------------------
# No manifest: groups are discovered from the file layout and FULL alignment is
# enforced (the only supported contract). The testdata layout (every group 2
# parts x 3000 rows) satisfies it.
V3TABLE="$DATA_ROOT/v3_notable"
cp -r "$DATA_ROOT/cnstk_ixday" "$V3TABLE"
rm -f "$V3TABLE/_table.json"
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; WITH t AS (SELECT * FROM aligned_table('v3_notable')) SELECT count(*), sum(CASE WHEN rowid != rowid_alpha OR rowid != rowid_ma THEN 1 ELSE 0 END) FROM t;")
if printf '%s' "$out" | grep -qE '^6000,0$'; then echo 'PASS: no-manifest rows (defaults + full alignment)'; else echo "FAIL: no-manifest rows ($out)"; failures=$((failures+1)); fi
# A non-aligned table must fail fast (never silently degrade): drop one alpha
# part so the group part counts diverge (index 2 vs alpha 1).
V3BAD="$DATA_ROOT/v3_bad"
cp -r "$DATA_ROOT/cnstk_ixday" "$V3BAD"
rm -f "$V3BAD/factor/alpha101/year=2026/month=08/date=2026-08-17/part-000000.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('v3_bad');" "full alignment required"; then
  echo 'PASS: non-aligned table rejected fail-fast'
else echo 'FAIL: non-aligned table'; failures=$((failures+1)); fi
rm -rf "$V3TABLE" "$V3BAD"

# --- error cases (expected failures 鈥?must not terminate the script) ---------
if run_duckdb_expect_error "SELECT * FROM aligned_table('no_such_table');" "no data root configured"; then
  echo 'PASS: missing root error'
else echo 'FAIL: missing root error'; failures=$((failures+1)); fi
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('no_such_table');" "_table.json"; then
  echo 'PASS: missing table error'
else echo 'FAIL: missing table error'; failures=$((failures+1)); fi

summary
