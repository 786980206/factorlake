#!/usr/bin/env bash
# test_aligned.sh — Linux equivalent of test_aligned.ps1 (v5 partition-aligned).
# Requires: scripts/gen_testdata.sh has been run, $ROOT/duckdb/build/duckdb built.
# Usage: bash scripts/test_aligned.sh   (env: DUCKDB, ALIGNED_DATA_ROOT)
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib_aligned.sh"

# For SQL whose identifiers contain quotes that -c would mangle,
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
# Layout: index month=2026-07 (1 part x 2000) + month=2026-08 (2 parts x 2000);
# alpha same months (07: 1 part 2000, 08: 1 part 4000 — last-part row count
# differs from index, only the partition total is contractual); ma only
# month=2026-08 (4000) — rows [0,2000) read as NULL for ma columns.
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; WITH t AS (SELECT * FROM aligned_table('cnstk_ixday')) SELECT count(*) AS c, count(alpha001) AS a1, count(alpha099) AS a99, count(rowid_ma) AS ma, sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) AS mis FROM t;")
vals=$(first_val "$out" | tr ',' ' ')
expect_equal 'total rows' "$(echo "$vals" | awk '{print $1}')" "6000"
expect_equal 'alpha001 non-null (r%5==0)' "$(echo "$vals" | awk '{print $2}')" "1200"
expect_equal 'alpha099 non-null (last part [4000,6000), r%3==0)' "$(echo "$vals" | awk '{print $3}')" "666"
expect_equal 'ma rows (missing month=2026-07 -> NULL)' "$(echo "$vals" | awk '{print $4}')" "4000"
expect_equal 'misaligned rows (alpha)' "$(echo "$vals" | awk '{print $5}')" "0"

# --- missing partition NULL fill (partition-aligned contract) ----------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT rowid, ma5 FROM aligned_table('cnstk_ixday') WHERE rowid IN (0, 1999, 2000, 5999) ORDER BY rowid;")
rows=$(printf '%s\n' "$out" | grep -E '^[0-9]' | tr '\n' ';')
if printf '%s' "$rows" | grep -q '^0,NULL;' && printf '%s' "$rows" | grep -q '1999,NULL;' \
  && printf '%s' "$rows" | grep -q '2000,0.0;' && printf '%s' "$rows" | grep -q '5999,1.9;'; then
  echo 'PASS: missing partition rows NULL-filled, present partition reads values'
else echo "FAIL: missing partition fill ($out)"; failures=$((failures+1)); fi

# --- last-part row count may differ across groups (partition total agrees) ---
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*), sum(rowid_alpha) FROM aligned_table('cnstk_ixday') WHERE rowid >= 2000;")
if printf '%s' "$out" | grep -qE '^4000,15998000$'; then echo 'PASS: alpha 08 (1 part x 4000) aligns with index 08 (2 parts x 2000)'; else echo "FAIL: cross-group last-part mismatch ($out)"; failures=$((failures+1)); fi

# --- schema evolution --------------------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 0;")
if printf '%s' "$out" | grep -qE '^(NULL|[[:space:]]*)$'; then
  echo 'PASS: alpha099 NULL in old partition'
else
  echo "FAIL: alpha099 in old partition ($out)"; failures=$((failures+1))
fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 4500;")
if printf '%s' "$out" | grep -q '4\.501'; then echo 'PASS: alpha099 value in evolution part'; else echo "FAIL: alpha099 value ($out)"; failures=$((failures+1)); fi

# --- boundary rows (partition boundary 2000, part boundary 4000) -------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT symbol FROM aligned_table('cnstk_ixday') WHERE rowid IN (1999, 2000, 3999, 4000) ORDER BY rowid;")
if printf '%s' "$out" | grep -q '002000' && printf '%s' "$out" | grep -q '002001' \
  && printf '%s' "$out" | grep -q '004000' && printf '%s' "$out" | grep -q '004001'; then
  echo 'PASS: partition/part boundary rows'
else echo "FAIL: boundary rows ($out)"; failures=$((failures+1)); fi

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
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha001, ma20, date FROM aligned_table('cnstk_ixday') WHERE rowid IN (2000, 4095) ORDER BY rowid;")
if printf '%s' "$out" | grep -q '20\.02' && printf '%s' "$out" | grep -q '40\.97' \
  && printf '%s' "$out" | grep -q '0\.375' && printf '%s' "$out" | grep -q '2026-08-01'; then
  echo 'PASS: projected multi-group query values'
else echo "FAIL: projected values ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(rowid_ma) FROM aligned_table('cnstk_ixday');")
if printf '%s' "$out" | grep -qE '^4000$'; then echo 'PASS: single-group projection (ma missing partition)'; else echo "FAIL: single-group projection ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 4500;")
if printf '%s' "$out" | grep -qE '^4\.501$'; then echo 'PASS: projection + schema evolution'; else echo "FAIL: projection + evolution ($out)"; failures=$((failures+1)); fi

# --- partition pruning (Phase 3, single-level month=) ------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-01';")
if printf '%s' "$out" | grep -qE '^4000$'; then echo 'PASS: partition pruning month=2026-08 (4000 rows)'; else echo "FAIL: pruning 08 ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-07-01';")
if printf '%s' "$out" | grep -qE '^2000$'; then echo 'PASS: partition pruning month=2026-07 (2000 rows)'; else echo "FAIL: pruning 07 ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-01' AND rowid < 100;")
if printf '%s' "$out" | grep -qE '^0$'; then echo 'PASS: pruning + row filter wrong partition = 0'; else echo "FAIL: pruning+filter ($out)"; failures=$((failures+1)); fi
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-01' AND rowid >= 2000;")
if printf '%s' "$out" | grep -qE '^4000$'; then echo 'PASS: pruning + row filter matching partition'; else echo "FAIL: pruning+match ($out)"; failures=$((failures+1)); fi

# --- parallel scan (Phase 4) -------------------------------------------------
out1=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=1; SELECT count(*), count(alpha001), count(alpha099), count(rowid_ma), sum(rowid) FROM aligned_table('cnstk_ixday');")
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(*), count(alpha001), count(alpha099), count(rowid_ma), sum(rowid) FROM aligned_table('cnstk_ixday');")
if [ "$out1" = "$out8" ] && printf '%s' "$out1" | grep -qE '^6000,1200,666,4000,17997000$'; then
  echo 'PASS: parallel scan aggregates (threads 1 == 8)'
else echo "FAIL: parallel aggregates ($out1 / $out8)"; failures=$((failures+1)); fi
out1=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=1; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;")
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;")
if [ "$out1" = "$out8" ] && printf '%s' "$out1" | grep -qE '^11$'; then
  echo 'PASS: parallel scan filters (threads 1 == 8)'
else echo "FAIL: parallel filters ($out1 / $out8)"; failures=$((failures+1)); fi
out8=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SET threads=8; SELECT count(rowid_ma), count(alpha099) FROM aligned_table('cnstk_ixday');")
if printf '%s' "$out8" | grep -qE '^4000,666$'; then echo 'PASS: parallel projection + missing partition + schema evolution'; else echo "FAIL: parallel projection ($out8)"; failures=$((failures+1)); fi

# --- metadata cache (Phase 4) -------------------------------------------------
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT current_setting('parquet_metadata_cache');")
if printf '%s' "$out" | grep -qE '^true$'; then echo 'PASS: parquet metadata cache default on'; else echo "FAIL: metadata cache default ($out)"; failures=$((failures+1)); fi

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
# e2: qualified names resolve per group (row 2000 = inside ma's month=2026-08)
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT \"factor.alpha101.vwap\" AS a, \"fieldset.ma.vwap\" AS m FROM aligned_table('cnstk_ixday') WHERE rowid = 2000;")
if printf '%s' "$out" | grep -qE '^250\.125,62\.53125$'; then echo 'PASS: e2 qualified vwap per group'; else echo "FAIL: e2 qualified vwap ($out)"; failures=$((failures+1)); fi
# e3: bare names of non-duplicated columns work (ma5 NULL in missing partition)
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT rowid_alpha, ma5 FROM aligned_table('cnstk_ixday') WHERE rowid = 2000;")
if printf '%s' "$out" | grep -qE '^2000,0\.0$'; then echo 'PASS: e3 bare non-duplicated columns'; else echo "FAIL: e3 bare columns ($out)"; failures=$((failures+1)); fi

# --- directory rules (contract 搂2.1b/c) --------------------------------------
# 搂2.1d: '_tmp' stray parts are ignored (proven by total rows = 6000 above)
# 搂2.1b: a table without the mandatory index group must fail (has alpha parts
# but no index parts at all)
BADIDX="$DATA_ROOT/badidx"
mkdir -p "$BADIDX/factor/alpha101/month=2026-07"
cp "$DATA_ROOT/cnstk_ixday/factor/alpha101/month=2026-07/0000-0000002000.parquet" "$BADIDX/factor/alpha101/month=2026-07/0000-0000002000.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badidx');" "mandatory group 'index'"; then
  echo 'PASS: 搂2.1b missing index group rejected'
else echo 'FAIL: 搂2.1b missing index'; failures=$((failures+1)); fi
rm -rf "$DATA_ROOT/badidx"
# 搂2.1c: a one-level non-index group must fail (a real part file in group 'single')
BADLVL="$DATA_ROOT/badlvl"
mkdir -p "$BADLVL/index/month=2026-07" "$BADLVL/index/month=2026-08" "$BADLVL/single/month=2026-07"
cp "$DATA_ROOT/cnstk_ixday/index/month=2026-07/0000-0000002000.parquet" "$BADLVL/index/month=2026-07/0000-0000002000.parquet"
cp "$DATA_ROOT/cnstk_ixday/index/month=2026-08/0000-0000002000.parquet" "$BADLVL/index/month=2026-08/0000-0000002000.parquet"
cp "$DATA_ROOT/cnstk_ixday/index/month=2026-08/0001-0000002000.parquet" "$BADLVL/index/month=2026-08/0001-0000002000.parquet"
cp "$DATA_ROOT/cnstk_ixday/index/month=2026-07/0000-0000002000.parquet" "$BADLVL/single/month=2026-07/0000-0000002000.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badlvl');" "two-level path"; then
  echo 'PASS: 2.1c one-level group rejected'
else echo 'FAIL: 2.1c one-level group'; failures=$((failures+1)); fi
rm -rf "$DATA_ROOT/badlvl"

# v6: a non-conforming part file name (not "{idx:04d}-{rows:10d}.parquet") must
# fail fast — the file-name row counts are the contract.
BADNAME="$DATA_ROOT/badname"
cp -r "$DATA_ROOT/cnstk_ixday" "$BADNAME"
mv "$BADNAME/index/month=2026-08/0001-0000002000.parquet" "$BADNAME/index/month=2026-08/part-000001.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badname');" "self-desc"; then
  echo 'PASS: v6 non-conforming part name rejected'
else echo 'FAIL: v6 non-conforming part name'; failures=$((failures+1)); fi
rm -rf "$BADNAME"

# v6: the index group's indexes must be consecutive from 0000 (a gap in the
# index is a contract violation — the index defines the row space).
BADIDX="$DATA_ROOT/badidx"
cp -r "$DATA_ROOT/cnstk_ixday" "$BADIDX"
rm -f "$BADIDX/index/month=2026-08/0000-0000002000.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badidx');" "consecutive from 0000"; then
  echo 'PASS: v6 index gap rejected'
else echo 'FAIL: v6 index gap'; failures=$((failures+1)); fi
rm -rf "$BADIDX"

# v6: a SHARED index must agree on its row count across groups (alpha 08 gains
# 0003-3000 -> partition total 5000 != index 4000 -> fail-fast).
BADROWS="$DATA_ROOT/badrows"
cp -r "$DATA_ROOT/cnstk_ixday" "$BADROWS"
cp "$BADROWS/factor/alpha101/month=2026-08/0002-0000002000.parquet" "$BADROWS/factor/alpha101/month=2026-08/0003-0000003000.parquet"
rm -f "$BADROWS/factor/alpha101/month=2026-08/0002-0000002000.parquet"
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('badrows');" "must agree"; then
  echo 'PASS: v6 cross-group row-count mismatch rejected'
else echo 'FAIL: v6 cross-group row-count mismatch'; failures=$((failures+1)); fi
rm -rf "$BADROWS"

# v8: the index schema's SECOND column must be DATE/TIMESTAMP (rebuild the
# index 2026-08 part with col0=symbol, col1=close (DOUBLE), date moved to col2).
BADDATE="$DATA_ROOT/baddate"
cp -r "$DATA_ROOT/cnstk_ixday" "$BADDATE"
run_duckdb "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST((r + 1) * 0.5 AS DOUBLE) AS close, DATE '2026-08-01' AS date FROM range(4000, 6000) t(r)) TO '$BADDATE/index/month=2026-08/0001-0000002000.parquet' (FORMAT PARQUET);" >/dev/null
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('baddate');" "second column must be DATE"; then
  echo 'PASS: v8 index date-field contract enforced'
else echo 'FAIL: v8 index date-field contract'; failures=$((failures+1)); fi
rm -rf "$BADDATE"

# v8: TIMESTAMP partition-source pruning (index col1 is TIMESTAMP).
# v8 contract: col0=symbol, col1=ts (TIMESTAMP).
TSTABLE="$DATA_ROOT/ts_ixday"
mkdir -p "$TSTABLE/index/date=2026-08-01" "$TSTABLE/index/date=2026-08-02"
run_duckdb "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST(DATE '2026-08-01' AS TIMESTAMP) AS ts, CAST(r AS BIGINT) AS rowid FROM range(0, 100) t(r)) TO '$TSTABLE/index/date=2026-08-01/0000-0000000100.parquet' (FORMAT PARQUET);" >/dev/null
run_duckdb "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST(DATE '2026-08-02' AS TIMESTAMP) AS ts, CAST(r AS BIGINT) AS rowid FROM range(100, 200) t(r)) TO '$TSTABLE/index/date=2026-08-02/0000-0000000100.parquet' (FORMAT PARQUET);" >/dev/null
out=$(run_duckdb "SET aligned_data_root='$DATA_ROOT'; SELECT count(*), sum(rowid) FROM aligned_table('ts_ixday') WHERE ts >= TIMESTAMP '2026-08-02';")
if printf '%s' "$out" | grep -qE '^100,14950$'; then echo 'PASS: TIMESTAMP-pruned rows'; else echo "FAIL: TIMESTAMP-pruned rows ($out)"; failures=$((failures+1)); fi
rm -rf "$TSTABLE"


# --- error cases (expected failures 鈥?must not terminate the script) ---------
if run_duckdb_expect_error "SELECT * FROM aligned_table('no_such_table');" "no data root configured"; then
  echo 'PASS: missing root error'
else echo 'FAIL: missing root error'; failures=$((failures+1)); fi
if run_duckdb_expect_error "SET aligned_data_root='$DATA_ROOT'; SELECT * FROM aligned_table('no_such_table');" "does not exist"; then
  echo 'PASS: missing table error'
else echo 'FAIL: missing table error'; failures=$((failures+1)); fi

summary