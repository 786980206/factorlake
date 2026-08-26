# test_aligned.ps1
# Acceptance tests for the aligned extension (v5 partition-aligned contract).
# Usage: powershell -ExecutionPolicy Bypass -File test\test_aligned.ps1
# Requires: test\gen_testdata.ps1 has been run, duckdb_aligned.exe built.

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db (run the duckdb build first)" }
$dataRoot = 'D:/proj/factorlake/testdata'

$failures = 0
function Expect-Equal([string]$name, $actual, $expected) {
    if ($actual -eq $expected) {
        Write-Host "PASS: $name = $actual"
    } else {
        Write-Host "FAIL: $name = $actual (expected $expected)"
        $script:failures++
    }
}

function Run-DuckDB([string]$sql) {
    $out = & $db -csv -noheader -c $sql 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql`n$out" }
    return $out
}

# SQL containing backtick/double-quoted identifiers cannot survive PS 5.1
# native-arg passing (-c mangles embedded quotes) 鈥?pipe via a temp file.
function Run-DuckDB-File([string]$sql) {
    $tmp = Join-Path $env:TEMP 'aligned_test_query.sql'
    Set-Content -Path $tmp -Encoding UTF8 -Value $sql
    $out = cmd /c "`"$db`" -csv -noheader < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    return $out
}

# --- counts + cross-group alignment -----------------------------------------
# Layout: index month=2026-07 (1 part x 2000) + month=2026-08 (2 parts x 2000);
# alpha same months (07: 1 part 2000, 08: 1 part 4000 鈥?last-part row count
# differs from index, only the partition total is contractual); ma only
# month=2026-08 (4000) 鈥?rows [0,2000) read as NULL for ma columns.
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; WITH t AS (SELECT * FROM aligned_scan('cnstk_ixday')) SELECT count(*) AS c, count(alpha001) AS a1, count(alpha099) AS a99, count(rowid_ma) AS ma, sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) AS mis FROM t;"
$vals = ($out -split "`n" | Where-Object { $_ -match '^\d' } | Select-Object -First 1) -split ','
Expect-Equal 'total rows' ([long]$vals[0]) 6000
Expect-Equal 'alpha001 non-null (r%5==0)' ([long]$vals[1]) 1200
Expect-Equal 'alpha099 non-null (last part [4000,6000), r%3==0)' ([long]$vals[2]) 666
Expect-Equal 'ma rows (missing month=2026-07 -> NULL)' ([long]$vals[3]) 4000
Expect-Equal 'misaligned rows (alpha)' ([long]$vals[4]) 0

# --- missing partition NULL fill (partition-aligned contract) ----------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rowid, ma5 FROM aligned_scan('cnstk_ixday') WHERE rowid IN (0, 1999, 2000, 5999) ORDER BY rowid;"
$rows = $out -split "`n" | Where-Object { $_ -match '^\d' }
if ($rows.Count -eq 4 -and $rows[0] -match '^0,NULL\r?$' -and $rows[1] -match '^1999,NULL\r?$' -and $rows[2] -match '^2000,0\.0\r?$' -and $rows[3] -match '^5999,1\.9\r?$') {
    Write-Host 'PASS: missing partition rows NULL-filled, present partition reads values'
} else { Write-Host "FAIL: missing partition fill ($out)"; $script:failures++ }

# --- alpha 08 skips index 0001 (gap legal in a non-index group) ---
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), sum(rowid_alpha) FROM aligned_scan('cnstk_ixday') WHERE rowid >= 2000;"
if ($out -match '(?m)^4000,15998000\r?$') { Write-Host 'PASS: alpha 08 (0000+0002) aligns with index 08 (0000+0001)' } else { Write-Host "FAIL: cross-group last-part mismatch ($out)"; $script:failures++ }

# --- schema evolution --------------------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_scan('cnstk_ixday') WHERE rowid = 0;"
if ($out -match '^$' -or $out -match 'NULL|^\s*$') {
    $valueLine = $out -split "`n" | Where-Object { $_ -match '^\d|^$' -and $_ -notmatch 'alpha099' } | Select-Object -First 1
    if ($valueLine -eq '') { Write-Host 'PASS: alpha099 NULL in old partition' } else { Write-Host "FAIL: alpha099 in old partition ($valueLine)"; $script:failures++ }
} else { Write-Host "FAIL: alpha099 in old partition"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_scan('cnstk_ixday') WHERE rowid = 4500;"
if ($out -match '4\.501') { Write-Host 'PASS: alpha099 value in evolution part' } else { Write-Host "FAIL: alpha099 value ($out)"; $script:failures++ }

# --- boundary rows (partition boundary 2000, part boundary 4000) -------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT symbol FROM aligned_scan('cnstk_ixday') WHERE rowid IN (1999, 2000, 3999, 4000) ORDER BY rowid;"
if ($out -match '002000' -and $out -match '002001' -and $out -match '004000' -and $out -match '004001') { Write-Host 'PASS: partition/part boundary rows' } else { Write-Host "FAIL: boundary rows ($out)"; $script:failures++ }

# --- aligned_scan with explicit root parameter ---
$out = Run-DuckDB "SELECT count(*) FROM aligned_scan('cnstk_ixday', root => '$dataRoot');"
if ($out -match '(?m)^6000\r?$') { Write-Host 'PASS: aligned_scan(root => ...) variant' } else { Write-Host "FAIL: aligned_scan variant ($out)"; $script:failures++ }

# --- aggregates / repeat queries ---------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT max(close), count(*) FROM aligned_scan('cnstk_ixday'); SELECT count(*) FROM aligned_scan('cnstk_ixday');"
$counts = [regex]::Matches($out, '(?m)^6000\r?$').Count
if ($out -match '(?m)^3000\.0,6000' -and $counts -ge 1) {
    Write-Host 'PASS: aggregates + repeat queries'
} else {
    Write-Host "FAIL: aggregates/repeat ($out)"; $script:failures++
}

# --- projection pushdown (Phase 2) -------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha001, ma20, date FROM aligned_scan('cnstk_ixday') WHERE rowid IN (2000, 4095) ORDER BY rowid;"
if ($out -match '20\.02' -and $out -match '40\.97' -and $out -match '0\.375' -and $out -match '2026-08-01') {
    Write-Host 'PASS: projected multi-group query values'
} else { Write-Host "FAIL: projected values ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(rowid_ma) FROM aligned_scan('cnstk_ixday');"
if ($out -match '(?m)^4000\r?$') { Write-Host 'PASS: single-group projection (ma missing partition)' } else { Write-Host "FAIL: single-group projection ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_scan('cnstk_ixday') WHERE rowid = 4500;"
if ($out -match '(?m)^4\.501\r?$') { Write-Host 'PASS: projection + schema evolution' } else { Write-Host "FAIL: projection + evolution ($out)"; $script:failures++ }

# --- partition pruning (Phase 3, single-level month=) ------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-08-01';"
if ($out -match '(?m)^4000\r?$') { Write-Host 'PASS: partition pruning month=2026-08 (4000 rows)' } else { Write-Host "FAIL: pruning 08 ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-07-01';"
if ($out -match '(?m)^2000\r?$') { Write-Host 'PASS: partition pruning month=2026-07 (2000 rows)' } else { Write-Host "FAIL: pruning 07 ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-08-01' AND rowid < 100;"
if ($out -match '(?m)^0\r?$') { Write-Host 'PASS: pruning + row filter wrong partition = 0' } else { Write-Host "FAIL: pruning+filter ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-08-01' AND rowid >= 2000;"
if ($out -match '(?m)^4000\r?$') { Write-Host 'PASS: pruning + row filter matching partition' } else { Write-Host "FAIL: pruning+match ($out)"; $script:failures++ }

# --- parallel scan (Phase 4) -------------------------------------------------
# Results must be identical regardless of thread count (shared cursor +
# per-thread filter states); count(*) / projection / filters / aggregates.
$out1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*), count(alpha001), count(alpha099), count(rowid_ma), sum(rowid) FROM aligned_scan('cnstk_ixday');"
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*), count(alpha001), count(alpha099), count(rowid_ma), sum(rowid) FROM aligned_scan('cnstk_ixday');"
if ($out1 -eq $out8 -and $out1 -match '(?m)^6000,1200,666,4000,17997000\r?$') { Write-Host 'PASS: parallel scan aggregates (threads 1 == 8)' } else { Write-Host "FAIL: parallel aggregates ($out1 / $out8)"; $script:failures++ }
$out1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;"
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*) FROM aligned_scan('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;"
if ($out1 -eq $out8 -and $out1 -match '(?m)^11\r?$') { Write-Host 'PASS: parallel scan filters (threads 1 == 8)' } else { Write-Host "FAIL: parallel filters ($out1 / $out8)"; $script:failures++ }
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(rowid_ma), count(alpha099) FROM aligned_scan('cnstk_ixday');"
if ($out8 -match '(?m)^4000,666\r?$') { Write-Host 'PASS: parallel projection + missing partition + schema evolution' } else { Write-Host "FAIL: parallel projection ($out8)"; $script:failures++ }

# --- metadata cache (Phase 4) -------------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT current_setting('parquet_metadata_cache');"
if ($out -match '(?m)^true\r?$') { Write-Host 'PASS: parquet metadata cache default on' } else { Write-Host "FAIL: metadata cache default ($out)"; $script:failures++ }

# --- column-name rules (contract 搂2.2e) --------------------------------------
# e1: columns duplicated with index resolve to the index copy (authoritative)
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close FROM aligned_scan('cnstk_ixday') WHERE rowid = 100;"
if ($out -match '(?m)^50\.5\r?$') { Write-Host 'PASS: e1 bare close = index (authoritative)' } else { Write-Host "FAIL: e1 bare close ($out)"; $script:failures++ }
# e1: the index-duplicated copy is ignored in the non-index group (qualified ref must fail)
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT ""factor.alpha101.close"" FROM aligned_scan('cnstk_ixday') LIMIT 1;"
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'not found') { Write-Host 'PASS: e1 qualified alpha101.close rejected' } else { Write-Host "FAIL: e1 qualified alpha101.close ($out)"; $script:failures++ }
# e2: bare name of a cross-group duplicate must fail
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT vwap FROM aligned_scan('cnstk_ixday') LIMIT 1;"
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'not found') { Write-Host 'PASS: e2 bare vwap rejected' } else { Write-Host "FAIL: e2 bare vwap ($out)"; $script:failures++ }
# e2: qualified names resolve per group (row 2000 = inside ma's month=2026-08)
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT ""factor.alpha101.vwap"" AS a, ""fieldset.ma.vwap"" AS m FROM aligned_scan('cnstk_ixday') WHERE rowid = 2000;"
if ($out -match '(?m)^250\.125,62\.53125\r?$') { Write-Host 'PASS: e2 qualified vwap per group' } else { Write-Host "FAIL: e2 qualified vwap ($out)"; $script:failures++ }
# e3: bare names of non-duplicated columns work (ma5 NULL in missing partition)
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rowid_alpha, ma5 FROM aligned_scan('cnstk_ixday') WHERE rowid = 2000;"
if ($out -match '(?m)^2000,0\.0\r?$') { Write-Host 'PASS: e3 bare non-duplicated columns' } else { Write-Host "FAIL: e3 bare columns ($out)"; $script:failures++ }

# --- directory rules (contract 搂2.1b/c) --------------------------------------
# 搂2.1d: '_tmp' stray parts are ignored (proven by total rows = 6000 above)
# 搂2.1b: a table without the mandatory index group must fail (has alpha parts
# but no index parts at all)
$badIdx = Join-Path $dataRoot 'badidx'
New-Item -ItemType Directory -Force -Path (Join-Path $badIdx 'factor\alpha101\month=2026-07') | Out-Null
Copy-Item (Join-Path $dataRoot 'cnstk_ixday\factor\alpha101\month=2026-07\0000-0000002000.parquet') (Join-Path $badIdx 'factor\alpha101\month=2026-07\0000-0000002000.parquet')
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('badidx');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match "mandatory group 'index'") { Write-Host 'PASS: 搂2.1b missing index group rejected' } else { Write-Host "FAIL: 搂2.1b missing index ($out)"; $script:failures++ }
Remove-Item (Join-Path $dataRoot 'badidx') -Recurse -Force
# 搂2.1c: a one-level non-index group must fail (a real part file in group 'single')
$badLvl = Join-Path $dataRoot 'badlvl'
New-Item -ItemType Directory -Force -Path (Join-Path $badLvl 'index\month=2026-07') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $badLvl 'index\month=2026-08') | Out-Null
Copy-Item (Join-Path $dataRoot 'cnstk_ixday\index\month=2026-07\0000-0000002000.parquet') (Join-Path $badLvl 'index\month=2026-07\0000-0000002000.parquet')
Copy-Item (Join-Path $dataRoot 'cnstk_ixday\index\month=2026-08\0000-0000002000.parquet') (Join-Path $badLvl 'index\month=2026-08\0000-0000002000.parquet')
Copy-Item (Join-Path $dataRoot 'cnstk_ixday\index\month=2026-08\0001-0000002000.parquet') (Join-Path $badLvl 'index\month=2026-08\0001-0000002000.parquet')
$badSingle = Join-Path $badLvl 'single\month=2026-07'
New-Item -ItemType Directory -Force -Path $badSingle | Out-Null
Copy-Item (Join-Path $dataRoot 'cnstk_ixday\index\month=2026-07\0000-0000002000.parquet') (Join-Path $badSingle '0000-0000002000.parquet')
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('badlvl');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match "two-level path") { Write-Host 'PASS: 搂2.1c one-level group rejected' } else { Write-Host "FAIL: 搂2.1c one-level group ($out)"; $script:failures++ }
Remove-Item (Join-Path $dataRoot 'badlvl') -Recurse -Force

# 搂v6: a non-conforming part file name (not "{idx:04d}-{rows:10d}.parquet")
# must fail fast 鈥?the file-name row counts are the contract.
$badName = Join-Path $dataRoot 'badname'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $badName -Recurse -Force
Rename-Item (Join-Path $badName 'index\month=2026-08\0001-0000002000.parquet') 'part-000001.parquet'
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('badname');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'self-desc') { Write-Host 'PASS: 搂v6 non-conforming part name rejected' } else { Write-Host "FAIL: 搂v6 non-conforming part name ($out)"; $script:failures++ }
Remove-Item $badName -Recurse -Force

# 搂v6: the index group's indexes must be consecutive from 0000 (a gap in the
# index is a contract violation 鈥?the index defines the row space).
$badIdx = Join-Path $dataRoot 'badidx'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $badIdx -Recurse -Force
Remove-Item (Join-Path $badIdx 'index\month=2026-08\0000-0000002000.parquet') -Force
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('badidx');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'consecu') { Write-Host 'PASS: 搂v6 index gap rejected' } else { Write-Host "FAIL: 搂v6 index gap ($out)"; $script:failures++ }
Remove-Item $badIdx -Recurse -Force

# 搂v6: a SHARED index must agree on its row count across groups (copy the
# index's 2000-row part over the alpha group's 2000-row part after rewriting
# its name to 3000 rows 鈥?the alpha partition then has 0000(2000)+0002(3000),
# total 5000 != index 4000, and the shared index 0002 is only in alpha, but the
# partition TOTAL already disagrees).
$badRows = Join-Path $dataRoot 'badrows'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $badRows -Recurse -Force
Copy-Item (Join-Path $badRows 'factor\alpha101\month=2026-08\0002-0000002000.parquet') (Join-Path $badRows 'factor\alpha101\month=2026-08\0003-0000003000.parquet')
Remove-Item (Join-Path $badRows 'factor\alpha101\month=2026-08\0002-0000002000.parquet') -Force
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('badrows');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'must agree') { Write-Host 'PASS: 搂v6 cross-group row-count mismatch rejected' } else { Write-Host "FAIL: 搂v6 cross-group row-count mismatch ($out)"; $script:failures++ }
Remove-Item $badRows -Recurse -Force

# 搂v8: the index schema's SECOND column must be DATE or TIMESTAMP (the
# partition source column). Rebuild the index's LAST part (the group schema
# source) with the date column NOT in the second position (e.g. col0=symbol,
# col1=close which is DOUBLE, date moved to col2).
$badDate = Join-Path $dataRoot 'baddate'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $badDate -Recurse -Force
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$sql = "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST((r + 1) * 0.5 AS DOUBLE) AS close, DATE '2026-08-01' AS date FROM range(4000, 6000) t(r)) TO '$($badDate.Replace('\','/'))/index/month=2026-08/0001-0000002000.parquet' (FORMAT PARQUET);"
& $db -c $sql 2>&1 | Out-Null
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('baddate');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'second column must be DATE') { Write-Host 'PASS: 搂v8 index date-field contract enforced' } else { Write-Host "FAIL: 搂v8 index date-field contract ($out)"; $script:failures++ }
Remove-Item $badDate -Recurse -Force

# 搂v8: TIMESTAMP partition-source pruning. A dedicated table whose index date
# column is TIMESTAMP; filtering on it must prune to the matching partition.
# v8 contract: col0=symbol, col1=ts (TIMESTAMP).
$tsTable = Join-Path $dataRoot 'ts_ixday'
New-Item -ItemType Directory -Force -Path (Join-Path $tsTable 'index\date=2026-08-01') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $tsTable 'index\date=2026-08-02') | Out-Null
$sql = "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST(DATE '2026-08-01' AS TIMESTAMP) AS ts, CAST(r AS BIGINT) AS rowid FROM range(0, 100) t(r)) TO '$($tsTable.Replace('\','/'))/index/date=2026-08-01/0000-0000000100.parquet' (FORMAT PARQUET);"
& $db -c $sql 2>&1 | Out-Null
$sql = "COPY (SELECT printf('%06d', r + 1) AS symbol, CAST(DATE '2026-08-02' AS TIMESTAMP) AS ts, CAST(r AS BIGINT) AS rowid FROM range(100, 200) t(r)) TO '$($tsTable.Replace('\','/'))/index/date=2026-08-02/0000-0000000100.parquet' (FORMAT PARQUET);"
& $db -c $sql 2>&1 | Out-Null
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), sum(rowid) FROM aligned_scan('ts_ixday') WHERE ts >= TIMESTAMP '2026-08-02';"
$vals = ($out -split "`n" | Where-Object { $_ -match '^\d' } | Select-Object -First 1) -split ','
Expect-Equal 'TIMESTAMP-pruned rows' ([long]$vals[0]) 100
Expect-Equal 'TIMESTAMP-pruned sum' ([long]$vals[1]) 14950
Remove-Item $tsTable -Recurse -Force

# --- partition-aligned contract (no manifest needed) -------------------------
# Groups are discovered from the file layout and the partition-aligned contract
# is enforced (ma missing month=2026-07 stays legal 鈥?partition subsets are
# allowed; the partition totals still agree).
$v3Table = Join-Path $dataRoot 'v3_notable'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $v3Table -Recurse -Force
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; WITH t AS (SELECT * FROM aligned_scan('v3_notable')) SELECT count(*), count(rowid_ma), sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) FROM t;"
$vals = ($out -split "`n" | Where-Object { $_ -match '^\d' } | Select-Object -First 1) -split ','
Expect-Equal 'no-manifest rows (defaults + partition alignment)' ([long]$vals[0]) 6000
Expect-Equal 'no-manifest ma rows (subset legal)' ([long]$vals[1]) 4000
Expect-Equal 'no-manifest misaligned' ([long]$vals[2]) 0
# A group with a partition the index does not have must fail fast: remove the
# index's month=2026-07 partition (alpha still has it).
$v3Bad = Join-Path $dataRoot 'v3_bad'
Copy-Item (Join-Path $dataRoot 'cnstk_ixday') $v3Bad -Recurse -Force
Remove-Item (Join-Path $v3Bad 'index\month=2026-07') -Recurse -Force
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('v3_bad');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'has partition[\s\S]*index\s+group does not') { Write-Host 'PASS: group partition outside the index rejected fail-fast' } else { Write-Host "FAIL: partition-subset violation ($out)"; $script:failures++ }
Remove-Item $v3Bad -Recurse -Force
Remove-Item $v3Table -Recurse -Force

# --- error cases (expected failures 鈥?must not terminate the script) ---------
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SELECT * FROM aligned_scan('no_such_table');" 2>&1 | Out-String
$code1 = $LASTEXITCODE
$out2 = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_scan('no_such_table');" 2>&1 | Out-String
$code2 = $LASTEXITCODE
$ErrorActionPreference = $prevEAP
if ($code1 -ne 0 -and $out -match 'no data root configured') { Write-Host 'PASS: missing root error' } else { Write-Host "FAIL: missing root error ($out)"; $script:failures++ }
if ($code2 -ne 0 -and $out2 -match 'does not exist') { Write-Host 'PASS: missing table error' } else { Write-Host "FAIL: missing table error ($out2)"; $script:failures++ }

Write-Host ''
if ($failures -eq 0) {
    Write-Host 'ALL TESTS PASSED'
} else {
    Write-Host "$failures TEST(S) FAILED"
    exit 1
}