# test_aligned.ps1
# Acceptance tests for the aligned extension (Phase 1 MVP).
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_aligned.ps1
# Requires: scripts\gen_testdata.ps1 has been run, duckdb_aligned.exe built.

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
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
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; WITH t AS (SELECT * FROM aligned_table('cnstk_ixday')) SELECT count(*) AS c, count(alpha001) AS a1, count(alpha099) AS a99, sum(CASE WHEN rowid != rowid_alpha OR rowid != rowid_ma THEN 1 ELSE 0 END) AS mis FROM t;"
$vals = ($out -split "`n" | Where-Object { $_ -match '^\d' } | Select-Object -First 1) -split ','
Expect-Equal 'total rows' ([long]$vals[0]) 6000
Expect-Equal 'alpha001 non-null (r%5==0)' ([long]$vals[1]) 1200
Expect-Equal 'alpha099 non-null (day18 only, r%3==0)' ([long]$vals[2]) 1000
Expect-Equal 'misaligned rows' ([long]$vals[3]) 0

# --- schema evolution --------------------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 0;"
if ($out -match '^$' -or $out -match 'NULL|^\s*$') {
    $valueLine = $out -split "`n" | Where-Object { $_ -match '^\d|^$' -and $_ -notmatch 'alpha099' } | Select-Object -First 1
    if ($valueLine -eq '') { Write-Host 'PASS: alpha099 NULL in old partition' } else { Write-Host "FAIL: alpha099 in old partition ($valueLine)"; $script:failures++ }
} else { Write-Host "FAIL: alpha099 in old partition"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 3000;"
if ($out -match '3\.001') { Write-Host 'PASS: alpha099 value in new partition' } else { Write-Host "FAIL: alpha099 value ($out)"; $script:failures++ }

# --- boundary rows (part boundary 2047/2048, RG boundary 4095/4096) ----------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT symbol FROM aligned_table('cnstk_ixday') WHERE rowid IN (2047, 2048) ORDER BY rowid;"
if ($out -match '002048' -and $out -match '002049') { Write-Host 'PASS: part boundary rows' } else { Write-Host "FAIL: part boundary ($out)"; $script:failures++ }

# --- aligned_scan variant ----------------------------------------------------
$out = Run-DuckDB "SELECT count(*) FROM aligned_scan('$dataRoot', 'cnstk_ixday');"
if ($out -match '(?m)^6000\r?$') { Write-Host 'PASS: aligned_scan(root, name) variant' } else { Write-Host "FAIL: aligned_scan variant ($out)"; $script:failures++ }

# --- aggregates / repeat queries ---------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT max(close), count(*) FROM aligned_table('cnstk_ixday'); SELECT count(*) FROM aligned_table('cnstk_ixday');"
$counts = [regex]::Matches($out, '(?m)^6000\r?$').Count
if ($out -match '(?m)^3000\.0,6000' -and $counts -ge 1) {
    Write-Host 'PASS: aggregates + repeat queries'
} else {
    Write-Host "FAIL: aggregates/repeat ($out)"; $script:failures++
}

# --- projection pushdown (Phase 2) -------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha001, ma20, date FROM aligned_table('cnstk_ixday') WHERE rowid IN (0, 4095) ORDER BY rowid;"
if ($out -match '0\.02' -and $out -match '40\.97' -and $out -match '2026-08-17' -and $out -match '2026-08-18') {
    Write-Host 'PASS: projected multi-group query values'
} else { Write-Host "FAIL: projected values ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(rowid_ma) FROM aligned_table('cnstk_ixday');"
if ($out -match '(?m)^6000\r?$') { Write-Host 'PASS: single-group projection' } else { Write-Host "FAIL: single-group projection ($out)"; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha099 FROM aligned_table('cnstk_ixday') WHERE rowid = 3000;"
if ($out -match '(?m)^3\.001\r?$') { Write-Host 'PASS: projection + schema evolution' } else { Write-Host "FAIL: projection + evolution ($out)"; $script:failures++ }

# --- parallel scan (Phase 4) -------------------------------------------------
# Results must be identical regardless of thread count (shared cursor +
# per-thread filter states); count(*) / projection / filters / aggregates.
$out1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*), count(alpha001), count(alpha099), sum(rowid) FROM aligned_table('cnstk_ixday');"
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*), count(alpha001), count(alpha099), sum(rowid) FROM aligned_table('cnstk_ixday');"
if ($out1 -eq $out8 -and $out1 -match '(?m)^6000,1200,1000,17997000\r?$') { Write-Host 'PASS: parallel scan aggregates (threads 1 == 8)' } else { Write-Host "FAIL: parallel aggregates ($out1 / $out8)"; $script:failures++ }
$out1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;"
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*) FROM aligned_table('cnstk_ixday') WHERE rowid BETWEEN 3000 AND 3010;"
if ($out1 -eq $out8 -and $out1 -match '(?m)^11\r?$') { Write-Host 'PASS: parallel scan filters (threads 1 == 8)' } else { Write-Host "FAIL: parallel filters ($out1 / $out8)"; $script:failures++ }
$out8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(rowid_ma), count(alpha099) FROM aligned_table('cnstk_ixday');"
if ($out8 -match '(?m)^6000,1000\r?$') { Write-Host 'PASS: parallel projection + schema evolution' } else { Write-Host "FAIL: parallel projection ($out8)"; $script:failures++ }

# --- metadata cache (Phase 4) -------------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT current_setting('parquet_metadata_cache');"
if ($out -match '(?m)^true\r?$') { Write-Host 'PASS: parquet metadata cache default on' } else { Write-Host "FAIL: metadata cache default ($out)"; $script:failures++ }

# --- column-name rules (contract 搂2.2e) --------------------------------------
# e1: columns duplicated with index resolve to the index copy (authoritative)
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close FROM aligned_table('cnstk_ixday') WHERE rowid = 100;"
if ($out -match '(?m)^50\.5\r?$') { Write-Host 'PASS: e1 bare close = index (authoritative)' } else { Write-Host "FAIL: e1 bare close ($out)"; $script:failures++ }
# e1: the index-duplicated copy is ignored in the non-index group (qualified ref must fail)
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT ""factor.alpha101.close"" FROM aligned_table('cnstk_ixday') LIMIT 1;"
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'not found') { Write-Host 'PASS: e1 qualified alpha101.close rejected' } else { Write-Host "FAIL: e1 qualified alpha101.close ($out)"; $script:failures++ }
# e2: bare name of a cross-group duplicate must fail
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT vwap FROM aligned_table('cnstk_ixday') LIMIT 1;"
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match 'not found') { Write-Host 'PASS: e2 bare vwap rejected' } else { Write-Host "FAIL: e2 bare vwap ($out)"; $script:failures++ }
# e2: qualified names resolve per group
$out = Run-DuckDB-File "SET aligned_data_root='$dataRoot'; SELECT ""factor.alpha101.vwap"" AS a, ""fieldset.ma.vwap"" AS m FROM aligned_table('cnstk_ixday') WHERE rowid = 100;"
if ($out -match '(?m)^12\.625,3\.15625\r?$') { Write-Host 'PASS: e2 qualified vwap per group' } else { Write-Host "FAIL: e2 qualified vwap ($out)"; $script:failures++ }
# e3: bare names of non-duplicated columns work
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rowid_alpha, ma5 FROM aligned_table('cnstk_ixday') WHERE rowid = 100;"
if ($out -match '(?m)^100,0\.0\r?$') { Write-Host 'PASS: e3 bare non-duplicated columns' } else { Write-Host "FAIL: e3 bare columns ($out)"; $script:failures++ }

# --- directory rules (contract 搂2.1b/c) --------------------------------------
# 搂2.1d: '_tmp' stray parts are ignored (proven by total rows = 6000 above;
# without the rule the stray 100-row part would break discovery/counts)
# 搂2.1b: a table without the mandatory index group must fail
$badIdx = Join-Path $dataRoot 'badidx\_table.json'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $badIdx) | Out-Null
'{"name":"badidx","version":1,"key":["date","symbol"],"canonical_order":"fixed","row_count":10,"groups":["factor/alpha101"]}' | Set-Content -Path $badIdx -Encoding Ascii
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_table('badidx');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match "mandatory group 'index'") { Write-Host 'PASS: 搂2.1b missing index group rejected' } else { Write-Host "FAIL: 搂2.1b missing index ($out)"; $script:failures++ }
Remove-Item (Join-Path $dataRoot 'badidx') -Recurse -Force
# 搂2.1c: a one-level non-index group must fail (malformed group first in the
# list so the check fires before any group dir is accessed)
$badLvl = Join-Path $dataRoot 'badlvl\_table.json'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $badLvl) | Out-Null
'{"name":"badlvl","version":1,"key":["date"],"canonical_order":"fixed","row_count":10,"groups":["single","index"]}' | Set-Content -Path $badLvl -Encoding Ascii
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_table('badlvl');" 2>&1 | Out-String
$ErrorActionPreference = $prevEAP
if ($LASTEXITCODE -ne 0 -and $out -match "two-level path") { Write-Host 'PASS: 搂2.1c one-level group rejected' } else { Write-Host "FAIL: 搂2.1c one-level group ($out)"; $script:failures++ }
Remove-Item (Join-Path $dataRoot 'badlvl') -Recurse -Force

# --- error cases (expected failures 鈥?must not terminate the script) ---------
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$out = & $db -c "SELECT * FROM aligned_table('no_such_table');" 2>&1 | Out-String
$code1 = $LASTEXITCODE
$out2 = & $db -c "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_table('no_such_table');" 2>&1 | Out-String
$code2 = $LASTEXITCODE
$ErrorActionPreference = $prevEAP
if ($code1 -ne 0 -and $out -match 'no data root configured') { Write-Host 'PASS: missing root error' } else { Write-Host "FAIL: missing root error ($out)"; $script:failures++ }
if ($code2 -ne 0 -and $out2 -match '_table.json') { Write-Host 'PASS: missing table error' } else { Write-Host "FAIL: missing table error ($out2)"; $script:failures++ }

Write-Host ''
if ($failures -eq 0) {
    Write-Host 'ALL TESTS PASSED'
} else {
    Write-Host "$failures TEST(S) FAILED"
    exit 1
}


