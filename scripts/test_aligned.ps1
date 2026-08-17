# test_aligned.ps1
# Acceptance tests for the aligned extension (Phase 1 MVP).
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_aligned.ps1
# Requires: scripts\gen_testdata.ps1 has been run, duckdb_aligned.exe built.

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build\duckdb_aligned.exe'
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

# --- error cases (expected failures — must not terminate the script) ---------
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
