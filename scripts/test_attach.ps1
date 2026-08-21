# test_attach.ps1 - Phase 8: aligned_attach / aligned_detach
#
# Covers: attach a logical table as a real DuckDB catalog table, then run
# standard SQL against the bare name: SELECT / INSERT / UPDATE / DELETE,
# plus detach cleanup. NOTE: attached tables live in the session's in-memory
# catalog, so every scenario runs inside ONE duckdb process.

param([string]$DuckDB = "")

$ErrorActionPreference = 'Stop'
$script:failures = 0

if (-not $DuckDB) {
    $repo = Split-Path -Parent $PSScriptRoot
    $DuckDB = Join-Path $repo 'duckdb\build3\duckdb_al3.exe'
}
if (-not (Test-Path $DuckDB)) { throw "duckdb binary not found: $DuckDB" }
$duckdb = (Resolve-Path $DuckDB).Path

$dataRoot = Join-Path (Split-Path -Parent $PSScriptRoot) 'testdata'
$dataRootFwd = $dataRoot.Replace('\', '/')
$table = 'cnstk_ixday'

function Run-DuckDB([string]$sql) {
    $tmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $tmp -Value $sql -Encoding UTF8
    $out = cmd /c "`"$duckdb`" -unsigned -csv -noheader < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    return $out.Trim()
}

function Expect-Equal([string]$name, [string]$actual, [string]$expected) {
    if ($actual -eq $expected) {
        Write-Host "PASS: $name"
    } else {
        Write-Host "FAIL: $name  expected='$expected' actual='$actual'"
        $script:failures++
    }
}

$setRoot = "SET aligned_data_root='$dataRootFwd';"

# ---- attach + bare-name SELECT -------------------------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT status FROM aligned_attach('$table') WHERE table_name='$table';
SELECT count(*) FROM $table;
"@
$lines = $out -split "`r?`n"
Expect-Equal 'attach status' $lines[0].Trim() 'attached'
Expect-Equal 'bare-name select after attach' $lines[-1].Trim() '6000'

# ---- standard INSERT ------------------------------------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT * FROM aligned_attach('$table');
INSERT INTO $table (date, symbol, close, alpha001, ma5) VALUES (DATE '2026-09-01', '009999', 99.5, 1.5, 2.5);
SELECT count(*) FROM $table;
"@
Expect-Equal 'rows after standard INSERT' (($out -split "`r?`n")[-1].Trim()) '6001'

# ---- standard UPDATE ------------------------------------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT * FROM aligned_attach('$table');
INSERT INTO $table (date, symbol, close, alpha001, ma5) VALUES (DATE '2026-09-01', '009999', 99.5, 1.5, 2.5);
UPDATE $table SET close = 123.4 WHERE symbol = '009999';
SELECT close FROM $table WHERE symbol = '009999';
"@
Expect-Equal 'standard UPDATE visible' (($out -split "`r?`n")[-1].Trim()) '123.4'

# ---- standard DELETE ------------------------------------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT * FROM aligned_attach('$table');
INSERT INTO $table (date, symbol, close) VALUES (DATE '2026-09-01', '009999', 99.5);
DELETE FROM $table WHERE symbol = '009999';
SELECT count(*) FROM $table;
"@
Expect-Equal 'rows after standard DELETE' (($out -split "`r?`n")[-1].Trim()) '6000'

# ---- detach + aligned storage untouched ------------------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT status FROM aligned_detach('$table') WHERE table_name='$table';
SELECT count(*) FROM aligned_table('$table');
"@
$lines = $out -split "`r?`n"
Expect-Equal 'detach status' $lines[0].Trim() 'detached'
Expect-Equal 'aligned storage untouched by attach/detach' $lines[-1].Trim() '6000'

Write-Host ''
if ($script:failures -eq 0) {
    Write-Host "ALL TESTS PASSED"
} else {
    Write-Host "$script:failures TEST(S) FAILED"
    exit 1
}
