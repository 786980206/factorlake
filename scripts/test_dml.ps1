# test_dml.ps1 - Phase 8: standard DML on DuckLake-style attached tables
#
# ATTACH a data root (TYPE ALIGNED) -> logical tables. Then standard SQL
# INSERT / UPDATE / DELETE write DIRECTLY into the parquet column groups via
# the catalog's PlanInsert/PlanUpdate/PlanDelete hooks. Every scenario runs
# inside ONE duckdb process (the attached catalog is session-scoped).

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
$table = 'upserttest'

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
$attach = "ATTACH '$dataRootFwd' AS al (TYPE ALIGNED);"

# ---- baseline (depends on suite run order) -----------------------------------
$out = Run-DuckDB @"
$setRoot
$attach
SELECT count(*) FROM al.$table;
"@
$base = ($out -split "`r?`n")[-1].Trim()
$afterInsert = [string]([int]$base + 1)
$afterDelete = $base

# ---- standard INSERT (new key, existing partition) ---------------------------
$out = Run-DuckDB @"
$setRoot
$attach
SELECT count(*) FROM al.$table;
INSERT INTO al.$table (date, symbol, close, alpha001, ma5) VALUES (DATE '2026-09-12', '009991', 10.0, 2.0, 3.0);
SELECT count(*) FROM al.$table;
"@
$lines = $out -split "`r?`n"
Expect-Equal 'count before insert' $lines[0].Trim() $base
Expect-Equal 'count after standard INSERT' $lines[-1].Trim() $afterInsert

$out = Run-DuckDB @"
$setRoot
$attach
SELECT close, alpha001, ma5 FROM al.$table WHERE symbol = '009991';
"@
Expect-Equal 'INSERT values visible' ($out -split "`r?`n")[-1].Trim() '10.0,2.0,3.0'

# ---- standard UPDATE (existing key) ------------------------------------------
$out = Run-DuckDB @"
$setRoot
$attach
UPDATE al.$table SET close = 555.5 WHERE symbol = '009991';
SELECT close FROM al.$table WHERE symbol = '009991';
"@
Expect-Equal 'standard UPDATE persisted' ($out -split "`r?`n")[-1].Trim() '555.5'

# ---- standard DELETE (existing key) ------------------------------------------
$out = Run-DuckDB @"
$setRoot
$attach
DELETE FROM al.$table WHERE symbol = '009991';
SELECT count(*) FROM al.$table;
"@
Expect-Equal 'count after standard DELETE' ($out -split "`r?`n")[-1].Trim() $afterDelete

$out = Run-DuckDB @"
$setRoot
$attach
SELECT count(*) FROM al.$table WHERE symbol = '009991';
"@
Expect-Equal 'deleted key gone' ($out -split "`r?`n")[-1].Trim() '0'

# ---- column groups untouched by reads elsewhere -------------------------------
$out = Run-DuckDB @"
$setRoot
SELECT count(*) FROM aligned_table('$table');
"@
Expect-Equal 'aligned_table sees same state' $out.Trim() $afterDelete

Write-Host ''
if ($script:failures -eq 0) {
    Write-Host "ALL TESTS PASSED"
} else {
    Write-Host "$script:failures TEST(S) FAILED"
    exit 1
}
