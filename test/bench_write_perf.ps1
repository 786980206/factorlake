# bench_write_perf.ps1 — Quick write performance benchmark
# Measures INSERT performance on aligned tables of various sizes.
# Usage: powershell -ExecutionPolicy Bypass -File test\bench_write_perf.ps1

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$duckdb = Join-Path $repo 'duckdb\build3\duckdb_al3.exe'
$dataRoot = 'D:/proj/factorlake/testdata_perf'

if (-not (Test-Path $dataRoot)) {
    New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null
}

function Run-SQL($sql) {
    $out = & $duckdb -csv -noheader -c $sql 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql`n$out" }
    return $out
}

function Run-SQL-Setup($sql) {
    $tmp = Join-Path $env:TEMP 'perf_setup.sql'
    Set-Content -Path $tmp -Encoding UTF8 -Value $sql
    $out = cmd /c "`"$duckdb`" -csv -noheader < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    return $out
}

# Setup: create a table with 100K rows
Write-Host "=== Setup: creating table with 100K rows ==="
$setup = @"
SET aligned_data_root='$dataRoot';
CALL aligned_create('perf_test', 'index', 'symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT');
CALL aligned_create('perf_test', 'factor/alpha', 'alpha001 DOUBLE, alpha002 DOUBLE');
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100), DATE '2026-01-01' + (r / 100),
       CAST(r AS DOUBLE), r, CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(100000) t(r);
"@
Run-SQL-Setup $setup
Write-Host "Table created with 100K rows"

# Test 1: Small batch INSERT (1K rows) — measures key resolution + rewrite overhead
Write-Host "`n=== Test 1: Small batch INSERT (1K rows) ==="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Run-SQL-Setup @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', 50 + r % 100), DATE '2026-01-01' + (r / 100),
       CAST(r AS DOUBLE), r, CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(1000) t(r);
"@
$sw.Stop()
Write-Host "1K rows: $($sw.ElapsedMilliseconds)ms"

# Test 2: Medium batch INSERT (10K rows)
Write-Host "`n=== Test 2: Medium batch INSERT (10K rows) ==="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Run-SQL-Setup @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100), DATE '2026-03-01' + (r / 100),
       CAST(r AS DOUBLE), r, CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(10000) t(r);
"@
$sw.Stop()
Write-Host "10K rows: $($sw.ElapsedMilliseconds)ms"

# Test 3: Large batch INSERT (100K rows) — new partition
Write-Host "`n=== Test 3: Large batch INSERT (100K rows, new partition) ==="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Run-SQL-Setup @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100), DATE '2026-06-01' + (r / 100),
       CAST(r AS DOUBLE), r, CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(100000) t(r);
"@
$sw.Stop()
Write-Host "100K rows (new partition): $($sw.ElapsedMilliseconds)ms"

# Test 4: Full overwrite (100K rows into existing partition — all updates)
Write-Host "`n=== Test 4: Full overwrite (100K rows, existing partition) ==="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Run-SQL-Setup @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100), DATE '2026-01-01' + (r / 100),
       CAST(r * 2 AS DOUBLE), r * 2, CAST(r AS DOUBLE) / 50, CAST(r AS DOUBLE) / 100
FROM range(100000) t(r);
"@
$sw.Stop()
Write-Host "100K rows (full overwrite): $($sw.ElapsedMilliseconds)ms"

# Cleanup
Remove-Item -Recurse -Force (Join-Path $dataRoot 'perf_test') -ErrorAction SilentlyContinue
Write-Host "`nDone."
