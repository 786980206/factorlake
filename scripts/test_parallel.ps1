# test_parallel.ps1
# Phase 4 acceptance: parallel scan correctness + scaling smoke test.
# Requires: scripts\gen_bench.ps1 has been run (bench_ixday, 1M rows), duckdb_aligned.exe built.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_parallel.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$bench = Join-Path $dataRoot 'bench_ixday\_table.json'
if (-not (Test-Path $bench)) { throw "bench data missing: run scripts\gen_bench.ps1 first" }

$failures = 0
function Run-DuckDB([string]$sql) {
    $out = & $db -csv -noheader -c $sql 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql`n$out" }
    return $out
}
function Expect-Equal([string]$name, $actual, $expected) {
    if ($actual -eq $expected) { Write-Host "PASS: $name = $actual" }
    else { Write-Host "FAIL: $name = $actual (expected $expected)"; $script:failures++ }
}

# --- correctness: identical results at any thread count ----------------------
$t1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*), count(alpha000), count(alpha099), sum(rowid) FROM aligned_table('bench_ixday');"
$t8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*), count(alpha000), count(alpha099), sum(rowid) FROM aligned_table('bench_ixday');"
Expect-Equal 'parallel aggregates (t1 == t8)' $t1.Trim() $t8.Trim()
if ($t1 -match '(?m)^1000000,142858,142858,499999500000\r?$') { Write-Host 'PASS: aggregate values (1M rows, r%7 sparse, sum 0..999999)' } else { Write-Host "FAIL: aggregate values ($($t1.Trim()))"; $script:failures++ }

$t1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(*) FROM aligned_table('bench_ixday') WHERE rowid BETWEEN 249990 AND 250010;"
$t8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(*) FROM aligned_table('bench_ixday') WHERE rowid BETWEEN 249990 AND 250010;"
Expect-Equal 'parallel filters across part boundary (t1 == t8)' $t1.Trim() $t8.Trim()
Expect-Equal 'filter row count' $t1.Trim() '21'

$t1 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=1; SELECT count(alpha099), count(rowid_ma) FROM aligned_table('bench_ixday');"
$t8 = Run-DuckDB "SET aligned_data_root='$dataRoot'; SET threads=8; SELECT count(alpha099), count(rowid_ma) FROM aligned_table('bench_ixday');"
Expect-Equal 'parallel projection (t1 == t8)' $t1.Trim() $t8.Trim()
Expect-Equal 'projection values' $t1.Trim() '142858,1000000'

# --- metadata cache default (Phase 4) ----------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT current_setting('parquet_metadata_cache');"
Expect-Equal 'parquet metadata cache default on' $out.Trim() 'true'

# --- scaling smoke test: 8 threads must be clearly faster than 1 -------------
function Time-Ms([string]$sql) {
    $tmp = Join-Path $env:TEMP 'aligned_timer.sql'
    ".timer on`n$sql" | Set-Content -Path $tmp -Encoding Ascii
    $raw = cmd /c "`"$db`" < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    $m = [regex]::Match($raw, 'real ([\d.]+)')
    if (-not $m.Success) { throw "no timer output: $raw" }
    return [double]$m.Groups[1].Value
}
$q = "SET aligned_data_root='$dataRoot'; SET threads={0}; SELECT count(alpha000), count(alpha050), sum(ma005), sum(rowid) FROM (SELECT alpha000, alpha050, ma005, rowid FROM aligned_table('bench_ixday'));"
$t1ms = Time-Ms ($q -f 1)
$t8ms = Time-Ms ($q -f 8)
Write-Host "scaling: 1 thread = $t1ms s, 8 threads = $t8ms s"
if ($t8ms -lt $t1ms * 0.6) { Write-Host 'PASS: parallel speedup (8t < 60% of 1t)' }
else { Write-Host "FAIL: no parallel speedup (1t=$t1ms s, 8t=$t8ms s)"; $script:failures++ }

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }


