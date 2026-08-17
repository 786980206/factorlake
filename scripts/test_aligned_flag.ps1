# test_aligned_flag.ps1
# Phase + new requirement: the manifest "aligned" flag controls how partition /
# row-group pruning results are coordinated across column groups (leaves).
#  - aligned=true (default): leaf pruning results map to a unified physical-group
#    coordinate and are INTERSECTED into one global scan range.
#  - aligned=false: leaves are independent; pruning must NOT be propagated via
#    intersection (union scan range). The scan must still return correct rows.
#
# This test verifies that a table with aligned=false scans correctly and that a
# partition-pruning filter still prunes each leaf's OWN kept parts.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_aligned_flag.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$table = 'alignedflag'
$tableDir = Join-Path $dataRoot $table
$duckdb = 'duckdb'

$failures = 0
function Expect-Equal([string]$name, $actual, $expected) {
    if ($actual -eq $expected) { Write-Host "PASS: $name = $actual" }
    else { Write-Host "FAIL: $name = $actual (expected $expected)"; $script:failures++ }
}
function Run-DuckDB([string]$sql) {
    $out = & $db -csv -noheader -c $sql 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql`n$out" }
    return $out
}
function Write-JsonFile([string]$path, $obj) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}

# ---- fresh table with aligned=false ------------------------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name = $table; version = 1; schema_version = 1; key = @('date', 'symbol')
    canonical_order = 'fixed'; aligned = $false; row_count = 0; row_group_size = 131072
    groups = @('index')
}
Write-JsonFile (Join-Path $tableDir 'index\_group.json') @{
    group = 'index'; row_count = 0; row_group_size = 2048
    partitioning = @(@{ template = 'date=%Y-%m-%d'; source = 'date' })
}

# ---- write two batches on different days --------------------------------------
$s1 = Join-Path $dataRoot 'af_s1.parquet'
$s2 = Join-Path $dataRoot 'af_s2.parquet'
& $duckdb -c "COPY (WITH r AS (SELECT range AS r FROM range(0, 2000)) SELECT DATE '2026-09-01' AS date, printf('%06d', r+1) AS symbol, CAST(r AS BIGINT) AS rowid FROM r) TO '$($s1.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 's1 failed' }
& $duckdb -c "COPY (WITH r AS (SELECT range AS r FROM range(2000, 4000)) SELECT DATE '2026-09-05' AS date, printf('%06d', r+1) AS symbol, CAST(r AS BIGINT) AS rowid FROM r) TO '$($s2.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 's2 failed' }
$mapping = "index:date,symbol,rowid"
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written FROM aligned_write('$table', '$($s1.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 1' $o.Trim() '2000,1'
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written FROM aligned_write('$table', '$($s2.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 2' $o.Trim() '2000,1'

# ---- manifest must carry aligned:false (writer preserves it) ------------------
$tblJson = Get-Content (Join-Path $tableDir '_table.json') -Raw
if ($tblJson -match '"aligned"\s*:\s*false') { Write-Host 'PASS: manifest keeps aligned=false' } else { Write-Host "FAIL: manifest aligned flag ($tblJson)"; $script:failures++ }

# ---- full scan on an aligned=false table returns all rows ---------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), sum(rowid) FROM aligned_table('$table');"
$vals = $o.Trim() -split ','
Expect-Equal 'count(*) 4000' $vals[0] '4000'
Expect-Equal 'sum(rowid) 0..3999' $vals[1] '7998000'

# ---- partition pruning on a filter still works (each leaf keeps its own parts)
# With aligned=false and a date filter, the index leaf prunes to day 2026-09-01's
# part only -> 2000 rows. (Per-leaf pruning is independent; it is not intersected
# away by another leaf, but with a single leaf here it simply prunes correctly.)
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table') WHERE date = DATE '2026-09-01';"
Expect-Equal 'prune to day1 (2000)' $o.Trim() '2000'
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table') WHERE date = DATE '2026-09-05';"
Expect-Equal 'prune to day5 (2000)' $o.Trim() '2000'

Remove-Item $s1, $s2 -Force -ErrorAction SilentlyContinue
Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }
