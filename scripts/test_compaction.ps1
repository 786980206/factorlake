# test_compaction.ps1
# Phase 7 acceptance: aligned_compact merges a group's parts per partition
# directory (atomic switch), preserving the row space.
# Pre-seeds 2 parts per group (index + alpha) in one partition dir, compacts
# all groups, verifies the merged part.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_compaction.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$table = 'compacttest'
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
# Writes two aligned parquet parts (2000 rows each, rows 0..3999) for one
# partition dir of the index and alpha groups. The index file carries the
# primary key (symbol, date); the alpha file carries rowid_alpha + factors.
function Make-Part([string]$group, [string]$partIdx, [int]$from, [int]$to) {
    $dir = Join-Path $tableDir "$group\month=2026-07"
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $path = Join-Path $dir "$partIdx-0000002000.parquet"
    if ($group -eq 'index') {
        & $duckdb -c "COPY (
  WITH r AS (SELECT range AS r FROM range($from, $to))
  SELECT printf('%06d', r + 1) AS symbol, DATE '2026-07-01' AS date, CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($path.Replace('\','/'))' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);" 2>&1 | Out-Null
    } else {
        & $duckdb -c "COPY (
  WITH r AS (SELECT range AS r FROM range($from, $to))
  SELECT CAST(r AS BIGINT) AS rowid_alpha,
         CASE WHEN r % 5 = 0 THEN CAST((r + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha001,
         CASE WHEN r % 11 = 0 THEN CAST((r + 2) * 0.02 AS DOUBLE) ELSE NULL END AS alpha002
  FROM r
) TO '$($path.Replace('\','/'))' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);" 2>&1 | Out-Null
    }
    if ($LASTEXITCODE -ne 0) { throw "part write failed: $group $partIdx" }
}

# ---- fresh table -------------------------------------------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null

# ---- seed 2 parts per group (same partition dir, 2 parts) --------------------
Make-Part 'index' 0000 0 2000
Make-Part 'index' 0001 2000 4000
Make-Part 'factor/alpha101' 0000 0 2000
Make-Part 'factor/alpha101' 0001 2000 4000

# ---- alpha dir should have 2 parts now ---------------------------------------
$alphaDir = Join-Path $tableDir 'factor\alpha101\month=2026-07'
$before = (Get-ChildItem $alphaDir -Filter '*.parquet').Count
Expect-Equal 'alpha parts before compact' $before 2

# ---- verify read-back correctness before compaction --------------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), sum(rowid), sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) FROM aligned_scan('$table');"
$vals = $o.Trim() -split ','
Expect-Equal 'total rows (4000)' $vals[0] '4000'
Expect-Equal 'alpha001 non-null (r%5==0)' $vals[1] '800'
Expect-Equal 'static misalign' $vals[3] '0'

# ---- compact ALL groups (one atomic transaction) ------------------------------
# The reader requires per-partition part counts to match across groups for
# aligned scanning, so compaction always processes every group together.
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT dirs_compacted, parts_before, parts_after FROM aligned_compact('$table', 'all');"
$vals = $o.Trim() -split ','
Expect-Equal 'dirs compacted (index + alpha)' $vals[0] '2'
Expect-Equal 'parts before' $vals[1] '4'
Expect-Equal 'parts after' $vals[2] '2'

# ---- both dirs should now have 1 part -----------------------------------------
$after = (Get-ChildItem $alphaDir -Filter '*.parquet').Count
Expect-Equal 'alpha parts after compact' $after 1
$idxDir = Join-Path $tableDir 'index\month=2026-07'
$idxParts = (Get-ChildItem $idxDir -Filter '*.parquet').Count
Expect-Equal 'index parts after compact' $idxParts 1

# ---- verify read-back correctness AFTER compaction ---------------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), count(alpha002), sum(rowid), sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) FROM aligned_scan('$table');"
$vals = $o.Trim() -split ','
Expect-Equal 'total rows (4000)' $vals[0] '4000'
Expect-Equal 'alpha001 non-null' $vals[1] '800'
Expect-Equal 'alpha002 non-null (r%11==0)' $vals[2] '364'
Expect-Equal 'sum(rowid) 0..3999' $vals[3] '7998000'
Expect-Equal 'misalign after compact' $vals[4] '0'

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }