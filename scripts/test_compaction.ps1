# test_compaction.ps1
# Phase 7 acceptance: aligned_compact merges a group's parts per partition
# directory (atomic switch), preserving the row space.
# Creates a fresh table, writes 3 small batches (multiple parts per dir) to the
# index and alpha groups, compacts the alpha group, verifies the merged part.
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
function Make-Staging([string]$path, [int]$from, [int]$to, [string]$date) {
    & $duckdb -c "COPY (
  WITH r AS (SELECT range AS r FROM range($from, $to))
  SELECT DATE '$date' AS date, printf('%06d', r + 1) AS symbol, CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST(r AS BIGINT) AS rowid, CAST(r AS BIGINT) AS rowid_alpha,
         CASE WHEN r % 5 = 0 THEN CAST((r + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha001,
         CASE WHEN r % 11 = 0 THEN CAST((r + 2) * 0.02 AS DOUBLE) ELSE NULL END AS alpha002
  FROM r
) TO '$($path.Replace('\','/'))' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'staging failed' }
}

# ---- fresh table -------------------------------------------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name = $table; version = 1; schema_version = 1; key = @('date', 'symbol')
    canonical_order = 'fixed'; row_count = 0; row_group_size = 131072
    groups = @('index', 'factor/alpha101')
    partitioning = @{
        'index' = @(@{ template = 'date=%Y-%m-%d'; source = 'date' })
        'factor/alpha101' = @(
            @{ template = 'year=%Y'; source = 'date' },
            @{ template = 'month=%Y-%m'; source = 'date' },
            @{ template = 'date=%Y-%m-%d'; source = 'date' }
        )
    }
}

# ---- write 2 batches on the SAME day (same partition dir, 2 parts) -----------
$s1 = Join-Path $dataRoot 'cmp_s1.parquet'
$s2 = Join-Path $dataRoot 'cmp_s2.parquet'
Make-Staging $s1 0 2000 '2026-07-01'
Make-Staging $s2 2000 4000 '2026-07-01'
$mapping = "index:date,symbol,close,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002"
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written, txid FROM aligned_write('$table', '$($s1.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 1' $o.Trim() '2000,2,1'
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written, txid FROM aligned_write('$table', '$($s2.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 2' $o.Trim() '2000,2,2'

# ---- alpha dir should have 2 parts now ---------------------------------------
$alphaDir = Join-Path $tableDir 'factor\alpha101\year=2026\month=2026-07\date=2026-07-01'
$before = (Get-ChildItem $alphaDir -Filter 'part-*.parquet').Count
Expect-Equal 'alpha parts before compact' $before 2

# ---- verify read-back correctness before compaction --------------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), sum(rowid), sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) FROM aligned_table('$table');"
$vals = $o.Trim() -split ','
Expect-Equal 'total rows (4000)' $vals[0] '4000'
Expect-Equal 'alpha001 non-null (r%5==0)' $vals[1] '800'
Expect-Equal 'static misalign' $vals[3] '0'

# ---- compact the alpha group -------------------------------------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT dirs_compacted, parts_before, parts_after FROM aligned_compact('$table', 'factor/alpha101');"
$vals = $o.Trim() -split ','
Expect-Equal 'dirs compacted' $vals[0] '1'
Expect-Equal 'parts before' $vals[1] '2'
Expect-Equal 'parts after' $vals[2] '1'

# ---- alpha dir should now have 1 part ----------------------------------------
$after = (Get-ChildItem $alphaDir -Filter 'part-*.parquet').Count
Expect-Equal 'alpha parts after compact' $after 1

# ---- verify read-back correctness AFTER compaction ---------------------------
$o = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), count(alpha002), sum(rowid), sum(CASE WHEN rowid != rowid_alpha THEN 1 ELSE 0 END) FROM aligned_table('$table');"
$vals = $o.Trim() -split ','
Expect-Equal 'total rows (4000)' $vals[0] '4000'
Expect-Equal 'alpha001 non-null' $vals[1] '800'
Expect-Equal 'alpha002 non-null (r%11==0)' $vals[2] '364'
Expect-Equal 'sum(rowid) 0..3999' $vals[3] '7998000'
Expect-Equal 'misalign after compact' $vals[4] '0'

# ---- index still has 2 parts (only alpha was compacted) ----------------------
$idxDir = Join-Path $tableDir 'index\date=2026-07-01'
$idxParts = (Get-ChildItem $idxDir -Filter 'part-*.parquet').Count
Expect-Equal 'index parts unchanged' $idxParts 2

# ---- cleanup -----------------------------------------------------------------
Remove-Item $s1, $s2 -Force -ErrorAction SilentlyContinue

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }
