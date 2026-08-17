# test_writer.ps1
# Phase 5 acceptance: aligned_write round trip (write -> append -> read back).
# Creates a fresh table 'writetest' with row_count=0, writes two batches from
# staging parquet files via aligned_write, then verifies the reader.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_writer.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$table = 'writetest'
$tableDir = Join-Path $dataRoot $table
$staging1 = Join-Path $dataRoot 'staging_w1.parquet'
$staging2 = Join-Path $dataRoot 'staging_w2.parquet'
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
function Run-DuckDB-ExpectError([string]$sql, [string]$pattern) {
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = & $db -csv -noheader -c $sql 2>&1 | Out-String
    $ErrorActionPreference = $prevEAP
    if ($LASTEXITCODE -ne 0 -and $out -match $pattern) { return $true }
    return $false
}

# ---- fresh table (manifests only, row_count 0) ------------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null
function Write-JsonFile([string]$path, $obj) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name = $table; version = 1; schema_version = 1; key = @('date', 'symbol')
    canonical_order = 'fixed'; row_count = 0; row_group_size = 131072
    groups = @('index', 'factor/alpha101', 'fieldset/ma')
}
Write-JsonFile (Join-Path $tableDir 'index\_group.json') @{
    group = 'index'; row_count = 0; row_group_size = 2048
    partitioning = @(@{ template = 'date=%Y-%m-%d'; source = 'date' })
}
Write-JsonFile (Join-Path $tableDir 'factor\alpha101\_group.json') @{
    group = 'factor/alpha101'; row_count = 0; row_group_size = 2048
    partitioning = @(
        @{ template = 'year=%Y'; source = 'date' },
        @{ template = 'month=%m'; source = 'date' },
        @{ template = 'day=%d'; source = 'date' }
    )
}
Write-JsonFile (Join-Path $tableDir 'fieldset\ma\_group.json') @{
    group = 'fieldset/ma'; row_count = 0; row_group_size = 2048
    partitioning = @(
        @{ template = 'year=%Y'; source = 'date' },
        @{ template = 'month=%m'; source = 'date' }
    )
}

# ---- staging batch 1: rows [0, 3000) on 2026-09-10 --------------------------
& $duckdb -c "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 3000))
  SELECT DATE '2026-09-10' AS date, printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid,
         CAST(r AS BIGINT) AS rowid_alpha,
         CASE WHEN r % 5 = 0 THEN CAST((r + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha001,
         CASE WHEN r % 7 = 0 THEN CAST((r + 2) * 0.02 AS DOUBLE) ELSE NULL END AS alpha002,
         CASE WHEN r % 11 = 0 THEN CAST((r + 3) * 0.03 AS DOUBLE) ELSE NULL END AS alpha003,
         CAST((r + 1) * 0.25 AS DOUBLE) AS vwap,
         CAST(r AS BIGINT) AS rowid_ma,
         CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
         CAST((r % 60) * 0.025 AS DOUBLE) AS ma20
  FROM r
) TO '$($staging1.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 1 failed' }

# ---- write batch 1 ----------------------------------------------------------
$mapping = "index:date,symbol,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003,vwap;fieldset/ma:rowid_ma,ma5,ma20,vwap"
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written, txid FROM aligned_write('$table', '$($staging1.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 1 summary (rows, parts, txid)' $out.Trim() '3000,3,1'

# ---- staging batch 2: rows [3000, 6000) on 2026-09-11 + new column alpha999 -
& $duckdb -c "COPY (
  WITH r AS (SELECT range AS r FROM range(3000, 6000))
  SELECT DATE '2026-09-11' AS date, printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid,
         CAST(r AS BIGINT) AS rowid_alpha,
         CASE WHEN r % 5 = 0 THEN CAST((r + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha001,
         CASE WHEN r % 7 = 0 THEN CAST((r + 2) * 0.02 AS DOUBLE) ELSE NULL END AS alpha002,
         CASE WHEN r % 11 = 0 THEN CAST((r + 3) * 0.03 AS DOUBLE) ELSE NULL END AS alpha003,
         CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha999,
         CAST((r + 1) * 0.25 AS DOUBLE) AS vwap,
         CAST(r AS BIGINT) AS rowid_ma,
         CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
         CAST((r % 60) * 0.025 AS DOUBLE) AS ma20
  FROM r
) TO '$($staging2.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 2 failed' }

# ---- append batch 2 (schema evolution: alpha999 appears in this part) -------
$mapping2 = "index:date,symbol,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003,alpha999,vwap;fieldset/ma:rowid_ma,ma5,ma20,vwap"
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_written, parts_written, txid FROM aligned_write('$table', '$($staging2.Replace('\','/'))', '$mapping2');"
Expect-Equal 'write 2 summary (rows, parts, txid)' $out.Trim() '3000,3,2'
# ---- read back ---------------------------------------------------------------
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), count(alpha999), sum(rowid), sum(CASE WHEN rowid != rowid_alpha OR rowid != rowid_ma THEN 1 ELSE 0 END) FROM aligned_table('$table');"
$vals = ($out.Trim() -split ',')
Expect-Equal 'total rows' $vals[0] '6000'
Expect-Equal 'alpha001 non-null (r%5==0)' $vals[1] '1200'
Expect-Equal 'alpha999 non-null (day2 only, r%3==0)' $vals[2] '1000'
Expect-Equal 'sum(rowid)' $vals[3] '17997000'
Expect-Equal 'misaligned rows' $vals[4] '0'

# alpha999: NULL in batch-1 rows, value in batch-2 rows
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha999 IS NULL FROM aligned_table('$table') WHERE rowid = 100;"
Expect-Equal 'alpha999 NULL in old part' $out.Trim() 'true'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha999 IS NULL FROM aligned_table('$table') WHERE rowid = 3000;"
Expect-Equal 'alpha999 value in new part' $out.Trim() 'false'

# duplicate-column rules on written data: bare close = index; qualified vwap
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close FROM aligned_table('$table') WHERE rowid = 100;"
Expect-Equal 'bare close = index (authoritative)' $out.Trim() '50.5'
$tmp = Join-Path $env:TEMP 'aligned_writer_q.sql'
"SET aligned_data_root='$dataRoot'; SELECT ""factor.alpha101.vwap"" FROM aligned_table('$table') WHERE rowid = 100;" | Set-Content -Path $tmp -Encoding UTF8
$out = cmd /c "`"$db`" -csv -noheader < `"$tmp`"" 2>&1 | Out-String
Remove-Item $tmp -Force -ErrorAction SilentlyContinue
Expect-Equal 'qualified factor.alpha101.vwap' $out.Trim() '25.25'

# manifest row counts bumped
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'table row_count reflects append' $out.Trim() '6000'

# ma coarse partition dir holds both parts (shared marker)
if ((Test-Path (Join-Path $tableDir 'fieldset\ma\year=2026\month=09\part-000000.parquet')) -and
    (Test-Path (Join-Path $tableDir 'fieldset\ma\year=2026\month=09\part-000001.parquet'))) {
    Write-Host 'PASS: ma coarse partition dir has both parts'
} else { Write-Host 'FAIL: ma parts missing'; $script:failures++ }
$marker = Get-Content (Join-Path $tableDir 'fieldset\ma\year=2026\month=09\.aligned-commit.json') -Raw
if ($marker -match 'part-000000' -and $marker -match 'part-000001') { Write-Host 'PASS: ma marker lists both parts' } else { Write-Host "FAIL: ma marker ($marker)"; $script:failures++ }

# no leftover staging tree
$leftover = Get-ChildItem $tableDir -Recurse -Directory -Filter '_tmp' -ErrorAction SilentlyContinue
if (-not $leftover) { Write-Host 'PASS: no leftover _tmp staging tree' } else { Write-Host 'FAIL: leftover _tmp tree'; $script:failures++ }

# ---- error cases -------------------------------------------------------------
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_write('$table', '$($staging1.Replace('\','/'))', '$mapping', start_row => 1000);" 'must start at the current table end') {
    Write-Host 'PASS: overlapping append rejected'
} else { Write-Host 'FAIL: overlapping append not rejected'; $script:failures++ }
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_write('$table', '$($staging1.Replace('\','/'))', 'no_such_group:date');" "unknown group") {
    Write-Host 'PASS: unknown group in mapping rejected'
} else { Write-Host 'FAIL: unknown group not rejected'; $script:failures++ }

# cleanup staging files
Remove-Item $staging1, $staging2 -Force -ErrorAction SilentlyContinue

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }


