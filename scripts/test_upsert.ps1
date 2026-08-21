# test_upsert.ps1
# Phase 8 acceptance: aligned_upsert / aligned_delete (the v7 mutator replacing
# aligned_write). Covers: empty-table first write, append to an existing
# partition (new part), in-place update, new partition, delete, delete-emptied
# partition removal, and fail-fast error paths.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\test_upsert.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$table = 'upserttest'
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
function Run-DuckDB-ExpectError([string]$sql, [string]$pattern) {
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = & $db -csv -noheader -c $sql 2>&1 | Out-String
    $ErrorActionPreference = $prevEAP
    if ($LASTEXITCODE -ne 0 -and $out -match $pattern) { return $true }
    return $false
}
function Write-JsonFile([string]$path, $obj) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}
# The CLI cannot carry multi-statement CTE quoting or backslash paths through
# -c on PS 5.1: run SQL from a temp file via cmd redirection.
function Run-DuckDB-File([string]$sql) {
    $tmp = Join-Path $env:TEMP ('aligned_upsert_' + [guid]::NewGuid().ToString('N') + '.sql')
    Set-Content -Path $tmp -Value $sql -Encoding Ascii
    cmd /c "`"$duckdb`" < `"$tmp`"" 2>&1 | Out-Null
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    if ($LASTEXITCODE -ne 0) { throw "duckdb file run failed: $sql" }
}
# Staging sources are generated with a helper SQL file (the CLI cannot carry
# multi-statement CTE quoting through -c on PS 5.1).
function Make-Staging([string]$path, [int]$from, [int]$to, [string]$date, [switch]$withAlpha999) {
    $tmp = Join-Path $env:TEMP "aligned_upsert_stage.sql"
    $a999 = if ($withAlpha999) { ", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha999" } else { '' }
    @"
COPY (
  WITH r AS (SELECT range AS r FROM range($from, $to))
  SELECT DATE '$date' AS date, printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid, CAST(r AS BIGINT) AS rowid_alpha,
         CASE WHEN r % 5 = 0 THEN CAST((r + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha001,
         CASE WHEN r % 7 = 0 THEN CAST((r + 2) * 0.02 AS DOUBLE) ELSE NULL END AS alpha002,
         CASE WHEN r % 11 = 0 THEN CAST((r + 3) * 0.03 AS DOUBLE) ELSE NULL END AS alpha003
         $a999,
         CAST(r AS BIGINT) AS rowid_ma,
         CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
         CAST((r % 60) * 0.025 AS DOUBLE) AS ma20
  FROM r
) TO '$($path.Replace('\','/'))' (FORMAT PARQUET);
"@ | Set-Content -Path $tmp -Encoding Ascii
    cmd /c "`"$duckdb`" < `"$tmp`"" 2>&1 | Out-Null
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    if ($LASTEXITCODE -ne 0) { throw "staging failed: $path" }
}

# ---- fresh table (manifest only, empty table) -------------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name = $table; version = 1
    groups = @('index', 'factor/alpha101', 'fieldset/ma')
    partitioning = @{
        'index' = @(@{ template = 'month=%Y-%m'; source = 'date' })
        'factor/alpha101' = @(@{ template = 'month=%Y-%m'; source = 'date' })
        'fieldset/ma' = @(@{ template = 'month=%Y-%m'; source = 'date' })
    }
}

# ---- batch 1: first write (empty table) on 2026-09-10 ----------------------
$s1 = Join-Path $dataRoot 'upsert_s1.parquet'
Make-Staging $s1 0 3000 '2026-09-10'
$mapping = "index:date,symbol,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003;fieldset/ma:rowid_ma,ma5,ma20"
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$table', '$($s1.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 1 summary (ins, upd, parts, txid)' $out.Trim() '3000,0,3,1'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), sum(rowid), sum(CASE WHEN rowid != rowid_alpha OR rowid != rowid_ma THEN 1 ELSE 0 END) FROM aligned_table('$table');"
$vals = ($out.Trim() -split ',')
Expect-Equal 'batch1 rows' $vals[0] '3000'
Expect-Equal 'batch1 alpha001 non-null (r%5==0)' $vals[1] '600'
Expect-Equal 'batch1 sum(rowid) 0..2999' $vals[2] '4498500'
Expect-Equal 'batch1 misalign' $vals[3] '0'

# ---- batch 2: append to the same partition (new part 0001) + alpha999 -------
$s2 = Join-Path $dataRoot 'upsert_s2.parquet'
Make-Staging $s2 3000 6000 '2026-09-11' -withAlpha999
$mapping2 = "index:date,symbol,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003,alpha999;fieldset/ma:rowid_ma,ma5,ma20"
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$table', '$($s2.Replace('\','/'))', '$mapping2');"
Expect-Equal 'write 2 summary (ins, upd, parts, txid)' $out.Trim() '3000,0,3,2'
if ((Test-Path (Join-Path $tableDir 'index\month=2026-09\0001-0000003000.parquet')) -and
    (Test-Path (Join-Path $tableDir 'factor\alpha101\month=2026-09\0001-0000003000.parquet')) -and
    (Test-Path (Join-Path $tableDir 'fieldset\ma\month=2026-09\0001-0000003000.parquet'))) {
    Write-Host 'PASS: append created a new part 0001 in every group'
} else { Write-Host 'FAIL: append part 0001 missing'; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*), count(alpha001), count(alpha999), sum(rowid) FROM aligned_table('$table');"
$vals = ($out.Trim() -split ',')
Expect-Equal 'batch1+2 rows' $vals[0] '6000'
Expect-Equal 'alpha001 non-null (r%5==0)' $vals[1] '1200'
Expect-Equal 'alpha999 non-null (r%3==0, rows 3000..5999)' $vals[2] '1000'
Expect-Equal 'sum(rowid) 0..5999' $vals[3] '17997000'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha999 IS NULL FROM aligned_table('$table') WHERE rowid = 100;"
Expect-Equal 'alpha999 NULL in old part' $out.Trim() 'true'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT alpha999 IS NULL FROM aligned_table('$table') WHERE rowid = 3000;"
Expect-Equal 'alpha999 value in new part' $out.Trim() 'false'

# ---- batch 3: in-place updates + a new partition ----------------------------
$s3 = Join-Path $dataRoot 'upsert_s3.parquet'
& $duckdb -c "COPY (
  WITH t(date, symbol, close, volume, rowid, rowid_alpha, alpha001, alpha002, alpha003, rowid_ma, ma5, ma10, ma20) AS (
    VALUES (DATE '2026-09-10', '002500', 5555.5, 555, 499, 499, 77.7, 78.8, 79.9, 499, 9.9, 10.1, 10.2),
           (DATE '2026-09-11', '005000', 8888.5, 888, 4999, 4999, 88.8, 89.9, 90.1, 4999, 11.1, 12.1, 13.1),
           (DATE '2026-10-01', '000001', 111.1, 111, -1, -1, 1.1, 2.2, 3.3, -1, 3.3, 4.4, 5.5)
  )
  SELECT * FROM t
) TO '$($s3.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 3 failed' }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$table', '$($s3.Replace('\','/'))', '$mapping');"
Expect-Equal 'write 3 summary (ins, upd, parts, txid)' $out.Trim() '1,2,9,3'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'after upsert 3 rows (6001)' $out.Trim() '6001'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close, alpha001, ma5 FROM aligned_table('$table') WHERE date = DATE '2026-09-10' AND symbol = '002500';"
Expect-Equal '002500 updated (close/alpha/ma)' $out.Trim() '5555.5,77.7,9.9'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close, alpha001, ma5 FROM aligned_table('$table') WHERE date = DATE '2026-09-11' AND symbol = '005000';"
Expect-Equal '005000 updated (close/alpha/ma)' $out.Trim() '8888.5,88.8,11.1'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT date, count(*) FROM aligned_table('$table') GROUP BY date ORDER BY date;"
$vals = ($out.Trim() -split "`n")
Expect-Equal 'three partitions after upsert' $vals.Count '3'

# ---- delete: 002999 (b1), 006000 (b2), 002500 (updated), 999999 (missing) ---
$s4 = Join-Path $dataRoot 'upsert_s4.parquet'
& $duckdb -c "COPY (
  WITH t(date, symbol) AS (
    VALUES (DATE '2026-09-10', '002999'), (DATE '2026-09-11', '006000'), (DATE '2026-09-10', '002500'), (DATE '2026-10-01', '999999')
  )
  SELECT * FROM t
) TO '$($s4.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 4 failed' }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_deleted, parts_rewritten, txid FROM aligned_delete('$table', '$($s4.Replace('\','/'))');"
Expect-Equal 'delete summary (rows, parts, txid)' $out.Trim() '3,6,4'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'after delete rows (5998)' $out.Trim() '5998'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table') WHERE symbol IN ('002999','006000','002500');"
Expect-Equal 'deleted symbols gone' $out.Trim() '0'

# ---- delete the only row of the 2026-10 partition (single-part removal) -----
$s5 = Join-Path $dataRoot 'upsert_s5.parquet'
& $duckdb -c "COPY (WITH t(date, symbol) AS (VALUES (DATE '2026-10-01', '000001')) SELECT * FROM t) TO '$($s5.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 5 failed' }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_deleted, parts_rewritten, txid FROM aligned_delete('$table', '$($s5.Replace('\','/'))');"
Expect-Equal 'empty-partition delete (rows, parts, txid)' $out.Trim() '1,0,5'
if (-not (Test-Path (Join-Path $tableDir 'index\month=2026-10')) -and
    -not (Test-Path (Join-Path $tableDir 'factor\alpha101\month=2026-10')) -and
    -not (Test-Path (Join-Path $tableDir 'fieldset\ma\month=2026-10'))) {
    Write-Host 'PASS: emptied partition removed from every group'
} else { Write-Host 'FAIL: emptied partition dir still present'; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'rows after partition removal (5997)' $out.Trim() '5997'

# ---- fail-fast: emptying a part of a multi-part partition -------------------
$s6 = Join-Path $dataRoot 'upsert_s6.parquet'
$s6f = $s6.Replace('\', '/')
$idx0000 = (Join-Path $tableDir 'index\month=2026-09\0000-*.parquet').Replace('\', '/')
$alp0000 = (Join-Path $tableDir 'factor\alpha101\month=2026-09\0000-*.parquet').Replace('\', '/')
$ma0000 = (Join-Path $tableDir 'fieldset\ma\month=2026-09\0000-*.parquet').Replace('\', '/')
Run-DuckDB-File @"
COPY (
  SELECT i.date, i.symbol, i.close, i.volume, i.rowid, a.rowid_alpha, a.alpha001, a.alpha002, a.alpha003, m.ma5, m.ma20
  FROM read_parquet('$idx0000') i
  JOIN read_parquet('$alp0000') a ON i.rowid = a.rowid_alpha
  JOIN read_parquet('$ma0000') m ON i.rowid = m.rowid_ma
) TO '$s6f' (FORMAT PARQUET);
"@
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_delete('$table', '$s6f');" 'run aligned_compact first') {
    Write-Host 'PASS: emptying a multi-part part rejected'
} else { Write-Host 'FAIL: multi-part empty delete not rejected'; $script:failures++ }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'table untouched after rejected delete' $out.Trim() '5997'

# ---- error cases ------------------------------------------------------------
$s1f = $s1.Replace('\', '/')
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('$table', '$s1f', 'no_such_group:date');" "unknown group") {
    Write-Host 'PASS: unknown group in mapping rejected'
} else { Write-Host 'FAIL: unknown group not rejected'; $script:failures++ }
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('$table', '$s1f', 'index:date,symbol,no_such_col');" "not found in source") {
    Write-Host 'PASS: missing mapped column rejected'
} else { Write-Host 'FAIL: missing mapped column not rejected'; $script:failures++ }
$s7 = Join-Path $dataRoot 'upsert_s7.parquet'
$s7f = $s7.Replace('\', '/')
& $duckdb -c "COPY (SELECT 1 AS close) TO '$s7f' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 7 failed' }
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_delete('$table', '$s7f');" "primary key columns") {
    Write-Host 'PASS: delete source missing key columns rejected'
} else { Write-Host 'FAIL: delete source key check not enforced'; $script:failures++ }

# ---- auto-derive mapping (no explicit mapping) on an existing table ----------
$s8 = Join-Path $dataRoot 'upsert_s8.parquet'
$s8f = $s8.Replace('\', '/')
& $duckdb -c "COPY (SELECT DATE '2026-09-10' AS date, '009999' AS symbol, 77.7::DOUBLE AS close, 8.8::DOUBLE AS alpha001, 9.9::DOUBLE AS ma5) TO '$s8f' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 8 failed' }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_inserted, rows_updated, parts_rewritten FROM aligned_upsert('$table', '$s8f');"
Expect-Equal 'auto-derive upsert (ins, upd, parts)' $out.Trim() '1,0,3'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT close, alpha001, ma5 FROM aligned_table('$table') WHERE symbol='009999';"
Expect-Equal 'auto-derive new row values' $out.Trim() '77.7,8.8,9.9'
# empty-table first write still REQUIRES explicit mapping (no schema to infer)
$emptyDir = Join-Path $dataRoot 'upsert_empty'
New-Item -ItemType Directory -Force -Path $emptyDir | Out-Null
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('upsert_empty', '$s8f');" 'mandatory group') {
    Write-Host 'PASS: empty-table first write without mapping rejected'
} else { Write-Host 'FAIL: empty-table first write should require mapping'; $script:failures++ }
Remove-Item $emptyDir -Recurse -Force -ErrorAction SilentlyContinue

# ---- cleanup -----------------------------------------------------------------
Remove-Item $s1, $s2, $s3, $s4, $s5, $s6, $s7, $s8 -Force -ErrorAction SilentlyContinue

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }