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
  SELECT printf('%06d', r + 1) AS symbol, DATE '$date' AS date,
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

# ---- fresh table (empty, first write populates it) ---------------------------
if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }

# ---- batch 1: first write (empty table) on 2026-09-10 ----------------------
$s1 = Join-Path $dataRoot 'upsert_s1.parquet'
Make-Staging $s1 0 3000 '2026-09-10'
$mapping = "index:symbol,date,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003;fieldset/ma:rowid_ma,ma5,ma20"
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
$mapping2 = "index:symbol,date,close,volume,rowid;factor/alpha101:rowid_alpha,alpha001,alpha002,alpha003,alpha999;fieldset/ma:rowid_ma,ma5,ma20"
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$table', '$($s2.Replace('\','/'))', '$mapping2');"
Expect-Equal 'write 2 summary (ins, upd, parts, txid)' $out.Trim() '3000,0,3,1'
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
Expect-Equal 'write 3 summary (ins, upd, parts, txid)' $out.Trim() '1,2,9,1'
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
Expect-Equal 'delete summary (rows, parts, txid)' $out.Trim() '3,6,1'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table');"
Expect-Equal 'after delete rows (5998)' $out.Trim() '5998'
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT count(*) FROM aligned_table('$table') WHERE symbol IN ('002999','006000','002500');"
Expect-Equal 'deleted symbols gone' $out.Trim() '0'

# ---- delete the only row of the 2026-10 partition (single-part removal) -----
$s5 = Join-Path $dataRoot 'upsert_s5.parquet'
& $duckdb -c "COPY (WITH t(date, symbol) AS (VALUES (DATE '2026-10-01', '000001')) SELECT * FROM t) TO '$($s5.Replace('\','/'))' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'staging 5 failed' }
$out = Run-DuckDB "SET aligned_data_root='$dataRoot'; SELECT rows_deleted, parts_rewritten, txid FROM aligned_delete('$table', '$($s5.Replace('\','/'))');"
Expect-Equal 'empty-partition delete (rows, parts, txid)' $out.Trim() '1,0,1'
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
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('upsert_empty2', '$s1f', 'no_such_group:date');" "the mapping must include an 'index' group") {
    Write-Host 'PASS: empty-table write without index group rejected'
} else { Write-Host 'FAIL: empty-table write without index group not rejected'; $script:failures++ }
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('$table', '$s1f', 'index:symbol,date,no_such_col');" "not found in source") {
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
if (Run-DuckDB-ExpectError "SET aligned_data_root='$dataRoot'; SELECT * FROM aligned_upsert('upsert_empty', '$s8f');" 'mapping is required for the first write') {
    Write-Host 'PASS: empty-table first write without mapping rejected'
} else { Write-Host 'FAIL: empty-table first write should require mapping'; $script:failures++ }
Remove-Item $emptyDir -Recurse -Force -ErrorAction SilentlyContinue

# ---- two-phase partial-group insert (M1 first, M2 later) ----------------------
# Two-phase partial-group insert: index holds ONLY the key columns;
# f/m1 owns v, g/m2 owns w. Same keys written twice - first with f/m1
# mapping (g/m2 partition does not exist yet), then with g/m2 mapping.
# The mutator must synthesize g/m2's first aligned part (R_i NULL rows,
# keyed rows carry w).
$m12dir = 'D:/proj/factorlake/testdata/upsert_m12'
Remove-Item $m12dir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$m12dir/t" | Out-Null
$s9f = 'D:/proj/factorlake/testdata/upsert_s9.parquet'
$s10f = 'D:/proj/factorlake/testdata/upsert_s10.parquet'
& $duckdb -c "COPY (SELECT 'a' AS symbol, DATE '2026-01-01' AS date, 1.0::DOUBLE AS v UNION ALL SELECT 'b', DATE '2026-01-01', 2.0) TO '$s9f' (FORMAT PARQUET);" 2>&1 | Out-Null
& $duckdb -c "COPY (SELECT 'a' AS symbol, DATE '2026-01-01' AS date, 10::BIGINT AS w UNION ALL SELECT 'b', DATE '2026-01-01', 20) TO '$s10f' (FORMAT PARQUET);" 2>&1 | Out-Null
$out = Run-DuckDB "SET aligned_data_root='$m12dir'; SELECT rows_inserted, rows_updated FROM aligned_upsert('t', '$s9f', 'index:symbol,date;f/m1:v');"
Expect-Equal 'M1 phase inserts' $out.Trim() '2,0'
$out = Run-DuckDB "SET aligned_data_root='$m12dir'; SELECT rows_inserted, rows_updated FROM aligned_upsert('t', '$s10f', 'index:symbol,date;g/m2:w');"
Expect-Equal 'M2 phase updates existing keys' $out.Trim() '0,2'
$out = Run-DuckDB "SET aligned_data_root='$m12dir'; SELECT v, w FROM aligned_table('t') WHERE symbol='b';"
Expect-Equal 'M1+M2 values merged' $out.Trim() '2.0,20'

# ---- append-to-last-part: grow the last part instead of creating a new one --
# Write 1000 rows to a fresh table, then append 500 more to the SAME partition
# with the SAME mapping (no schema evolution). The last part should grow from
# 0000-0000001000 to 0000-0000001500 (in-place append), NOT create 0001.
$atldir = 'D:/proj/factorlake/testdata/upsert_atl'
$atlTable = 'atltest'
$atlTableDir = Join-Path $atldir $atlTable
Remove-Item $atldir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$atldir/t" | Out-Null
$sA1 = 'D:/proj/factorlake/testdata/atl_s1.parquet'
$sA2 = 'D:/proj/factorlake/testdata/atl_s2.parquet'
$atlMap = "index:symbol,date,close,volume;factor/alpha101:alpha001,alpha002"
& $duckdb -c "COPY (WITH r AS (SELECT range AS r FROM range(0, 1000)) SELECT printf('%06d', r+1) AS symbol, DATE '2026-03-15' AS date, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CASE WHEN r%5=0 THEN CAST((r+1)*0.01 AS DOUBLE) ELSE NULL END AS alpha001, CASE WHEN r%7=0 THEN CAST((r+2)*0.02 AS DOUBLE) ELSE NULL END AS alpha002 FROM r) TO '$sA1' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'atl staging 1 failed' }
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$atlTable', '$sA1', '$atlMap');"
Expect-Equal 'atl write 1 (ins, upd, parts, txid)' $out.Trim() '1000,0,2,1'
# Verify the initial part 0000 has 1000 rows
$part0 = Join-Path $atlTableDir 'index\month=2026-03\0000-0000001000.parquet'
if (Test-Path $part0) { Write-Host 'PASS: atl initial part 0000-0000001000 exists' }
else { Write-Host 'FAIL: atl initial part missing'; $script:failures++ }
# Append 500 more rows to the same partition (same symbols range but offset,
# sorting AFTER the existing rows). Use symbols 010001..010500 so they sort
# after 000001..001000.
& $duckdb -c "COPY (WITH r AS (SELECT range AS r FROM range(1000, 1500)) SELECT printf('%06d', r+1) AS symbol, DATE '2026-03-15' AS date, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CASE WHEN r%5=0 THEN CAST((r+1)*0.01 AS DOUBLE) ELSE NULL END AS alpha001, CASE WHEN r%7=0 THEN CAST((r+2)*0.02 AS DOUBLE) ELSE NULL END AS alpha002 FROM r) TO '$sA2' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'atl staging 2 failed' }
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT rows_inserted, rows_updated, parts_rewritten, txid FROM aligned_upsert('$atlTable', '$sA2', '$atlMap');"
Expect-Equal 'atl append-to-last (ins, upd, parts, txid)' $out.Trim() '500,0,2,1'
# The last part should now be 0000-0000001500 (grown, not a new 0001 part)
$part0grown = Join-Path $atlTableDir 'index\month=2026-03\0000-0000001500.parquet'
$part1 = Join-Path $atlTableDir 'index\month=2026-03\0001-0000000500.parquet'
if ((Test-Path $part0grown) -and -not (Test-Path $part1)) {
    Write-Host 'PASS: atl last part grown in-place (0000-0000001500), no new part 0001'
} elseif (Test-Path $part1) {
    Write-Host 'FAIL: atl created a new part 0001 instead of growing 0000'; $script:failures++
} else {
    Write-Host 'FAIL: atl grown part 0000-0000001500 missing'; $script:failures++
}
# Also verify the alpha101 group grew its last part
$alphaGrown = Join-Path $atlTableDir 'factor\alpha101\month=2026-03\0000-0000001500.parquet'
if (Test-Path $alphaGrown) { Write-Host 'PASS: atl alpha101 last part also grown' }
else { Write-Host 'FAIL: atl alpha101 last part not grown'; $script:failures++ }
# Verify row count and data correctness
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT count(*), count(alpha001), sum(close) FROM aligned_table('$atlTable');"
$vals = ($out.Trim() -split ',')
Expect-Equal 'atl total rows after append' $vals[0] '1500'
Expect-Equal 'atl sum(close) 1..1500' $vals[2] '562875.0'
# Verify ordering is correct (symbol 001000 followed by 001001)
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT symbol FROM aligned_table('$atlTable') WHERE symbol IN ('001000','001001') ORDER BY symbol;"
$vals = ($out.Trim() -split "[\r\n]+")
Expect-Equal 'atl boundary symbols count' $vals.Count '2'
Expect-Equal 'atl first boundary symbol' $vals[0].Trim() '001000'
Expect-Equal 'atl second boundary symbol' $vals[1].Trim() '001001'
# Verify the old part no longer exists (superseded)
$part0old = Join-Path $atlTableDir 'index\month=2026-03\0000-0000001000.parquet'
if (-not (Test-Path $part0old)) { Write-Host 'PASS: atl old part 0000-0000001000 removed' }
else { Write-Host 'FAIL: atl old part 0000-0000001000 still exists'; $script:failures++ }

# ---- append-to-last with schema evolution falls back to new part -------------
# Using the same atl table, append with a NEW column (alpha003) that doesn't
# exist in the last part. This should trigger the schema-evolution fallback
# and create a new part 0001 instead of growing 0000.
$sA3 = 'D:/proj/factorlake/testdata/atl_s3.parquet'
$atlMap3 = "index:symbol,date,close,volume;factor/alpha101:alpha001,alpha002,alpha003"
& $duckdb -c "COPY (WITH r AS (SELECT range AS r FROM range(1500, 2000)) SELECT printf('%06d', r+1) AS symbol, DATE '2026-03-15' AS date, CAST((r+1)*0.5 AS DOUBLE) AS close, CAST((r+1)*100 AS BIGINT) AS volume, CASE WHEN r%5=0 THEN CAST((r+1)*0.01 AS DOUBLE) ELSE NULL END AS alpha001, CASE WHEN r%7=0 THEN CAST((r+2)*0.02 AS DOUBLE) ELSE NULL END AS alpha002, CASE WHEN r%11=0 THEN CAST((r+3)*0.03 AS DOUBLE) ELSE NULL END AS alpha003 FROM r) TO '$sA3' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'atl staging 3 failed' }
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT rows_inserted, rows_updated, parts_rewritten FROM aligned_upsert('$atlTable', '$sA3', '$atlMap3');"
Expect-Equal 'atl schema-evolution fallback (ins, upd, parts)' $out.Trim() '500,0,2'
# A new part 0001 should be created (not growing 0000)
$part1new = Join-Path $atlTableDir 'index\month=2026-03\0001-0000000500.parquet'
$part0still1500 = Join-Path $atlTableDir 'index\month=2026-03\0000-0000001500.parquet'
if ((Test-Path $part1new) -and (Test-Path $part0still1500)) {
    Write-Host 'PASS: atl schema-evolution created new part 0001, 0000 unchanged'
} else {
    Write-Host 'FAIL: atl schema-evolution fallback part layout wrong'; $script:failures++
}
# Total rows should now be 2000
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT count(*) FROM aligned_table('$atlTable');"
Expect-Equal 'atl total after schema-evo append' $out.Trim() '2000'
# alpha003 should be NULL in old rows and non-NULL in new rows
$out = Run-DuckDB "SET aligned_data_root='$atldir'; SELECT count(alpha003) FROM aligned_table('$atlTable');"
Expect-Equal 'atl alpha003 non-null (r%11==0 in 1500..1999)' $out.Trim() '45'

Remove-Item $sA1, $sA2, $sA3 -Force -ErrorAction SilentlyContinue
Remove-Item $atldir -Recurse -Force -ErrorAction SilentlyContinue

# ---- concurrent write lock (stale lock file blocks writes) --------------------
$lockdir = 'D:/proj/factorlake/testdata/upsert_lock'
$lockTable = 'locktest'
$lockTableDir = Join-Path $lockdir $lockTable
Remove-Item $lockdir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $lockdir | Out-Null
$sL1 = 'D:/proj/factorlake/testdata/lock_s1.parquet'
& $duckdb -c "COPY (SELECT 'a' AS symbol, DATE '2026-01-01' AS date, 1.0::DOUBLE AS v) TO '$sL1' (FORMAT PARQUET);" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'lock staging failed' }
$lockMap = "index:symbol,date;f/m1:v"
$out = Run-DuckDB "SET aligned_data_root='$lockdir'; SELECT rows_inserted FROM aligned_upsert('$lockTable', '$sL1', '$lockMap');"
Expect-Equal 'lock write 1 inserts' $out.Trim() '1'
# Create a stale lock file — the next write should be blocked
$lockFile = Join-Path $lockTableDir '.aligned_write.lock'
if (-not (Test-Path $lockTableDir)) { New-Item -ItemType Directory -Force -Path $lockTableDir | Out-Null }
Set-Content -Path $lockFile -Value 'locked' -Encoding Ascii
if (Run-DuckDB-ExpectError "SET aligned_data_root='$lockdir'; SELECT * FROM aligned_upsert('$lockTable', '$sL1', '$lockMap');" 'another write is in progress') {
    Write-Host 'PASS: stale lock file blocks concurrent write'
} else { Write-Host 'FAIL: stale lock file did not block write'; $script:failures++ }
# After removing the lock, writes should succeed again (same key = update, not insert)
Remove-Item $lockFile -Force
$out = Run-DuckDB "SET aligned_data_root='$lockdir'; SELECT rows_inserted, rows_updated FROM aligned_upsert('$lockTable', '$sL1', '$lockMap');"
Expect-Equal 'lock write after cleanup (ins, upd)' $out.Trim() '0,1'
# Verify the lock file is cleaned up after a successful write
if (-not (Test-Path $lockFile)) { Write-Host 'PASS: lock file removed after write' }
else { Write-Host 'FAIL: lock file not removed after write'; $script:failures++ }
Remove-Item $sL1 -Force -ErrorAction SilentlyContinue
Remove-Item $lockdir -Recurse -Force -ErrorAction SilentlyContinue

# ---- cleanup -----------------------------------------------------------------
Remove-Item $s1, $s2, $s3, $s4, $s5, $s6, $s7, $s8 -Force -ErrorAction SilentlyContinue
Remove-Item $s9f, $s10f -Force -ErrorAction SilentlyContinue

Write-Host ''
if ($failures -eq 0) { Write-Host 'ALL TESTS PASSED' } else { Write-Host "$failures TEST(S) FAILED"; exit 1 }