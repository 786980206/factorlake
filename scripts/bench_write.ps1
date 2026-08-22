# bench_write.ps1 - Write-path benchmark: aligned upsert vs DuckDB-native parquet rewrite
#
# Measures the cost of applying a batch of changes to an EXISTING table:
#   aligned : aligned_upsert (only affected parts rewritten, atomic commit)
#   native  : DuckDB standard approach - read base parquet + apply batch in SQL
#             + COPY TO a fresh parquet file (full dataset rewrite), then swap.
#
# Workloads: append B new rows; update M existing rows. B/M in {1k, 10k, 100k}.
# Timing: fresh-process Stopwatch, single measured run per point (warm FS).
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\bench_write.ps1

$ErrorActionPreference = 'Stop'
$duckdb = 'D:\proj\factorlake\duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $duckdb)) { throw "duckdb binary not found: $duckdb" }
$oc = 'C:\Users\winds\AppData\Local\Temp\opencode'
New-Item -ItemType Directory -Force -Path $oc | Out-Null

$root = 'D:/proj/factorlake/testdata/bench_write'
$baseRows = 600000   # existing rows in the table
$day = '2026-05-01'  # all base rows in ONE partition month=2026-05 (worst case for aligned update: single part)

# ---- fresh workspace ---------------------------------------------------------
Remove-Item $root.Replace('/', '\') -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$root/aligned/t" | Out-Null
[IO.File]::WriteAllText("$root/aligned/t/_table.json",
  '{"groups":["index"],"partitioning":{"index":[{"template":"month=%Y-%m","source":"date"}]}}')

function Run-SqlFile([string]$path) {
    cmd /c "`"$duckdb`" -unsigned < `"$path`"" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "sql failed: $path" }
}
function Timed-Run([string]$sql, [string]$tag) {
    $f = Join-Path $oc ("w_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $f -Value $sql -Encoding Ascii
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    cmd /c "`"$duckdb`" -unsigned < `"$f`"" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "timed run failed: $tag" }
    $sw.Stop()
    Remove-Item $f -Force -ErrorAction SilentlyContinue
    return $sw.Elapsed.TotalSeconds
}

$results = [System.Collections.Generic.List[string]]::new()
$results.Add("scenario,batch,engine,seconds")

foreach ($batch in 1000, 10000, 100000) {

    # ================= scenario A: APPEND batch new keys =====================
    # --- aligned: reset table to base state, then one aligned_upsert ---
    Remove-Item "$root/aligned/t" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/aligned/t" | Out-Null
    [IO.File]::WriteAllText("$root/aligned/t/_table.json",
      '{"groups":["index"],"partitioning":{"index":[{"template":"month=%Y-%m","source":"date"}]}}')
    # base data via one big upsert (setup, not timed)
    $setupA = @"
SET aligned_data_root='$root/aligned';
COPY (SELECT DATE '$day' AS date, FORMAT('{:07d}', i) AS symbol, i::DOUBLE AS v FROM range($baseRows) t(i))
  TO '$root/base_a.parquet' (FORMAT PARQUET);
SELECT * FROM aligned_upsert('t','$root/base_a.parquet','index:date,symbol,v');
"@
    $sf = Join-Path $oc ("ws_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $sf -Value $setupA -Encoding Ascii
    Run-SqlFile $sf

    # batch staging parquet (new keys, next day partition)
    $batchStart = $baseRows
    $setupB = @"
COPY (SELECT DATE '2026-06-01' AS date, FORMAT('{:07d}', ${batchStart} + i) AS symbol, (${batchStart}+i)::DOUBLE AS v
      FROM range(${batch}) t(i)) TO '$root/batch_a.parquet' (FORMAT PARQUET);
"@
    $bf = Join-Path $oc ("wb_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $bf -Value $setupB -Encoding Ascii
    Run-SqlFile $bf
    Remove-Item $bf -Force -ErrorAction SilentlyContinue

    $alignedSql = @"
SET aligned_data_root='$root/aligned';
SELECT * FROM aligned_upsert('t','$root/batch_a.parquet','index:date,symbol,v');
"@
    $tAligned = Timed-Run $alignedSql "append-aligned-$batch"
    $results.Add("append,$batch,aligned,$('{0:F3}' -f $tAligned)")

    # restore base state for the native measurement (fresh dir each time)
    Remove-Item "$root/native" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/native" | Out-Null
    $nativeSql = @"
COPY (SELECT DATE '$day' AS date, FORMAT('{:07d}', i) AS symbol, i::DOUBLE AS v FROM range($baseRows) t(i))
  TO '$root/native/t.parquet' (FORMAT PARQUET);
COPY (SELECT DATE '2026-06-01' AS date, FORMAT('{:07d}', ${batchStart} + i) AS symbol, (${batchStart}+i)::DOUBLE AS v
      FROM range(${batch}) t(i)) TO '$root/native/batch.parquet' (FORMAT PARQUET);
COPY (
  SELECT * FROM read_parquet('$root/native/t.parquet')
  UNION ALL
  SELECT * FROM read_parquet('$root/native/batch.parquet')
) TO '$root/native/t_new.parquet' (FORMAT PARQUET);
"@
    $tNative = Timed-Run $nativeSql "append-native-$batch"
    $results.Add("append,$batch,native,$('{0:F3}' -f $tNative)")

    Write-Host ("append  batch={0,-6} aligned={1,7:F3}s  native={2,7:F3}s" -f $batch, $tAligned, $tNative)

    # ================= scenario U: UPDATE half the base rows =================
    # --- aligned: same-key upsert with new values ---
    Remove-Item "$root/aligned/t" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/aligned/t" | Out-Null
    [IO.File]::WriteAllText("$root/aligned/t/_table.json",
      '{"groups":["index"],"partitioning":{"index":[{"template":"month=%Y-%m","source":"date"}]}}')
    Run-SqlFile $sf   # rebuild base (same setupA)
    $updKeys = [long]($baseRows / 2)
    $setupU = @"
COPY (SELECT DATE '$day' AS date, FORMAT('{:07d}', i) AS symbol, 999.0::DOUBLE AS v
      FROM range(${updKeys}) t(i)) TO '$root/upd.parquet' (FORMAT PARQUET);
"@
    $uf = Join-Path $oc ("wu_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $uf -Value $setupU -Encoding Ascii
    Run-SqlFile $uf
    Remove-Item $uf -Force -ErrorAction SilentlyContinue

    $upsertSql = @"
SET aligned_data_root='$root/aligned';
SELECT * FROM aligned_upsert('t','$root/upd.parquet','index:date,symbol,v');
"@
    $tAligned2 = Timed-Run $upsertSql "update-aligned-$batch"
    $results.Add("update,$updKeys,aligned,$('{0:F3}' -f $tAligned2)")

    # --- native: SQL join-update + full rewrite ---
    Remove-Item "$root/native" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/native" | Out-Null
    # native update = full rewrite via base LEFT JOIN updates (coalesce values)
    $nativeSql2 = @"
COPY (SELECT DATE '$day' AS date, FORMAT('{:07d}', i) AS symbol, i::DOUBLE AS v FROM range($baseRows) t(i))
  TO '$root/native/t.parquet' (FORMAT PARQUET);
COPY (SELECT DATE '$day' AS date, FORMAT('{:07d}', i) AS symbol, 999.0::DOUBLE AS v
      FROM range(${updKeys}) t(i)) TO '$root/native/upd.parquet' (FORMAT PARQUET);
CREATE TEMP TABLE merged AS
  SELECT b.date, b.symbol, COALESCE(u.v, b.v) AS v
  FROM read_parquet('$root/native/t.parquet') b
  LEFT JOIN read_parquet('$root/native/upd.parquet') u
    ON b.date = u.date AND b.symbol = u.symbol;
COPY (SELECT date, symbol, v FROM merged) TO '$root/native/t_new.parquet' (FORMAT PARQUET);
"@
    $tNative2 = Timed-Run $nativeSql2 "update-native-$updKeys"
    $results.Add("update,$updKeys,native,$('{0:F3}' -f $tNative2)")

    Write-Host ("update  keys={0,-6} aligned={1,7:F3}s  native={2,7:F3}s" -f $updKeys, $tAligned2, $tNative2)
    Remove-Item $sf -Force -ErrorAction SilentlyContinue
}

Write-Host ''
Write-Host "==== RESULTS ===="
$results | ForEach-Object { Write-Host $_ }
$results | Set-Content -Path 'D:\proj\factorlake\docs\bench_write_results.csv' -Encoding Ascii
Write-Host "saved: docs\bench_write_results.csv"
