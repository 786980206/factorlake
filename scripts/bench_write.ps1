# bench_write.ps1 - Write-path benchmark: aligned DML vs DuckDB-native parquet rewrite
#
# Measures the cost of applying a batch of changes to an EXISTING table:
#   aligned : ATTACH + standard INSERT/UPDATE (only affected parts rewritten, atomic commit)
#   native  : DuckDB standard approach - read base parquet + apply batch in SQL
#             + COPY TO a fresh parquet file (full dataset rewrite), then swap.
#
# Workloads: append B new rows; update M existing rows. B/M in {1k, 10k, 100k}.
# Timing: fresh-process Stopwatch, single measured run per point (warm FS).
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\bench_write.ps1

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$duckdb = Join-Path $repo 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $duckdb)) { throw "duckdb binary not found: $duckdb" }
$oc = $env:TEMP
New-Item -ItemType Directory -Force -Path $oc | Out-Null

$root = 'D:/proj/factorlake/testdata/bench_write'
$day = '2026-05-01'
$baseRows = 1000000

function Run-SqlFile($path) {
    $out = cmd /c "`"$duckdb`" -unsigned -csv -noheader < `"$path`"" 2>&1
    return $out
}

function Timed-Run($sql, $label) {
    $f = Join-Path $env:TEMP ("bt_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $f -Value $sql -Encoding Ascii
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Run-SqlFile $f | Out-Null
    $sw.Stop()
    Remove-Item $f -Force -ErrorAction SilentlyContinue
    return $sw.Elapsed.TotalSeconds
}

$results = [System.Collections.ArrayList]@("scenario,batch,engine,seconds")

foreach ($batch in 1000, 10000, 100000) {

    # ================= scenario A: APPEND batch new keys =====================
    # --- aligned: reset table, then INSERT batch ---
    Remove-Item "$root/aligned" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/aligned" | Out-Null
    # base data via aligned_create + INSERT (setup, not timed)
    $setupA = @"
SET aligned_data_root='$root/aligned';
SELECT * FROM aligned_create('t', 'index', 'symbol VARCHAR, date DATE, v DOUBLE');
ATTACH '$root/aligned' AS al (TYPE ALIGNED);
INSERT INTO al.t SELECT FORMAT('{:07d}', i) AS symbol, DATE '$day' AS date, i::DOUBLE AS v FROM range($baseRows) t(i);
"@
    $sf = Join-Path $oc ("ws_" + [guid]::NewGuid().ToString('N') + ".sql")
    Set-Content -Path $sf -Value $setupA -Encoding Ascii
    Run-SqlFile $sf

    $batchStart = $baseRows
    $alignedSql = @"
SET aligned_data_root='$root/aligned';
ATTACH '$root/aligned' AS al (TYPE ALIGNED);
INSERT INTO al.t SELECT FORMAT('{:07d}', ${batchStart} + i) AS symbol, DATE '2026-06-01' AS date, (${batchStart}+i)::DOUBLE AS v FROM range(${batch}) t(i);
"@
    $tAligned = Timed-Run $alignedSql "append-aligned-$batch"
    $results.Add("append,$batch,aligned,$('{0:F3}' -f $tAligned)")

    # restore base state for the native measurement (fresh dir each time)
    Remove-Item "$root/native" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/native" | Out-Null
    $nativeSql = @"
COPY (SELECT FORMAT('{:07d}', i) AS symbol, DATE '$day' AS date, i::DOUBLE AS v FROM range($baseRows) t(i))
  TO '$root/native/t.parquet' (FORMAT PARQUET);
COPY (SELECT FORMAT('{:07d}', ${batchStart} + i) AS symbol, DATE '2026-06-01' AS date, (${batchStart}+i)::DOUBLE AS v
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
    # --- aligned: same-key UPDATE with new values ---
    Remove-Item "$root/aligned" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/aligned" | Out-Null
    Run-SqlFile $sf   # rebuild base (same setupA)
    $updKeys = [long]($baseRows / 2)
    $upsertSql = @"
SET aligned_data_root='$root/aligned';
ATTACH '$root/aligned' AS al (TYPE ALIGNED);
UPDATE al.t SET v = 999.0 WHERE symbol IN (SELECT FORMAT('{:07d}', i) FROM range(${updKeys}) t(i));
"@
    $tAligned2 = Timed-Run $upsertSql "update-aligned-$batch"
    $results.Add("update,$updKeys,aligned,$('{0:F3}' -f $tAligned2)")

    # --- native: SQL join-update + full rewrite ---
    Remove-Item "$root/native" -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path "$root/native" | Out-Null
    # native update = full rewrite via base LEFT JOIN updates (coalesce values)
    $nativeSql2 = @"
COPY (SELECT FORMAT('{:07d}', i) AS symbol, DATE '$day' AS date, i::DOUBLE AS v FROM range($baseRows) t(i))
  TO '$root/native/t.parquet' (FORMAT PARQUET);
CREATE TEMP TABLE merged AS
  SELECT b.symbol, b.date, COALESCE(u.v, b.v) AS v
  FROM read_parquet('$root/native/t.parquet') b
  LEFT JOIN (SELECT FORMAT('{:07d}', i) AS symbol, 999.0::DOUBLE AS v FROM range(${updKeys}) t(i)) u
  ON b.symbol = u.symbol;
COPY (SELECT * FROM merged) TO '$root/native/t_new.parquet' (FORMAT PARQUET);
"@
    $tNative2 = Timed-Run $nativeSql2 "update-native-$batch"
    $results.Add("update,$updKeys,native,$('{0:F3}' -f $tNative2)")

    Write-Host ("update  batch={0,-6} aligned={1,7:F3}s  native={2,7:F3}s" -f $updKeys, $tAligned2, $tNative2)

    Remove-Item $sf -Force -ErrorAction SilentlyContinue
}

# Write results CSV
$csv = Join-Path $root 'bench_results.csv'
$results | Out-File -FilePath $csv -Encoding UTF8
Write-Host "`nResults written to $csv"
