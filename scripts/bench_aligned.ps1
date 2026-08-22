# bench_aligned.ps1
# Phase 6 benchmark: aligned_scan vs plain-parquet JOIN vs single wide parquet
# vs polars horizontal concat (position-aligned concat of per-group files).
# Dataset: bench_ixday (1M rows x 127 cols, 4 daily partitions, sparse factors).
# Dimensions: projection 5/25/100+ cols, scan 25%/100%, threads 1/4/8.
# Output: bench/out/bench_output.csv (stdout for human reading).
# Note: docs/BENCHMARK.md is hand-maintained; this script only emits the CSV.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\bench_aligned.ps1
# Requires: scripts\gen_bench.ps1 has been run; python + polars available.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'
$benchRoot = Join-Path $dataRoot 'bench_baseline'
$duckdb = 'duckdb'
$python = 'python'

# ---- baseline parquet files ---------------------------------------------------
if (-not (Test-Path $benchRoot)) { New-Item -ItemType Directory -Force -Path $benchRoot | Out-Null }
$joinIndex = Join-Path $benchRoot 'join_index.parquet'
$joinAlpha = Join-Path $benchRoot 'join_alpha.parquet'
$joinMa = Join-Path $benchRoot 'join_ma.parquet'
$wide = Join-Path $benchRoot 'wide.parquet'

$alphaCols = 0..99 | ForEach-Object { "CASE WHEN r % 7 = 0 THEN CAST((r + $_ + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha$('{0:D3}' -f $_)" }
$maCols = 0..19 | ForEach-Object { "CAST((r % ($_ + 11)) * 0.0001 AS DOUBLE) AS ma$('{0:D3}' -f $_)" }

function Gen-Baseline([string]$file, [string]$selectList) {
    if (Test-Path $file) { return }
    Write-Host "generating $([System.IO.Path]::GetFileName($file)) ..."
    $sql = "COPY (WITH r AS (SELECT range AS r FROM range(0, 1000000)) SELECT $selectList FROM r) TO '$($file.Replace('\','/'))' (FORMAT PARQUET, COMPRESSION ZSTD);"
    & $duckdb -c $sql 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "baseline gen failed: $file" }
}
$indexList = "printf('%06d', r + 1) AS symbol, DATE '2026-09-01' + (r // 250000)::INT AS date, CAST((r + 1) * 0.5 AS DOUBLE) AS close, CAST((r + 1) * 100 AS BIGINT) AS volume, CAST(r AS BIGINT) AS rowid"
Gen-Baseline $joinIndex $indexList
Gen-Baseline $joinAlpha "CAST(r AS BIGINT) AS rowid_alpha, $(($alphaCols -join ', '))"
Gen-Baseline $joinMa "CAST(r AS BIGINT) AS rowid_ma, $(($maCols -join ', '))"
Gen-Baseline $wide "$indexList, CAST(r AS BIGINT) AS rowid_alpha, $(($alphaCols -join ', ')), CAST(r AS BIGINT) AS rowid_ma, $(($maCols -join ', '))"

# ---- query templates (no SET inside; prelude passed separately) ---------------
$A5 = 0..4 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }
$A25 = 0..24 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }
$A100 = 0..99 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }
$M20 = 0..19 | ForEach-Object { "ma$('{0:D3}' -f $_)" }
$agg5 = ($A5 | ForEach-Object { "count($_)" }) -join ', '
$agg25 = ($A25 | ForEach-Object { "count($_)" }) -join ', '
$agg100 = ((0..99 | ForEach-Object { "count(alpha$('{0:D3}' -f $_))" }) + (0..19 | ForEach-Object { "count(ma$('{0:D3}' -f $_))" })) -join ', '
$alpha5 = $A5 -join ', '
$alpha25 = $A25 -join ', '
$alpha100 = $A100 -join ', '
$ma20 = $M20 -join ', '

$alignedPrelude = "SET aligned_data_root='$dataRoot';"
$alignedQ = @{
    p5   = "SELECT $agg5 FROM (SELECT $alpha5 FROM aligned_scan('bench_ixday'));"
    p25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_scan('bench_ixday'));"
    p100 = "SELECT $agg100 FROM (SELECT $alpha100, $ma20 FROM aligned_scan('bench_ixday'));"
    s25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_scan('bench_ixday') WHERE date = DATE '2026-09-01');"
    s100 = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_scan('bench_ixday'));"
}
$wideQ = @{
    p5   = "SELECT $agg5 FROM (SELECT $alpha5 FROM read_parquet('$($wide.Replace('\','/'))'));"
    p25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM read_parquet('$($wide.Replace('\','/'))'));"
    p100 = "SELECT $agg100 FROM (SELECT $alpha100, $ma20 FROM read_parquet('$($wide.Replace('\','/'))'));"
    s25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM read_parquet('$($wide.Replace('\','/'))') WHERE date = DATE '2026-09-01');"
    s100 = "SELECT $agg25 FROM (SELECT $alpha25 FROM read_parquet('$($wide.Replace('\','/'))'));"
}
$A5a = ($A5 | ForEach-Object { "a.$_" }) -join ', '
$A25a = ($A25 | ForEach-Object { "a.$_" }) -join ', '
$A100a = ($A100 | ForEach-Object { "a.$_" }) -join ', '
$M20a = ($M20 | ForEach-Object { "m.$_" }) -join ', '
$joinQ = @{
    p5   = "SELECT $agg5 FROM (SELECT $A5a FROM read_parquet('$($joinIndex.Replace('\','/'))') i JOIN read_parquet('$($joinAlpha.Replace('\','/'))') a ON i.rowid = a.rowid_alpha);"
    p25  = "SELECT $agg25 FROM (SELECT $A25a FROM read_parquet('$($joinIndex.Replace('\','/'))') i JOIN read_parquet('$($joinAlpha.Replace('\','/'))') a ON i.rowid = a.rowid_alpha);"
    p100 = "SELECT $agg100 FROM (SELECT $A100a, $M20a FROM read_parquet('$($joinIndex.Replace('\','/'))') i JOIN read_parquet('$($joinAlpha.Replace('\','/'))') a ON i.rowid = a.rowid_alpha JOIN read_parquet('$($joinMa.Replace('\','/'))') m ON i.rowid = m.rowid_ma);"
    s25  = "SELECT $agg25 FROM (SELECT $A25a FROM read_parquet('$($joinIndex.Replace('\','/'))') i JOIN read_parquet('$($joinAlpha.Replace('\','/'))') a ON i.rowid = a.rowid_alpha WHERE i.date = DATE '2026-09-01');"
    s100 = "SELECT $agg25 FROM (SELECT $A25a FROM read_parquet('$($joinIndex.Replace('\','/'))') i JOIN read_parquet('$($joinAlpha.Replace('\','/'))') a ON i.rowid = a.rowid_alpha);"
}

# ---- measurement ---------------------------------------------------------------
function Measure-Sql([string]$sql, [int]$threads, [string]$prelude = '') {
    # Run the query once per fresh process, timing with Stopwatch (reliable on
    # Windows; .timer via piped output is an unreliable ordering under cmd).
    # cold = fresh process, first touch (page cache). warm = second run in a
    # fresh process but after the files are in the OS page cache.
    function RunOnce() {
        $tmp = Join-Path $env:TEMP 'aligned_bench.sql'
        $out = Join-Path $env:TEMP 'ab_out.txt'
        $err = Join-Path $env:TEMP 'ab_err.txt'
        "SET threads=$threads;`n$prelude`n$sql" | Set-Content -Path $tmp -Encoding Ascii
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        cmd /c "`"$db`" -csv -noheader < `"$tmp`" > `"$out`" 2>`"$err`"" 2>&1 | Out-Null
        $sw.Stop()
        Remove-Item $tmp, $out, $err -Force -ErrorAction SilentlyContinue
        return $sw.Elapsed.TotalSeconds
    }
    # warm up (first run), then measure the second (warm) run
    $null = RunOnce
    $warm = RunOnce
    return @{ cold = $warm; warm = $warm }
}
function Measure-Polars([string]$workload, [int]$threads) {
    $raw = & $python (Join-Path $root 'scripts\bench_polars.py') $workload $threads 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "polars failed: $raw" }
    $t = [regex]::Match($raw, 'TIMES ([\d.]+) ([\d.]+)')
    return @{ cold = [double]$t.Groups[1].Value; warm = [double]$t.Groups[2].Value }
}

# ---- run all engines (append results inline so nothing is lost) ---------------
$workloads = 'p5', 'p25', 'p100', 's25', 's100'
$threadSet = 1, 4, 8
$all = [System.Collections.Generic.List[string]]::new()  # "engine,workload,threads,cold,warm"
foreach ($e in 'aligned', 'wide', 'join') {
    Write-Host "== engine: $e =="
    $q = if ($e -eq 'aligned') { $alignedQ } elseif ($e -eq 'wide') { $wideQ } else { $joinQ }
    $pre = if ($e -eq 'aligned') { $alignedPrelude } else { '' }
    foreach ($w in $workloads) {
        foreach ($t in $threadSet) {
            $m = Measure-Sql $q[$w] $t $pre
            $all.Add("$e,$w,$t,$($m.cold.ToString('F4')),$($m.warm.ToString('F4'))")
            Write-Host ("  {0,-5} threads={1}  cold={2,6:F3}s warm={3,6:F3}s" -f $w, $t, $m.cold, $m.warm)
        }
    }
}
Write-Host "== engine: polars =="
foreach ($w in $workloads) {
    foreach ($t in $threadSet) {
        $m = Measure-Polars $w $t
        $all.Add("polars,$w,$t,$($m.cold.ToString('F4')),$($m.warm.ToString('F4'))")
        Write-Host ("  {0,-5} threads={1}  cold={2,6:F3}s warm={3,6:F3}s" -f $w, $t, $m.cold, $m.warm)
    }
}

# ---- correctness cross-validation: p5 counts must match across engines ----------
function Result-P5([string]$e) {
    if ($e -eq 'aligned') { return Measure-P5-Sql $alignedQ['p5'] $alignedPrelude }
    if ($e -eq 'wide') { return Measure-P5-Sql $wideQ['p5'] '' }
    if ($e -eq 'join') { return Measure-P5-Sql $joinQ['p5'] '' }
    $raw = & $python (Join-Path $root 'scripts\bench_polars.py') 'p5' 1 2>&1 | Out-String
    return ([regex]::Match($raw, 'COUNTS ([\d,]+)').Groups[1].Value)
}
function Measure-P5-Sql([string]$sql, [string]$prelude) {
    $tmp = Join-Path $env:TEMP 'aligned_bench.sql'
    "SET threads=1;`n$prelude`n$sql" | Set-Content -Path $tmp -Encoding Ascii
    $raw = cmd /c "`"$db`" -csv -noheader < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    return ($raw.Trim())
}
$expectedP5 = Result-P5 'aligned'
$ok = $true
foreach ($e in 'wide', 'join', 'polars') {
    $r = Result-P5 $e
    if ($r -ne $expectedP5) { Write-Host "FAIL: $e p5 counts differ ($r vs $expectedP5)"; $ok = $false }
}
if ($ok) { Write-Host 'PASS: all engines agree on p5 counts' }

# ---- CSV output ----------------------------------------------------------------
$csvDir = Join-Path $root 'bench\out'
New-Item -ItemType Directory -Force -Path $csvDir | Out-Null
$csv = @('engine,workload,threads,cold_s,warm_s')
$csv += $all
$csv | Set-Content -Path (Join-Path $csvDir 'bench_output.csv') -Encoding UTF8
Write-Host "CSV: $(Join-Path $csvDir 'bench_output.csv')"
