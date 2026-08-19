# bench_modes.ps1
# v3 aligned-mode benchmark: same logical data (1M rows x 127 cols, 4 daily
# partitions) in three part layouts so the v3 probe resolves each table to a
# different mode:
#
#   bench_all   -> "all"   (every group 4 parts x 250000)
#   bench_group -> "group" (index 4x250000, alpha/ma 8x125000)
#   bench_none  -> "none"  (index 16 parts/day-65536x3+53392 == v2 bench_ixday
#                           layout; alpha/ma 4x250000)
#
# Workloads p5/p25/p100/s25/s100 x threads 1/4/8, aligned engine only
# (engine code identical; only the layout / probe mode differs). Results are
# compared against the v2 numbers in docs/BENCHMARK.md (aligned engine).
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\bench_modes.ps1
# Requires: scripts\gen_bench_modes.ps1 has been run.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$db = Join-Path $root 'duckdb\build3\duckdb_al3.exe'
if (-not (Test-Path $db)) { throw "build missing: $db" }
$dataRoot = 'D:/proj/factorlake/testdata'

# v2 baseline (aligned engine, docs/BENCHMARK.md, warm seconds)
$v2 = @{
    p5   = @(0.163, 0.119, 0.102)
    p25  = @(0.508, 0.271, 0.205)
    p100 = @(1.986, 0.951, 0.952)
    s25  = @(0.155, 0.106, 0.100)
    s100 = @(0.511, 0.271, 0.203)
}

$A5 = 0..4 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }
$A25 = 0..24 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }
$agg5 = ($A5 | ForEach-Object { "count($_)" }) -join ', '
$agg25 = ($A25 | ForEach-Object { "count($_)" }) -join ', '
$agg100 = ((0..99 | ForEach-Object { "count(alpha$('{0:D3}' -f $_))" }) + (0..19 | ForEach-Object { "count(ma$('{0:D3}' -f $_))" })) -join ', '
$alpha5 = $A5 -join ', '
$alpha25 = $A25 -join ', '
$alpha100 = (0..99 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }) -join ', '
$ma20 = (0..19 | ForEach-Object { "ma$('{0:D3}' -f $_)" }) -join ', '

function Q([string]$table) {
    return @{
        p5   = "SELECT $agg5 FROM (SELECT $alpha5 FROM aligned_table('$table'));"
        p25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_table('$table'));"
        p100 = "SELECT $agg100 FROM (SELECT $alpha100, $ma20 FROM aligned_table('$table'));"
        s25  = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_table('$table') WHERE date = DATE '2026-09-01');"
        s100 = "SELECT $agg25 FROM (SELECT $alpha25 FROM aligned_table('$table'));"
    }
}

function Measure-Sql([string]$sql, [int]$threads) {
    function RunOnce() {
        $tmp = Join-Path $env:TEMP 'aligned_modes.sql'
        $out = Join-Path $env:TEMP 'am_out.txt'
        $err = Join-Path $env:TEMP 'am_err.txt'
        "SET threads=$threads;`nSET aligned_data_root='$dataRoot';`n$sql" | Set-Content -Path $tmp -Encoding Ascii
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        cmd /c "`"$db`" -csv -noheader < `"$tmp`" > `"$out`" 2>`"$err`"" 2>&1 | Out-Null
        $sw.Stop()
        Remove-Item $tmp, $out, $err -Force -ErrorAction SilentlyContinue
        return $sw.Elapsed.TotalSeconds
    }
    $null = RunOnce
    return (RunOnce)
}

$tables = 'bench_all', 'bench_group', 'bench_none'
$workloads = 'p5', 'p25', 'p100', 's25', 's100'
$threadSet = 1, 4, 8
$all = [System.Collections.Generic.List[string]]::new()

foreach ($t in $tables) {
    $q = Q $t
    Write-Host "== table: $t =="
    foreach ($w in $workloads) {
        foreach ($i in 0..2) {
            $th = $threadSet[$i]
            $sec = Measure-Sql $q[$w] $th
            $all.Add("$t,$w,$th,$($sec.ToString('F4'))")
            $v2s = $v2[$w][$i]
            $ratio = if ($v2s -gt 0) { $sec / $v2s } else { 0 }
            Write-Host ("  {0,-5} threads={1}  {2,6:F3}s   v2={3,6:F3}s   ratio={4,5:F2}" -f $w, $th, $sec, $v2s, $ratio)
        }
    }
}

$report = Join-Path $root 'docs\BENCHMARK_MODES.md'
$rows = @()
foreach ($line in $all) {
    $p = $line -split ','
    $t = $p[0]; $w = $p[1]; $th = [int]$p[2]; $sec = [double]$p[3]
    $v2s = $v2[$w][@(1, 4, 8).IndexOf($th)]
    $ratio = if ($v2s -gt 0) { $sec / $v2s } else { 0 }
    $rows += "| $t | $w | $th | $('{0:F3}' -f $sec) | $('{0:F3}' -f $v2s) | $('{0:F2}' -f $ratio) |"
}
$lines = @(
    "# AlignedTable v3 Mode Benchmark (all / group / none)",
    "",
    "Date: $(Get-Date -Format 'yyyy-MM-dd')  Machine: local Windows (see AGENTS.md 16)",
    "Dataset: 1,000,000 rows x 127 columns (index 5 + alpha101 101 + ma 21), 4 daily",
    "partitions (2026-09-01..04), factors sparse (non-null 1/7). Three tables with",
    "**identical logical content** but different part layouts, no _table.json: the v3",
    "probe chain (all -> group -> none) resolves each table to its mode.",
    "",
    "- **bench_all**: every group 4 parts x 250000 -> probe **all** (formula start_row = i*part_rows)",
    "- **bench_group**: index 4x250000, alpha/ma 8x125000 -> probe **group** (per-group formula)",
    "- **bench_none**: index 16 parts (65536x3+53392/day, exactly the v2 bench_ixday layout);",
    "  alpha/ma 4x250000 -> probe **none** (footer accumulation)",
    "",
    "Engine: aligned_table() with projection pushdown, partition pruning, parallel range",
    "scan, metadata cache, window carry reuse (same binary for all three tables).",
    "",
    "## Workloads",
    "",
    "| id | description |",
    "|----|-------------|",
    "| p5 | project 5 factor columns, full scan |",
    "| p25 | project 25 factor columns, full scan |",
    "| p100 | project 120 columns (100 alpha + 20 ma), full scan |",
    "| s25 | project 25 columns, WHERE date = '2026-09-01' (25% scan, partition pruning) |",
    "| s100 | project 25 columns, full scan |",
    "",
    "## Results (seconds, warm run in fresh process; v2 = docs/BENCHMARK.md aligned engine,",
    "same machine, bench_ixday layout identical to bench_none)",
    "",
    "| table (mode) | workload | threads | v3 | v2 | v3/v2 |",
    "|--------------|----------|---------|------|------|-------|",
    $rows,
    "",
    "## Observations",
    "",
    "(filled in by the analysis step)"
)
$lines | Set-Content -Path $report -Encoding UTF8
$csv = @('table,workload,threads,v3_s,v2_s,ratio')
foreach ($row in $rows) {
    $p = $row -replace '^\| ', '' -split ' \| '
    $csv += "$($p[0]),$($p[1]),$($p[2]),$($p[3]),$($p[4]),$($p[5])"
}
$csv | Set-Content -Path (Join-Path $root 'scripts\bench_modes_output.csv') -Encoding UTF8
Write-Host "Report: $report"
