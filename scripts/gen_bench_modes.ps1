# gen_bench_modes.ps1
# Generates three AlignedTable datasets with identical logical content
# (1M rows x 127 cols: index 5 + alpha101 101 + ma 21, 4 daily partitions,
# sparse factors 1/7) but different part layouts, so that the v3 aligned-mode
# probe resolves each table to a different mode:
#
#   bench_all    - every group 4 parts x 250000 rows   -> probe: "all"
#   bench_group  - index 4x250000, alpha/ma 8x125000   -> probe: "group"
#   bench_none   - index 16 parts (65536x3+53392/day,  -> probe: "none"
#                  exactly the v2 bench_ixday layout)
#
# No _table.json is written: the v3 defaults + probe chain apply.
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\gen_bench_modes.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $root 'testdata'
$duckdb = 'duckdb'

$TotalRows = 1000000
$Days = @('2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04')
$rowsPerDay = [long]($TotalRows / $Days.Count)

function Part-Name([int]$i) { 'part-{0:D6}' -f $i }
function Run-DuckDB([string]$sql) {
    & $duckdb -c $sql 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql" }
}

$alphaCols = 0..99 | ForEach-Object {
    "CASE WHEN r % 7 = 0 THEN CAST((r + $_ + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha$('{0:D3}' -f $_)"
}
$maCols = 0..19 | ForEach-Object {
    "CAST((r % ($_ + 11)) * 0.0001 AS DOUBLE) AS ma$('{0:D3}' -f $_)"
}

# index part generator: (start, end) -> SQL for one or more COPY statements.
# $chunk divides the day into $chunk-sized pieces (v2 layout) or one piece.
function Write-Index([string]$dir, [long]$start, [long]$end, [string]$date, [long]$chunk) {
    $ps = [long]0
    $i = 0
    while ($ps -lt ($end - $start)) {
        $pc = [Math]::Min($chunk, ($end - $start) - $ps)
        $gStart = $start + $ps
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($gStart, $($gStart + $pc)))
  SELECT DATE '$date' AS date, printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($dir.Replace('\','/'))/$(Part-Name $i).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 32768, COMPRESSION ZSTD);"
        Run-DuckDB $sql
        $ps += $pc
        $i++
    }
}

# group part writer for alpha/ma: splits [start,end) into $parts parts.
# Part names are globally unique across days ($base = day's first part index)
# because alpha/ma directories are shared between days.
function Write-Group([string]$dir, [long]$start, [long]$end, [int]$parts, [int]$rgs, [string]$colList, [int]$base) {
    $step = [long](($end - $start) / $parts)
    for ($i = 0; $i -lt $parts; $i++) {
        $s = $start + $i * $step
        $e = if ($i -eq $parts - 1) { $end } else { $s + $step }
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($s, $e))
  SELECT $colList
  FROM r
) TO '$($dir.Replace('\','/'))/$(Part-Name ($base + $i)).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $rgs, COMPRESSION ZSTD);"
        Run-DuckDB $sql
    }
}

$layouts = @(
    @{ table = 'bench_all';    index_parts = 1; alpha_parts = 1; ma_parts = 1 },
    @{ table = 'bench_group';  index_parts = 1; alpha_parts = 2; ma_parts = 2 },
    @{ table = 'bench_none';   index_parts = 4; alpha_parts = 1; ma_parts = 1 }
)

foreach ($L in $layouts) {
    $table = $L.table
    $tableDir = Join-Path $dataRoot $table
    if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $tableDir | Out-Null
    Write-Host "generating $table ..."

    $dayStart = [long]0
    for ($d = 0; $d -lt $Days.Count; $d++) {
        $date = $Days[$d]
        $start = $dayStart
        $end = $start + $rowsPerDay

        # index: chunked only in the "none" layout (4 chunks/day, v2 layout)
        $indexDir = Join-Path $tableDir "index\date=$date"
        New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
        if ($L.index_parts -eq 4) {
            Write-Index $indexDir $start $end $date 65536
        } else {
            Write-Index $indexDir $start $end $date ($end - $start)
        }

        # alpha: 1 or 2 parts per day (v3 layout; v2 = 1 part/day)
        $alphaDir = Join-Path $tableDir "factor\alpha101\year=2026\month=2026-09\date=$date"
        New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
        $alphaColsList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols
        Write-Group $alphaDir $start $end $L.alpha_parts 65536 ($alphaColsList -join ', ') ($d * $L.alpha_parts)

        # ma: coarse year/month partition (part names global across days)
        $maDir = Join-Path $tableDir 'fieldset\ma\year=2026\month=2026-09'
        New-Item -ItemType Directory -Force -Path $maDir | Out-Null
        $maColsList = @('CAST(r AS BIGINT) AS rowid_ma') + $maCols
        Write-Group $maDir $start $end $L.ma_parts 65536 ($maColsList -join ', ') ($d * $L.ma_parts)

        $dayStart += $rowsPerDay
    }
    Write-Host "  done: $table (no _table.json - v3 defaults + probe)"
}

Write-Host 'Done. Expected probe results: bench_all -> all, bench_group -> group, bench_none -> none'