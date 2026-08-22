# gen_bench.ps1
# Generates a larger AlignedTable test dataset for Phase 4 (parallel scan)
# and Phase 6 (benchmark). v5 contract: index mandatory, two-level
# non-index groups, single-level partition (year= / month= / date= — the SAME
# kind for every group), only the footer is authoritative (no sidecars, no
# commit markers, no _group.json), _tmp ignored.
#
#   Layout (partition-aligned — index, alpha and ma share the date= kind; each
#   partition holds 1 part per group (v6 self-describing name "0000-{rows:10d}"),
#   so partition rows == part rows and every group has full coverage):
#   bench_ixday/
#     index/   date=2026-09-01..04/   1 part per day (rowsPerDay), RGS 32768
#     factor/alpha101/ date=2026-09-01..04/   1 part per day, RGS 65536, 100 sparse cols
#     fieldset/ma/     date=2026-09-01..04/   1 part per day, RGS 65536, 20 cols
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\gen_bench.ps1 [-TotalRows 1000000]

param([long]$TotalRows = 1000000)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $root 'testdata'
$table = 'bench_ixday'
$tableDir = Join-Path $dataRoot $table
$duckdb = 'duckdb'

if (Test-Path $tableDir) { Remove-Item $tableDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null

$PART_ROWS = 65536
$RGS_INDEX = 32768
$RGS_BIG = 65536
$Days = @(
    @{ date = '2026-09-01' },
    @{ date = '2026-09-02' },
    @{ date = '2026-09-03' },
    @{ date = '2026-09-04' }
)
$rowsPerDay = [long]($TotalRows / $Days.Count)
$PART_ROWS = $rowsPerDay

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

$txid = 0
$dayStart = [long]0
foreach ($d in $Days) {
    $txid++
    $date = $d.date
    $start = $dayStart
    $rows = $rowsPerDay
    $end = $start + $rows

    # ---- index: 1 part per day (fully aligned with alpha/ma), RGS 32768 -----
    $indexDir = Join-Path $tableDir "index\date=$date"
    New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT printf('%06d', r + 1) AS symbol, DATE '$date' AS date,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($indexDir.Replace('\','/'))/0000-$($rows.ToString('D10')).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    # ---- alpha101: 1 part per day (RGS 65536), 100 sparse cols --------------
    $alphaDir = Join-Path $tableDir "factor\alpha101\date=$date"
    New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
    $colList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($colList -join ', '))
  FROM r
) TO '$($alphaDir.Replace('\','/'))/0000-$($rows.ToString('D10')).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    # ---- ma: day-level partition, 1 part per day (same kind as index/alpha) --
    $maDir = Join-Path $tableDir "fieldset\ma\date=$date"
    New-Item -ItemType Directory -Force -Path $maDir | Out-Null
    $colList = @('CAST(r AS BIGINT) AS rowid_ma') + $maCols
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($colList -join ', '))
  FROM r
) TO '$($maDir.Replace('\','/'))/0000-$($rows.ToString('D10')).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    $dayStart += $rows
}

Write-Host "Benchmark data generated under $dataRoot\$table"
Write-Host "  rows: $TotalRows, days: $($Days.Count), cols: index 5 + alpha 101 + ma 21"
