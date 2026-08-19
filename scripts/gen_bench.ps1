# gen_bench.ps1
# Generates a larger AlignedTable test dataset for Phase 4 (parallel scan)
# and Phase 6 (benchmark). Contract-compliant: index mandatory, two-level
# non-index groups, no sidecars, no commit markers, no _group.json, _tmp
# ignored. Partition names: year= / month= / date= only.
#
# Layout:
#   bench_ixday/
#     _table.json                     row_count = $TotalRows
#     index/   date=2026-09-01..04/   4 parts per day (PART_ROWS each), RGS 32768
#     factor/alpha101/ year=2026/month=2026-09/date=2026-09-01..04/   1 part per day, RGS 65536, 100 sparse cols
#     fieldset/ma/     year=2026/month=2026-09/                      4 parts (one per day), RGS 65536, 20 cols
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

function Write-JsonFile([string]$path, $obj) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}
function Part-Name([int]$i) { 'part-{0:D6}' -f $i }
function Run-DuckDB([string]$sql) {
    & $duckdb -c $sql 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql" }
}

Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name            = $table
    version         = 1
    schema_version  = 1
    key             = @('date', 'symbol')
    canonical_order = 'fixed'
    row_count       = $TotalRows
    row_group_size  = 131072
    groups          = @('index', 'factor/alpha101', 'fieldset/ma')
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

    # ---- index: 4 parts per day (PART_ROWS each), RGS 32768 -----------------
    $indexDir = Join-Path $tableDir "index\date=$date"
    New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
    $ps = 0
    while ($ps -lt $rows) {
        $pc = [Math]::Min($PART_ROWS, $rows - $ps)
        $gStart = $start + $ps
        $partName = Part-Name ($ps / $PART_ROWS)
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($gStart, $($gStart + $pc)))
  SELECT DATE '$date' AS date, printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($indexDir.Replace('\','/'))/$partName.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_INDEX, COMPRESSION ZSTD);"
        Run-DuckDB $sql
        $ps += $pc
    }

    # ---- alpha101: 1 part per day (RGS 65536), 100 sparse cols --------------
    $alphaDir = Join-Path $tableDir "factor\alpha101\year=2026\month=2026-09\date=$date"
    New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
    $colList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($colList -join ', '))
  FROM r
) TO '$($alphaDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    # ---- ma: coarse year/month partition, 1 part per day --------------------
    $maDir = Join-Path $tableDir 'fieldset\ma\year=2026\month=2026-09'
    New-Item -ItemType Directory -Force -Path $maDir | Out-Null
    $partName = Part-Name ($txid - 1)
    $colList = @('CAST(r AS BIGINT) AS rowid_ma') + $maCols
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($colList -join ', '))
  FROM r
) TO '$($maDir.Replace('\','/'))/$partName.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE $RGS_BIG, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    $dayStart += $rows
}

Write-Host "Benchmark data generated under $dataRoot\$table"
Write-Host "  rows: $TotalRows, days: $($Days.Count), cols: index 5 + alpha 101 + ma 21"
