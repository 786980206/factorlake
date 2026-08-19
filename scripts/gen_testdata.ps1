# gen_testdata.ps1
# Generates an AlignedTable test dataset under testdata/ (v3 contract).
#
# Layout exercised by this script (fully aligned — every group has the same
# part count / part size / last-part size, the only supported contract):
#   cnstk_ixday/
#     _table.json                              (includes optional part_rows override)
#     index/        date=2026-08-17/  (1 part, 3000 rows, RGS 2048)
#                   date=2026-08-18/  (1 part, 3000 rows, RGS 2048)
#     factor/alpha101/ year=2026/month=08/date=17/  (1 part, 3000 rows, RGS 1000)
#                      year=2026/month=08/date=18/  (1 part, 3000 rows, RGS 1000, +alpha099 schema evolution)
#     fieldset/ma/  year=2026/month=08/           (2 parts from 2 transactions — coarse partitioning)
#
# Global row space: [0,3000) = 2026-08-17, [3000,6000) = 2026-08-18.
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.
#
# v3 contract (2026-08):
#   * Metadata is read from _table.json + per-directory layout + Parquet footer
#     (only the footer is authoritative; no sidecars, no commit markers).
#   * Partition names must be one of: year=, month=, date= (no day=).
#   * PART_ROWS defaults to 4_194_304; optional override in _table.json's part_rows.

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $root 'testdata'
$table = 'cnstk_ixday'
$tableDir = Join-Path $dataRoot $table
$duckdb = 'duckdb'

if (Test-Path $dataRoot) { Remove-Item $dataRoot -Recurse -Force }
New-Item -ItemType Directory -Force -Path $tableDir | Out-Null

function Write-JsonFile([string]$path, $obj) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}

function Run-DuckDB([string]$sql) {
    & $duckdb -c $sql 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql" }
}

function Part-Name([int]$i) { 'part-{0:D6}' -f $i }

# ---- _table.json ------------------------------------------------------------
# The only manifest. Group metadata (row counts, partitioning) is derived from
# the directory layout + Parquet footers; no _group.json files exist. The
# explicit partitioning map is omitted on purpose so the reader's directory
# derivation path is exercised.
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name            = $table
    version         = 1
    part_rows       = 4194304
    groups          = @('index', 'factor/alpha101', 'fieldset/ma')
}

# ---- logical partitions -----------------------------------------------------
$Partitions = @(
    @{ date = '2026-08-17'; start = 0;    rows = 3000 },
    @{ date = '2026-08-18'; start = 3000; rows = 3000 }
)

foreach ($p in $Partitions) {
    $date = $p.date
    $start = [int]$p.start
    $rows = [int]$p.rows
    $end = $start + $rows

    # ---- index group: date-level partition, 1 part per day (RGS 2048) -------
    $indexDir = Join-Path $tableDir "index\date=$date"
    New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT DATE '$date' AS date,
         printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($indexDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    # ---- alpha101 group: year/month/date partition, 1 part (RGS 1000) --------
    $alphaDir = Join-Path $tableDir "factor\alpha101\year=2026\month=08\date=$date"
    New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
    $alphaCols = 1..10 | ForEach-Object {
        "CASE WHEN r % 5 = 0 THEN CAST((r + $_ + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha$('{0:D3}' -f $_)"
    }
    $extra = ''
    if ($date -eq '2026-08-18') {
        # schema evolution: this partition introduces a new column
        $extra = ", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha099"
    }
    $alphaColList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols +
        @('CAST((r + 1) * 0.25 AS DOUBLE) AS close', 'CAST((r + 1) * 0.125 AS DOUBLE) AS vwap')
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($alphaColList -join ', '))$extra
  FROM r
) TO '$($alphaDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000, COMPRESSION ZSTD);"
    Run-DuckDB $sql

    # ---- ma group: COARSE year/month partition (both days share one dir) ----
    $maDir = Join-Path $tableDir 'fieldset\ma\year=2026\month=08'
    New-Item -ItemType Directory -Force -Path $maDir | Out-Null
    $partIdx = $Partitions.IndexOf($p)
    $partName = Part-Name $partIdx
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT CAST(r AS BIGINT) AS rowid_ma,
         CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
         CAST((r % 30) * 0.05 AS DOUBLE) AS ma10,
         CAST((r % 60) * 0.025 AS DOUBLE) AS ma20,
         CAST((r + 1) * 0.0625 AS DOUBLE) AS close,
         CAST((r + 1) * 0.03125 AS DOUBLE) AS vwap
  FROM r
) TO '$($maDir.Replace('\','/'))/$partName.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
    Run-DuckDB $sql
}

# ---- ignored directory (contract §2.1d): a _tmp directory with stray parts --
$tmpDir = Join-Path $tableDir '_tmp\transaction-999\index\date=2026-08-17'
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
$sql = "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 100))
  SELECT DATE '2026-08-17' AS date, printf('%06d', r + 1) AS symbol, CAST(r AS DOUBLE) AS close
  FROM r
) TO '$($tmpDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET);"
Run-DuckDB $sql

Write-Host "Test data generated under $dataRoot"
Write-Host "  table: $table, rows: 6000, groups: index / factor/alpha101 / fieldset/ma"
