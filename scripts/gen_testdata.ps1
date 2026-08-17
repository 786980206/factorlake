# gen_testdata.ps1
# Generates an AlignedTable test dataset under testdata/ (contract-compliant).
#
# Layout exercised by this script:
#   cnstk_ixday/
#     _table.json
#     index/  date=2026-08-17/  (2 parts, RGS 2048)   date=2026-08-18/  (2 parts, RGS 2048)
#     factor/alpha101/  year=2026/month=08/day=17/  (1 part, RGS 1000 — irregular RG boundaries)
#                       year=2026/month=08/day=18/  (1 part, RGS 1000, +alpha099: schema evolution)
#     fieldset/ma/      year=2026/month=08/         (2 parts from 2 transactions — coarse partitioning,
#                                                    one commit marker listing both, contract v1.1)
#
# Global row space: [0,3000) = 2026-08-17, [3000,6000) = 2026-08-18.
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.

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
    # ASCII encoding => no BOM, valid UTF-8 JSON (all test content is ASCII)
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -Path $path -Encoding Ascii
}

function Run-DuckDB([string]$sql) {
    & $duckdb -c $sql 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "duckdb failed: $sql" }
}

function Part-Name([int]$i) { 'part-{0:D6}' -f $i }

# ---- _table.json ------------------------------------------------------------
Write-JsonFile (Join-Path $tableDir '_table.json') @{
    name            = $table
    version         = 1
    schema_version  = 1
    key             = @('date', 'symbol')
    canonical_order = 'fixed'
    row_count       = 6000
    row_group_size  = 131072
    groups          = @('index', 'factor/alpha101', 'fieldset/ma')
}

# ---- group manifests --------------------------------------------------------
Write-JsonFile (Join-Path $tableDir 'index\_group.json') @{
    group          = 'index'
    row_count      = 6000
    row_group_size = 2048
    partitioning   = @(@{ template = 'date=%Y-%m-%d'; source = 'date' })
}

Write-JsonFile (Join-Path $tableDir 'factor\alpha101\_group.json') @{
    group          = 'factor/alpha101'
    row_count      = 6000
    row_group_size = 1000
    partitioning   = @(
        @{ template = 'year=%Y'; source = 'date' },
        @{ template = 'month=%m'; source = 'date' },
        @{ template = 'day=%d'; source = 'date' }
    )
}

Write-JsonFile (Join-Path $tableDir 'fieldset\ma\_group.json') @{
    group          = 'fieldset/ma'
    row_count      = 6000
    row_group_size = 2048
    partitioning   = @(
        @{ template = 'year=%Y'; source = 'date' },
        @{ template = 'month=%m'; source = 'date' }
    )
}

# ---- logical partitions -----------------------------------------------------
$Partitions = @(
    @{ date = '2026-08-17'; start = 0;    rows = 3000; txid = 1 },
    @{ date = '2026-08-18'; start = 3000; rows = 3000; txid = 2 }
)

foreach ($p in $Partitions) {
    $date = $p.date
    $start = [int]$p.start
    $rows = [int]$p.rows
    $end = $start + $rows
    $txid = [int]$p.txid

    # ---- index group: day-level partition, 2 parts per day (RGS 2048) -------
    $indexDir = Join-Path $tableDir "index\date=$date"
    New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
    $partStarts = @(0, 2048)
    for ($i = 0; $i -lt $partStarts.Count; $i++) {
        $ps = $partStarts[$i]
        $pc = [Math]::Min(2048, $rows - $ps)
        $gStart = $start + $ps
        $gEnd = $gStart + $pc
        $partName = Part-Name $i
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($gStart, $gEnd))
  SELECT DATE '$date' AS date,
         printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($indexDir.Replace('\','/'))/$partName.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
        Run-DuckDB $sql
        Write-JsonFile (Join-Path $indexDir "$partName.aligned.json") @{
            table          = $table
            group          = 'index'
            part           = $partName
            start_row      = $gStart
            row_count      = $pc
            row_group_size = 2048
            columns        = @('date', 'symbol', 'close', 'volume', 'rowid')
        }
    }
    Write-JsonFile (Join-Path $indexDir '.aligned-commit.json') @{
        txid  = $txid
        parts = @((Part-Name 0), (Part-Name 1))
    }

    # ---- alpha101 group: year/month/day partition, 1 part (RGS 1000) --------
    $alphaDir = Join-Path $tableDir "factor\alpha101\year=2026\month=08\day=$date"
    New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
    $alphaCols = 1..10 | ForEach-Object {
        "CASE WHEN r % 5 = 0 THEN CAST((r + $_ + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha$('{0:D3}' -f $_)"
    }
    $extra = ''
    if ($date -eq '2026-08-18') {
        # schema evolution: this partition introduces a new column
        $extra = ", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha099"
    }
    # close duplicates the index column (contract §2.2e.1: ignored by the reader);
    # vwap duplicates across non-index groups (contract §2.2e.2: qualified names only)
    $alphaColList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols +
        @('CAST((r + 1) * 0.25 AS DOUBLE) AS close', 'CAST((r + 1) * 0.125 AS DOUBLE) AS vwap')
    $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($start, $end))
  SELECT $(($alphaColList -join ', '))$extra
  FROM r
) TO '$($alphaDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000, COMPRESSION ZSTD);"
    Run-DuckDB $sql
    $alphaColumns = @('rowid_alpha') + (1..10 | ForEach-Object { "alpha$('{0:D3}' -f $_)" }) +
        @('close', 'vwap')
    if ($extra) { $alphaColumns += 'alpha099' }
    Write-JsonFile (Join-Path $alphaDir 'part-000000.aligned.json') @{
        table          = $table
        group          = 'factor/alpha101'
        part           = 'part-000000'
        start_row      = $start
        row_count      = $rows
        row_group_size = 1000
        columns        = $alphaColumns
    }
    Write-JsonFile (Join-Path $alphaDir '.aligned-commit.json') @{
        txid  = $txid
        parts = @('part-000000')
    }

    # ---- ma group: COARSE year/month partition (both days share one dir) ----
    $maDir = Join-Path $tableDir 'fieldset\ma\year=2026\month=08'
    New-Item -ItemType Directory -Force -Path $maDir | Out-Null
    $partIdx = $p.txid - 1
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
    Write-JsonFile (Join-Path $maDir "$partName.aligned.json") @{
        table          = $table
        group          = 'fieldset/ma'
        part           = $partName
        start_row      = $start
        row_count      = $rows
        row_group_size = 2048
        columns        = @('rowid_ma', 'ma5', 'ma10', 'ma20', 'close', 'vwap')
    }
    # second transaction appends its part to the shared marker (contract v1.1)
    $markerParts = @()
    for ($k = 0; $k -le $partIdx; $k++) { $markerParts += (Part-Name $k) }
    Write-JsonFile (Join-Path $maDir '.aligned-commit.json') @{
        txid  = $txid
        parts = $markerParts
    }
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
