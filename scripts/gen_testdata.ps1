# gen_testdata.ps1
# Generates an AlignedTable test dataset under testdata/ (v6 contract).
#
# Layout exercised by this script (partition-aligned, single-level month=
# partition — every group uses the SAME partition kind; self-describing part
# names "{idx:04d}-{rows:10d}.parquet"):
#   cnstk_ixday/
#     index/        month=2026-07/  0000-0000002000.parquet (1 part, 2000 rows)
#                   month=2026-08/  0000-0000002000.parquet
#                                   0001-0000002000.parquet (2 parts, 2000+2000)
#     factor/alpha101/ month=2026-07/  0000-0000002000.parquet (1 part, 2000 rows)
#                      month=2026-08/  0000-0000002000.parquet
#                                      0002-0000002000.parquet  <-- index 0001 SKIPPED
#                                                               (deletion: gaps allowed
#                                                                in non-index groups)
#                                                              (0002 part adds alpha099
#                                                               schema evolution)
#     fieldset/ma/  month=2026-08/  0000-0000002000.parquet
#                                   0001-0000002000.parquet  <-- MISSING month=2026-07
#
# Global row space: [0,2000) = 2026-07, [2000,6000) = 2026-08. Row counts and
# start rows come from the FILE NAMES (no footer reads at plan time). The
# index group's indexes are consecutive from 0000; alpha101 SKIPS index 0001
# in month=2026-08 (legal — the index is only a group-local label; the shared
# partition's TOTAL row count still matches the index: 2000+2000=4000, and the
# shared index 0000 agrees on 2000 rows). The ma group omits 2026-07 entirely:
# rows [0,2000) read as NULL for its columns.
#
# Every group carries a `rowid` BIGINT column (test-only oracle) equal to the
# global row number, so alignment can be verified by cross-group comparison.
#
# v6 contract (2026-08):
#   * Single-level partition (year= / month= / date=); index and every group
#     use the same kind. Group partition keys must be a subset of the index's.
#   * Part files are named "{idx:04d}-{rows:10d}.parquet"; row counts and
#     start rows are derived from the names (zero footer reads at plan time;
#     ONE footer per group for the schema / index date-field contract).
#   * Index group indexes are consecutive from 0000; non-index groups may skip
#     indexes (deletion). Shared partitions must agree on the TOTAL row count
#     and every SHARED index's row count (fail-fast).
#   * The index schema's first two columns must contain a DATE/TIMESTAMP field
#     (the partition source column; here 'date').
#   * Missing partitions read as NULL (row space stays index-defined).

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

# v6 self-describing part name: "{idx:04d}-{rows:10d}.parquet"
function Part-Name([int]$idx, [int]$rows) { '{0:D4}-{1:D10}' -f $idx, $rows }

# ---- logical partitions -----------------------------------------------------
# per-part row count is 2000 everywhere (part_rows bookkeeping is from names).
$per = 2000
$Partitions = @(
    @{ month = '2026-07'; start = 0;    rows = 2000; date = '2026-07-01'; indexParts = @(0);    alphaParts = @(0);     maParts = @() },
    @{ month = '2026-08'; start = 2000; rows = 4000; date = '2026-08-01'; indexParts = @(0, 1); alphaParts = @(0, 2);  maParts = @(0, 1) }
)

foreach ($p in $Partitions) {
    $month = $p.month
    $start = [int]$p.start
    $rows = [int]$p.rows
    $date = $p.date

    # ---- index group: month-level partition, consecutive indexes from 0000 ----
    foreach ($partIdx in $p.indexParts) {
        $indexDir = Join-Path $tableDir "index\month=$month"
        New-Item -ItemType Directory -Force -Path $indexDir | Out-Null
        $s = $start + $partIdx * $per
        $e = $s + $per
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($s, $e))
  SELECT DATE '$date' AS date,
         printf('%06d', r + 1) AS symbol,
         CAST((r + 1) * 0.5 AS DOUBLE) AS close,
         CAST((r + 1) * 100 AS BIGINT) AS volume,
         CAST(r AS BIGINT) AS rowid
  FROM r
) TO '$($indexDir.Replace('\','/'))/$(Part-Name $partIdx $per).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
        Run-DuckDB $sql
    }

    # ---- alpha101 group: month-level partition. month=2026-08 skips index
    # 0001 (deletion) — a legal gap in a non-index group. The LAST part (0002)
    # carries the alpha099 schema-evolution column. NOTE: the DATA range of a
    # part follows its position within the partition (0-based, ignoring gaps) —
    # part 0002 is the partition's 2nd part and holds rows [4000,6000), even
    # though its file-name index is 2 (the start row is derived by accumulating
    # the LOWER-INDEX parts' row counts).
    for ($pos = 0; $pos -lt $p.alphaParts.Count; $pos++) {
        $partIdx = $p.alphaParts[$pos]
        $alphaDir = Join-Path $tableDir "factor\alpha101\month=$month"
        New-Item -ItemType Directory -Force -Path $alphaDir | Out-Null
        $alphaCols = 1..10 | ForEach-Object {
            "CASE WHEN r % 5 = 0 THEN CAST((r + $_ + 1) * 0.01 AS DOUBLE) ELSE NULL END AS alpha$('{0:D3}' -f $_)"
        }
        $alphaColList = @('CAST(r AS BIGINT) AS rowid_alpha') + $alphaCols +
            @('CAST((r + 1) * 0.25 AS DOUBLE) AS close', 'CAST((r + 1) * 0.125 AS DOUBLE) AS vwap')
        $extra = ''
        if ($month -eq '2026-08' -and $partIdx -eq 2) {
            # schema evolution on the group's LAST part: introduces alpha099
            $extra = ", CASE WHEN r % 3 = 0 THEN CAST((r + 1) * 0.001 AS DOUBLE) ELSE NULL END AS alpha099"
        }
        $s = $start + $pos * $per
        $e = $s + $per
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($s, $e))
  SELECT $(($alphaColList -join ', '))$extra
  FROM r
) TO '$($alphaDir.Replace('\','/'))/$(Part-Name $partIdx $per).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000, COMPRESSION ZSTD);"
        Run-DuckDB $sql
    }

    # ---- ma group: month-level partition, matching the index's indexes. The
    # 2026-07 partition is SKIPPED on purpose (missing partition -> NULL fill).
    for ($pos = 0; $pos -lt $p.maParts.Count; $pos++) {
        $partIdx = $p.maParts[$pos]
        $maDir = Join-Path $tableDir "fieldset\ma\month=$month"
        New-Item -ItemType Directory -Force -Path $maDir | Out-Null
        $s = $start + $pos * $per
        $e = $s + $per
        $sql = "COPY (
  WITH r AS (SELECT range AS r FROM range($s, $e))
  SELECT CAST(r AS BIGINT) AS rowid_ma,
         CAST((r % 20) * 0.1 AS DOUBLE) AS ma5,
         CAST((r % 30) * 0.05 AS DOUBLE) AS ma10,
         CAST((r % 60) * 0.025 AS DOUBLE) AS ma20,
         CAST((r + 1) * 0.0625 AS DOUBLE) AS close,
         CAST((r + 1) * 0.03125 AS DOUBLE) AS vwap
  FROM r
) TO '$($maDir.Replace('\','/'))/$(Part-Name $partIdx $per).parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION ZSTD);"
        Run-DuckDB $sql
    }
}

# ---- ignored directory (contract §2.1d): a _tmp directory with stray parts --
$tmpDir = Join-Path $tableDir '_tmp\transaction-999\index\month=2026-07'
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
$sql = "COPY (
  WITH r AS (SELECT range AS r FROM range(0, 100))
  SELECT DATE '2026-07-01' AS date, printf('%06d', r + 1) AS symbol, CAST(r AS DOUBLE) AS close
  FROM r
) TO '$($tmpDir.Replace('\','/'))/part-000000.parquet' (FORMAT PARQUET);"
Run-DuckDB $sql

Write-Host "Test data generated under $dataRoot"
Write-Host "  table: $table, rows: 6000, groups: index / factor/alpha101 / fieldset/ma (ma missing month=2026-07; alpha101 month=2026-08 skips index 0001)"