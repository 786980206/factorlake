# bench_write_perf.ps1 — Aligned extension write-path performance benchmark
#
# Measures INSERT performance for the three primary write scenarios:
#   1. Full table rewrite (全量重写)
#   2. Single-group full rewrite (单组全量重写)
#   3. Last-partition rewrite (末分区重写)
#
# Plus baseline scenarios for comparison:
#   4. Small batch INSERT (1K new rows)
#   5. Large batch INSERT into new partition
#
# Usage: powershell -ExecutionPolicy Bypass -File test\bench_write_perf.ps1
# Prerequisite: scripts\build.ps1 (duckdb_al3.exe must exist)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$duckdb = Join-Path $repo 'duckdb\build3\duckdb_al3.exe'
$dataRoot = 'D:/proj/factorlake/testdata_perf'

if (-not (Test-Path $duckdb)) {
    Write-Host "ERROR: duckdb_al3.exe not found at $duckdb"
    Write-Host "Run scripts\build.ps1 first."
    exit 1
}

# Clean up previous benchmark data
Remove-Item -Recurse -Force $dataRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null

function Run-SQL-File($sql, $label) {
    $tmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $tmp -Encoding UTF8 -Value $sql
    $out = cmd /c "`"$duckdb`" -csv -noheader < `"$tmp`"" 2>&1 | Out-String
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $label"
        Write-Host $out
        exit 1
    }
    return $out
}

function Measure-SQL($sql, $label) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Run-SQL-File $sql $label | Out-Null
    $sw.Stop()
    return $sw.ElapsedMilliseconds
}

# ============================================================
# Setup: Create table with 2 groups, 2 partitions, 200K rows
# Partition layout: month=2026-01 (100K rows), month=2026-02 (100K rows)
# 100 symbols × 1000 dates per partition
# ============================================================
Write-Host "=== Setup: Creating table with 200K rows (2 partitions, 2 groups) ==="
$setupMs = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
CREATE TABLE al.perf_test (
    symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT,
    alpha001 DOUBLE, alpha002 DOUBLE
) WITH (groups='index:close,volume;factor/alpha:alpha001,alpha002', partition_template='month=%Y-%m');

INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       DATE '2026-01-01' + CAST(r / 100 AS INTEGER),
       CAST(r AS DOUBLE), r,
       CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(100000) t(r);

INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       DATE '2026-02-01' + CAST(r / 100 AS INTEGER),
       CAST(r AS DOUBLE), r,
       CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(100000) t(r);
"@ "setup"
Write-Host "Setup: ${setupMs}ms"
Write-Host ""

$results = @()

# ============================================================
# Scenario 1: Full table rewrite (全量重写)
# Overwrite all 200K rows — all updates, all partitions, all groups
# ============================================================
Write-Host "=== Scenario 1: Full table rewrite (200K rows, all updates) ==="
$ms = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       CASE WHEN r < 100000
            THEN DATE '2026-01-01' + CAST(r / 100 AS INTEGER)
            ELSE DATE '2026-02-01' + CAST((r - 100000) / 100 AS INTEGER)
       END,
       CAST(r * 2 AS DOUBLE), r * 2,
       CAST(r AS DOUBLE) / 50, CAST(r AS DOUBLE) / 100
FROM range(200000) t(r);
"@ "full-rewrite-200k"
Write-Host "  200K full rewrite: ${ms}ms"
$results += [PSCustomObject]@{Scenario="Full table rewrite (200K)"; Rows="200K"; Type="update"; Ms=$ms}

# ============================================================
# Scenario 2: Single-group full rewrite (单组全量重写)
# Overwrite only factor/alpha group columns for all 200K rows
# ============================================================
Write-Host "`n=== Scenario 2: Single-group rewrite (200K rows, factor/alpha only) ==="
$ms = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       CASE WHEN r < 100000
            THEN DATE '2026-01-01' + CAST(r / 100 AS INTEGER)
            ELSE DATE '2026-02-01' + CAST((r - 100000) / 100 AS INTEGER)
       END,
       CAST(r AS DOUBLE) / 30, CAST(r AS DOUBLE) / 60
FROM range(200000) t(r);
"@ "single-group-rewrite-200k"
Write-Host "  200K single-group: ${ms}ms"
$results += [PSCustomObject]@{Scenario="Single-group rewrite (200K)"; Rows="200K"; Type="update"; Ms=$ms}

# ============================================================
# Scenario 3: Last-partition rewrite (末分区重写)
# Overwrite the last partition (month=2026-02, 100K rows)
# ============================================================
Write-Host "`n=== Scenario 3: Last-partition rewrite (100K rows) ==="
$ms = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       DATE '2026-02-01' + CAST(r / 100 AS INTEGER),
       CAST(r * 3 AS DOUBLE), r * 3,
       CAST(r AS DOUBLE) / 70, CAST(r AS DOUBLE) / 140
FROM range(100000) t(r);
"@ "last-partition-rewrite-100k"
Write-Host "  100K last-partition: ${ms}ms"
$results += [PSCustomObject]@{Scenario="Last-partition rewrite (100K)"; Rows="100K"; Type="update"; Ms=$ms}

# ============================================================
# Scenario 4: Small batch INSERT (1K new rows) — new partition
# ============================================================
Write-Host "`n=== Scenario 4: Small batch INSERT (1K new rows, new partition) ==="
$ms = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       DATE '2026-06-01' + CAST(r / 100 AS INTEGER),
       CAST(r AS DOUBLE), r,
       CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(1000) t(r);
"@ "small-insert-1k"
Write-Host "  1K new partition: ${ms}ms"
$results += [PSCustomObject]@{Scenario="Small batch INSERT (1K)"; Rows="1K"; Type="insert"; Ms=$ms}

# ============================================================
# Scenario 5: Large batch INSERT (100K new rows) — new partition
# ============================================================
Write-Host "`n=== Scenario 5: Large batch INSERT (100K new rows, new partition) ==="
$ms = Measure-SQL @"
SET aligned_data_root='$dataRoot';
ATTACH '$dataRoot' AS al (TYPE ALIGNED);
INSERT INTO al.perf_test (symbol, date, close, volume, alpha001, alpha002)
SELECT printf('%06d', r % 100),
       DATE '2026-07-01' + CAST(r / 100 AS INTEGER),
       CAST(r AS DOUBLE), r,
       CAST(r AS DOUBLE) / 100, CAST(r AS DOUBLE) / 200
FROM range(100000) t(r);
"@ "large-insert-100k"
Write-Host "  100K new partition: ${ms}ms"
$results += [PSCustomObject]@{Scenario="Large batch INSERT (100K)"; Rows="100K"; Type="insert"; Ms=$ms}

# ============================================================
# Summary
# ============================================================
Write-Host "`n============================================================"
Write-Host "Summary"
Write-Host "============================================================"
$results | Format-Table -AutoSize

# Cleanup
Remove-Item -Recurse -Force $dataRoot -ErrorAction SilentlyContinue
Write-Host "`nDone."
