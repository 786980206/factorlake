# bench_copy_to.ps1 — Progressive benchmark: COPY TO (FORMAT aligned) vs native COPY TO PARQUET
#
# Tests multiple data scales to compare write performance:
#   aligned index (2 cols, year partition)
#   aligned panel/ma (7 cols, year partition)
#   native parquet year-partitioned (7 cols, year partition)
#   native parquet flat (7 cols, no partition)
#
# Usage: powershell -ExecutionPolicy Bypass -File test\bench_copy_to.ps1
# Prerequisite: scripts\build.ps1 (duckdb_al3.exe must exist)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$duckdb = Join-Path $repo 'duckdb\build\duckdb_al3.exe'
if (-not (Test-Path $duckdb)) { throw "duckdb binary not found: $duckdb" }

$dataRoot = 'D:/proj/factorlake/bench_copy_data'
Remove-Item -Recurse -Force $dataRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null

# Scales: number of symbols (400 symbols x 13159 days = 5.26M rows)
$scales = @(1, 4, 20, 80, 400)

$results = @()

foreach ($nsym in $scales) {
    $label = "${nsym}sym"
    Write-Host "`n=== Scale: $nsym symbols ===" -ForegroundColor Cyan

    # Clean previous run's output
    Remove-Item -Recurse -Force (Join-Path $dataRoot 'bench_copy') -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force (Join-Path $dataRoot 'bench_native_year') -ErrorAction SilentlyContinue
    Remove-Item -Force (Join-Path $dataRoot 'bench_native_flat.parquet') -ErrorAction SilentlyContinue

    $sql = @"
LOAD 'aligned';
SET aligned_data_root = '$dataRoot';
SET preserve_insertion_order = false;
SET threads = 8;

CREATE TEMP TABLE mock AS SELECT
    s.symbol,
    d.date,
    ROUND(100 + RANDOM() * 50, 2) AS o,
    ROUND(105 + RANDOM() * 50, 2) AS h,
    ROUND(95  + RANDOM() * 50, 2) AS l,
    ROUND(100 + RANDOM() * 50, 2) AS c,
    ROUND(100 + RANDOM() * 50, 2) AS v
FROM
    UNNEST(generate_series('1990-01-01'::DATE, '2026-01-10'::DATE, INTERVAL 1 DAY)) AS d(date)
CROSS JOIN
    UNNEST(ARRAY(SELECT LPAD(i::VARCHAR, 6, '0') FROM generate_series(1, $nsym) AS t(i))) AS s(symbol);

--ROWCOUNT
SELECT count(*) FROM mock;
SELECT * FROM aligned_create('bench_copy', 'index', 'symbol VARCHAR, date DATE', partition_template => 'year=%Y');
SELECT * FROM aligned_create('bench_copy', 'panel/ma');

.timer ON
COPY (SELECT * FROM mock) TO 'bench_copy' (FORMAT aligned, GROUP 'index');
.timer OFF
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock) TO 'bench_copy' (FORMAT aligned, GROUP 'panel/ma');
.timer OFF
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v, strftime(date, 'year=%Y') AS year_part FROM mock)
    TO 'bench_native_year' (FORMAT PARQUET, PARTITION_BY (year_part), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock)
    TO 'bench_native_flat.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF
"@

    $sqlFile = Join-Path $env:TEMP "bench_$label.sql"
    [System.IO.File]::WriteAllText($sqlFile, $sql)

    $output = Get-Content $sqlFile | & $duckdb -unsigned 2>&1

    # Parse .timer ON/OFF lines from output
    $times = @()
    foreach ($line in $output) {
        if ($line -match 'Run Time.*real\s+([\d.]+)') {
            $times += [double]$Matches[1]
        }
    }

    # Parse total rows: the line after --ROWCOUNT marker in the output
    # DuckDB echoes comment lines that start with -- in some modes, but
    # with stdin piping the count(*) result appears as a row in the table.
    # We look for the first standalone integer line in the output.
    $totalRows = 0
    $foundMarker = $false
    foreach ($line in $output) {
        $lineStr = "$line"
        if ($lineStr -match '--ROWCOUNT') { $foundMarker = $true; continue }
        if ($foundMarker -and $lineStr -match '^\s*(\d+)\s*$') {
            $totalRows = [int64]$Matches[1]
            break
        }
    }
    # Fallback: compute from nsym * 13159 days
    if ($totalRows -eq 0) {
        $totalRows = $nsym * 13159
    }

    $result = [PSCustomObject]@{
        Scale = $label
        Rows  = $totalRows
        AlignedIndex  = if ($times.Count -ge 1) { $times[0] } else { 0 }
        AlignedPanel   = if ($times.Count -ge 2) { $times[1] } else { 0 }
        NativeYearPart = if ($times.Count -ge 3) { $times[2] } else { 0 }
        NativeFlat     = if ($times.Count -ge 4) { $times[3] } else { 0 }
    }
    $results += $result

    Write-Host ("  Rows: {0:N0}" -f $result.Rows)
    Write-Host ("  Aligned index (2 col):     {0:F3}s" -f $result.AlignedIndex)
    Write-Host ("  Aligned panel (7 col):      {0:F3}s" -f $result.AlignedPanel)
    Write-Host ("  Native year-part (7 col):   {0:F3}s" -f $result.NativeYearPart)
    Write-Host ("  Native flat (7 col):         {0:F3}s" -f $result.NativeFlat)
}

# Summary table
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
$results | Format-Table -AutoSize

# Compute ratios
Write-Host "`n=== Ratios (aligned panel / native year-part) ===" -ForegroundColor Cyan
foreach ($r in $results) {
    if ($r.NativeYearPart -gt 0) {
        $ratio = $r.AlignedPanel / $r.NativeYearPart
        Write-Host ("  {0}: {1:F2}x (lower = aligned faster)" -f $r.Scale, $ratio)
    }
}

# Clean up
Remove-Item -Recurse -Force $dataRoot -ErrorAction SilentlyContinue
