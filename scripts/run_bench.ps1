# run_bench.ps1 - Benchmark aligned_table on testdata
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
# Path to aligned DuckDB binary (already built with aligned extension)
$duckdb = 'D:/proj/factorlake/duckdb/build/duckdb_aligned.exe'
if (-not (Test-Path $duckdb)) { throw "aligned binary not found at $duckdb" }
$dataRoot = 'D:/proj/factorlake/testdata'
# Workload queries (simple count aggregations)
$workloads = @{
    p5 = "SELECT count(alpha001), count(alpha002), count(alpha003), count(alpha004), count(alpha005) FROM (SELECT alpha001, alpha002, alpha003, alpha004, alpha005 FROM aligned_table('cnstk_ixday'))"
    p25 = "SELECT " + (0..24 | ForEach-Object { "count(alpha$(('{0:D3}' -f $_)))" }) -join ", " + " FROM (SELECT " + (0..24 | ForEach-Object { "alpha$(('{0:D3}' -f $_))" }) -join ", " + " FROM aligned_table('cnstk_ixday'))"
    p100 = "SELECT " + ((0..99)+(0..19) | ForEach-Object { "count(alpha$(('{0:D3}' -f $_)))" }) -join ", " + " FROM (SELECT " + (0..99 | ForEach-Object { "alpha$(('{0:D3}' -f $_))" }) + (0..19 | ForEach-Object { "ma$(('{0:D3}' -f $_))" }) -join ", " + " FROM aligned_table('cnstk_ixday'))"
    s25 = "SELECT " + (0..24 | ForEach-Object { "count(alpha$(('{0:D3}' -f $_)))" }) -join ", " + " FROM (SELECT " + (0..24 | ForEach-Object { "alpha$(('{0:D3}' -f $_))" }) -join ", " + " FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-17')"
    s100 = "SELECT " + (0..24 | ForEach-Object { "count(alpha$(('{0:D3}' -f $_)))" }) -join ", " + " FROM (SELECT " + (0..24 | ForEach-Object { "alpha$(('{0:D3}' -f $_))" }) -join ", " + " FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-18')"
}
$threadSet = @(1,4,8)
$csvLines = @('engine,workload,threads,cold_s,warm_s')
foreach ($threads in $threadSet) {
    foreach ($w in $workloads.Keys) {
        $sql = "SET aligned_data_root='$dataRoot'; SET threads=$threads; " + $workloads[$w] + ";"
        $tmp = [IO.Path]::GetTempFileName()
        Set-Content -Path $tmp -Value $sql -Encoding ASCII
        # Warm up (first run)
        cmd /c "`"$duckdb`" -csv -noheader -quiet < `"$tmp`" > $null 2>&1"
        # Cold measurement (second run)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        cmd /c "`"$duckdb`" -csv -noheader -quiet < `"$tmp`" > $null 2>&1"
        $sw.Stop()
        $cold = $sw.Elapsed.TotalSeconds
        # Warm measurement (third run)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        cmd /c "`"$duckdb`" -csv -noheader -quiet < `"$tmp`" > $null 2>&1"
        $sw.Stop()
        $warm = $sw.Elapsed.TotalSeconds
        $csvLines += "aligned,$w,$threads,$($cold.ToString('F4')),$($warm.ToString('F4'))"
        Write-Host "aligned $w threads=$threads cold=$($cold.ToString('F3'))s warm=$($warm.ToString('F3'))s"
        Remove-Item $tmp -Force
    }
}
$csvPath = Join-Path $root 'aligned_benchmark_results.csv'
$csvLines | Set-Content -Path $csvPath -Encoding UTF8
Write-Host "Benchmark CSV written to $csvPath"
