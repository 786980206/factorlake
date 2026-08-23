# build.ps1 — Build the full DuckDB binary with the aligned extension statically linked.
#
# Usage:
#   .\scripts\build.ps1              # incremental (ninja decides what to rebuild)
#   .\scripts\build.ps1 -Clean       # force full rebuild (delete all obj first)
#
# Prerequisites: DuckDB source vendored at duckdb/, build3/ configured.
# Product: duckdb/build3/duckdb_al3.exe

param([switch]$Clean)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$buildDir = Join-Path $repo 'duckdb\build3'

if (-not (Test-Path (Join-Path $buildDir 'build.ninja'))) {
    throw "Build directory not configured: $buildDir`nRun cmake configuration first (see AGENTS.md §11)."
}

if ($Clean) {
    Write-Host "Cleaning build artifacts..."
    # Delete all obj directories to force full rebuild
    Get-ChildItem $buildDir -Recurse -Directory -Filter 'CMakeFiles' | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    # Also delete built libs/exes
    Remove-Item (Join-Path $buildDir 'duckdb_al3.exe') -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $buildDir 'src\duckdb.dll') -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $buildDir 'src\duckdb_static.lib') -Force -ErrorAction SilentlyContinue
}

Write-Host "Building duckdb_al3.exe (vcvars64 + ninja)..."
$bat = Join-Path $buildDir 'build_al3.bat'
if (-not (Test-Path $bat)) {
    throw "Build script not found: $bat"
}
& cmd /c "`"$bat`""
if ($LASTEXITCODE -ne 0) { throw "Build failed (exit $LASTEXITCODE)" }

$exe = Join-Path $buildDir 'duckdb_al3.exe'
if (Test-Path $exe) {
    $size = [math]::Round((Get-Item $exe).Length / 1MB, 1)
    Write-Host "Build OK: $exe ($size MB)"
} else {
    throw "Build reported success but $exe not found"
}
