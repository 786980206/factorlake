# build_extension.ps1 — Build the loadable aligned extension (.duckdb_extension).
#
# Usage:
#   .\scripts\build_extension.ps1           # incremental
#   .\scripts\build_extension.ps1 -Clean    # force full rebuild
#   .\scripts\build_extension.ps1 -Copy     # also copy to release\aligned.duckdb_extension
#
# Prerequisites: DuckDB source at duckdb/, build-rel/ configured with
#   -DEXTENSION_STATIC_BUILD=1 -DDUCKDB_EXTENSION_CONFIGS=.../aligned_extension_config.cmake
# Product: duckdb/build-rel/extension/aligned/aligned.duckdb_extension (~23 MB, self-contained)

param([switch]$Clean, [switch]$Copy)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$buildDir = Join-Path $repo 'duckdb\build-rel'

if (-not (Test-Path (Join-Path $buildDir 'build.ninja'))) {
    throw "Build directory not configured: $buildDir`nRun: cmake -S duckdb -B duckdb\build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release -DEXTENSION_STATIC_BUILD=1 -DDUCKDB_EXTENSION_CONFIGS=$(Join-Path $repo 'scripts\aligned_extension_config.cmake')"
}

if ($Clean) {
    Write-Host "Cleaning extension build artifacts..."
    $extDir = Join-Path $buildDir 'extension\aligned\CMakeFiles\aligned_loadable_extension.dir'
    if (Test-Path $extDir) { Remove-Item $extDir -Recurse -Force }
}

Write-Host "Building aligned.duckdb_extension (vcvars64 + ninja)..."
$bat = Join-Path $buildDir 'build_rel.bat'
if (-not (Test-Path $bat)) {
    throw "Build script not found: $bat`nCreate it with: vcvars64.bat + ninja aligned_loadable_extension"
}
& cmd /c "`"$bat`""
if ($LASTEXITCODE -ne 0) { throw "Extension build failed (exit $LASTEXITCODE)" }

$ext = Join-Path $buildDir 'extension\aligned\aligned.duckdb_extension'
if (-not (Test-Path $ext)) { throw "Build reported success but $ext not found" }

$size = [math]::Round((Get-Item $ext).Length / 1MB, 1)
Write-Host "Build OK: $ext ($size MB)"

if ($Copy) {
    $dest = Join-Path $repo 'release\aligned.duckdb_extension'
    Copy-Item $ext $dest -Force
    Write-Host "Copied to: $dest"
}
