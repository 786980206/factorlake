# build.ps1 — Build the full DuckDB binary with the aligned extension statically linked.
#
# Usage:
#   .\scripts\build.ps1              # incremental (ninja decides what to rebuild)
#   .\scripts\build.ps1 -Clean       # force full rebuild (delete all obj first)
#
# Prerequisites: DuckDB source vendored at duckdb/, build/ configured.
# Product: duckdb/build/duckdb_al3.exe

param([switch]$Clean)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$buildDir = Join-Path $repo 'duckdb\build'

# The submodule patch enables DUCKDB_SHELL_OUTPUT_NAME (duckdb_al3.exe target);
# ninja re-runs cmake when it sees the changed CMakeLists.txt.
$patch = Join-Path $PSScriptRoot 'patches\duckdb-shell-output-name.patch'
Push-Location (Join-Path $repo 'duckdb')
$prevEap = $ErrorActionPreference
$ErrorActionPreference = 'Continue'  # git stderr must not be terminating under PS 5.1
try {
    git apply --check $patch 2>$null
    if ($LASTEXITCODE -eq 0) {
        git apply $patch
        if ($LASTEXITCODE -ne 0) { throw "Failed to apply $patch" }
        Write-Host "Applied submodule patch: duckdb-shell-output-name.patch"
    } else {
        git apply --check --reverse $patch 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "$patch no longer applies to the duckdb submodule (upstream changed?). Regenerate: git -C duckdb diff > $patch"
        }
        Write-Host "Submodule patch already applied"
    }
} finally {
    $ErrorActionPreference = $prevEap
    Pop-Location
}

if (-not (Test-Path (Join-Path $buildDir 'build.ninja'))) {
    throw "Build directory not configured: $buildDir`nRun: cmake -S duckdb -B duckdb\build -G Ninja -DDUCKDB_EXTENSION_CONFIGS=$(Join-Path $repo 'scripts\aligned_extension_config.cmake') -DDUCKDB_SHELL_OUTPUT_NAME=duckdb_al3`n(DuckDB defaults to -DCMAKE_BUILD_TYPE=Debug.)`nThen create $buildDir\build_al3.bat: call vcvars64.bat, then ninja."
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
