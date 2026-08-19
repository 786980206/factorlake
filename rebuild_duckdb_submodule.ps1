# rebuild_duckdb_submodule.ps1 - rebuild the duckdb submodule with aligned extension
$ErrorActionPreference = 'Stop'
$vcvars = 'C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat'
& $vcvars
cd 'D:\proj\factorlake\duckdb'
# configure if not yet
if (-Not (Test-Path 'build')) {
    cmake -S . -B build -G Ninja
}
# build
ninja -C build