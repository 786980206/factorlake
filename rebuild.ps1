# rebuild.ps1 - Rebuild aligned DuckDB binary
$ErrorActionPreference = 'Stop'
$vcvars = 'C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat'
& $vcvars
cd 'D:\proj\factorlake'
ninja -C build duckdb_aligned