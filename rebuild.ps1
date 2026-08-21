# rebuild.ps1 - Rebuild aligned DuckDB binary (build3 / duckdb_al3.exe)
$ErrorActionPreference = 'Stop'
$vcvars = 'C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat'
& $vcvars
cd 'D:\proj\factorlake\duckdb'
ninja -C build3 duckdb_al3.exe