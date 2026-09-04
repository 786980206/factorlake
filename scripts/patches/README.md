# duckdb submodule patches

## duckdb-shell-output-name.patch

Allow overriding the shell binary output name via
`-DDUCKDB_SHELL_OUTPUT_NAME=duckdb_al3` (workaround for a zombie duckdb.exe
holding a lock on `duckdb.exe` — building as `duckdb_al3.exe` avoids it).

The submodule tracks upstream `duckdb/duckdb` (no push access), so this patch
cannot be committed there. It lives here instead and is applied
idempotently by `scripts/build.ps1` before configure/build.

Regenerate after a submodule bump:

```
git -C duckdb diff -- tools/shell/CMakeLists.txt > scripts/patches/duckdb-shell-output-name.patch
```
