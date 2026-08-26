# run_tests.ps1 — Run all test suites (SQLLogicTest + 4 PowerShell acceptance suites).
#
# Usage:
#   .\scripts\run_tests.ps1              # run all suites
#   .\scripts\run_tests.ps1 -Suite sql   # SQLLogicTest only
#   .\scripts\run_tests.ps1 -Suite aligned # test_aligned.ps1 only
#   .\scripts\run_tests.ps1 -Suite dml    # test_dml.ps1 only
#   .\scripts\run_tests.ps1 -Suite compaction # test_compaction.ps1 only
#   .\scripts\run_tests.ps1 -Suite parallel    # test_parallel.ps1 only
#
# Prerequisites:
#   - duckdb/build/duckdb_al3.exe built (run scripts\build.ps1)
#   - testdata/ generated (run test\gen_testdata.ps1)
#   - bench data generated for test_parallel (run test\gen_bench.ps1)

param([string]$Suite = 'all')

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$failures = 0

function Run-Suite([string]$name, [scriptblock]$block) {
    Write-Host "`n=== $name ===" -ForegroundColor Cyan
    try {
        & $block
        Write-Host "  -> $name PASSED" -ForegroundColor Green
    } catch {
        Write-Host "  -> $name FAILED: $_" -ForegroundColor Red
        $script:failures++
    }
}

if ($Suite -eq 'all' -or $Suite -eq 'sql') {
    Run-Suite 'SQLLogicTest' {
        $out = & python (Join-Path $repo 'test\run_sqllogictest.py') 2>&1 | Out-String
        Write-Host $out
        if ($out -notmatch 'ALL TESTS PASSED' -and $out -notmatch '0 failed') {
            throw "SQLLogicTest reported failures"
        }
    }
}

if ($Suite -eq 'all' -or $Suite -eq 'aligned') {
    Run-Suite 'test_aligned' {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repo 'test\test_aligned.ps1')
        if ($LASTEXITCODE -ne 0) { throw "exit $LASTEXITCODE" }
    }
}

if ($Suite -eq 'all' -or $Suite -eq 'dml') {
    Run-Suite 'test_dml' {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repo 'test\test_dml.ps1')
        if ($LASTEXITCODE -ne 0) { throw "exit $LASTEXITCODE" }
    }
}

if ($Suite -eq 'all' -or $Suite -eq 'compaction') {
    Run-Suite 'test_compaction' {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repo 'test\test_compaction.ps1')
        if ($LASTEXITCODE -ne 0) { throw "exit $LASTEXITCODE" }
    }
}

if ($Suite -eq 'all' -or $Suite -eq 'parallel') {
    Run-Suite 'test_parallel' {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repo 'test\test_parallel.ps1')
        if ($LASTEXITCODE -ne 0) { throw "exit $LASTEXITCODE" }
    }
}

Write-Host ""
if ($failures -eq 0) {
    Write-Host "ALL TEST SUITES PASSED" -ForegroundColor Green
    exit 0
} else {
    Write-Host "$failures SUITE(S) FAILED" -ForegroundColor Red
    exit 1
}
