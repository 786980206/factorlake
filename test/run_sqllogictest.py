#!/usr/bin/env python3
"""
SQLLogicTest runner for the aligned extension.

Parses DuckDB-style .test files and executes them against the aligned-enabled
duckdb binary (duckdb_al3.exe on Windows, ./duckdb on Linux).

Usage:
    python test/run_sqllogictest.py [--duckdb PATH] [--data-root PATH] [test_files...]

If no test_files are given, runs all *.test files under test/aligned/.

Supported directives:
    statement ok           SQL must succeed
    statement error        SQL must fail; next line is expected error substring
    query <types>          SQL must return rows; types = T/I/R per column
                           optional 2nd param: rowsort | nosort | valuesort
    ----                   separates SQL from expected result (in query)
    halt                   stop processing this file
    #...                   comment
    {DATA_ROOT}            replaced with the data root path
    {TEST_DIR}             replaced with a per-test temp directory
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

class TestConfig:
    def __init__(self, duckdb: str, data_root: str):
        self.duckdb = duckdb
        self.data_root = data_root
        self.passed = 0
        self.failed = 0
        self.errors = []

    def find_duckdb(self):
        """Locate the aligned-enabled duckdb binary."""
        if self.duckdb:
            p = Path(self.duckdb)
            if p.exists():
                return str(p)
            raise FileNotFoundError(f"duckdb binary not found: {self.duckdb}")
        # Auto-detect
        candidates = [
            REPO_ROOT / "duckdb" / "build3" / "duckdb_al3.exe",
            REPO_ROOT / "duckdb" / "build" / "duckdb",
            REPO_ROOT / "duckdb" / "build" / "duckdb.exe",
        ]
        for c in candidates:
            if c.exists():
                return str(c)
        raise FileNotFoundError(
            f"No duckdb binary found. Use --duckdb PATH. Tried: {[str(c) for c in candidates]}"
        )

# ---------------------------------------------------------------------------
# Test file parser
# ---------------------------------------------------------------------------

class TestParser:
    """Parses a .test file into a list of commands."""

    def __init__(self, lines: list[str], filename: str):
        self.lines = lines
        self.filename = filename
        self.idx = 0

    def peek(self) -> str | None:
        if self.idx < len(self.lines):
            return self.lines[self.idx].rstrip("\n\r")
        return None

    def next_line(self) -> str | None:
        if self.idx < len(self.lines):
            line = self.lines[self.idx].rstrip("\n\r")
            self.idx += 1
            return line
        return None

    def extract_statement(self) -> str:
        """Extract a SQL statement (may span multiple lines, ends at ----, blank line, or directive)."""
        parts = []
        while True:
            line = self.peek()
            if line is None:
                break
            stripped = line.strip()
            if stripped == "":
                self.next_line()  # consume blank line
                break
            if stripped == "----":
                # Don't consume — extract_expected_results will handle it
                break
            if stripped.startswith("#"):
                break
            # Check if this line is a new directive
            if self._is_directive(stripped):
                break
            parts.append(stripped)
            self.next_line()
        return " ".join(parts)

    def extract_expected_results(self) -> list[str]:
        """Extract expected result lines after ---- separator."""
        results = []
        # Skip to the ---- line
        while True:
            line = self.peek()
            if line is None:
                break
            if line.strip() == "----":
                self.next_line()  # consume ----
                break
            self.next_line()  # skip SQL continuation
        # Read result lines until blank line or directive or EOF
        while True:
            line = self.peek()
            if line is None:
                break
            stripped = line.strip()
            if stripped == "":
                self.next_line()
                break
            if self._is_directive(stripped):
                break
            if stripped.startswith("#"):
                break
            results.append(stripped)
            self.next_line()
        return results

    def _is_directive(self, line: str) -> bool:
        """Check if a line starts with a known SQLLogicTest directive."""
        directives = ("statement ", "query ", "halt", "mode ", "skipif",
                      "onlyif", "require ", "loop", "endloop", "hash-threshold",
                      "load", "restore", "sleep", "set ", "test-env",
                      "require-env", "tags", "mkdir ", "writefile ", "writejson ")
        return any(line.startswith(d) or line == d.rstrip() for d in directives)


def parse_test_file(filepath: str) -> list[dict]:
    """Parse a .test file into a list of command dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    parser = TestParser(lines, filepath)
    commands = []

    while parser.peek() is not None:
        line = parser.next_line()
        if line is None:
            break
        stripped = line.strip()
        if stripped == "" or stripped.startswith("#"):
            continue
        if stripped == "halt":
            commands.append({"type": "halt"})
            break
        if stripped.startswith("statement"):
            parts = stripped.split(None, 2)
            stmt_type = parts[1] if len(parts) > 1 else "ok"
            sql = parser.extract_statement()
            if stmt_type == "error":
                # For statement error, the expected error text is after ----
                expected_errors = parser.extract_expected_results()
                expected_error = "; ".join(expected_errors) if expected_errors else ""
                commands.append({
                    "type": "statement_error",
                    "sql": sql,
                    "expected_error": expected_error.strip(),
                })
            else:
                commands.append({
                    "type": "statement_ok",
                    "sql": sql,
                })
        elif stripped.startswith("query"):
            parts = stripped.split()
            type_str = parts[1] if len(parts) > 1 else "I"
            sort_style = parts[2] if len(parts) > 2 else "nosort"
            sql = parser.extract_statement()
            expected = parser.extract_expected_results()
            commands.append({
                "type": "query",
                "column_types": type_str,
                "sort_style": sort_style,
                "sql": sql,
                "expected": expected,
            })
        elif stripped.startswith("require"):
            # Skip require directives (we handle our own dependencies)
            pass
        elif stripped.startswith("set "):
            # set <key> <value> - internal test config, skip for now
            pass
        elif stripped.startswith("mode "):
            # mode <mode> - skip
            pass
        elif stripped.startswith("skipif") or stripped.startswith("onlyif"):
            # Skip platform-specific skip/only directives
            pass
        elif stripped.startswith("hash-threshold"):
            pass
        elif stripped.startswith("loop") or stripped.startswith("endloop"):
            pass
        elif stripped.startswith("test-env") or stripped.startswith("require-env"):
            pass
        elif stripped.startswith("tags"):
            pass
        elif stripped.startswith("sleep"):
            pass
        elif stripped.startswith("load") or stripped.startswith("restore"):
            pass
        elif stripped.startswith("mkdir"):
            # Custom directive: mkdir <path> — create directory (runner-side)
            path = stripped[len("mkdir"):].strip()
            commands.append({"type": "mkdir", "path": path})
        elif stripped.startswith("writejson"):
            # Custom directive: writejson <path> <json>
            rest = stripped[len("writejson"):].strip()
            # Split on first space: path is first token, rest is JSON
            sp = rest.find(" ")
            if sp > 0:
                path = rest[:sp]
                json_str = rest[sp+1:]
                commands.append({"type": "writefile", "path": path, "content": json_str})
        elif stripped.startswith("writefile"):
            # Custom directive: writefile <path>\n<content until blank line>
            path = stripped[len("writefile"):].strip()
            content_lines = []
            while True:
                line = parser.peek()
                if line is None or line.strip() == "":
                    break
                content_lines.append(parser.next_line())
            commands.append({"type": "writefile", "path": path, "content": "\n".join(content_lines)})
        else:
            # Unknown directive — skip silently
            pass

    return commands

# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

def run_duckdb(duckdb: str, sql: str, data_root: str = None) -> tuple[int, str]:
    """Run SQL against duckdb. Returns (exit_code, output)."""
    # Write SQL to a temp file to avoid shell quoting issues
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False,
                                      encoding="utf-8", newline="\n") as f:
        f.write(sql)
        f.write("\n")
        tmp_path = f.name
    try:
        cmd = [duckdb, "-csv", "-noheader"]
        with open(tmp_path, "r") as f:
            result = subprocess.run(
                cmd, stdin=f, capture_output=True, text=True, timeout=120,
                encoding="utf-8", errors="replace"
            )
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        output = stdout + stderr
        return result.returncode, output
    finally:
        os.unlink(tmp_path)


def run_duckdb_batch(duckdb: str, statements: list[str]) -> tuple[int, str]:
    """Run multiple SQL statements in one duckdb process (preserves session state).
    Returns (exit_code, combined_output). The last statement's output is what
    matters for query verification."""
    sql = ";\n".join(statements)
    return run_duckdb(duckdb, sql)


def normalize_value(val: str) -> str:
    """Normalize a CSV cell value for comparison."""
    val = val.strip()
    if val == "":
        return "NULL"
    return val


def compare_results(actual_lines: list[str], expected_lines: list[str],
                    column_types: str, sort_style: str) -> tuple[bool, str]:
    """Compare actual CSV output with expected results."""
    # Parse actual output into rows
    actual_rows = []
    for line in actual_lines:
        line = line.strip()
        if not line:
            continue
        actual_rows.append([normalize_value(v) for v in line.split(",")])

    # Parse expected into rows
    expected_rows = []
    for line in expected_lines:
        line = line.strip()
        if not line:
            continue
        expected_rows.append([normalize_value(v) for v in line.split(",")])

    # Apply sorting
    if sort_style == "rowsort":
        actual_rows.sort()
        expected_rows.sort()

    if len(actual_rows) != len(expected_rows):
        return False, f"row count mismatch: got {len(actual_rows)}, expected {len(expected_rows)}"

    for i, (arow, erow) in enumerate(zip(actual_rows, expected_rows)):
        if len(arow) != len(erow):
            return False, f"row {i}: column count mismatch: got {len(arow)}, expected {len(erow)}"
        for j, (a, e) in enumerate(zip(arow, erow)):
            # For R (real) columns, allow floating point comparison
            if j < len(column_types) and column_types[j] == "R":
                try:
                    if abs(float(a) - float(e)) < 1e-9:
                        continue
                except ValueError:
                    pass
            if a != e:
                return False, f"row {i} col {j}: got '{a}', expected '{e}'"

    return True, ""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_test_file(filepath: str, config: TestConfig, duckdb_path: str) -> bool:
    """Run a single .test file. Returns True if all commands passed.

    Strategy: batch-execute statements in a single duckdb process to preserve
    session state (SET aligned_data_root, etc.). SET statements and statement ok
    commands are accumulated as "preamble" — they run before each query or
    statement_error to preserve state. After each batch, the preamble is kept
    for the next batch.
    """
    filename = os.path.basename(filepath)
    commands = parse_test_file(filepath)

    # Create a per-test temp directory
    test_dir = tempfile.mkdtemp(prefix=f"aligned_test_{Path(filepath).stem}_")
    test_name = Path(filepath).stem

    def substitute(sql: str) -> str:
        sql = sql.replace("{DATA_ROOT}", config.data_root)
        sql = sql.replace("{TEST_DIR}", test_dir.replace("\\", "/"))
        sql = sql.replace("{TEMP_DIR}", test_dir.replace("\\", "/"))
        return sql

    # Preamble = SET statements that persist across batches
    preamble: list[str] = []
    all_passed = True

    for cmd in commands:
        if cmd["type"] == "halt":
            break
        elif cmd["type"] == "mkdir":
            path = substitute(cmd["path"])
            os.makedirs(path, exist_ok=True)
        elif cmd["type"] == "writefile":
            path = substitute(cmd["path"])
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(cmd["content"])
        elif cmd["type"] == "statement_ok":
            sql = substitute(cmd["sql"])
            # Check if this is a pure SET/ATTACH/DETACH statement — add to preamble
            stripped = sql.strip()
            stripped_upper = stripped.upper()
            is_preamble = (stripped_upper.startswith("SET ") or
                          stripped_upper.startswith("ATTACH ") or
                          stripped_upper.startswith("DETACH "))
            if is_preamble and ";" not in stripped.rstrip(";"):
                preamble.append(sql)
            # Run preamble + this statement in one batch
            batch = preamble + [sql]
            rc, output = run_duckdb_batch(duckdb_path, batch)
            if rc != 0:
                error_msg = output.strip()[:200]
                print(f"  FAIL: {test_name}: statement ok failed: {error_msg}")
                print(f"    SQL: {sql[:100]}")
                config.failed += 1
                all_passed = False
            else:
                config.passed += 1
        elif cmd["type"] == "statement_error":
            sql = substitute(cmd["sql"])
            # Run preamble + this statement (preamble preserves SET state)
            batch = preamble + [sql]
            rc, output = run_duckdb_batch(duckdb_path, batch)
            expected_err = cmd["expected_error"]
            if rc == 0:
                print(f"  FAIL: {test_name}: expected error but got success")
                print(f"    SQL: {sql[:100]}")
                config.failed += 1
                all_passed = False
            elif expected_err and expected_err not in output:
                # Try case-insensitive match (DuckDB wraps error lines)
                if expected_err.lower() not in output.lower().replace("\n", " "):
                    print(f"  FAIL: {test_name}: error mismatch")
                    print(f"    Expected: '{expected_err}'")
                    print(f"    Got: {output.strip()[:200]}")
                    config.failed += 1
                    all_passed = False
                else:
                    config.passed += 1
            else:
                config.passed += 1
        elif cmd["type"] == "query":
            sql = substitute(cmd["sql"])
            # Check if query is a pure SET statement (no semicolons = single statement)
            stripped = sql.strip()
            if stripped.upper().startswith("SET ") and ";" not in stripped.rstrip(";"):
                preamble.append(sql)
                config.passed += 1
                continue
            # Run preamble + query in one batch
            batch = preamble + [sql]
            rc, output = run_duckdb_batch(duckdb_path, batch)
            if rc != 0:
                print(f"  FAIL: {test_name}: query failed: {output.strip()[:200]}")
                print(f"    SQL: {sql[:100]}")
                config.failed += 1
                all_passed = False
                continue

            actual_lines = [l for l in output.split("\n") if l.strip()]
            ok, msg = compare_results(
                actual_lines, cmd["expected"],
                cmd["column_types"], cmd["sort_style"]
            )
            if ok:
                config.passed += 1
            else:
                print(f"  FAIL: {test_name}: result mismatch: {msg}")
                print(f"    SQL: {sql[:120]}")
                print(f"    Expected: {cmd['expected'][:3]}")
                print(f"    Actual:   {actual_lines[:3]}")
                config.failed += 1
                all_passed = False

    # Cleanup
    shutil.rmtree(test_dir, ignore_errors=True)
    return all_passed


def main():
    parser = argparse.ArgumentParser(description="Run SQLLogicTest .test files")
    parser.add_argument("--duckdb", default="", help="Path to duckdb binary")
    parser.add_argument("--data-root", default="", help="Path to test data root")
    parser.add_argument("test_files", nargs="*", help=".test files to run (default: test/aligned/*.test)")
    args = parser.parse_args()

    # Determine data root
    data_root = args.data_root
    if not data_root:
        data_root = str(REPO_ROOT / "testdata").replace("\\", "/")
    data_root = data_root.replace("\\", "/")

    config = TestConfig(args.duckdb, data_root)
    duckdb_path = config.find_duckdb()
    print(f"DuckDB: {duckdb_path}")
    print(f"Data root: {config.data_root}")

    # Find test files
    if args.test_files:
        test_files = args.test_files
    else:
        test_dir = REPO_ROOT / "test" / "aligned"
        test_files = sorted(str(test_dir / f) for f in test_dir.glob("*.test"))

    if not test_files:
        print("No .test files found!")
        sys.exit(1)

    print(f"Running {len(test_files)} test file(s)...\n")

    all_passed = True
    for tf in test_files:
        print(f"=== {os.path.basename(tf)} ===")
        passed = run_test_file(tf, config, duckdb_path)
        if passed:
            print(f"  OK")
        else:
            all_passed = False
        print()

    print(f"{'='*60}")
    print(f"Results: {config.passed} passed, {config.failed} failed")
    if all_passed and config.failed == 0:
        print("ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
