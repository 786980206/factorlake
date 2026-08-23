# lib_aligned.sh — shared helpers for the Linux (bash) acceptance tests.
# Sources the build/binary + data-root conventions used by all test/*.sh.
#
# Environment overrides:
#   DUCKDB            path to the aligned-enabled duckdb binary (default: $ROOT/duckdb/build/duckdb)
#   ALIGNED_DATA_ROOT path to the test data root (default: $ROOT/testdata)
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB="${DUCKDB:-$ROOT/duckdb/build/duckdb}"
DATA_ROOT="${ALIGNED_DATA_ROOT:-$ROOT/testdata}"

failures=0

if [ ! -x "$DUCKDB" ]; then
  echo "build missing: $DUCKDB (run the duckdb build first, see AGENTS.md 搂16)" >&2
  exit 1
fi

# expect_equal name actual expected
expect_equal() {
  local name="$1" actual="$2" expected="$3"
  if [ "$actual" = "$expected" ]; then
    echo "PASS: $name = $actual"
  else
    echo "FAIL: $name = $actual (expected $expected)"
    failures=$((failures + 1))
  fi
}

# run_duckdb <sql> -> stdout (csv, no header). Fails the script on error.
run_duckdb() {
  local out
  out="$("$DUCKDB" -light-mode -csv -noheader -c "$1" 2>&1)" || {
    echo "duckdb failed: $1" >&2
    echo "$out" >&2
    exit 1
  }
  printf '%s\n' "$out"
}

# run_duckdb_expect_error <sql> <pattern> -> 0 (true) if duckdb errors and output matches pattern
run_duckdb_expect_error() {
  local out
  out="$("$DUCKDB" -light-mode -csv -noheader -c "$1" 2>&1)"
  if [ $? -ne 0 ] && printf '%s' "$out" | grep -qi "$2"; then
    return 0
  fi
  return 1
}

summary() {
  echo ""
  if [ "$failures" -eq 0 ]; then
    echo "ALL TESTS PASSED"
  else
    echo "$failures TEST(S) FAILED"
    exit 1
  fi
}
