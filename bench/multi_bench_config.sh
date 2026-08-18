#!/usr/bin/env bash
# multi_bench_config.sh — standardized Group Settings for the AlignedTable
# multi-scenario benchmark (Phase 6+). This is the single source of truth for
# every test variable. Source it from run_multi_bench.sh (do NOT execute).
#
# Grouping (per the spec):
#   1. Engine Groups      D-WIDE D-JOIN P-CONCAT P-JOIN A-ALIGNED A-NORMAL
#   2. Scale & Width      R1..R4 x W1..W3
#   3. Sparsity           DENSE SPARSE-90 SPARSE-99
#   4. Query Projection   Q1..Q5
#   5. Filter & Selectiv  F1..F5 x S0..S3
#   6. Physical & Env     COLD/WARM, threads, file count, partition scheme
#   7. Tier composition   Tier A (smoke) / B (main) / C (stress) + special
set -u

# ---------------------------------------------------------------------------
# 1. Engine Groups
# ---------------------------------------------------------------------------
# A-ALIGNED / A-NORMAL are the plugin (aligned=true / aligned=false).
# P-CONCAT / P-JOIN need polars; they are added to ENGINES only if polars is
# importable (see run_multi_bench.sh).
DDB_ENGINES=(D-WIDE D-JOIN)        # DuckDB baselines
ALIGNED_ENGINES=(A-ALIGNED A-NORMAL) # plugin, aligned flag differs
POLARS_ENGINES=(P-CONCAT P-JOIN)   # require the polars package

# ---------------------------------------------------------------------------
# 2. Scale & Width
# ---------------------------------------------------------------------------
# Row groups: R1 smoke ... R4 stress. Run the harness at whatever scale the
# machine supports; these are the canonical sizes.
ROWS=(1000000 10000000 100000000 1000000000)                 # R1 R2 R3 R4
ROW_TAG=(R1 R2 R3 R4)
# Width groups: total logical columns.
declare -A WIDTH_NCOL
WIDTH_NCOL[W1]=128
WIDTH_NCOL[W2]=1024
WIDTH_NCOL[W3]=10240

# ---------------------------------------------------------------------------
# 3. Sparsity Groups (NULL fraction on factor columns)
# ---------------------------------------------------------------------------
declare -A SPARSITY_NULL
SPARSITY_NULL[DENSE]=0
SPARSITY_NULL[SPARSE-90]=90
SPARSITY_NULL[SPARSE-99]=99

# ---------------------------------------------------------------------------
# 4. Query Projection Groups (number of output columns)
#    Q1 ~3 | Q2 ~35 | Q3 ~500 | Q4 ~5000 | Q5 ALL
# ---------------------------------------------------------------------------
declare -A Q_NCOL
Q_NCOL[Q1]=3
Q_NCOL[Q2]=35
Q_NCOL[Q3]=500
Q_NCOL[Q4]=5000
Q_NCOL[Q5]=-1            # -1 => SELECT * (all columns)

# ---------------------------------------------------------------------------
# 5. Filter types & selectivity
# ---------------------------------------------------------------------------
FILTER_TYPES=(F1 F2 F3 F4 F5)
# selectivities as returned-row fractions (S0 100% .. S3 0.1%)
declare -A SEL_FRAC
SEL_FRAC[S0]=1.0
SEL_FRAC[S1]=0.10
SEL_FRAC[S2]=0.01
SEL_FRAC[S3]=0.001

# ---------------------------------------------------------------------------
# 6. Physical & Env
# ---------------------------------------------------------------------------
THREAD_SET=(1 2 4 8 16)    # thread scaling grid (extend to 64 on large nodes)
FILE_COUNT_SET=(100 1000 10000 100000)   # files at constant total rows
PARTITION_SCHEMES=(P1 P2 P3 P4)          # P1 date= ; P2 year/month/day ; P3 year/month ; P4 none

# ---------------------------------------------------------------------------
# 7. Tier composition
# ---------------------------------------------------------------------------
# Tier A — Smoke (CI, daily). engines=all, R1xW1, Q2 + Q5, F1 + F2, S0.
TIER_A_ROWS=R1
TIER_A_WIDTH=W1
TIER_A_QUERIES=(Q2 Q5)
TIER_A_FILTERS=(F1 F2)
TIER_A_SEL=(S0)

# Tier B — Main benchmark (paper/report core).
# engines=all, R3xW3, Q1+Q2+Q3+Q5, F1+F2+F3, S0+S1+S2.
TIER_B_ROWS=R3
TIER_B_WIDTH=W3
TIER_B_QUERIES=(Q1 Q2 Q3 Q5)
TIER_B_FILTERS=(F1 F2 F3)
TIER_B_SEL=(S0 S1 S2)

# Tier C — Stress. engines = A-ALIGNED A-NORMAL D-WIDE D-JOIN, R4xW3,
# Q1+Q2+Q5, F1+F2, S0+S1.
TIER_C_ROWS=R4
TIER_C_WIDTH=W3
TIER_C_QUERIES=(Q1 Q2 Q5)
TIER_C_FILTERS=(F1 F2)
TIER_C_SEL=(S0 S1)
TIER_C_ENGINES=(A-ALIGNED A-NORMAL D-WIDE D-JOIN)

# Special single-run groups (run separately, not part of Tier A/B/C):
#  - Sparsity:     R3 x W3 x Q2 x F2 x S1 over DENSE/SPARSE-90/SPARSE-99
#  - Thread scale: R3 x W3 x Q2 x F1 x S0 over THREAD_SET
#  - File count:   R3(100M) x W2 x Q2 x F1 over FILE_COUNT_SET

# Helper: resolve a row tag (R1..R4) to its row count and a width tag to its
# column count.
row_of() { local tag="$1" i; for i in "${!ROW_TAG[@]}"; do
    if [ "${ROW_TAG[$i]}" = "$tag" ]; then echo "${ROWS[$i]}"; return; fi; done; echo ""; }
width_of() { echo "${WIDTH_NCOL["$1"]:-}"; }

# ---------------------------------------------------------------------------
# Column layout shared by gen_multi_bench.sh and run_multi_bench.sh
#   index 20 (date,symbol,close,volume,rowid + ix001..ix015)
#   alpha = 3/4 of (width-20), fieldset = the rest. Total == width.
#   ix columns are indexed Phase differently (ix001..ix015) so `range_cols`
#   takes an explicit start/end for the family.
# ---------------------------------------------------------------------------
INDEX_COLS=20
layout_alpha() { echo $(( ( $1 - INDEX_COLS ) * 3 / 4 )); }
layout_fieldset() { echo $(( $1 - INDEX_COLS - ( ( $1 - INDEX_COLS ) * 3 / 4 ) )); }

# range_cols <prefix> <start> <count> -> comma list prefix<start=lo..hi>
# e.g. range_cols alpha 0 20 -> alpha000,...,alpha019
# (separate `local` so the arithmetic sees already-bound lo/cnt under set -u)
range_cols() {
  local base="$1" lo="$2" cnt="$3"
  local n hi=$(( lo + cnt - 1 ))
  for n in $(seq "$lo" "$hi"); do printf '%s%03d' "$base" "$n"; [ $n -lt "$hi" ] && printf ','; done; }
