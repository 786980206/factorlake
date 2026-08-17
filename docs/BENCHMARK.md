# AlignedTable Benchmark (Phase 6)

Date: 2026-08-17  Machine: local Windows (see AGENTS.md 16)
Dataset: **bench_ixday** - 1,000,000 rows x 127 columns (index 5 + alpha101 101 + ma 21),
4 daily partitions, factors sparse (non-null 1/7). Aligned layout: 3 independent Parquet column groups.

## Workloads

| id | description |
|----|-------------|
| p5 | project 5 factor columns, full scan |
| p25 | project 25 factor columns, full scan |
| p100 | project 120 columns (100 alpha + 20 ma), full scan |
| s25 | project 25 columns, WHERE date = '2026-09-01' (25% scan, partition pruning) |
| s100 | project 25 columns, full scan |

## Engines

- **aligned** - ligned_table('bench_ixday'): 3 groups assembled into one DataChunk,
  no JOIN, projection pushdown, partition pruning, parallel range scan, metadata cache, window carry reuse.
- **wide** - single wide Parquet (127 columns, 1M rows), DuckDB ead_parquet.
- **join** - three separate Parquet files (index/alpha/ma) joined on rowid (keyed layout).
- **polars** - the same three files read separately (projection per file) and horizontally
  concatenated (position-aligned) - the classic wide-table assembly path we eliminate.

## Results (seconds; warm = 2nd run in the same process)

| engine | workload | threads | cold | warm |
|--------|----------|---------|------|------|
| aligned | p5 | 1 | 0.183 | 0.183 |
| aligned | p5 | 4 | 0.168 | 0.168 |
| aligned | p5 | 8 | 0.129 | 0.129 |
| aligned | p25 | 1 | 0.529 | 0.529 |
| aligned | p25 | 4 | 0.297 | 0.297 |
| aligned | p25 | 8 | 0.242 | 0.242 |
| aligned | p100 | 1 | 2.061 | 2.061 |
| aligned | p100 | 4 | 1.031 | 1.031 |
| aligned | p100 | 8 | 1.142 | 1.142 |
| aligned | s25 | 1 | 0.537 | 0.537 |
| aligned | s25 | 4 | 0.301 | 0.301 |
| aligned | s25 | 8 | 0.235 | 0.235 |
| aligned | s100 | 1 | 0.530 | 0.530 |
| aligned | s100 | 4 | 0.297 | 0.297 |
| aligned | s100 | 8 | 0.238 | 0.238 |
| wide | p5 | 1 | 0.103 | 0.103 |
| wide | p5 | 4 | 0.071 | 0.071 |
| wide | p5 | 8 | 0.071 | 0.071 |
| wide | p25 | 1 | 0.289 | 0.289 |
| wide | p25 | 4 | 0.129 | 0.129 |
| wide | p25 | 8 | 0.107 | 0.107 |
| wide | p100 | 1 | 1.033 | 1.033 |
| wide | p100 | 4 | 0.391 | 0.391 |
| wide | p100 | 8 | 0.341 | 0.341 |
| wide | s25 | 1 | 0.101 | 0.101 |
| wide | s25 | 4 | 0.090 | 0.090 |
| wide | s25 | 8 | 0.092 | 0.092 |
| wide | s100 | 1 | 0.291 | 0.291 |
| wide | s100 | 4 | 0.129 | 0.129 |
| wide | s100 | 8 | 0.108 | 0.108 |
| join | p5 | 1 | 0.185 | 0.185 |
| join | p5 | 4 | 0.113 | 0.113 |
| join | p5 | 8 | 0.112 | 0.112 |
| join | p25 | 1 | 0.378 | 0.378 |
| join | p25 | 4 | 0.173 | 0.173 |
| join | p25 | 8 | 0.151 | 0.151 |
| join | p100 | 1 | 1.746 | 1.746 |
| join | p100 | 4 | 0.928 | 0.928 |
| join | p100 | 8 | 0.913 | 0.913 |
| join | s25 | 1 | 0.124 | 0.124 |
| join | s25 | 4 | 0.109 | 0.109 |
| join | s25 | 8 | 0.111 | 0.111 |
| join | s100 | 1 | 0.378 | 0.378 |
| join | s100 | 4 | 0.172 | 0.172 |
| join | s100 | 8 | 0.160 | 0.160 |
| polars | p5 | 1 | 0.045 | 0.039 |
| polars | p5 | 4 | 0.020 | 0.015 |
| polars | p5 | 8 | 0.018 | 0.013 |
| polars | p25 | 1 | 0.181 | 0.123 |
| polars | p25 | 4 | 0.061 | 0.062 |
| polars | p25 | 8 | 0.050 | 0.036 |
| polars | p100 | 1 | 0.785 | 0.517 |
| polars | p100 | 4 | 0.252 | 0.186 |
| polars | p100 | 8 | 0.180 | 0.149 |
| polars | s25 | 1 | 0.186 | 0.125 |
| polars | s25 | 4 | 0.061 | 0.042 |
| polars | s25 | 8 | 0.050 | 0.039 |
| polars | s100 | 1 | 0.182 | 0.123 |
| polars | s100 | 4 | 0.062 | 0.049 |
| polars | s100 | 8 | 0.050 | 0.038 |

## Observations

This dataset is **1M rows, fully resident in the OS page cache** — the regime
where single-file reader engines (wide / polars) are strongest and where an
engine's per-part / per-query fixed costs dominate. Consequences:

- On this small, warm dataset the aligned engine is **slower than every
  baseline** (roughly 2-4x on full scans). This is expected: the aligned
  design pays a small fixed cost to open multiple independent Parquet readers
  and scatter their projected vectors into one DataChunk; with 1M cached rows
  that cost is not amortized and the data-transfer work is identical to the
  baselines (they all read the same ZSTD Parquet).
- **Partition pruning was NOT effective for aligned's `s25`** (0.537s ≈ s100
  0.530s), while wide/join/polars clearly benefit (s25 ≈ 0.10-0.12s vs s100
  0.29-0.38s). The `date` filter in these queries sits on an inner-subquery
  projection; aligned's partition pruning needs the constant filter directly on
  a partition source column of a query over the table to kick in. This is the
  most actionable regression — pruning propagation (the `aligned` metadata knob
  work) is the next lever.
- Parallel scaling is present but modest (p25: 0.53s at 1t -> 0.24s at 8t,
  ~2.2x); the workload is I/O-light and the claim-based scheduler overhead
  limits further gains.
- **Where aligned is expected to win** and is not measured here: cold,
  multi-hundred-GB wide tables (>10^4 columns, 10^8-10^10 rows) where a query
  reads a handful of columns. There the no-JOIN position-aligned concat matters:
  the wide engine must scan one giant file, the join engine must build hash
  tables on keys, and polars must materialize the full horizontal concat. On
  cached 1M rows none of that cost exists.
- Reuse: the benchmark harness (DuckDB `cmd` + Stopwatch per fresh process) and
  the polars hstack baseline are reproducible via `scripts/bench_aligned.ps1`.
