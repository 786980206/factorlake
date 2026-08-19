# AlignedTable Benchmark (Phase 6)

Date: 2026-08-19  Machine: local Windows (see AGENTS.md 16)
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

- **aligned** - aligned_table('bench_ixday'): 3 groups assembled into one DataChunk,
  no JOIN, projection pushdown, partition pruning, parallel range scan, metadata cache, window carry reuse.
- **wide** - single wide Parquet (127 columns, 1M rows), DuckDB read_parquet.
- **join** - three separate Parquet files (index/alpha/ma) joined on rowid (keyed layout).
- **polars** - the same three files read separately (projection per file) and horizontally
  concatenated (position-aligned) - the classic wide-table assembly path we eliminate.

## Results (seconds; warm = 2nd run in the same process)

| engine | workload | threads | cold | warm |
|--------|----------|---------|------|------|
| aligned | p5 | 1 | 0.163 | 0.163 |
| aligned | p5 | 4 | 0.119 | 0.119 |
| aligned | p5 | 8 | 0.102 | 0.102 |
| aligned | p25 | 1 | 0.508 | 0.508 |
| aligned | p25 | 4 | 0.271 | 0.271 |
| aligned | p25 | 8 | 0.205 | 0.205 |
| aligned | p100 | 1 | 1.986 | 1.986 |
| aligned | p100 | 4 | 0.951 | 0.951 |
| aligned | p100 | 8 | 0.952 | 0.952 |
| aligned | s25 | 1 | 0.155 | 0.155 |
| aligned | s25 | 4 | 0.106 | 0.106 |
| aligned | s25 | 8 | 0.100 | 0.100 |
| aligned | s100 | 1 | 0.511 | 0.511 |
| aligned | s100 | 4 | 0.271 | 0.271 |
| aligned | s100 | 8 | 0.203 | 0.203 |
| wide | p5 | 1 | 0.095 | 0.095 |
| wide | p5 | 4 | 0.065 | 0.065 |
| wide | p5 | 8 | 0.062 | 0.062 |
| wide | p25 | 1 | 0.276 | 0.276 |
| wide | p25 | 4 | 0.121 | 0.121 |
| wide | p25 | 8 | 0.099 | 0.099 |
| wide | p100 | 1 | 1.009 | 1.009 |
| wide | p100 | 4 | 0.360 | 0.360 |
| wide | p100 | 8 | 0.316 | 0.316 |
| wide | s25 | 1 | 0.121 | 0.121 |
| wide | s25 | 4 | 0.085 | 0.085 |
| wide | s25 | 8 | 0.085 | 0.085 |
| wide | s100 | 1 | 0.276 | 0.276 |
| wide | s100 | 4 | 0.121 | 0.121 |
| wide | s100 | 8 | 0.100 | 0.100 |
| join | p5 | 1 | 0.175 | 0.175 |
| join | p5 | 4 | 0.104 | 0.104 |
| join | p5 | 8 | 0.095 | 0.095 |
| join | p25 | 1 | 0.362 | 0.362 |
| join | p25 | 4 | 0.164 | 0.164 |
| join | p25 | 8 | 0.134 | 0.134 |
| join | p100 | 1 | 1.677 | 1.677 |
| join | p100 | 4 | 0.846 | 0.846 |
| join | p100 | 8 | 0.777 | 0.777 |
| join | s25 | 1 | 0.161 | 0.161 |
| join | s25 | 4 | 0.108 | 0.108 |
| join | s25 | 8 | 0.107 | 0.107 |
| join | s100 | 1 | 0.364 | 0.364 |
| join | s100 | 4 | 0.162 | 0.162 |
| join | s100 | 8 | 0.136 | 0.136 |
| polars | p5 | 1 | 0.040 | 0.031 |
| polars | p5 | 4 | 0.018 | 0.014 |
| polars | p5 | 8 | 0.015 | 0.011 |
| polars | p25 | 1 | 0.163 | 0.121 |
| polars | p25 | 4 | 0.055 | 0.039 |
| polars | p25 | 8 | 0.041 | 0.035 |
| polars | p100 | 1 | 0.689 | 0.509 |
| polars | p100 | 4 | 0.222 | 0.157 |
| polars | p100 | 8 | 0.147 | 0.135 |
| polars | s25 | 1 | 0.168 | 0.123 |
| polars | s25 | 4 | 0.056 | 0.041 |
| polars | s25 | 8 | 0.040 | 0.037 |
| polars | s100 | 1 | 0.163 | 0.121 |
| polars | s100 | 4 | 0.055 | 0.042 |
| polars | s100 | 8 | 0.042 | 0.034 |

## Historical note: v2 contract vs v1 (archived v1.0 branch)

Same machine, same bench_ixday 1M rows, same script. v1 (old contract, archived to
the `v1.0` branch) vs v2 (footer-driven row ranges). Baseline engines (wide/join/polars)
were identical between the two runs, so the comparison is valid.

| workload | threads | v1 (s) | v2 (s) | delta |
|----------|---------|--------|--------|-------|
| p5  | 1 | 0.176 | 0.163 | -7% |
| p5  | 4 | 0.130 | 0.119 | -8% |
| p5  | 8 | 0.113 | 0.102 | -10% |
| p25 | 1 | 0.528 | 0.508 | -4% |
| p25 | 4 | 0.287 | 0.271 | -6% |
| p25 | 8 | 0.215 | 0.205 | -5% |
| p100 | 1 | 2.008 | 1.986 | -1% |
| p100 | 4 | 1.005 | 0.951 | -5% |
| p100 | 8 | 0.999 | 0.952 | -5% |
| s25 | 1 | 0.165 | 0.155 | -6% |
| s25 | 4 | 0.116 | 0.106 | -9% |
| s25 | 8 | 0.108 | 0.100 | -7% |
| s100 | 1 | 0.522 | 0.511 | -2% |
| s100 | 4 | 0.285 | 0.271 | -5% |
| s100 | 8 | 0.215 | 0.203 | -6% |

v2 is 1-10% faster everywhere (plan build drops sidecar/marker reads; footer metadata
is cached). No regression. This comparison is historical — v3/v4 keep the same
footer-based plan build; the three-mode benchmark (all/group/none) was deleted with
the v4 contract (full alignment is the only mode).

## Observations

(filled in by the analysis step)
