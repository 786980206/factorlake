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

- **aligned** - ligned_table('bench_ixday'): 3 groups assembled into one DataChunk,
  no JOIN, projection pushdown, partition pruning, parallel range scan, metadata cache, window carry reuse.
- **wide** - single wide Parquet (127 columns, 1M rows), DuckDB ead_parquet.
- **join** - three separate Parquet files (index/alpha/ma) joined on rowid (keyed layout).
- **polars** - the same three files read separately (projection per file) and horizontally
  concatenated (position-aligned) - the classic wide-table assembly path we eliminate.

## Results (seconds; warm = 2nd run in the same process)

| engine | workload | threads | cold | warm |
|--------|----------|---------|------|------|
| aligned | p5 | 1 | 0.170 | 0.170 |
| aligned | p5 | 4 | 0.120 | 0.120 |
| aligned | p5 | 8 | 0.102 | 0.102 |
| aligned | p25 | 1 | 0.514 | 0.514 |
| aligned | p25 | 4 | 0.273 | 0.273 |
| aligned | p25 | 8 | 0.217 | 0.217 |
| aligned | p100 | 1 | 2.002 | 2.002 |
| aligned | p100 | 4 | 0.957 | 0.957 |
| aligned | p100 | 8 | 0.949 | 0.949 |
| aligned | s25 | 1 | 0.155 | 0.155 |
| aligned | s25 | 4 | 0.106 | 0.106 |
| aligned | s25 | 8 | 0.100 | 0.100 |
| aligned | s100 | 1 | 0.510 | 0.510 |
| aligned | s100 | 4 | 0.278 | 0.278 |
| aligned | s100 | 8 | 0.202 | 0.202 |
| wide | p5 | 1 | 0.095 | 0.095 |
| wide | p5 | 4 | 0.065 | 0.065 |
| wide | p5 | 8 | 0.059 | 0.059 |
| wide | p25 | 1 | 0.277 | 0.277 |
| wide | p25 | 4 | 0.121 | 0.121 |
| wide | p25 | 8 | 0.099 | 0.099 |
| wide | p100 | 1 | 1.003 | 1.003 |
| wide | p100 | 4 | 0.365 | 0.365 |
| wide | p100 | 8 | 0.306 | 0.306 |
| wide | s25 | 1 | 0.123 | 0.123 |
| wide | s25 | 4 | 0.084 | 0.084 |
| wide | s25 | 8 | 0.087 | 0.087 |
| wide | s100 | 1 | 0.276 | 0.276 |
| wide | s100 | 4 | 0.122 | 0.122 |
| wide | s100 | 8 | 0.098 | 0.098 |
| join | p5 | 1 | 0.178 | 0.178 |
| join | p5 | 4 | 0.106 | 0.106 |
| join | p5 | 8 | 0.094 | 0.094 |
| join | p25 | 1 | 0.359 | 0.359 |
| join | p25 | 4 | 0.163 | 0.163 |
| join | p25 | 8 | 0.134 | 0.134 |
| join | p100 | 1 | 1.669 | 1.669 |
| join | p100 | 4 | 0.853 | 0.853 |
| join | p100 | 8 | 0.787 | 0.787 |
| join | s25 | 1 | 0.160 | 0.160 |
| join | s25 | 4 | 0.107 | 0.107 |
| join | s25 | 8 | 0.108 | 0.108 |
| join | s100 | 1 | 0.367 | 0.367 |
| join | s100 | 4 | 0.181 | 0.181 |
| join | s100 | 8 | 0.136 | 0.136 |
| polars | p5 | 1 | 0.206 | 0.032 |
| polars | p5 | 4 | 0.021 | 0.014 |
| polars | p5 | 8 | 0.015 | 0.011 |
| polars | p25 | 1 | 0.163 | 0.122 |
| polars | p25 | 4 | 0.056 | 0.042 |
| polars | p25 | 8 | 0.043 | 0.034 |
| polars | p100 | 1 | 0.700 | 0.511 |
| polars | p100 | 4 | 0.230 | 0.160 |
| polars | p100 | 8 | 0.149 | 0.123 |
| polars | s25 | 1 | 0.197 | 0.123 |
| polars | s25 | 4 | 0.056 | 0.043 |
| polars | s25 | 8 | 0.043 | 0.033 |
| polars | s100 | 1 | 0.163 | 0.122 |
| polars | s100 | 4 | 0.065 | 0.043 |
| polars | s100 | 8 | 0.042 | 0.032 |

## Observations

(filled in by the analysis step)
