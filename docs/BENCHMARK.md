# AlignedTable Benchmark (Phase 6)

Date: 2026-08-18  Machine: local Linux (see AGENTS.md §16.2);
aligned engine: /home/windsing/proj/factorlake/duckdb/build/duckdb
Dataset: **bench_ixday** - 400000 rows x 127 columns (index 5 + alpha101 101 + ma 21),
4 daily partitions, factors sparse (non-null 1/7). Aligned layout: 3 independent Parquet column groups.

## Workloads

| id | description |
|----|-------------|
| p5 | project 5 factor columns, full scan |
| p25 | project 25 factor columns, full scan |
| p100 | project 120 columns (100 alpha + 20 ma), full scan |
| s25 | project 25 columns, WHERE date = DATE '2026-09-01' (25% scan, partition pruning) |
| s100 | project 25 columns, full scan |

## Engines

- **aligned** - `aligned_table('bench_ixday')`: 3 groups assembled into one DataChunk,
  no JOIN, projection pushdown, partition pruning, parallel range scan, metadata cache, window carry reuse.
- **wide** - single wide Parquet (400000 rows), DuckDB `read_parquet`.
- **join** - three separate Parquet files (index/alpha/ma) joined on rowid (keyed layout).

## Results (seconds; cold = fresh process first touch, warm = second run after OS cache warmed)

| engine | workload | threads | cold | warm |
|--------|----------|---------|------|------|
| aligned | p5 | 1 | 0.070 | 0.063 |
| aligned | p5 | 4 | 0.052 | 0.046 |
| aligned | p5 | 8 | 0.047 | 0.045 |
| aligned | p25 | 1 | 0.200 | 0.204 |
| aligned | p25 | 4 | 0.099 | 0.123 |
| aligned | p25 | 8 | 0.097 | 0.100 |
| aligned | p100 | 1 | 0.770 | 0.713 |
| aligned | p100 | 4 | 0.360 | 0.429 |
| aligned | p100 | 8 | 0.359 | 0.337 |
| aligned | s25 | 1 | 0.066 | 0.058 |
| aligned | s25 | 4 | 0.049 | 0.052 |
| aligned | s25 | 8 | 0.052 | 0.048 |
| aligned | s100 | 1 | 0.182 | 0.211 |
| aligned | s100 | 4 | 0.119 | 0.118 |
| aligned | s100 | 8 | 0.094 | 0.091 |
| wide | p5 | 1 | 0.041 | 0.046 |
| wide | p5 | 4 | 0.031 | 0.031 |
| wide | p5 | 8 | 0.036 | 0.033 |
| wide | p25 | 1 | 0.099 | 0.100 |
| wide | p25 | 4 | 0.064 | 0.055 |
| wide | p25 | 8 | 0.063 | 0.053 |
| wide | p100 | 1 | 0.354 | 0.348 |
| wide | p100 | 4 | 0.148 | 0.146 |
| wide | p100 | 8 | 0.191 | 0.188 |
| wide | s25 | 1 | 0.048 | 0.046 |
| wide | s25 | 4 | 0.049 | 0.046 |
| wide | s25 | 8 | 0.046 | 0.048 |
| wide | s100 | 1 | 0.110 | 0.108 |
| wide | s100 | 4 | 0.056 | 0.066 |
| wide | s100 | 8 | 0.064 | 0.068 |
| join | p5 | 1 | 0.083 | 0.077 |
| join | p5 | 4 | 0.061 | 0.058 |
| join | p5 | 8 | 0.060 | 0.065 |
| join | p25 | 1 | 0.145 | 0.135 |
| join | p25 | 4 | 0.084 | 0.091 |
| join | p25 | 8 | 0.099 | 0.093 |
| join | p100 | 1 | 0.652 | 0.652 |
| join | p100 | 4 | 0.446 | 0.436 |
| join | p100 | 8 | 0.434 | 0.416 |
| join | s25 | 1 | 0.072 | 0.060 |
| join | s25 | 4 | 0.061 | 0.062 |
| join | s25 | 8 | 0.066 | 0.066 |
| join | s100 | 1 | 0.139 | 0.137 |
| join | s100 | 4 | 0.088 | 0.091 |
| join | s100 | 8 | 0.096 | 0.096 |

## Notes

- Warm measurement is a fresh-process second run (OS page cache warmed by run 1).
- Partition-pruning self-check passed: `date='2026-09-02'` prunes to 100000 rows.
