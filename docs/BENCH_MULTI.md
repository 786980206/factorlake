# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=10000000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: A-ALIGNED A-NORMAL; threads: 1 2 4 8

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0681 | 0.0372 |
| A-ALIGNED | Q2 | F2 | S0 | 2 | 0.0593 | 0.0310 |
| A-ALIGNED | Q2 | F2 | S0 | 4 | 0.0524 | 0.0236 |
| A-ALIGNED | Q2 | F2 | S0 | 8 | 0.0535 | 0.0229 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0904 | 0.0425 |
| A-NORMAL | Q2 | F2 | S0 | 2 | 0.0653 | 0.0353 |
| A-NORMAL | Q2 | F2 | S0 | 4 | 0.0526 | 0.0265 |
| A-NORMAL | Q2 | F2 | S0 | 8 | 0.0496 | 0.0245 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
