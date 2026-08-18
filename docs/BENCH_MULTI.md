# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=1000000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL P-CONCAT P-JOIN; threads: 1

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0253 | 0.0244 |
| D-JOIN | Q2 | F2 | S0 | 1 | 0.0749 | 0.0737 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0280 | 0.0257 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0273 | 0.0268 |
| P-CONCAT | Q2 | F2 | S0 | 1 | 0.3341 | 0.1952 |
| P-JOIN | Q2 | F2 | S0 | 1 | 1.3024 | 1.0242 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
