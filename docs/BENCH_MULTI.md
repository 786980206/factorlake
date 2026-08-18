# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=1000000 cols=128 (idx20+alpha81+fs27) sparse=99% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL; threads: 1 4

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F2 | S1 | 1 | 0.0229 | 0.0065 |
| D-WIDE | Q2 | F2 | S1 | 4 | 0.0242 | 0.0068 |
| D-JOIN | Q2 | F2 | S1 | 1 | 0.0485 | 0.0209 |
| D-JOIN | Q2 | F2 | S1 | 4 | 0.0495 | 0.0224 |
| A-ALIGNED | Q2 | F2 | S1 | 1 | 0.0278 | 0.0097 |
| A-ALIGNED | Q2 | F2 | S1 | 4 | 0.0278 | 0.0086 |
| A-NORMAL | Q2 | F2 | S1 | 1 | 0.0262 | 0.0099 |
| A-NORMAL | Q2 | F2 | S1 | 4 | 0.0258 | 0.0085 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (99% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
