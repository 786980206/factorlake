# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=10000000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL P-CONCAT P-JOIN; threads: 1

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F1 | S0 | 1 | 0.0480 | 0.0157 |
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0486 | 0.0233 |
| D-JOIN | Q2 | F1 | S0 | 1 | 4.4458 | 4.3199 |
| D-JOIN | Q2 | F2 | S0 | 1 | 2.1215 | 2.0051 |
| A-ALIGNED | Q2 | F1 | S0 | 1 | 0.1523 | 0.1088 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0700 | 0.0476 |
| A-NORMAL | Q2 | F1 | S0 | 1 | 0.1503 | 0.1135 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0693 | 0.0440 |
| P-CONCAT | Q2 | F1 | S0 | 1 | 3.2596 | 2.6032 |
| P-CONCAT | Q2 | F2 | S0 | 1 | 3.1952 | 2.7309 |
| P-JOIN | Q2 | F1 | S0 | 1 | 18.8746 | 16.5083 |
| P-JOIN | Q2 | F2 | S0 | 1 | 17.8780 | 16.1570 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
