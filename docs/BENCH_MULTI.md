# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=500000 cols=1024 (idx20+alpha753+fs251) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL; threads: 1

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q3 | F1 | S0 | 1 | 0.0348 | 0.0115 |
| D-WIDE | Q3 | F2 | S0 | 1 | 0.0377 | 0.0159 |
| D-WIDE | Q5 | F1 | S0 | 1 | 0.0458 | 0.0187 |
| D-WIDE | Q5 | F2 | S0 | 1 | 0.0467 | 0.0191 |
| D-JOIN | Q3 | F1 | S0 | 1 | 0.1156 | 0.0737 |
| D-JOIN | Q3 | F2 | S0 | 1 | 0.0700 | 0.0359 |
| D-JOIN | Q5 | F1 | S0 | 1 | 0.1331 | 0.0804 |
| D-JOIN | Q5 | F2 | S0 | 1 | 0.0802 | 0.0486 |
| A-ALIGNED | Q3 | F1 | S0 | 1 | 0.0512 | 0.0250 |
| A-ALIGNED | Q3 | F2 | S0 | 1 | 0.0447 | 0.0270 |
| A-ALIGNED | Q5 | F1 | S0 | 1 | 0.0532 | 0.0267 |
| A-ALIGNED | Q5 | F2 | S0 | 1 | 0.0423 | 0.0212 |
| A-NORMAL | Q3 | F1 | S0 | 1 | 0.0451 | 0.0207 |
| A-NORMAL | Q3 | F2 | S0 | 1 | 0.0395 | 0.0218 |
| A-NORMAL | Q5 | F1 | S0 | 1 | 0.0562 | 0.0254 |
| A-NORMAL | Q5 | F2 | S0 | 1 | 0.0484 | 0.0239 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha753 sparse (90% NULL), fs251. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
