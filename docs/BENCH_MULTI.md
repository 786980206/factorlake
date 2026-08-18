# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=1000000 cols=1024 (idx20+alpha753+fs251) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL; threads: 1 4

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F1 | S0 | 1 | 0.0422 | 0.0093 |
| D-WIDE | Q2 | F1 | S0 | 4 | 0.0397 | 0.0099 |
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0415 | 0.0135 |
| D-WIDE | Q2 | F2 | S0 | 4 | 0.0578 | 0.0118 |
| D-WIDE | Q5 | F1 | S0 | 1 | 0.0615 | 0.0246 |
| D-WIDE | Q5 | F1 | S0 | 4 | 0.0669 | 0.0257 |
| D-WIDE | Q5 | F2 | S0 | 1 | 0.0561 | 0.0239 |
| D-WIDE | Q5 | F2 | S0 | 4 | 0.0551 | 0.0237 |
| D-JOIN | Q2 | F1 | S0 | 1 | 0.1961 | 0.1306 |
| D-JOIN | Q2 | F1 | S0 | 4 | 0.1213 | 0.0656 |
| D-JOIN | Q2 | F2 | S0 | 1 | 0.0992 | 0.0500 |
| D-JOIN | Q2 | F2 | S0 | 4 | 0.0755 | 0.0370 |
| D-JOIN | Q5 | F1 | S0 | 1 | 0.2069 | 0.1506 |
| D-JOIN | Q5 | F1 | S0 | 4 | 0.1349 | 0.0779 |
| D-JOIN | Q5 | F2 | S0 | 1 | 0.1197 | 0.0647 |
| D-JOIN | Q5 | F2 | S0 | 4 | 0.0947 | 0.0511 |
| A-ALIGNED | Q2 | F1 | S0 | 1 | 0.0501 | 0.0290 |
| A-ALIGNED | Q2 | F1 | S0 | 4 | 0.0457 | 0.0203 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0482 | 0.0185 |
| A-ALIGNED | Q2 | F2 | S0 | 4 | 0.0416 | 0.0168 |
| A-ALIGNED | Q5 | F1 | S0 | 1 | 0.0538 | 0.0297 |
| A-ALIGNED | Q5 | F1 | S0 | 4 | 0.0492 | 0.0291 |
| A-ALIGNED | Q5 | F2 | S0 | 1 | 0.0470 | 0.0230 |
| A-ALIGNED | Q5 | F2 | S0 | 4 | 0.0464 | 0.0221 |
| A-NORMAL | Q2 | F1 | S0 | 1 | 0.0527 | 0.0265 |
| A-NORMAL | Q2 | F1 | S0 | 4 | 0.0478 | 0.0216 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0438 | 0.0188 |
| A-NORMAL | Q2 | F2 | S0 | 4 | 0.0495 | 0.0185 |
| A-NORMAL | Q5 | F1 | S0 | 1 | 0.0600 | 0.0301 |
| A-NORMAL | Q5 | F1 | S0 | 4 | 0.0711 | 0.0272 |
| A-NORMAL | Q5 | F2 | S0 | 1 | 0.0483 | 0.0239 |
| A-NORMAL | Q5 | F2 | S0 | 4 | 0.0482 | 0.0229 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha753 sparse (90% NULL), fs251. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
