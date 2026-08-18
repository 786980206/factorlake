# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=250000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL P-CONCAT P-JOIN; threads: 1 4

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F1 | S0 | 1 | 0.0221 | 0.0056 |
| D-WIDE | Q2 | F1 | S0 | 4 | 0.0302 | 0.0054 |
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0234 | 0.0058 |
| D-WIDE | Q2 | F2 | S0 | 4 | 0.0239 | 0.0059 |
| D-WIDE | Q5 | F1 | S0 | 1 | 0.0213 | 0.0059 |
| D-WIDE | Q5 | F1 | S0 | 4 | 0.0264 | 0.0057 |
| D-WIDE | Q5 | F2 | S0 | 1 | 0.0237 | 0.0071 |
| D-WIDE | Q5 | F2 | S0 | 4 | 0.0241 | 0.0066 |
| D-JOIN | Q2 | F1 | S0 | 1 | 0.0701 | 0.0379 |
| D-JOIN | Q2 | F1 | S0 | 4 | 0.0431 | 0.0248 |
| D-JOIN | Q2 | F2 | S0 | 1 | 0.0591 | 0.0255 |
| D-JOIN | Q2 | F2 | S0 | 4 | 0.0439 | 0.0219 |
| D-JOIN | Q5 | F1 | S0 | 1 | 0.0641 | 0.0384 |
| D-JOIN | Q5 | F1 | S0 | 4 | 0.0445 | 0.0229 |
| D-JOIN | Q5 | F2 | S0 | 1 | 0.0463 | 0.0247 |
| D-JOIN | Q5 | F2 | S0 | 4 | 0.0429 | 0.0207 |
| A-ALIGNED | Q2 | F1 | S0 | 1 | 0.0260 | 0.0085 |
| A-ALIGNED | Q2 | F1 | S0 | 4 | 0.0255 | 0.0079 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0239 | 0.0070 |
| A-ALIGNED | Q2 | F2 | S0 | 4 | 0.0261 | 0.0080 |
| A-ALIGNED | Q5 | F1 | S0 | 1 | 0.0274 | 0.0086 |
| A-ALIGNED | Q5 | F1 | S0 | 4 | 0.0261 | 0.0099 |
| A-ALIGNED | Q5 | F2 | S0 | 1 | 0.0246 | 0.0084 |
| A-ALIGNED | Q5 | F2 | S0 | 4 | 0.0255 | 0.0077 |
| A-NORMAL | Q2 | F1 | S0 | 1 | 0.0252 | 0.0089 |
| A-NORMAL | Q2 | F1 | S0 | 4 | 0.0256 | 0.0080 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0240 | 0.0071 |
| A-NORMAL | Q2 | F2 | S0 | 4 | 0.0258 | 0.0075 |
| A-NORMAL | Q5 | F1 | S0 | 1 | 0.0279 | 0.0097 |
| A-NORMAL | Q5 | F1 | S0 | 4 | 0.0260 | 0.0091 |
| A-NORMAL | Q5 | F2 | S0 | 1 | 0.0254 | 0.0078 |
| A-NORMAL | Q5 | F2 | S0 | 4 | 0.0258 | 0.0096 |
| P-CONCAT | Q2 | F1 | S0 | 1 | 0.0885 | 0.0514 |
| P-CONCAT | Q2 | F1 | S0 | 4 | 0.0635 | 0.0361 |
| P-CONCAT | Q2 | F2 | S0 | 1 | 0.0897 | 0.0513 |
| P-CONCAT | Q2 | F2 | S0 | 4 | 0.0535 | 0.0406 |
| P-CONCAT | Q5 | F1 | S0 | 1 | 0.2129 | 0.1046 |
| P-CONCAT | Q5 | F1 | S0 | 4 | 0.1203 | 0.1268 |
| P-CONCAT | Q5 | F2 | S0 | 1 | 0.2150 | 0.1043 |
| P-CONCAT | Q5 | F2 | S0 | 4 | 0.1177 | 0.1013 |
| P-JOIN | Q2 | F1 | S0 | 1 | 0.2874 | 0.1627 |
| P-JOIN | Q2 | F1 | S0 | 4 | 0.1105 | 0.0729 |
| P-JOIN | Q2 | F2 | S0 | 1 | 0.2666 | 0.1582 |
| P-JOIN | Q2 | F2 | S0 | 4 | 0.1100 | 0.0720 |
| P-JOIN | Q5 | F1 | S0 | 1 | 0.8446 | 0.3959 |
| P-JOIN | Q5 | F1 | S0 | 4 | 0.2840 | 0.1719 |
| P-JOIN | Q5 | F2 | S0 | 1 | 0.7030 | 0.3742 |
| P-JOIN | Q5 | F2 | S0 | 4 | 0.2619 | 0.1950 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
