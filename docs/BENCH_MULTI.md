# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=200000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL P-CONCAT P-JOIN; threads: 1 4 8

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F1 | S0 | 1 | 0.0234 | 0.0226 |
| D-WIDE | Q2 | F1 | S0 | 4 | 0.0241 | 0.0269 |
| D-WIDE | Q2 | F1 | S0 | 8 | 0.0250 | 0.0241 |
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0236 | 0.0234 |
| D-WIDE | Q2 | F2 | S0 | 4 | 0.0245 | 0.0257 |
| D-WIDE | Q2 | F2 | S0 | 8 | 0.0254 | 0.0251 |
| D-WIDE | Q5 | F1 | S0 | 1 | 0.0233 | 0.0238 |
| D-WIDE | Q5 | F1 | S0 | 4 | 0.0242 | 0.0250 |
| D-WIDE | Q5 | F1 | S0 | 8 | 0.0263 | 0.0253 |
| D-WIDE | Q5 | F2 | S0 | 1 | 0.0238 | 0.0256 |
| D-WIDE | Q5 | F2 | S0 | 4 | 0.0298 | 0.0283 |
| D-WIDE | Q5 | F2 | S0 | 8 | 0.0282 | 0.0268 |
| D-JOIN | Q2 | F1 | S0 | 1 | 0.0560 | 0.0553 |
| D-JOIN | Q2 | F1 | S0 | 4 | 0.0453 | 0.0420 |
| D-JOIN | Q2 | F1 | S0 | 8 | 0.0452 | 0.0457 |
| D-JOIN | Q2 | F2 | S0 | 1 | 0.0412 | 0.0419 |
| D-JOIN | Q2 | F2 | S0 | 4 | 0.0420 | 0.0424 |
| D-JOIN | Q2 | F2 | S0 | 8 | 0.0432 | 0.0438 |
| D-JOIN | Q5 | F1 | S0 | 1 | 0.0569 | 0.0573 |
| D-JOIN | Q5 | F1 | S0 | 4 | 0.0439 | 0.0436 |
| D-JOIN | Q5 | F1 | S0 | 8 | 0.0448 | 0.0452 |
| D-JOIN | Q5 | F2 | S0 | 1 | 0.0399 | 0.0462 |
| D-JOIN | Q5 | F2 | S0 | 4 | 0.0416 | 0.0425 |
| D-JOIN | Q5 | F2 | S0 | 8 | 0.0448 | 0.0444 |
| A-ALIGNED | Q2 | F1 | S0 | 1 | 0.0274 | 0.0278 |
| A-ALIGNED | Q2 | F1 | S0 | 4 | 0.0276 | 0.0297 |
| A-ALIGNED | Q2 | F1 | S0 | 8 | 0.0337 | 0.0285 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0282 | 0.0268 |
| A-ALIGNED | Q2 | F2 | S0 | 4 | 0.0380 | 0.0285 |
| A-ALIGNED | Q2 | F2 | S0 | 8 | 0.0292 | 0.0298 |
| A-ALIGNED | Q5 | F1 | S0 | 1 | 0.0292 | 0.0311 |
| A-ALIGNED | Q5 | F1 | S0 | 4 | 0.0348 | 0.0367 |
| A-ALIGNED | Q5 | F1 | S0 | 8 | 0.0333 | 0.0295 |
| A-ALIGNED | Q5 | F2 | S0 | 1 | 0.0280 | 0.0292 |
| A-ALIGNED | Q5 | F2 | S0 | 4 | 0.0292 | 0.0271 |
| A-ALIGNED | Q5 | F2 | S0 | 8 | 0.0272 | 0.0262 |
| A-NORMAL | Q2 | F1 | S0 | 1 | 0.0264 | 0.0289 |
| A-NORMAL | Q2 | F1 | S0 | 4 | 0.0275 | 0.0265 |
| A-NORMAL | Q2 | F1 | S0 | 8 | 0.0276 | 0.0270 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0243 | 0.0247 |
| A-NORMAL | Q2 | F2 | S0 | 4 | 0.0257 | 0.0255 |
| A-NORMAL | Q2 | F2 | S0 | 8 | 0.0266 | 0.0261 |
| A-NORMAL | Q5 | F1 | S0 | 1 | 0.0270 | 0.0283 |
| A-NORMAL | Q5 | F1 | S0 | 4 | 0.0268 | 0.0305 |
| A-NORMAL | Q5 | F1 | S0 | 8 | 0.0285 | 0.0283 |
| A-NORMAL | Q5 | F2 | S0 | 1 | 0.0316 | 0.0303 |
| A-NORMAL | Q5 | F2 | S0 | 4 | 0.0279 | 0.0270 |
| A-NORMAL | Q5 | F2 | S0 | 8 | 0.0273 | 0.0265 |
| P-CONCAT | Q2 | F1 | S0 | 1 | 0.0743 | 0.0422 |
| P-CONCAT | Q2 | F1 | S0 | 4 | 0.0528 | 0.0364 |
| P-CONCAT | Q2 | F1 | S0 | 8 | 0.0571 | 0.0462 |
| P-CONCAT | Q2 | F2 | S0 | 1 | 0.0755 | 0.0428 |
| P-CONCAT | Q2 | F2 | S0 | 4 | 0.0538 | 0.0369 |
| P-CONCAT | Q2 | F2 | S0 | 8 | 0.0598 | 0.0467 |
| P-CONCAT | Q5 | F1 | S0 | 1 | 0.2211 | 0.0915 |
| P-CONCAT | Q5 | F1 | S0 | 4 | 0.1238 | 0.0851 |
| P-CONCAT | Q5 | F1 | S0 | 8 | 0.1261 | 0.1717 |
| P-CONCAT | Q5 | F2 | S0 | 1 | 0.1847 | 0.0932 |
| P-CONCAT | Q5 | F2 | S0 | 4 | 0.1220 | 0.1360 |
| P-CONCAT | Q5 | F2 | S0 | 8 | 0.1315 | 0.0991 |
| P-JOIN | Q2 | F1 | S0 | 1 | 0.2054 | 0.1257 |
| P-JOIN | Q2 | F1 | S0 | 4 | 0.1055 | 0.0596 |
| P-JOIN | Q2 | F1 | S0 | 8 | 0.0962 | 0.0705 |
| P-JOIN | Q2 | F2 | S0 | 1 | 0.2058 | 0.1246 |
| P-JOIN | Q2 | F2 | S0 | 4 | 0.1011 | 0.0671 |
| P-JOIN | Q2 | F2 | S0 | 8 | 0.1020 | 0.0674 |
| P-JOIN | Q5 | F1 | S0 | 1 | 0.6596 | 0.2695 |
| P-JOIN | Q5 | F1 | S0 | 4 | 0.2343 | 0.1299 |
| P-JOIN | Q5 | F1 | S0 | 8 | 0.2034 | 0.2099 |
| P-JOIN | Q5 | F2 | S0 | 1 | 0.5304 | 0.2995 |
| P-JOIN | Q5 | F2 | S0 | 4 | 0.2426 | 0.1696 |
| P-JOIN | Q5 | F2 | S0 | 8 | 0.1993 | 0.2395 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
