# AlignedTable Multi-Scenario Benchmark

Date: 2026-08-18  Machine: local Linux (AGENTS.md §16.2); tier=A
Dataset: bench_mb rows=40000 cols=128 (idx20+alpha81+fs27) sparse=90% NULL
Engines: D-WIDE D-JOIN A-ALIGNED A-NORMAL P-CONCAT P-JOIN; threads: 1 4

| engine | query | filter | sel | threads | cold_s | warm_s |
|--------|-------|--------|-----|---------|--------|--------|
| D-WIDE | Q2 | F1 | S0 | 1 | 0.0221 | 0.0215 |
| D-WIDE | Q2 | F1 | S0 | 4 | 0.0222 | 0.0220 |
| D-WIDE | Q2 | F2 | S0 | 1 | 0.0235 | 0.0224 |
| D-WIDE | Q2 | F2 | S0 | 4 | 0.0243 | 0.0244 |
| D-WIDE | Q5 | F1 | S0 | 1 | 0.0231 | 0.0220 |
| D-WIDE | Q5 | F1 | S0 | 4 | 0.0231 | 0.0226 |
| D-WIDE | Q5 | F2 | S0 | 1 | 0.0233 | 0.0228 |
| D-WIDE | Q5 | F2 | S0 | 4 | 0.0240 | 0.0240 |
| D-JOIN | Q2 | F1 | S0 | 1 | 0.0310 | 0.0295 |
| D-JOIN | Q2 | F1 | S0 | 4 | 0.0304 | 0.0297 |
| D-JOIN | Q2 | F2 | S0 | 1 | 0.0286 | 0.0285 |
| D-JOIN | Q2 | F2 | S0 | 4 | 0.0313 | 0.0439 |
| D-JOIN | Q5 | F1 | S0 | 1 | 0.0424 | 0.0321 |
| D-JOIN | Q5 | F1 | S0 | 4 | 0.0316 | 0.0327 |
| D-JOIN | Q5 | F2 | S0 | 1 | 0.0306 | 0.0314 |
| D-JOIN | Q5 | F2 | S0 | 4 | 0.0349 | 0.0359 |
| A-ALIGNED | Q2 | F1 | S0 | 1 | 0.0268 | 0.0264 |
| A-ALIGNED | Q2 | F1 | S0 | 4 | 0.0267 | 0.0265 |
| A-ALIGNED | Q2 | F2 | S0 | 1 | 0.0250 | 0.0249 |
| A-ALIGNED | Q2 | F2 | S0 | 4 | 0.0271 | 0.0338 |
| A-ALIGNED | Q5 | F1 | S0 | 1 | 0.0272 | 0.0274 |
| A-ALIGNED | Q5 | F1 | S0 | 4 | 0.0322 | 0.0278 |
| A-ALIGNED | Q5 | F2 | S0 | 1 | 0.0274 | 0.0255 |
| A-ALIGNED | Q5 | F2 | S0 | 4 | 0.0277 | 0.0269 |
| A-NORMAL | Q2 | F1 | S0 | 1 | 0.0256 | 0.0333 |
| A-NORMAL | Q2 | F1 | S0 | 4 | 0.0293 | 0.0274 |
| A-NORMAL | Q2 | F2 | S0 | 1 | 0.0252 | 0.0323 |
| A-NORMAL | Q2 | F2 | S0 | 4 | 0.0264 | 0.0254 |
| A-NORMAL | Q5 | F1 | S0 | 1 | 0.0261 | 0.0261 |
| A-NORMAL | Q5 | F1 | S0 | 4 | 0.0272 | 0.0269 |
| A-NORMAL | Q5 | F2 | S0 | 1 | 0.0259 | 0.0256 |
| A-NORMAL | Q5 | F2 | S0 | 4 | 0.0272 | 0.0272 |
| P-CONCAT | Q2 | F1 | S0 | 1 | 0.0240 | 0.0132 |
| P-CONCAT | Q2 | F1 | S0 | 4 | 0.0261 | 0.0182 |
| P-CONCAT | Q2 | F2 | S0 | 1 | 0.0229 | 0.0132 |
| P-CONCAT | Q2 | F2 | S0 | 4 | 0.0278 | 0.0155 |
| P-CONCAT | Q5 | F1 | S0 | 1 | 0.0457 | 0.0253 |
| P-CONCAT | Q5 | F1 | S0 | 4 | 0.0487 | 0.0419 |
| P-CONCAT | Q5 | F2 | S0 | 1 | 0.0460 | 0.0239 |
| P-CONCAT | Q5 | F2 | S0 | 4 | 0.0516 | 0.0390 |
| P-JOIN | Q2 | F1 | S0 | 1 | 0.0422 | 0.0286 |
| P-JOIN | Q2 | F1 | S0 | 4 | 0.0366 | 0.0225 |
| P-JOIN | Q2 | F2 | S0 | 1 | 0.0446 | 0.0237 |
| P-JOIN | Q2 | F2 | S0 | 4 | 0.0373 | 0.0241 |
| P-JOIN | Q5 | F1 | S0 | 1 | 0.1269 | 0.0504 |
| P-JOIN | Q5 | F1 | S0 | 4 | 0.0720 | 0.0622 |
| P-JOIN | Q5 | F2 | S0 | 1 | 0.1129 | 0.0501 |
| P-JOIN | Q5 | F2 | S0 | 4 | 0.0698 | 0.0436 |

> Column layout: index 20 (date,symbol,close,volume,rowid + ix001..ix015),
> alpha81 sparse (90% NULL), fs27. Q1~3 Q2~35 Q3~500 Q4~5000 Q5=ALL.
> warm = fresh-process 2nd run (page cache warmed); cold = 1st touch in fresh process.
> A-NORMAL is the plugin's aligned=false (union-interval per-leaf pruning), not a key join.
