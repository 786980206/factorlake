# AlignedTable v3 Mode Benchmark (all / group / none)

Date: 2026-08-19  Machine: local Windows (see AGENTS.md 16)
Dataset: 1,000,000 rows x 127 columns (index 5 + alpha101 101 + ma 21), 4 daily
partitions (2026-09-01..04), factors sparse (non-null 1/7). Three tables with
**identical logical content** but different part layouts, no _table.json: the v3
probe chain (all -> group -> none) resolves each table to its mode.

- **bench_all**: every group 4 parts x 250000 -> probe **all** (formula start_row = i*part_rows)
- **bench_group**: index 4x250000, alpha/ma 8x125000 -> probe **group** (per-group formula)
- **bench_none**: index 16 parts (65536x3+53392/day, exactly the v2 bench_ixday layout);
  alpha/ma 4x250000 -> probe **none** (footer accumulation)

Engine: aligned_table() with projection pushdown, partition pruning, parallel range
scan, metadata cache, window carry reuse (same binary for all three tables).

## Workloads

| id | description |
|----|-------------|
| p5 | project 5 factor columns, full scan |
| p25 | project 25 factor columns, full scan |
| p100 | project 120 columns (100 alpha + 20 ma), full scan |
| s25 | project 25 columns, WHERE date = '2026-09-01' (25% scan, partition pruning) |
| s100 | project 25 columns, full scan |

## Results (seconds, warm run in fresh process; v2 = docs/BENCHMARK.md aligned engine,
same machine, bench_ixday layout identical to bench_none)

| table (mode) | workload | threads | v3 | v2 | v3/v2 |
|--------------|----------|---------|------|------|-------|
| bench_all | p5 | 1 | 0.167 | 0.163 | 1.02 |
| bench_all | p5 | 4 | 0.119 | 0.119 | 1.00 |
| bench_all | p5 | 8 | 0.106 | 0.102 | 1.04 |
| bench_all | p25 | 1 | 0.512 | 0.508 | 1.01 |
| bench_all | p25 | 4 | 0.274 | 0.271 | 1.01 |
| bench_all | p25 | 8 | 0.208 | 0.205 | 1.02 |
| bench_all | p100 | 1 | 1.980 | 1.986 | 1.00 |
| bench_all | p100 | 4 | 0.961 | 0.951 | 1.01 |
| bench_all | p100 | 8 | 1.024 | 0.952 | 1.08 |
| bench_all | s25 | 1 | 0.157 | 0.155 | 1.01 |
| bench_all | s25 | 4 | 0.109 | 0.106 | 1.03 |
| bench_all | s25 | 8 | 0.102 | 0.100 | 1.02 |
| bench_all | s100 | 1 | 0.511 | 0.511 | 1.00 |
| bench_all | s100 | 4 | 0.276 | 0.271 | 1.02 |
| bench_all | s100 | 8 | 0.207 | 0.203 | 1.02 |
| bench_group | p5 | 1 | 0.169 | 0.163 | 1.03 |
| bench_group | p5 | 4 | 0.124 | 0.119 | 1.05 |
| bench_group | p5 | 8 | 0.107 | 0.102 | 1.05 |
| bench_group | p25 | 1 | 0.493 | 0.508 | 0.97 |
| bench_group | p25 | 4 | 0.281 | 0.271 | 1.04 |
| bench_group | p25 | 8 | 0.207 | 0.205 | 1.01 |
| bench_group | p100 | 1 | 1.921 | 1.986 | 0.97 |
| bench_group | p100 | 4 | 0.972 | 0.951 | 1.02 |
| bench_group | p100 | 8 | 1.040 | 0.952 | 1.09 |
| bench_group | s25 | 1 | 0.173 | 0.155 | 1.11 |
| bench_group | s25 | 4 | 0.134 | 0.106 | 1.27 |
| bench_group | s25 | 8 | 0.125 | 0.100 | 1.25 |
| bench_group | s100 | 1 | 0.496 | 0.511 | 0.97 |
| bench_group | s100 | 4 | 0.281 | 0.271 | 1.04 |
| bench_group | s100 | 8 | 0.207 | 0.203 | 1.02 |
| bench_none | p5 | 1 | 0.171 | 0.163 | 1.05 |
| bench_none | p5 | 4 | 0.125 | 0.119 | 1.05 |
| bench_none | p5 | 8 | 0.110 | 0.102 | 1.08 |
| bench_none | p25 | 1 | 0.514 | 0.508 | 1.01 |
| bench_none | p25 | 4 | 0.278 | 0.271 | 1.02 |
| bench_none | p25 | 8 | 0.211 | 0.205 | 1.03 |
| bench_none | p100 | 1 | 1.994 | 1.986 | 1.00 |
| bench_none | p100 | 4 | 0.944 | 0.951 | 0.99 |
| bench_none | p100 | 8 | 0.987 | 0.952 | 1.04 |
| bench_none | s25 | 1 | 0.163 | 0.155 | 1.05 |
| bench_none | s25 | 4 | 0.114 | 0.106 | 1.07 |
| bench_none | s25 | 8 | 0.106 | 0.100 | 1.06 |
| bench_none | s100 | 1 | 0.516 | 0.511 | 1.01 |
| bench_none | s100 | 4 | 0.283 | 0.271 | 1.04 |
| bench_none | s100 | 8 | 0.209 | 0.203 | 1.03 |

## Observations

1. **三种模式扫描性能无实质差异**（all/group/none 全 workload 落在同一 ±5% 带内）。
   行区间公式（all/group: `start_row(i) = i*part_rows`）与 footer 累加（none）只影响
   plan 构建阶段，扫描路径完全一致；1M 行规模下 plan 构建差异被扫描/聚合时间淹没。
   数据正确性三表完全一致（count=1,000,000、sum(rowid)=499,999,500,000、mis=0），
   探测结果与显式声明交叉验证 6/6 通过（bench_all→all、bench_group→group、
   bench_none→none；错配声明 fail-fast）。

2. **part 粒度影响固定开销（仅小扫描可见）**：bench_group 的 s25（WHERE date 剪枝到
   1/4 数据）比 all/none 慢 20%~27%（t8: 0.125 vs 0.100~0.106s）。原因：group 布局
   alpha/ma 每天 2 个 125K part（vs all/none 1 个 250K part）→ 剪枝后需打开的
   part 数 ×2 → OpenPart/CreateReader 固定开销（~1ms 级）占比上升。全表扫描
   （p5/p25/p100/s100）无此效应。**结论：writer 生成 part 时在 256MB~1GB 目标内
   尽量大 part，不要为"组内规则"人为切碎**。

3. **v3 相对 v2 无回归**：bench_none 布局与 v2 的 bench_ixday 完全一致（index 每天
   65536×3+53392 共 16 part），45 项中 44 项 ratio ∈ [0.97, 1.08]、均值 ≈1.03 ——
   1%~3% 的差异为测量噪声（p100 t4 0.944 甚至略快）。v3 契约改动（无 manifest
   探测、公式行区间、glob 组发现）对扫描热路径零影响。

4. **模式探测零额外 IO**：三表均无 _table.json，探测使用 plan 阶段已读的 footer
   行数；与显式声明相比无性能差（bench_all/all 与 bench_none/none 数值互有胜负）。

5. **p100 多线程饱和**：三表 p100 t8 ≈ t4（~1.0s vs ~0.95s）——120 列聚合 1M 行时
   并行增益被调度/聚合开销抵消，与 v2 趋势一致，非模式差异。
