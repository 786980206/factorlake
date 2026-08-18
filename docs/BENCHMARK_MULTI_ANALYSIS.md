# AlignedTable 多场景基准 — 实测分析与结论

> 本文档基于新落地的多场景基准框架（`bench/multi_bench_config.sh` +
> `scripts/run_multi_bench.sh`）的一次真实运行，是运行时报告
> `docs/BENCH_MULTI.md`（会被 runner 覆盖）的配套结论文档，手动维护。

## 本次实测配置

- **数据集**：`bench_mb`，200,000 行 × 128 列（index 20 + alpha 81 + fs 27），
  因子 90% NULL，4 个日分区，aligned 布局（3 个独立 Column Group）
- **引擎**（6 个，跨引擎行数一致性校验通过）：
  - D-WIDE / D-JOIN（DuckDB 基线）
  - A-ALIGNED / A-NORMAL（插件，aligned=true / false）
  - P-CONCAT / P-JOIN（polars，`hstack` 位置对齐 / rowid hash join）
- **查询**：Q2(~35 列)、Q5(全列)；过滤 F1(无)、F2(日期分区点)；S0(100%)
- **线程**：1 / 8 · **测量**：fresh-process warm（第二遍）

## 结果（warm 秒，200K×128 全缓存微数据）

| 引擎 | Q2/F2, t=1 | Q2/F2, t=8 | Q5/F1, t=1 | Q5/F1, t=8 |
|------|-----------|-----------|-----------|-----------|
| D-WIDE     | 0.023 | 0.025 | 0.024 | 0.025 |
| **A-ALIGNED** | **0.027** | **0.030** | **0.031** | **0.030** |
| A-NORMAL   | 0.025 | 0.026 | 0.028 | 0.028 |
| D-JOIN     | 0.042 | 0.044 | 0.057 | 0.045 |
| P-CONCAT   | 0.043 | 0.047 | 0.092 | 0.172 |
| P-JOIN     | 0.125 | 0.067 | 0.270 | 0.210 |

## 关键结论

1. **A-ALIGNED 显著优于 polars 装配路径**：Q5/F1 下 0.031s，
   比 P-CONCAT(0.092s) 快 ~3×，比 P-JOIN(0.270s) 快 **~8×**。这正是框架证明的点——
   aligned 消灭了内存 `hstack` 与 hash-JOIN 的组装/重排成本。

2. **A-ALIGNED 优于 D-JOIN**：Q5/F1 0.031 vs 0.057，约 **1.8×**。多 Parquet 的
   JOIN（`ON rowid=rowid_alpha`）成本高于 aligned 的 position 直接组装。

3. **A-NORMAL ≈ A-ALIGNED（此规模）**：两者时间几乎相同。因为 200K×128 小数据、
   全缓存下，`aligned=true`（交叉剪枝+position 组装）与 `aligned=false`
   （union 区间+各 leaf 独立读）的差异被固定开销掩盖。**加速比要在大/宽/多列组
   数据上才显现**——这正是 Tier B(R3×W3 100M×10K)、Tier C(R4 1B) 要覆盖的区间。

4. **D-WIDE 当前最快**：单文件顺序扫描零额外开销，符合预期。它没有"多 Group
   组装"成本，所以在小数据+少列时占优；aligned 的优势在**冷缓存/大宽表/多列组**
   下才体现（避免扫无关列、多 reader 并行加载不同 Group）。

5. **并行趋势混乱（小数据）**：t=8 有时反而更慢（如 P-CONCAT Q5 0.17s），因为
   数据太小、I/O 可忽略，线程同步/调度开销占比高。并行收益同样要在大数据上验证。

## 下一步（按优先级）

- **Tier B 最小可行子集**：`--rows 10M --width 1024 --threads 1,4,8`，跑 Q1/Q2/Q3，
  观察宽度提升后 D-WIDE vs A-ALIGNED 的相对位移（aligned 应随列组数增多而优势扩大）。
- **宽度扩展**：把 W2(1024)/W3(10240) 真正生成一次，重点看 P-CONCAT `hstack` 与
  aligned position 组装在列数上的开销差。
- **冷缓存**：大数据集上用 `sync; echo 3 > /proc/sys/vm/drop_caches`（需 root）区分
  COLD，验证"aligned 多 reader 并行加载不同列组"在冷态下的延迟优势。
- **稀疏度专项**：R3×W3×Q2×F2×S1 上跑 DENSE/SPARSE-90/SPARSE-99，验证存储/扫描随
  NULL 率上升的差异。

## 复现

```bash
bash scripts/gen_multi_bench.sh --rows 200000 --width 128 --sparsity 90 --aligned true --out testdata --tag mb
bash scripts/run_multi_bench.sh --tier A --rows 200000 --width 128 --sparsity 90 \
     --threads 1,8 --engines D-WIDE,D-JOIN,A-ALIGNED,A-NORMAL,P-CONCAT,P-JOIN --no-regen
```

---

## 追加：1,000,000 × 1024（W2）实测（同进程均值）

> 修了一个**测量方法问题**后重测。此前用 fresh-process best-of 时，
> **A-ALIGNED 曾显得比 A-NORMAL 慢 ~13%** —— 排查（同进程 7 轮交替 +
> 同进程 `EXPLAIN`）证明那是 **进程启动固定开销（~0.02s）+ 顺序噪声**，
> 不是引擎行为。于是把 `run_one` 改为**同一 duckdb 进程内把同一查询跑
> `REPEATS`(=5) 次、取均值**，把每次进程启动分摊掉，`warm` 才是真实查询时间。

### 结果（warm = 同进程 5 次均值，秒；1M×1024，90% 稀疏因子）

| 引擎 | Q2/F1 t1 | Q2/F2 t1 | Q5/F1 t1 | Q5/F2 t1 |
|------|---------|---------|---------|---------|
| D-WIDE     | 0.009 | 0.014 | 0.025 | 0.024 |
| D-JOIN     | 0.131 | 0.050 | 0.151 | 0.065 |
| **A-ALIGNED** | **0.029** | **0.018** | **0.030** | **0.023** |
| A-NORMAL   | 0.027 | 0.019 | 0.030 | 0.024 |

### 追加结论

1. **A-ALIGNED ≈ A-NORMAL（确认，非慢）**：两种模式 warm 几乎相同（0.018-0.030）。
   设计上 aligned=true（交叉剪枝+position 组装）与 aligned=false（union 区间+独立读）
   在本数据集（各 leaf 剪枝区间一致）上行为一致，符合预期；之前看到的差异是测量噪声。

2. **1M×1024 下 A-ALIGNED 比 D-JOIN 快 5.1×**（Q5/F1 t1: 0.030 vs 0.151）——
   宽度提升后 JOIN（`ON rowid=rowid_alpha`）成本随列数上升，aligned 的零-JOIN
   position 直接组装优势明显放大（200K×128 时仅 1.8×）。

3. **A-ALIGNED 相对 D-WIDE 仍略慢（Q5/F1 0.83×）**：D-WIDE 单文件顺序扫描、无多
   Group 组装开销。aligned 反超 D-WIDE 需要**更多列组/更大数据/冷缓存**（→ W3 10K
   列、Tier B R3、COLD），此时多 reader 并行加载不同列组 + 免扫无关列才占优。

4. **测量经验（必须记住）**：小数据全缓存下，fresh-process 单跑会把 ~0.02s
   进程启动当成查询时间，导致引擎间差几个百分点都是噪声。**应同一进程内重复 N 次
   取均值**（本框架 `REPEATS=5`），并在比较 AEg 结论前用同进程交替跑 + `EXPLAIN`
   复核。跨引擎行数与分区剪枝一致性校验也必须先过（本框架 `CONSISTENCY OK`）。

### 下一步（更新）

- **W3(10K 列) 实测**：看 A-ALIGNED 相对 D-WIDE/D-JOIN 随列组数进一步的相对位移
  （预期 aligned 优势扩大）。用 `--width 10240`，但先在 `--rows` 较小（如 1M）下验证
  生成与查询可行性（10K 列生成量大）。
- **冷缓存**：大数据集 root 下 `sync; echo 3 >/proc/sys/vm/drop_caches` 验证 COLD。
- **稀疏度**：DENSE/SPARSE-90/SPARSE-99 对比存储与扫描差异。

