# AlignedTable 多场景基准 — 权威实测结论

> 本文档是 **AlignedTable 多场景基准的权威结论文档**（长期维护）。
> 它汇总 `bench/out/summary` 里每一次真实运行的结果与结论，以及如何用
> `scripts/bench_scenarios.sh` 一键复现。
>
> 相关文档：
> - `AGENTS.md` — 项目权威记忆（环境/构建/当前进度）
> - `docs/STORAGE_CONTRACT.md` — 存储格式契约
> - `bench/out/SUMMARY.csv` + `bench/out/g-*.csv` — 各阶段原始结果（由
>   `bench_scenarios.sh` 生成，gitignored）。

---

## 1. 评测矩阵（Group Settings）

见 `scripts/multi_bench_config.sh`（唯一事实来源），六大类：

| 类 | 内容 |
|----|------|
| 引擎组 | `D-WIDE`(DuckDB 单宽表) `D-JOIN`(DuckDB key JOIN) `P-CONCAT`(polars hstack) `P-JOIN`(polars hash join) `A-ALIGNED`(插件，全对齐 position 组装) |
| 规模 | R1..R4（1M/10M/100M/1B 行）× W1..W3（128/1024/10240 列） |
| 稀疏 | DENSE(0%) / SPARSE-90 / SPARSE-99 |
| 查询 | Q1(~3) Q2(~35) Q3(~500) Q4(~5000) Q5(ALL) |
| 过滤 | F1(无) F2(分区点) F3(分区范围) F4(非分区统计) F5(key 等值) |
| 选择率 | S0(100%) S1(10%) S2(1%) S3(0.1%) |
| 环境 | COLD/WARM、线程 1..64、文件数、物理分区 P1..P4 |

分层执行：**Tier A**(smoke)/**Tier B**(main, R3×W3)/**Tier C**(stress, R4) + 专项
（稀疏 / 线程 / 文件数）。

### 已实测阶段（`bench/out/g-*.csv`）

| stage | 规模 | 引擎 | 查询/过滤/选择率 | 线程 | 结果文件 |
|-------|------|------|------|------|------|
| g-250k | 250K×128 | 6 | Q2,Q5 × F1,F2 × S0 | 1,4 | g-250k.csv |
| g-1m | 1M×128 | 6 | Q2 × F2 × S0 | 1 | g-1m.csv |
| g-1m-q | 1M×128 | 4 | Q1..Q5 × F1..F5 × S0,S1 | 1,4 | g-1m-q.csv |
| g-10m | 10M×128 | 6 | Q2 × F1,F2 × S0 | 1 | g-10m.csv |
| g-10m-thread | 10M×128 | 2 | Q2 × F2 × S0 | 1,2,4,8 | g-10m-thread.csv |
| g-500k-w2 | 500K×1024 | 4 | Q3,Q5 × F1,F2 × S0 | 1 | g-500k-w2.csv |
| g-sparse-* | 1M×128 | 4 | Q2 × F2 × S1 | 1,4 | g-sparse-dense/90/99.csv |
| g-thread | 1M×128 | 2 | Q2 × F1 × S0 | 1,2,4,8 | g-thread.csv |

---

## 2. 核心结论

### 2.1 A-ALIGNED 的零-JOIN position 组装优势随数据量急剧放大

同为一个逻辑宽表，拆成多个 Column Group 后：

| 引擎 | 1M×128 Q2/F1 | 10M×128 Q2/F1 | 相对 A-ALIGNED(10M) |
|------|------|------|------|
| **A-ALIGNED** | **0.026s** | **0.109s** | 基准 |
| D-JOIN | 0.075s | 4.32s | **~40×** |
| P-CONCAT | 0.195s | 2.60s | **~24×** |
| P-JOIN | 1.02s | 16.5s | **~152×** |
| D-WIDE | 0.013s | 0.016s | 快 6.6× |

**结论**：D-JOIN 相对 A-ALIGNED 从 1M 的 ~5× 放大到 10M 的 ~40×，P-JOIN 到
~150×。因为 position 组装接近线性，而 hash-JOIN（构建大哈希表）/polars hstack
（内存重排）成本随行数**超线性**增长。这直接证明"利用 row-aligned 先验消灭
JOIN/横向 concat"的价值随数据量上升。

### 2.2 D-WIDE 在窄表小数据仍最快，差距随宽度收窄

| 场景 | D-WIDE | A-ALIGNED | 倍率 |
|------|------|------|------|
| 1M×128 Q2(35 列投影) | 0.013 | 0.026 | D-WIDE 快 2× |
| 10M×128 Q2 | 0.016 | 0.109 | D-WIDE 快 6.6× |
| 500K×**1024** Q5(全列) | 0.019 | 0.027 | **收窄到 1.4×** |

单文件顺序扫描在窄表全缓存下仍最优；但列数/列组数上升（W2/W3）时，aligned 的
"只读被请求列 + 多 reader 并行加载不同列组"优势开始收窄与 D-WIDE 的差距。
预期 W3(10K 列) + 冷缓存下 aligned 将与 D-WIDE 打平或反超。

### 2.4 并行收益在小数据/窄投影下有限（符合预期）

- 1M×128 Q2/F1：A-ALIGNED t1→t8 约 1.5×
- 10M×128 Q2/F2：A-ALIGNED t1 0.037 → t8 0.023（~1.6×）
- 扫描本身很快，并行调度/启动开销占比高；更大数据 / 更宽投影才会体现并行收益。

### 2.5 稀疏度（DENSE/90/99）对投影类查询影响小

Q2/F2/S1 三档下各引擎差异 <10%（都在 0.005-0.011s）。投影只扫被请求列、数据全缓存，
稀疏列当 NULL 跳过 vs 读取的差异被掩盖；稀疏度优势需在**存储体积 / 冷 I/O** 上验证。

---

## 3. 测量方法与经验（必须遵守）

1. **同一 duckdb 进程内把同一查询跑 `REPEATS`(默认 5) 次取均值**——小数据全缓存下
   fresh-process 单跑会把 ~0.02s 进程启动当成查询时间，引擎间几个百分点都是噪声。
   比较结论前用同进程交替跑 + `EXPLAIN` 复核。
2. **每条结论必须过跨引擎一致性校验**：`run_multi_bench.sh` 的 `SELF-CHECK`
   （分区剪枝正确性）+ `CONSISTENCY`（所有引擎对同一查询返回相同行数）先通过，
   计时才有意义。
3. **超宽 SQL（W3 上万列）会超 OS ARG_MAX**：所有 DuckDB 调用走 stdin 文件
   （`gen_multi_bench.sh run_duck`、`run_multi_bench.sh ddb_run`），不能 `-c`。
4. **测量 warm 环境变量 `REPEATS=N` 是 env 前缀，不是位置参数**。
5. 全程监控内存/磁盘（`bench_scenarios.sh` 每阶段打印 before/after available）。

---

## 4. 一键复现

```bash
# ① 生成指定规模数据（可选，脚本也会自动生成）
bash scripts/gen_multi_bench.sh --rows 1000000 --width 128 --sparsity 90 \
     --aligned true --out testdata --tag mb

# ② 一键跑全部阶段（逐级递增 + 资源监控），或只跑一个 stage
bash scripts/bench_scenarios.sh                 # g-250k→g-1m→g-1m-q→g-10m→g-thread...  
bash scripts/bench_scenarios.sh --only g-10m     # 只复现 10M 阶段
bash scripts/bench_scenarios.sh --only g-1m --skip-regen   # 数据已匹配则跳过重生成

# ③ 低层 runner（单引擎组合 + 环境变量覆盖查询/过滤/选择率 + REPEATS）
REPEATS=5 QS_OVERRIDE=Q2 FS_OVERRIDE=F2 SS_OVERRIDE=S0 \
  bash scripts/run_multi_bench.sh --tier A --rows 1000000 --width 128 \
       --sparsity 90 --threads 1 --engines D-WIDE,D-JOIN,A-ALIGNED \
       --no-regen

# 输出都在 bench/out/（g-*.csv + SUMMARY.csv）
```

### 环境依赖
- `duckdb/build/duckdb`（aligned 扩展，静态链接；构建见 `AGENTS.md §16.2`）
- polars 引擎（P-CONCAT / P-JOIN）：`uv venv --python 3.13 .venv-bench` +
  `uv pip install --python .venv-bench/bin/python polars`；缺失时脚本自动跳过 P-*。

---

## 5. 已发现并修复的问题（测试驱动的）

1. **分区剪枝游标 Bug（非首分区崩溃）**：`next_row` 初值 0，剪枝后首个区间起点 >0
   时 `cursor - part.start_row` 无符号下溢 → 崩溃。已修（claim 时钳到区间起点）。
2. **DuckDB 基线日期切分 `(r/N)::INT` 会四舍五入**：把分区分到 N/2 而非 N，导致
   wide/join 与 aligned 的 4 分区错位。改用 floor 除法 `//` 修复（牵连 3 个脚本）。
3. **数据重生成后 `_table.json` 不匹配**：已改为每次重建。
4. **`--skip-regen` 不校验数据规模/sparsity**：会拿 250K 数据跑 1M 声明。现用
   `.gen-meta` 标记（rows,width,sparsity）校验，不符自动重生成。
5. **测量方法**：fresh-process 启动抖动污染小数据测量 → 同进程 REPEATS 均值。
6. **超宽 SQL ARG_MAX**：stdin 文件输入解决。
7. **`$TMPDIR` 未设置秒崩**：`ddb_run` 里 `${TMPDIR:-/tmp}` 修复。

---

_（2026-08-18 起持续维护；每次跑完 `bench_scenarios.sh` 后把新结论回填本节。）_
