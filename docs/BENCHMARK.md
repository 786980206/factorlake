# AlignedTable 基准测试

> 数据：100 万行 × 127 列（index 5 + alpha101 100 + ma 20），4 个日分区，因子稀疏（非空 1/7）。
> 3 个独立 Parquet 列组（aligned 布局）。所有时间为 warm（第二次运行，OS 页缓存命中）。

---

## 1. 读取基准

### 1.1 测试矩阵

| 编号 | 说明 |
|------|------|
| p5   | 投影 5 个 alpha 列，全扫描 |
| p25  | 投影 25 个 alpha 列，全扫描 |
| p100 | 投影 120 列（100 alpha + 20 ma），全扫描 |
| s25  | 投影 25 列，WHERE date = '2026-09-01'（25% 扫描，分区剪枝） |
| s100 | 投影 25 列，全扫描 |

### 1.2 引擎说明

| 引擎 | 说明 |
|------|------|
| **aligned** | `aligned_scan('bench_ixday')`：3 组直接组装进同一 DataChunk，无 JOIN，投影下推，分区剪枝，并行范围扫描 |
| **wide** | 单宽 Parquet（127 列），DuckDB `read_parquet` |
| **join** | 3 个独立 Parquet 文件按 rowid JOIN（键布局） |
| **polars** | 同 3 文件分别读取后水平 concat（position-aligned） |

### 1.3 测试结果（秒，warm）

> **2026-09-28 更新**：禁用 ParquetWriter 字典编码（`dictionary_size_limit=0`）+
> memcpy 快速路径消除 `VectorOperations::Copy` 逐元素循环。1M 行 100 列读取
> 1 线程从 1.811s 降至 0.816s（2.2× 加速），8 线程从 0.723s 降至 0.189s（3.8× 加速）。

#### 新基准（PLAIN 编码 + memcpy 快速路径）

| 引擎 | 负载 | 1 线程 | 8 线程 |
|------|------|--------|--------|
| aligned | 1 col | 0.027 | 0.012 |
| aligned | 10 cols | 0.085 | 0.031 |
| aligned | 50 cols | 0.391 | 0.101 |
| aligned | 100 cols | 0.816 | 0.189 |
| join | 1 col | 0.099 | 0.046 |
| join | 10 cols | 0.185 | 0.064 |
| join | 50 cols | 0.571 | 0.157 |
| join | 100 cols | 1.050 | 0.336 |
| read_parquet | 103 cols | 0.740 | — |

**aligned vs join 全场景胜出**：

| 负载 | 1 线程 | 8 线程 |
|------|--------|--------|
| 1 col | 0.027 vs 0.099（**aligned 快 3.7×**） | 0.012 vs 0.046（**aligned 快 3.8×**） |
| 10 cols | 0.085 vs 0.185（**aligned 快 2.2×**） | 0.031 vs 0.064（**aligned 快 2.1×**） |
| 50 cols | 0.391 vs 0.571（**aligned 快 1.5×**） | 0.101 vs 0.157（**aligned 快 1.6×**） |
| 100 cols | 0.816 vs 1.050（**aligned 快 1.3×**） | 0.189 vs 0.336（**aligned 快 1.8×**） |

#### 旧基准（字典编码，已存档）

| 引擎 | 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|------|--------|--------|--------|
| aligned | p5 | 0.268 | 0.146 | 0.136 |
| aligned | p25 | 0.555 | 0.359 | 0.268 |
| aligned | p100 | 2.137 | 0.950 | 0.723 |
| aligned | s25 | 0.162 | 0.117 | 0.118 |
| aligned | s100 | 0.547 | 0.230 | 0.181 |
| wide | p5 | 0.104 | 0.073 | 0.069 |
| wide | p25 | 0.296 | 0.170 | 0.137 |
| wide | p100 | 1.173 | 0.523 | 0.482 |
| wide | s25 | 0.173 | 0.119 | 0.115 |
| wide | s100 | 0.361 | 0.183 | 0.148 |
| join | p5 | 0.203 | 0.122 | 0.119 |
| join | p25 | 0.442 | 0.176 | 0.209 |
| join | p100 | 2.276 | 1.236 | 0.858 |
| join | s25 | 0.172 | 0.120 | 0.145 |
| join | s100 | 0.379 | 0.176 | 0.158 |
| polars | p5 | 0.039 | 0.018 | 0.014 |
| polars | p25 | 0.124 | 0.044 | 0.044 |
| polars | p100 | 0.524 | 0.195 | 0.151 |
| polars | s25 | 0.125 | 0.056 | 0.038 |
| polars | s100 | 0.140 | 0.043 | 0.039 |

### 1.4 结论

**aligned vs join（核心对比）**：

> **2026-09-28 优化后**：禁用字典编码 + memcpy 快速路径使 aligned 在全场景
> 胜出 join，从 1 列（3.7×）到 100 列（1.3×）。之前的窄投影劣势已消除。

#### 新基准结论

- **全场景 aligned 胜出 join**：从 1 列（3.7×）到 100 列（1.3×）。
  position 组装（无 hash 表、无 key 比较）+ memcpy 快速路径（消除逐元素
  `sel.get_index()` 循环）使 aligned 在任何投影宽度都更快。
- **接近 read_parquet**：100 列 1 线程 0.816s vs read_parquet 0.740s——
  仅有 10% 的 aligned scan 框架开销（分区管理 + 列组路由）。
- **8 线程 100 列 0.189s**：从旧的 0.723s 提升 3.8×。

#### 旧基准结论（字典编码，已存档）

**aligned vs join（核心对比）**：

| 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|--------|--------|--------|
| p5 | 0.268 vs 0.203（aligned 慢 1.32×） | 0.146 vs 0.122（aligned 慢 1.20×） | 0.136 vs 0.119（aligned 慢 1.14×） |
| p25 | 0.555 vs 0.442（aligned 慢 1.26×） | 0.359 vs 0.176（aligned 慢 2.04×） | 0.268 vs 0.209（aligned 慢 1.28×） |
| p100 | 2.137 vs 2.276（**aligned 快 1.07×**） | 0.950 vs 1.236（**aligned 快 1.30×**） | 0.723 vs 0.858（**aligned 快 1.19×**） |
| s25 | 0.162 vs 0.172（aligned 快 1.06×） | 0.117 vs 0.120（持平） | 0.118 vs 0.145（aligned 快 1.23×） |

- **宽投影全场景 aligned 胜出 join**：p100（120 列全扫描）1/4/8 线程均 faster
  than join。position 组装（无 hash 表、无 key 比较）在大投影场景优势明显——
  hash-JOIN 构建大哈希表成本随行数超线性增长。
- **窄投影（p5/p25）aligned 仍慢于 join**：aligned 有 per-column
  VectorOperations::Copy 开销（parquet DICTIONARY → FLAT 展平），窄投影时
  组装开销占比高。p5（5 列）约慢 1.14~1.32×。这是后续优化的方向。
- **分区剪枝（s25）aligned 胜出**：3 组独立分区剪枝跳过 4 个分区中的 3 个。

**wide（单宽 Parquet）在窄表小数据最快**：单文件读取无组装开销，1M 行 × 127 列
占优。aligned 的优势在超宽表（10K+ 列）显现——单宽 Parquet 难以管理，
而多组拆分只读被请求列。

**polars 在小投影最快**：p5 warm 8 线程仅 0.014s——但这是单进程全内存专用引擎。

**并行扩展性**：

| 引擎 | p100 1→4 线程 | p100 4→8 线程 |
|------|---------------|---------------|
| aligned | 2.25×（好） | 1.31×（仍有收益） |
| join | 1.84× | 1.44× |
| wide | 2.24× | 1.08× |

> **性能优化历程**：
> 1. claim range 从 16×2048=32768 行增大到 64×2048=131072 行（= 一个 Row Group），
>    减少高并发 cursor lock 竞争。p100 8 线程 1.191s→0.723s。
> 2. 禁用 ParquetWriter 字典编码（`dictionary_size_limit=0`）+ memcpy 快速路径
>    消除 `VectorOperations::Copy` 逐元素循环。p100 8 线程 0.723s→0.189s（3.8× 加速）。

---

## 2. 写入基准（COPY TO FORMAT aligned vs 原生 PARQUET）

### 2.1 测试说明

递增规模（100/500/1000/2000/4000 标的 × 13159 天，最大 52.6M 行），7 列，
按年分区，ZSTD 压缩，8 线程。

| 引擎 | 说明 |
|------|------|
| Aligned index | COPY TO ... GROUP 'index'（2 列：symbol, date） |
| Aligned panel | COPY TO ... GROUP 'panel/ma'（7 列：symbol, date, o, h, l, c, v） |
| Native year-part | DuckDB COPY TO PARQUET PARTITION_BY(year)，7 列 |
| Native flat | DuckDB COPY TO PARQUET 无分区，7 列 |

### 2.2 测试结果

| 规模 | 行数 | Aligned index | Aligned panel | Native year-part | Native flat |
|------|------|---------------|---------------|------------------|-------------|
| 100 sym | 1,315,900 | 0.133s | 0.264s | 0.731s | 0.233s |
| 500 sym | 6,579,500 | 0.571s | 1.036s | 1.449s | 0.558s |
| 1000 sym | 13,159,000 | 1.426s | 2.713s | 2.519s | 1.554s |
| 2000 sym | 26,318,000 | 3.691s | 5.846s | 4.569s | 2.926s |
| 4000 sym | 52,636,000 | 12.102s | 21.199s | 8.389s | 5.163s |

### 2.3 倍率分析（Aligned panel / Native year-part）

| 规模 | 倍率 | 结论 |
|------|------|------|
| 100 sym | 0.36× | **aligned 快 2.8×** |
| 500 sym | 0.71× | **aligned 快 1.4×** |
| 1000 sym | 1.08× | 持平 |
| 2000 sym | 1.28× | aligned 慢 1.3× |
| 4000 sym | 2.53× | aligned 慢 2.5× |

### 2.4 结论

- **中小规模（≤1000 sym）aligned 与 native year-part 持平或更快**：aligned 的
  per-partition CDC 缓冲 + run-length 批处理优于 native 的 per-row 分区路由。
  100 sym 时 aligned 快 2.8×。
- **大规模（2000+ sym）aligned 慢于 native**：超线性增长来自 SortAndFlushPartition
  的 O(n log n) 排序 + 多次 O(n) 数据拷贝。4000 sym 时 aligned panel 21.2s vs
  native 8.4s。这是后续优化的方向——可考虑：(1) 检测输入已排序时跳过排序，
  (2) 用更高效的列式排序替代 std::stable_sort + per-element VectorOperations::Copy。
- **Aligned vs native flat：慢 1.0~4.1×**：native flat 无分区开销。
  4000 sym 时 21.2s vs 5.2s。差距主要来自排序 + 分区管理。
- **写入优化已实施**：消除 merge 阶段（1.48s→0.02s），RG 批量 flush 替代
  全量中间 CDC（build+flush 15.1s→5.7s）。
- **宽表（100+ 列）：aligned 优势放大**——列裁剪 + 投影下推在读取路径
  节省远超分区管理成本。7 列基准是 aligned 最差场景（窄表，分区开销占比高）。

---

## 3. 测试脚本

| 脚本 | 说明 |
|------|------|
| `test/gen_bench.ps1` | 生成 bench_ixday 测试数据（1M 行 × 127 列 × 4 日分区） |
| `test/bench_read.ps1` | 读取基准：aligned vs wide vs join vs polars（5 负载 × 3 线程） |
| `test/bench_copy_to.ps1` | 写入基准：COPY TO FORMAT aligned vs native PARQUET（5 规模） |
| `test/bench_polars.py` | polars 引擎辅助脚本（bench_read.ps1 调用） |

### 复现

```powershell
# 1. 生成测试数据（仅首次需要）
powershell -ExecutionPolicy Bypass -File test\gen_bench.ps1

# 2. 读取基准
powershell -ExecutionPolicy Bypass -File test\bench_read.ps1

# 3. 写入基准
powershell -ExecutionPolicy Bypass -File test\bench_copy_to.ps1
```

### 依赖

- `duckdb/build/duckdb_al3.exe`（含 aligned 扩展的调试构建）
- Python + polars（读取基准的 polars 引擎对比，缺失时自动跳过）
- Windows / PowerShell 5.1+

### 测量方法

- **读取**：同一查询在同一进程内跑两次（第一次 warm OS 页缓存，第二次计时）。
  小数据全缓存下 fresh-process 单跑会把 ~0.02s 进程启动当查询时间。
- **写入**：fresh-process Stopwatch，每次跑前清理目标目录。
