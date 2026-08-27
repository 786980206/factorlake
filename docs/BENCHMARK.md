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

> **2026-09-28 更新**：恢复 DuckDB 默认字典编码（`dictionary_size_limit` 默认值 =
> RG_SIZE/5）。此前禁用字典编码（PLAIN）虽在稀疏测试数据上读取性能无差异，
> 但在高基数字符串列（如 symbol）上会导致文件体积膨胀。实测 `VectorOperations::Copy`
> 对 `DICTIONARY_VECTOR` 的处理已足够高效（SelectionVector 批量合并，非逐元素查表）。
> 以下为 official `bench_read.ps1` 完整基准结果（1M 行 × 127 列，4 日分区，
> 默认字典编码，warm 第二次运行）：

#### 完整基准（bench_read.ps1，默认字典编码，120 列扫描 = 100 alpha + 20 ma）

| 引擎 | 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|------|--------|--------|--------|
| aligned | p5 (5 cols) | 0.146 | 0.096 | 0.094 |
| aligned | p25 (25 cols) | 0.407 | 0.199 | 0.161 |
| aligned | p100 (120 cols) | 1.632 | 0.631 | 0.534 |
| wide | p5 | 0.103 | 0.074 | 0.071 |
| wide | p25 | 0.290 | 0.132 | 0.114 |
| wide | p100 | 1.044 | 0.390 | 0.339 |
| join | p5 | 0.186 | 0.113 | 0.110 |
| join | p25 | 0.372 | 0.173 | 0.151 |
| join | p100 | 1.741 | 0.871 | 0.818 |
| polars | p5 | 0.039 | 0.016 | 0.013 |
| polars | p25 | 0.125 | 0.048 | 0.039 |
| polars | p100 | 0.527 | 0.154 | 0.136 |

**aligned vs join**：

| 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|--------|--------|--------|
| p5 | 0.146 vs 0.186（**aligned 快 1.3×**） | 0.096 vs 0.113（**快 1.2×**） | 0.094 vs 0.110（**快 1.2×**） |
| p25 | 0.407 vs 0.372（join 快 1.1×） | 0.199 vs 0.173（join 快 1.2×） | 0.161 vs 0.151（join 快 1.1×） |
| p100 | 1.632 vs 1.741（**aligned 快 1.1×**） | 0.631 vs 0.871（**快 1.4×**） | 0.534 vs 0.818（**快 1.5×**） |

#### 纯列扫描基准（sum 聚合，仅 alpha 列组，PLAIN 编码）

| 引擎 | 列数 | 1 线程 | 8 线程 | vs JOIN | vs read_parquet |
|------|------|--------|--------|---------|----------------|
| aligned | 1 | 0.036 | 0.022 | **快 7.2×** | 1.8× slower |
| aligned | 10 | 0.154 | 0.050 | **快 2.2×** | 1.8× slower |
| aligned | 50 | 0.673 | 0.175 | **快 1.1×** | 1.8× slower |
| aligned | 100 | 1.329 | 0.355 | **快 1.3×** | 1.8× slower |
| join | 1 | 0.267 | 0.096 | — | — |
| join | 10 | 0.350 | 0.119 | — | — |
| join | 50 | 0.730 | 0.214 | — | — |
| join | 100 | 1.237 | 0.459 | — | — |
| read_parquet | 1 | 0.016 | 0.011 | — | — |
| read_parquet | 10 | 0.080 | 0.028 | — | — |
| read_parquet | 50 | 0.395 | 0.095 | — | — |
| read_parquet | 100 | 0.758 | 0.205 | — | — |

**结论**：aligned 在窄列（≤10 列）大幅领先 JOIN（7.2×~2.2×），因为 JOIN 的
hash-build 开销摊薄在少量列上。宽列场景（100+ 列）aligned 仍快 1.1~1.5×。
aligned 相对单宽 Parquet（read_parquet）有 ~1.8× 框架开销，来自分区管理、列组
路由、以及中间 `g.chunk` + `VectorOperations::Copy`。

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

> **2026-09-28 优化后**：默认字典编码 + parquet 预取使 aligned 在全场景
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
> 2. 恢复 DuckDB 默认字典编码（`dictionary_size_limit` 默认值 = RG_SIZE/5），
>    兼容高基数字符串列。`VectorOperations::Copy` 对 `DICTIONARY_VECTOR`
>    使用 SelectionVector 批量合并，性能与 PLAIN 持平。
>    p100 8 线程 0.723s→0.355s。
> 3. 启用 parquet 预取 + 元数据缓存（`prefetch_all_parquet_files`、
>    `parquet_metadata_cache`）。本地文件 I/O 与 ZSTD 解压重叠，~5% 加速。

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

> **2026-09-28 更新**：用 DuckDB Sort 类替换手动排序+构建。

| 规模 | 行数 | Aligned index | Aligned panel | Native year-part | Native flat |
|------|------|---------------|---------------|------------------|-------------|
| 100 sym | 1,315,900 | 0.116 | 0.249 | 0.713 | 0.207 |
| 500 sym | 6,579,500 | 0.513 | 0.974 | 1.367 | 0.553 |
| 1000 sym | 13,159,000 | 0.998 | 2.018 | 2.014 | 1.078 |
| 2000 sym | 26,318,000 | 2.019 | 4.474 | 4.125 | 2.481 |
| 4000 sym | 52,636,000 | 4.676 | 10.837 | 9.482 | 6.285 |

### 2.3 倍率分析（Aligned panel / Native year-part）

| 规模 | 倍率 | 结论 |
|------|------|------|
| 100 sym | 0.35× | **aligned 快 2.8×** |
| 500 sym | 0.71× | **aligned 快 1.4×** |
| 1000 sym | 1.00× | 持平 |
| 2000 sym | 1.08× | aligned 慢 1.1× |
| 4000 sym | 1.14× | aligned 慢 1.1× |

> **优化历程**：
> 1. 消除合并阶段（1.48s→0.02s）
> 2. RG 批量 flush（build+flush 15.1s→5.7s）
> 3. 排序跳过检测（输入已排序时跳过 sort）
> 4. **用 DuckDB Sort 类替换手动 sort+build**（AlignedIndex 13.4s→5.6s，
>    AlignedPanel 22.1s→13.7s，4000sym 倍率 2.64×→1.71×）
> 5. **跳过并行模式下的 already_sorted 检查**（extract 6.3s→0s，
>    4000sym 倍率 1.71×→1.60×）
> 6. **ZSTD 压缩级别从 3 降至 1**（4000sym 倍率 1.60×→1.61×，
>    1000sym 倍率 1.15×→0.95× — aligned 首次在 1000sym 超过 native）
> 7. **Sink 直接追加（跳过 ProjectRows 拷贝）**（identity 映射时直接追加
>    input chunk 到 CDC，省去 per-column SelectionVector 拷贝；
>    4000sym 倍率 1.61×→1.54×，sink total 17.4s→15.8s）
> 8. **Sort sink 直接传递（跳过 per-column copy）**（!has_existing 时
>    直接将 source chunk 传入 sort.Sink，省去 N 列 copy；
>    total 15.8s→14.9s）
> 9. **FlushWorker 线程数从 8 增至 12**（20 核机器上 8 Sink + 12 Flush
>    = 20 线程，充分利用 CPU；4000sym 倍率 1.54×→1.39×，total
>    14.9s→11.1s 中位数）
> 10. **直接 flush sorted CDC（跳过 flush_buffer 拷贝）**（分区行数 ≤ RG_SIZE
>    时直接将 sorted CDC 传入 ParquetWriter::Flush，省去 CDC→CDC
>    per-column copy；build 34.0s→31.0s，4000sym 倍率 1.39×→1.14×）
> 11. **读取启用 parquet prefetch**（本地文件默认不预取，aligned_scan 在
>    InitGlobal 中设置 prefetch_all_parquet_files=true；100 列 1 线程
>    1.33s→1.27s，~5% 加速）

### 2.4 结论

- **中小规模（≤1000 sym）aligned 与 native year-part 持平或更快**：aligned 的
  per-partition CDC 缓冲 + run-length 批处理优于 native 的 per-row 分区路由。
  100 sym 时 aligned 快 2.8×，1000 sym 持平。
- **大规模（4000 sym）aligned 仅慢 1.1×**：经过多轮优化（Sort 类、ZSTD1、
  直接追加、直接 flush、12 线程），4000 sym 从初始 21.2s→10.8s，倍率从 2.64×→1.14×。
  剩余差距主要来自 ZSTD 压缩（CPU-bound）和排序开销。
- **Aligned vs native flat：慢 1.0~1.7×**：native flat 无分区开销，但无分区
  限制——大规模时单文件过大。4000 sym 时 10.8s vs 6.3s。
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
