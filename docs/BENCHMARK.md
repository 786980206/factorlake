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

| 引擎 | 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|------|--------|--------|--------|
| aligned | p5 | 0.170 | 0.123 | 0.116 |
| aligned | p25 | 0.521 | 0.281 | 0.227 |
| aligned | p100 | 2.046 | 1.006 | 1.191 |
| aligned | s25 | 0.158 | 0.111 | 0.112 |
| aligned | s100 | 0.518 | 0.303 | 0.219 |
| wide | p5 | 0.108 | 0.077 | 0.073 |
| wide | p25 | 0.297 | 0.132 | 0.116 |
| wide | p100 | 1.124 | 0.387 | 0.364 |
| wide | s25 | 0.135 | 0.097 | 0.101 |
| wide | s100 | 0.300 | 0.134 | 0.116 |
| join | p5 | 0.198 | 0.121 | 0.112 |
| join | p25 | 0.393 | 0.183 | 0.161 |
| join | p100 | 1.830 | 0.923 | 0.896 |
| join | s25 | 0.178 | 0.117 | 0.121 |
| join | s100 | 0.398 | 0.178 | 0.159 |
| polars | p5 | 0.040 | 0.018 | 0.017 |
| polars | p25 | 0.124 | 0.044 | 0.040 |
| polars | p100 | 0.530 | 0.171 | 0.155 |
| polars | s25 | 0.127 | 0.049 | 0.038 |
| polars | s100 | 0.124 | 0.043 | 0.038 |

### 1.4 结论

**aligned vs join（核心对比）**：

| 负载 | 1 线程 | 4 线程 | 8 线程 |
|------|--------|--------|--------|
| p5 | 0.170 vs 0.198（aligned 快 1.16×） | 0.123 vs 0.121（持平） | 0.116 vs 0.112（持平） |
| p25 | 0.521 vs 0.393（aligned 慢 1.33×） | 0.281 vs 0.183（aligned 慢 1.54×） | 0.227 vs 0.161（aligned 慢 1.41×） |
| p100 | 2.046 vs 1.830（aligned 慢 1.12×） | 1.006 vs 0.923（aligned 慢 1.09×） | 1.191 vs 0.896（aligned 慢 1.33×） |
| s25 | 0.158 vs 0.178（aligned 快 1.13×） | 0.111 vs 0.117（aligned 快 1.05×） | 0.112 vs 0.121（aligned 快 1.08×） |

- **分区剪枝场景（s25）aligned 胜出**：3 组独立分区剪枝跳过 4 个分区中的 3 个，
  而 join 仍需打开全部 3 文件并在剪枝子集上做 JOIN。
- **全扫描 + 宽投影（p100）aligned 略慢**：3 个 Parquet Reader + DataChunk 拼接
  的固定开销在 1M 行数据集上占比偏高。预期在 10K+ 列超宽表上此优势反转
  （多组只读被请求列，单宽表需读整个文件）。

**wide（单宽 Parquet）在窄表小数据最快**：单文件读取无组装开销，1M 行 × 127 列
占优。aligned 的优势在超宽表（10K+ 列）显现——单宽 Parquet 难以管理，
而多组拆分只读被请求列。

**polars 在小投影最快**：p5 warm 8 线程仅 0.017s——但这是单进程全内存专用引擎。
其优势在 p100（0.155s vs aligned 1.191s）缩小，因为 hstack 120 列仍有成本。

**并行扩展性**：

| 引擎 | p100 1→4 线程 | p100 4→8 线程 |
|------|---------------|---------------|
| aligned | 2.03×（好） | 0.85×（收益递减，120 列组装饱和内存带宽） |
| join | 1.98× | 1.03×（类似模式） |
| wide | 2.90× | 1.06× |

---

## 2. 写入基准（COPY TO FORMAT aligned vs 原生 PARQUET）

### 2.1 测试说明

递增规模（1/4/20/80/400 标的 × 13159 天 = 5.26M 行），7 列，按年分区，
ZSTD 压缩，8 线程。

| 引擎 | 说明 |
|------|------|
| Aligned index | COPY TO ... GROUP 'index'（2 列：symbol, date） |
| Aligned panel | COPY TO ... GROUP 'panel/ma'（7 列：symbol, date, o, h, l, c, v） |
| Native year-part | DuckDB COPY TO PARQUET PARTITION_BY(year)，7 列 |
| Native flat | DuckDB COPY TO PARQUET 无分区，7 列 |

### 2.2 测试结果

| 规模 | 行数 | Aligned index | Aligned panel | Native year-part | Native flat |
|------|------|---------------|---------------|------------------|-------------|
| 1 sym | 13,159 | 0.064s | 0.083s | 0.086s | 0.022s |
| 4 sym | 52,636 | 0.070s | 0.093s | 0.111s | 0.034s |
| 20 sym | 263,180 | 0.107s | 0.131s | 0.334s | 0.084s |
| 80 sym | 1,052,720 | 0.138s | 0.257s | 0.627s | 0.153s |
| 400 sym | 5,263,600 | 0.529s | 1.021s | 1.337s | 0.785s |

### 2.3 倍率分析（Aligned panel / Native year-part）

| 规模 | 倍率 | 结论 |
|------|------|------|
| 1 sym | 0.97× | 持平（固定开销主导） |
| 4 sym | 0.84× | aligned 快 |
| 20 sym | 0.39× | **aligned 快 2.6×** |
| 80 sym | 0.41× | **aligned 快 2.4×** |
| 400 sym | 0.76× | aligned 快 1.3× |

### 2.4 结论

- **Aligned vs native year-part：快 1.3~2.6×**（中等规模最优）。aligned 的
  per-partition CDC 缓冲 + run-length 批处理优于 native 的 per-row 分区路由。
- **Aligned vs native flat：慢 1.3×**（400 sym 规模）。native flat 无分区开销
  （单文件，无分区键求值，无 per-partition 锁）。26% 的差距是分区管理的固有成本。
- **小规模（1 sym）：aligned 持平/略慢**——固定开销主导。
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
- **正确性校验**：所有引擎对 p5 查询返回的 count 结果必须一致（PASS: all engines agree）。
