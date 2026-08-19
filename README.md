# FactorLake / AlignedTable

基于 **DuckDB Extension + Parquet** 的超宽表存储/查询引擎。

> 逻辑上是一张几万甚至上十万列、10^8~10^10 行的宽表；
> 物理上拆成多个 **Column Group**，每个 Group 独立用 Parquet 存储；
> **Key 列只保存一份**；所有 Group 在 **Logical Row Space** 上严格 position-aligned；
> 查询时由 DuckDB Extension 把多个 Parquet Reader 的 Vector **直接组装进同一个 DataChunk**，
> **不做 JOIN、不做横向 materialize**。

**核心价值**：利用"业务数据天然 row-aligned"这一先验，把传统 OLAP 的 JOIN 成本、
Key duplication、不需要的列扫描、横向 concat 全部消灭——查询只剩
Projection + Partition Pruning + Row Group Pruning + Vectorized Aligned Scan。

## 用法

```sql
LOAD aligned;
SET aligned_data_root = '/data';

-- 逻辑上是一张超宽表：
SELECT date, symbol, close, alpha001, alpha002, ma20
FROM aligned_table('cnstk_ixday')
WHERE date = DATE '2026-08-17';
```

底层可能只打开三个目录：`index/`、`factor/alpha101/`、`fieldset/ma/`。
`WHERE date = ...` 会做分区剪枝，`WHERE symbol = ...` 会做 Parquet min/max
Row Group 剪枝；跨 Group 的所有行天然对齐，无需 JOIN。

```sql
-- 或者显式指定根目录：
SELECT * FROM aligned_scan('/data', 'cnstk_ixday');

-- 写入（append-only，原子提交）：
SELECT * FROM aligned_write('cnstk_ixday', '/data/stage/2026-08-17.parquet',
                             'index:date,symbol,close;factor/alpha101:alpha001,alpha002;fieldset/ma:ma20',
                             root='/data');

-- 合并 part（按分区目录，原子切换）：
SELECT * FROM aligned_compact('cnstk_ixday', 'factor/alpha101', root='/data');
```

## 核心概念

- **Logical Table**：`cnstk_ixday` 是一个逻辑表，目录即 Catalog（无 Catalog DB）。
- **Column Group**：`index/`、`factor/alpha101/`、`fieldset/ma/` 等叶子目录各是一个
  Group，独立用 Parquet 存储，物理 Partition 布局可以各不相同。
- **Logical Row Space**：`index[row N] == alpha[row N] == ma[row N]` 被直接视为同一行
  （Canonical Row Space），查询阶段**绝不通过 Key 做 JOIN**。
- **Key 只存一份**：`date`/`symbol` 只在 `index/` 保存。

```
<data_root>/
├── cnstk_ixday/            ← Logical Table
│   ├── _table.json         ← 可选 manifest（缺失时用默认值，见契约 v4）
│   ├── index/              ← Column Group（Key + 基础行情，必须存在）
│   │   └── date=2026-08-17/
│   ├── factor/alpha101/    ← Column Group（lv1/lv2 两级路径）
│   └── fieldset/ma/        ← Column Group
└── cnstk_klm01/            ← 另一张 Logical Table
```

7 条核心 Invariant（详见 `docs/STORAGE_CONTRACT.md` v4）：

| # | Invariant |
|---|-----------|
| 1 | Logical Table 有唯一 Canonical Row Space |
| 2 | Key Columns 只存一份 |
| 3 | 所有 Column Group 使用相同 Row Ordering |
| 4 | 所有 Column Group 使用相同 Logical Row Coordinate |
| 5 | Partition Scheme 可以不同 |
| 6 | Physical Files 可以完全不同 |
| 7 | 查询阶段绝不通过 Key 做 JOIN |

**对齐契约（唯一，无模式选择）**：所有 Group 必须全对齐（full alignment）——part 数、
part 大小、末 part 大小一致，行区间由公式 `start_row(i) = i * part_rows` 推导
（`part_rows` = index 首 part 行数）。违反即 fail-fast（"full alignment required"）。
`_table.json` 的 `aligned` 字段（v3 三模式）已被删除：读端忽略，不存在探测降级链。

## 已实现功能

| 能力 | 状态 | 说明 |
|------|------|------|
| 读取 | ✅ | `aligned_table()` / `aligned_scan()`，多 Group 并行读、跨 part/RG 行窗口、Schema Evolution（缺失列补 NULL）、跨 Group 重复列遮蔽、Row Space 校验 |
| 投影下推 | ✅ | 只打开被选中的 Group、只读被选中的列（`SELECT alpha001` 只碰 alpha101） |
| 过滤下推 | ✅ | Hive 分区剪枝 + Parquet Row Group stats 剪枝 + 行级 filter（`WHERE date=...` 剪到 1/4 数据时约 2-3× 收益） |
| 并行扫描 | ✅ | Aligned Row Group 为任务单元，8 线程实测 ≈4.2× 加速 |
| 元数据缓存 | ✅ | 复用 DuckDB ObjectCache（LRU 8GiB），footer/schema/RG stats 跨查询共享 |
| 写入 | ✅ | `aligned_write()`：append-only、按分区切 part、`_tmp` 暂存 + 原子提交（last_txid+1） |
| 合并 | ✅ | `aligned_compact()`：单事务合并**所有组**（保持组间 part 数一致），按分区目录合并 part，原子切换 |
| 扩展发布 | ✅ | 独立 `aligned.duckdb_extension`（24MB 自包含），`INSTALL` + `LOAD` 即用（见 `docs/EXTENSION_RELEASE.md`） |

不支持（第一版明确不做）：UPDATE/DELETE、Tombstone/Delta、事务并发写。

## 构建

Windows（MSVC + Ninja）与 Linux（gcc + Ninja）均可，DuckDB 源码 v1.5.4
（vendored 到 `duckdb/`，gitignored）。详见 `AGENTS.md` §16。

```bash
# Linux（注册扩展进 DuckDB 构建）
cd duckdb
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DDUCKDB_EXTENSION_CONFIGS=/path/to/factorlake/scripts/aligned_extension_config.cmake
ninja -C build duckdb

# 发布用 loadable 扩展（自包含单文件）
cmake -S . -B build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release -DEXTENSION_STATIC_BUILD=1 \
      -DDUCKDB_EXTENSION_CONFIGS=/path/to/factorlake/scripts/aligned_extension_config.cmake
ninja -C build-rel aligned_loadable_extension
# → build-rel/extension/aligned/aligned.duckdb_extension
```

测试（数据生成 + 验收脚本，Windows 用 `.ps1`、Linux 用 `.sh`，当前 28/28 PASS）：

```bash
bash scripts/gen_testdata.sh   # 生成 testdata/（6000 行 × 3 组）
bash scripts/test_aligned.sh   # 读取/投影/分区剪枝/并行/契约校验
```

## 性能结论（详见 docs/）

- **`docs/BENCHMARK.md`**：1M 行 × 127 列，投影 5/25/120 列 × 扫描 25%/100% × 线程 1/4/8。
  8 线程 ≈4.2× 加速；分区剪枝 25% 扫描 ≈2-3× 收益。
- **`docs/BENCHMARK_MULTI_ANALYSIS.md`**：6 引擎对比（DuckDB 宽表/JOIN、polars
  横向 concat/JOIN、aligned）。A-ALIGNED vs D-JOIN 在 10M 行 ≈ **40×**、vs polars
  JOIN ≈ **152×**（position 组装近似线性 vs JOIN/hstack 超线性）。
- **三模式基准（all/group/none）已随 v4 删除**：全对齐契约固定为 all，无性能差异
  可比较；part 粒度影响固定开销的结论仍在（writer 应尽量大 part）。

## 文档索引

| 文档 | 内容 |
|------|------|
| `AGENTS.md` | 项目权威记忆文件（架构决策、进度、关键经验，agent 必读） |
| `docs/STORAGE_CONTRACT.md` | 存储契约 v4（目录规则、列名规则、全对齐契约、行区间、Manifest） |
| `docs/BENCHMARK*.md` | 各轮基准测试方法与结论 |
| `docs/EXTENSION_RELEASE.md` | 扩展发布机制（INSTALL/LOAD、签名、GitHub Release） |
| `docs/READ_OPTIMIZATIONS.md` | 读取链路现状分析与优化计划 |
| `docs/WRITE_PLAN.md` | 写入功能现状与开发计划 |
| `plan.md` | 原始完整技术 Plan（做什么、为什么、归属哪个 Phase） |

## 路线图

1. **读取链路优化**（见 `docs/READ_OPTIMIZATIONS.md`）：✅ filter_prune=true
   （过滤列不投影也可剪枝、省掉多余 PROJECTION 层）、✅ 列类型解析复用；
   待做：NULL 填充向量化、reader 缓存、批量 footer、聚合 stats 快速路径
   （依赖 DuckDB ≥ v1.6）。
2. **写入增强**（见 `docs/WRITE_PLAN.md`）：目录源、part_rows 上限切分、并发写
   互斥（last_txid CAS）、写后校验、group 间并行。
3. **Catalog Integration**：最终目标 `SELECT * FROM cnstk_ixday;` 直接可用。