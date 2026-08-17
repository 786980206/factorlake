# AGENTS.md — FactorLake / AlignedTable

> 本文件是项目的**唯一权威记忆文件**。任何 agent 开始工作前必须先读本文件；
> 任何架构决策、Phase 进度更新必须写回本文件（文末「当前进度」节）。

---

## 0. 一句话定位

基于 **DuckDB Extension + Parquet** 的超宽表存储/查询引擎：

> 逻辑上是一张几万甚至上十万列、10^8~10^10 行的宽表；
> 物理上拆成多个 **Column Group**，每个 Group 独立用 Parquet 存储；
> **Key 列只保存一份**；所有 Group 在 **Logical Row Space** 上严格 position-aligned；
> 查询时由 DuckDB Extension 把多个 Parquet Reader 的 Vector **直接组装进同一个 DataChunk**，
> **不做 JOIN、不做横向 materialize**。

最终用户完全无感知：

```sql
SELECT date, symbol, close, alpha001, alpha002, ma20, yoy_revenue
FROM cnstk_ixday
WHERE date = DATE '2026-08-17';
```

底层实际可能只读三个目录：`index/`、`factor/alpha101/`、`fieldset/ma/`。

**项目真正的价值**：利用"业务数据天然 row-aligned"这一先验，把传统 OLAP 的
JOIN 成本、Key duplication、不需要的列扫描、横向 concat **全部消灭**，
最终只剩 Projection + Partition Pruning + Row Group Pruning + Vectorized Aligned Scan。

---

## 1. 核心需求

- **超宽**：10,000+ 列，10^8 ~ 10^10 行，禁止单一巨型 Parquet 作为唯一物理布局。
- **Sparse / NULL-heavy**：大量因子 90%~99% NULL。上层看到普通 column，
  下层自动靠 Parquet encoding/compression 省空间，用户不需要知道 Sparse Column。
- **Column Group**：`factor/alpha101`、`fieldset/ma`、`panel/cnstk_icday` 等叶子目录各是一个 Group。
- **Key 只存一份**：`date`/`symbol` 只在 `index/` 里保存，其他 Group 不再保存。
- **绝不 JOIN**：`index[row N] == alpha[row N] == ma[row N]` 被直接视为同一行。

---

## 2. 目录设计

```
<data_root>/
├── cnidx_klday/            ← Logical Table
├── cnstk_ixday/            ← Logical Table
│   ├── _table.json
│   ├── index/
│   ├── factor/
│   │   ├── alpha101/
│   │   └── alpha191/
│   ├── fieldset/
│   │   ├── ema/  ma/  qoq/  ttm/  yoy/
│   └── panel/
│       ├── cnstk_icday/  cnstk_ixday/  cnstk_klday/
└── cnstk_klm01/            ← Logical Table
```

**语义**：`cnstk_ixday` 是 Logical Table；`cnstk_ixday/index`、
`cnstk_ixday/factor/alpha101` 等是这张表的 **Column Groups**。
这是整个设计最重要的语义。

---

## 3. Logical Row Space 与 7 条核心 Invariant

每张 Logical Table 定义一个 **Canonical Row Space**（如 row 0 → 2026-08-17/000001，
row 1 → 2026-08-17/000002 …）。所有 Group 必须满足：

| # | Invariant |
|---|-----------|
| 1 | Logical Table 有唯一 Canonical Row Space |
| 2 | Key Columns 只存一份 |
| 3 | 所有 Column Group 使用相同 Row Ordering |
| 4 | 所有 Column Group 使用相同 Logical Row Coordinate |
| 5 | Partition Scheme 可以不同 |
| 6 | Physical Files 可以完全不同 |
| 7 | 查询阶段绝不通过 Key 做 JOIN |

必须一致：`Logical Row N`；可以不同：Partition、File、File size、File name、
Physical partition key、Parquet 文件数量。

---

## 4. 物理 Partition 可不同（Logical vs Physical Partition）

不同 Group 的物理目录结构**允许不同**：

```
index/            date=2026-08-17/
factor/alpha101/  year=2026/month=08/day=17/
fieldset/ma/      year=2026/month=08/
```

插件维护 **Logical Row Space → Physical Partition Resolver**，把
`WHERE date = '2026-08-17'` 分别解析到各 Group 的文件，最终映射到同一个
**Logical Row Range**。

---

## 5. Manifest（轻量，不做 Catalog DB）

目录本身就是 Catalog，文件发现用 Hive layout。每张 Logical Table 一个 `_table.json`：

```json
{
  "name": "cnstk_ixday",
  "version": 1,
  "key": ["date", "symbol"],
  "canonical_order": "fixed",
  "groups": ["index", "factor/alpha101", "factor/alpha191", "fieldset/ema",
             "fieldset/ma", "fieldset/qoq", "fieldset/ttm", "fieldset/yoy",
             "panel/cnstk_icday", "panel/cnstk_ixday", "panel/cnstk_klday"]
}
```

每个 Group 一个 `_group.json`：

```json
{
  "group": "factor/alpha101",
  "row_count": 1234567890,
  "partitioning": ["year", "month", "day"],
  "row_group_size": 131072
}
```

Manifest 不记录所有 Parquet 文件，只负责：Logical Table 定义、Key、
Schema version、Column Group 列表、Row count、Canonical Row Space metadata、Group metadata。

---

## 6. Row Group 对齐（比文件对齐更重要）

统一 `row_group_size = 131072`（可配）。不要求 `index/part-000 == alpha/part-000`，
但最好 `index RG17 == alpha RG17 == ma RG17` 表示同一 Logical Row Range：

```
RG17 → index vectors + alpha vectors + ma vectors → 同一个 DataChunk
```

**文件大小策略**：目标 256MB~512MB（本地 NVMe 可 128MB~512MB，对象存储 256MB~1GB）。
优先级：**Row Group alignment > File size**，不要为了凑文件大小破坏 Row Group。

---

## 7. Reader 架构

```
AlignedTableScan
└── AlignedScanState
    ├── Partition Resolver
    ├── Column Group Resolver
    ├── Parquet Scanner #1 / #2 / #3 ...
    └── Output Mapping
```

例：`SELECT close, alpha001, ma20` → 只打开 `index/`、`factor/alpha101/`、`fieldset/ma/` 三个 Group。

### 7.1 Projection Pushdown（第一优先级优化）
`SELECT alpha001` 时：Parquet 内只读 alpha001 一列；index/fieldset/panel 完全不打开。

### 7.2 Filter Pushdown
`WHERE date = '2026-08-17'` → Hive Partition Pruning → Parquet Row Group Pruning
→ Column Projection → Vectorized Scan。`WHERE symbol = '000001'` 可用 Parquet min/max stats 进一步裁剪。

### 7.3 DataChunk Assembly（插件最核心代码）
不是把几个表 concat，而是：**多个 Parquet Column Reader 直接填充同一个 DuckDB DataChunk**。
每列一个 Vector，不需要横向 materialize。

---

## 8. 最大程度复用 DuckDB

**不自己实现**：Parquet parser、compression、dictionary encoding、RLE、statistics、
filesystem、S3、HTTP range request。
**复用**：Parquet Reader、FileSystem、MultiFileReader、Parquet metadata、
Row Group statistics、Projection Pushdown、Filter Pushdown、Vectorized execution、Buffer Manager。

Extension 只负责：`Logical Table → Column Group resolution → Partition resolution
→ Aligned Scanner → DataChunk`。

---

## 9. Writer

```
AlignedTableWriter: Arrow RecordBatch → 按 Column Group 拆多个 Parquet Writer
                    所有 Writer 共享同一 Row Boundary
```

- 第一版 **Immutable + Append-only**：`part-001/002/003...`，新数据只追加，不改旧文件。
- **Atomic Commit**：先写 `_tmp/transaction-<id>/`，全部成功后 commit 成正式 partition；
  崩溃则丢弃 transaction，读取端永远不会看到"index 有数据、alpha 没写完"的非法状态。
- **Compaction**（后续）：后台合并 old parts → 新 part，保持 Canonical Row Space /
  Row Ordering / Row Group Boundary 不变，atomic switch，查询不中断。
- **Schema Evolution**（后续）：新列在老 partition 上读到 NULL，不重写历史。
- **Sparse**：第一版不自己实现，靠 Parquet RLE/Dictionary/Compression 天然压缩；
  未来极端 sparse 列再考虑独立 Group（如 `sparse/alpha999`）。
- **第一版不支持 UPDATE/DELETE**，不要提前做 Tombstone/Delta。

---

## 10. 并行执行

执行单位 = **Aligned Row Group**：

```
Task 1 → rows 0..131071      (index RG + alpha RG + ma RG → DataChunk)
Task 2 → rows 131072..262143
Task 3 → rows 262144..393215
```

每个 Task 组装 DataChunk 后交给 DuckDB execution pipeline，自然利用多核。

---

## 11. Metadata Cache

缓存 Parquet Footer、Schema、Row Group Metadata、Min/Max Statistics，用 LRU。
第一版**不缓存实际数据**。工作负载反复查 index/alpha101/ma，收益会很高。

---

## 12. 技术栈

- Query Engine：**DuckDB Extension**（SQL/Planner/Table Function/Execution/DataChunk/Parallelism）
- Storage：**Parquet**（Encoding/Compression/Statistics/Column Storage/Row Groups）
- Writer 第一版：**C++ / DuckDB / Arrow + Parquet**
  （先打通 extension execution integration，比引入 Rust FFI 更重要；
  未来 Writer/Compaction 复杂化再考虑 Rust Storage Engine）

---

## 13. Extension API

- 第一版：`SELECT * FROM aligned_table('cnstk_ixday');` 或
  `SELECT * FROM aligned_scan('/data', 'cnstk_ixday');`
- 最终目标：Catalog Integration，用户直接 `SELECT * FROM cnstk_ixday;`

---

## 14. 代码结构（位于 extension/aligned/ 内）

```
src/
├── extension.cpp
├── catalog/       logical_table.cpp  schema.cpp  manifest.cpp
├── resolver/      group_resolver.cpp  partition_resolver.cpp  row_space.cpp
├── scan/          aligned_scan.cpp  aligned_scan_state.cpp  group_scan.cpp  scheduler.cpp
├── parquet/       duckdb_parquet_adapter.cpp
├── writer/        aligned_writer.cpp  group_writer.cpp  transaction.cpp
├── compaction/    compactor.cpp
└── optimizer/     projection.cpp  filter.cpp
```

---

## 15. 开发阶段

| Phase | 内容 | 验收标准 |
|-------|------|----------|
| 0 | **Storage Contract**：Logical Table / Canonical Key / Row Ordering / Row Group / Partition / Column Group / Schema Evolution 全部写死，尤其定义**什么叫 row N 对齐** | docs/STORAGE_CONTRACT.md |
| 1 | **Read MVP**：`aligned_table()` 跑通，支持 Logical Table + Column Group + Parquet + Row alignment + DataChunk，不做复杂优化 | `SELECT * FROM aligned_table('cnstk_ixday')` |
| 2 | **Projection**：只扫描被选中的 Group | `SELECT close, alpha001, ma20` 只开 3 个 Group |
| 3 | **Partition / Predicate Pushdown**：Hive pruning + Parquet stats + RG pruning + projection | 有实用价值 |
| 4 | **Parallel Scan**：Aligned Row Group 为任务单元，多核 | 多核利用率 |
| 5 | **Writer**：RecordBatch → Column Group split → Aligned Parquet Writer → Atomic Commit（append-only） | 写入+读取闭环 |
| 6 | **Benchmark**：10^8/10^9 rows × 1K/10K/50K cols；查 5/100/1K/10K 列；1%/10%/100% scan；并发 1/4/16/32/64；对比普通 Parquet / JOIN / DuckDB 宽表 | 报告 |
| 7 | **Compaction / Evolution**：最后再做，不要提前 | — |

---

## 16. 环境与构建（Windows + scoop）

### 已确认环境（2026-08）
- scoop（buckets: main/extras/versions/nerf-fonts/java/dbx），aria2 加速可用
- 已装：`git 2.44.0`、`cmake 4.0.3`、`duckdb 1.5.4`(CLI)、`python310`、`vcredist2022`、`7zip`
- **MSVC**：VS2022 BuildTools 装在 `C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools`
  - `vcvars64.bat` → `...\BuildTools\VC\Auxiliary\Build\vcvars64.bat`
  - cl.exe 版本 14.44.35207（MSVC 14.44）
- **ninja**：`scoop install ninja`（2026-08 安装中/已完成）
- 目标 DuckDB 源码版本：**v1.5.4**（与 CLI 严格一致，extension ABI 版本锁定）

### 构建命令（MSVC 环境必须在 PATH 里）
PowerShell 里通过 cmd 导入 vcvars64 再跑 cmake：

```powershell
cmd /c "call ""C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"" && cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release ..."
```

测试用 `duckdb -unsigned -c "LOAD 'build/release/extension/aligned/aligned.duckdb_extension'; ..."`
（自定义扩展需 `-unsigned`；或直接用同源码树构建出的 duckdb CLI，避免 platform 校验问题）。

---

## 17. 当前进度

- [x] 完整 Plan 定稿（本文件）
- [ ] scoop 安装 ninja
- [ ] duckdb v1.5.4 源码 clone 到 `duckdb/`
- [ ] Phase 0：`docs/STORAGE_CONTRACT.md` 定稿
- [ ] `extension/aligned/` 骨架（CMakeLists + manifest/row_space 解析）
- [ ] 首次 MSVC 构建 + `LOAD aligned` 验证

> 规则：每完成一项，把本节的 `[ ]` 改为 `[x]` 并记录日期/要点；
> 新决策必须写回对应小节，禁止只存在于对话里。
