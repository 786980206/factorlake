# FactorLake / AlignedTable — 完整技术 Plan

> 本文档是项目的**完整技术 Plan**（v1，2026-08 定稿）。
> 操作层面的权威记忆文件是 `AGENTS.md`；存储格式规格是 `docs/STORAGE_CONTRACT.md`。
> 本文档只收录"做什么、为什么、归属哪个 Phase"。

---

## 1. 项目目标

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

## 2. 核心需求

- **超宽**：10,000+ 列，10^8 ~ 10^10 行，禁止单一巨型 Parquet 作为唯一物理布局。
- **Sparse / NULL-heavy**：大量因子 90%~99% NULL。上层看到普通 column，
  下层自动靠 Parquet encoding/compression 省空间，用户不需要知道 Sparse Column。
- **Column Group**：`factor/alpha101`、`fieldset/ma`、`panel/cnstk_icday` 等叶子目录各是一个 Group。
- **Key 只存一份**：`date`/`symbol` 只在 `index/` 里保存，其他 Group 不再保存。
- **绝不 JOIN**：`index[row N] == alpha[row N] == ma[row N]` 被直接视为同一行。

## 3. 目录设计

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

### 3.1 目录硬性规则（2026-08 用户补充，归属 Phase 0 契约 + Phase 1 Reader 实现）

a. `<table_name>` 即表名（如 `cnstk_ixday`）。
b. **`<table>/index` 必须存在**——每张表必须有 index Group（物理目录 + `_group.json`），
   缺失即报错。Key 列与基础行情列都住在 index。
c. **其余 Group 必须以两级目录存在**：`<table>/<lv1>/<lv2>/`（如 `factor/alpha101`、
   `fieldset/ma`、`panel/cnstk_klday`）。group 路径深度固定为 2；
   manifest 的 `groups` 条目必须形如 `lv1/lv2`，否则报错。
   （Physical Partition 目录在 group 目录之下，不受此限制。）
d. **目录发现时忽略任何以 `.` 或 `_` 开头的目录**（如 `_tmp/`、`.hidden/`）——
   glob 结果必须过滤这些路径段；`_table.json` / `_group.json` / sidecar 等
   以 `.`/`_` 开头的**文件**不受影响。

## 4. 列名规则（2026-08 用户补充，归属 Phase 1/2 Reader 实现）

e. **重复列名的处理**（按优先级）：

   1. **与 index 中重复的列名，全部忽略**：非 index Group 里出现 `date`、`symbol`、
      `close` 等同名列 → 该列不进入表 schema（index 的列是权威的）。reader 读取时
      也直接跳过这些列（不读、不填）。
   2. **非 index Group 之间重复的列名，必须用限定名引用**：
      `lv1_name.lv2_name.col_name`（如 `factor.alpha101.close`、`panel.cnstk_klday.close`）。
      表 schema 中这类列只以限定名注册（裸名不注册）；用户引用裸名 →
      **报"列不存在"**；引用限定名（DuckDB 中需反引号：`` `factor.alpha101.close` ``）→ 正常。
   3. 非重复的列名照常以裸名注册。

   物理映射：Group 内部仍按裸名与 sidecar/parquet 列匹配；`lv1`/`lv2` 来自 group 路径。

## 5. Logical Row Space 与 7 条核心 Invariant

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

## 6. 物理 Partition 可不同（Logical vs Physical Partition）

不同 Group 的物理目录结构**允许不同**：

```
index/            date=2026-08-17/
factor/alpha101/  year=2026/month=08/day=17/
fieldset/ma/      year=2026/month=08/
```

插件维护 **Logical Row Space → Physical Partition Resolver**，把
`WHERE date = '2026-08-17'` 分别解析到各 Group 的文件，最终映射到同一个
**Logical Row Range**。

## 7. Manifest（轻量，不做 Catalog DB）

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

## 8. Row Group 对齐（比文件对齐更重要）

统一 `row_group_size = 131072`（可配）。不要求 `index/part-000 == alpha/part-000`，
但最好 `index RG17 == alpha RG17 == ma RG17` 表示同一 Logical Row Range：

```
RG17 → index vectors + alpha vectors + ma vectors → 同一个 DataChunk
```

**文件大小策略**：目标 256MB~512MB（本地 NVMe 可 128MB~512MB，对象存储 256MB~1GB）。
优先级：**Row Group alignment > File size**，不要为了凑文件大小破坏 Row Group。

## 9. Reader 架构

```
AlignedTableScan
└── AlignedScanState
    ├── Partition Resolver
    ├── Column Group Resolver
    ├── Parquet Scanner #1 / #2 / #3 ...
    └── Output Mapping
```

例：`SELECT close, alpha001, ma20` → 只打开 `index/`、`factor/alpha101/`、`fieldset/ma/` 三个 Group。

### 9.1 Projection Pushdown（第一优先级优化，Phase 2 ✓）
`SELECT alpha001` 时：Parquet 内只读 alpha001 一列；index/fieldset/panel 完全不打开。

### 9.2 Filter Pushdown（Phase 3）
`WHERE date = '2026-08-17'` → Hive Partition Pruning → Parquet Row Group Pruning
→ Column Projection → Vectorized Scan。`WHERE symbol = '000001'` 可用 Parquet min/max stats 进一步裁剪。

### 9.3 DataChunk Assembly（插件最核心代码）
不是把几个表 concat，而是：**多个 Parquet Column Reader 直接填充同一个 DuckDB DataChunk**。
每列一个 Vector，不需要横向 materialize。

## 10. 最大程度复用 DuckDB

**不自己实现**：Parquet parser、compression、dictionary encoding、RLE、statistics、
filesystem、S3、HTTP range request。
**复用**：Parquet Reader、FileSystem、MultiFileReader、Parquet metadata、
Row Group statistics、Projection Pushdown、Filter Pushdown、Vectorized execution、Buffer Manager。

Extension 只负责：`Logical Table → Column Group resolution → Partition resolution
→ Aligned Scanner → DataChunk`。

## 11. Writer

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

## 12. 并行执行（Phase 4）

执行单位 = **Aligned Row Group**：

```
Task 1 → rows 0..131071      (index RG + alpha RG + ma RG → DataChunk)
Task 2 → rows 131072..262143
Task 3 → rows 262144..393215
```

每个 Task 组装 DataChunk 后交给 DuckDB execution pipeline，自然利用多核。

## 13. Metadata Cache（Phase 4）

缓存 Parquet Footer、Schema、Row Group Metadata、Min/Max Statistics，用 LRU。
第一版**不缓存实际数据**。工作负载反复查 index/alpha101/ma，收益会很高。

## 14. 技术栈

- Query Engine：**DuckDB Extension**（SQL/Planner/Table Function/Execution/DataChunk/Parallelism）
- Storage：**Parquet**（Encoding/Compression/Statistics/Column Storage/Row Groups）
- Writer 第一版：**C++ / DuckDB / Arrow + Parquet**
  （先打通 extension execution integration，比引入 Rust FFI 更重要；
  未来 Writer/Compaction 复杂化再考虑 Rust Storage Engine）

## 15. Extension API

- 第一版：`SELECT * FROM aligned_table('cnstk_ixday');` 或
  `SELECT * FROM aligned_scan('/data', 'cnstk_ixday');`
- 最终目标：Catalog Integration，用户直接 `SELECT * FROM cnstk_ixday;`

## 16. 代码结构（位于 extension/aligned/ 内）

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

## 17. 开发阶段与状态

| Phase | 内容 | 状态 |
|-------|------|------|
| 0 | **Storage Contract**：Logical Table / Canonical Key / Row Ordering / Row Group / Partition / Column Group / Schema Evolution / 目录硬性规则(§3.1) / 列名规则(§4) 全部写死 | ✅ `docs/STORAGE_CONTRACT.md` v1.1 |
| 1 | **Read MVP**：`aligned_table()` 跑通（Logical Table + Column Group + Parquet + Row alignment + DataChunk + §3.1/§4 规则） | ✅ 验收 14/14 |
| 2 | **Projection**：只扫描被选中的 Group（含 §4 限定名映射） | ✅ 验收 14/14 |
| 3 | **Partition / Predicate Pushdown**：Hive pruning + Parquet stats + RG pruning + projection + row-level filter（§3.1/§4 规则自动化测试） | ✅ 验收 21/21（2026-08） |
| 4 | **Parallel Scan**：Aligned Row Group 为任务单元 + Metadata Cache（ObjectCache LRU） | ✅ 验收：`test_parallel.ps1` 全 PASS，8 线程 ≈ 4.2× 加速（2026-08） |
| 5 | **Writer**：`aligned_write(table, source, mapping)` → 按 Column Group 拆列 → Parquet Writer（逐组同 Row Boundary）→ `_tmp` 暂存 + sidecar + commit marker 原子提交（append-only） | ✅ 验收：`test_writer.ps1` 全 PASS（空表首写 + 追加 + 进化列 + 闭环读回，2026-08） |
| 6 | **Benchmark**：1M rows × 127 cols；投影 5/25/120 列；扫描 25%/100%；并发 1/4/8；对比 wide / JOIN / polars 横向 concat | ✅ 旧：`bench_aligned.ps1`+`bench_polars.py`；**新**:`bench_scenarios.sh` 多场景框架（6 引擎、250K→10M 逐级递增、内存监控、一键复现），结论见 `docs/BENCHMARK_MULTI_ANALYSIS.md`（A-ALIGNED vs D-JOIN 10M≈40×、vs P-JOIN≈152×） |
| 7 | **Compaction / Evolution**：`aligned_compact(table, group)` 合并 part（同分区目录→新 part，原子 marker 切换，旧文件删除），写前模拟校验 | ✅ 验收：`test_compaction.ps1` 全 PASS（2026-08） |
| + | **元数据 `aligned` 开关**：`_table.json` `aligned`(bool, 默认 true)；true=leaf 剪枝结果映射统一坐标后相交为全局扫描区间；false=各 leaf 独立剪枝/规划，不做跨 leaf 交集（union 扫描） | ✅ `test_aligned_flag.ps1` 全 PASS（2026-08） |

> §3.1 / §4 的规则归属：目录/发现规则（a-d）→ Phase 0 契约 + Phase 1 Reader；
> 列名规则（e）→ Phase 1/2 Reader（index 优先在 ResolveColumnTypes，限定名在 schema 注册）。

## 18. 最终数据流

```
                 SQL
                  │
                  ▼
           DuckDB Optimizer
                  │
       Projection / Filter
                  │
                  ▼
          Aligned Table Scan
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
      index    alpha101     ma
        │         │         │
     Parquet   Parquet   Parquet
        │         │         │
        └─────────┼─────────┘
                  │
         Canonical Row Space
                  │
                  ▼
             DataChunk
                  │
                  ▼
          DuckDB Execution
                  │
                  ▼
               Result
```

## 19. 最终物理模型

```
                    Logical Table
                     cnstk_ixday
                          │
             ┌────────────┼────────────┐
             │            │            │
           index        factor       fieldset
             │            │            │
          Key+Base     alpha101       ma
                         alpha191      ema
                                      qoq
                                      ...
             │            │            │
             └────────────┼────────────┘
                          │
                  Canonical Row Space
                          │
               ┌──────────┼──────────┐
               ▼          ▼          ▼
           Partition A Partition B Partition C
               │          │          │
             Parquet    Parquet    Parquet
```

逻辑上：`date | symbol | close | alpha001 | ma20 | ...`
物理上：多个独立 Parquet；查询层像普通宽表，存储层像 Columnar Object Store，
执行层像一个专门针对 position-aligned columns 优化的 Vectorized Scan。

> 核心价值一句话：**利用"业务数据天然 row-aligned"这一先验，把传统 OLAP 的 JOIN 成本直接消灭**——
> 普通数据库看到 index/factor/ma 会认为是三张表；本系统知道它们只是同一张超宽表的三个物理 Column Group。
