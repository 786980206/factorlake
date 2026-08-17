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
- [x] scoop 安装 ninja（1.13.2）
- [x] duckdb v1.5.4 源码 clone 到 `duckdb/`（vendored，gitignored；重新拉取命令见 §16）
- [x] Phase 0：`docs/STORAGE_CONTRACT.md` 定稿（v1.1：commit marker 含 part 名单，见 §9）
- [x] **Phase 1 Read MVP 完成（2026-08）**：`aligned_table()` / `aligned_scan()` 跑通
  - 多 Group 并行读取、RG 窗口调度、跨 part/RG 行窗口、Schema Evolution（缺失列补 NULL）、
    跨 Group 重复列遮蔽、Row Space 校验、commit marker 过滤
  - 验收：`scripts/test_aligned.ps1` 11/11 PASS（6000 行 × 3 组、对齐断言 misaligned=0、
    边界行、错误场景）
  - 构建产物：`duckdb/build/duckdb_aligned.exe`（shell 输出名已改为可配置 `DUCKDB_SHELL_OUTPUT_NAME`，
    原因见下）
- [x] **Phase 2 Projection Pushdown 完成（2026-08）**：
  - `projection_pushdown = true`；bind 仍返回全量 schema；`init_global` 消费 `input.column_ids`
    构建 全量列→投影输出位 的映射；扫描只填被请求列、只开被请求 Group
  - **实测验证**：`SELECT rowid_alpha` 只打开 alpha101 的 2 个 part（index/ma 零打开）；
    `SELECT date, ma20` 只打开 index+ma（alpha101 零打开）
  - `count(*)`（空 column_ids）走 0 向量 chunk、只报 cardinality 的路径 ✓
  - 验收：`scripts/test_aligned.ps1` **14/14 PASS**
- [x] `scripts/gen_testdata.ps1` 测试数据生成器
- [x] git 仓库（提交历史见 git log）
- [x] **Phase 3 Partition / Predicate Pushdown 完成（2026-08）**：
  - Partition pruning：`PrunePartsByFilter`（等值走目录路径；范围走 part 路径反向重建 part 日期，
    有界、不迭代日期）
  - Row Group stats pruning：`ComputeRowGroupWindow` 用 parquet 列统计 `CheckStatistics`，
    FILTER_ALWAYS_FALSE/FALSE_OR_NULL 的 RG 不进扫描窗口、整段 NULL 填充（行级 filter 必拒）
  - Row-level filter：`TableFilterState::Initialize` + `ColumnSegment::FilterSelection` 链式
    （尊重传入 selection，支持 AND 组合），经 scratch chunk 组装/Slice 后 `Reference` 输出
  - §3.1/§4 用户约束全部落地：index 必须存在（§2.1b）、非 index 必须两级目录 lv1/lv2（§2.1c）、
    路径段以 `.`/`_` 开头一律忽略（§2.1d）、重复列规则（§2.2e：与 index 重复→忽略非 index 副本；
    非 index 组间重复→仅限定名 `lv1.lv2.col`；裸名→not found）
  - 验收：`scripts/test_aligned.ps1` **21/21 PASS**（新增 §2.1b/c、§2.2e1/e2/e3 自动化测试）
- [x] **Phase 4 Parallel Scan + Metadata Cache 完成（2026-08）**：
  - **并行扫描**：共享游标（mutex 保护 `{interval_idx, next_row}`）按**连续 Range**（16 chunks =
    32768 行）发放给各 pipeline 线程；线程在自身 Range 内顺序扫描（窗口复用零重定位），
    Range 边界才重新定位（InitializeScan + discard 到目标行）
  - 行级 filter state 改为**每线程一份**（全局只存 filter 定义；FilterSelection 可能改 state）
  - **元数据缓存**：复用 DuckDB ObjectCache（LRU，8GiB，带 IsValid 校验）——把
    `parquet_metadata_cache` 默认置 ON（`DBConfig::SetOptionByName`），footer/schema/RG stats
    跨查询跨线程共享
  - 验收：`scripts/test_aligned.ps1` 21/21 + `scripts/test_parallel.ps1` 全 PASS；
    bench_ixday 1M 行 × 127 列：1 线程 0.89s → 8 线程 0.21s（**≈4.2× 加速**）
- [ ] Phase 5 Writer（RecordBatch → Column Group split → Aligned Parquet Writer → Atomic Commit）

### Phase 1 关键经验（必须记住，避免重踩）

1. **`VectorOperations::Copy` 的 5 参语义**：`Copy(source, target, source_count, source_offset, target_offset)`
   中 `source_count` 是**排他结束下标**，拷贝行数 = `source_count - source_offset`。
   传"行数"会在 `source_offset > count` 时下溢 → 字典向量 selection 越界读 → 崩溃。
   正确调用：`Copy(src, dst, src_offset + copy_count, src_offset, dst_offset)`。
2. **Parquet 字符串/字典是零拷贝**：`DictionaryDecoder::Read` 在 `result_offset==0` 时把结果向量
   做成 DICTIONARY_VECTOR（`Vector::Dictionary`），引用解码器内部的可复用字典。**每个 RG 窗口必须用
   全新的 `ParquetReaderScanState` 和 `DataChunk`**（勿跨窗口复用；复用会在第二次数据读取时 UAF）。
3. **1.5 的 `table_function_t` 返回 void**，用 `output.SetCardinality(0)` 表示结束。
4. **`ParquetReader::Scan` 每次切换 Row Group 会先返回一个空 chunk（setup call）**，不算结束；
   只有 `AsyncResultType::FINISHED/BLOCKED` 才是结束。
5. **`TableFunctionInput::bind_data` 是 `optional_ptr<const FunctionData>`**——扫描回调里对 bind data
   的引用是 const 的。
6. **C++11 标准**：`string::data()` 返回 `const char*`，写缓冲用 `&s[0]`。
7. **扩展的 include 路径要自己加**：`DUCKDB_EXTENSION_*_INCLUDE_PATH` 只是元数据，
   必须在扩展 CMakeLists 里 `include_directories` 自己的 `src/include` + parquet 头文件目录。
8. **调试经历**（2026-08）：UAF 崩溃排查用 SetUnhandledExceptionFilter + 寄存器转储 +
   `dumpbin /disasm` 定位（RVA 0x17236B = ValidityMask 位测试）。崩溃进程会锁住 duckdb.exe
   （taskkill 拒绝）→ shell 输出名改为 `duckdb_aligned`。
9. **测试数据里的 `rowid`/`rowid_alpha`/`rowid_ma` 是对齐断言列**（每 Group 独立命名，
   值 = 全局行号），`misaligned = 0` 即证明跨 Group 对齐。

### Phase 2 关键经验

1. **1.5 投影下推模型**：bind 永远返回全量 schema（bind 输入没有 column_ids）；
   `init_global`/`init_local` 的 `TableFunctionInitInput::column_ids` 才是被请求列
   （全量 schema 下标子集）。输出 chunk 向量数 = `column_ids.size()`（executor 按
   `op.types` = 投影后类型初始化）。`column_ids` 为空 = 无列请求（如 count(*)）→
   扫描只设 cardinality 不填向量。`COLUMN_IDENTIFIER_ROW_ID`（= -1）是虚拟列，不支持。
2. **全量列→投影位映射**放在 `AlignedScanGlobalState`（init_global 构建一次）：
   `projected_pos[full_id] = chunk 位置`；Group 级 `group_active` 决定是否打开。
   OpenPart 只读被请求列（`column_ids` 传给 ParquetReader 实现文件内列投影）。

### Phase 3 关键经验

1. **`ScanGroupWindow` 的 src_offset 计算（最隐蔽的数据错误，2026-08）**：
   `src_offset = copy_from - win_pos` **只在窗口起点 == RG 起点时正确**（flow_off == rg_off）。
   窗口从 RG 中部开始（flow_off > 0，如 chunk 2 请求 index day-18 part-000000 的 local
   [1096,2048)，RG 是 [0,2048)）时，必须用：
   `src_offset = seg.flow_off + (copy_from - seg.win_start) - rg_off`
   （窗口行 w ↔ RG 行 flow_off + (w - win_start) ↔ chunk 行 = 该值 - rg_off）。
   错误症状极具迷惑性：`count(rowid)` 显示 6000（被 count(*) 优化掩盖）、
   `misaligned = 0`（index 与 alpha 被**同样方式**破坏 → 互相"对齐"）、
   只有 count(alpha001)/count(alpha099) 各差 +1（952 行错位恰好各多 1 个命中）。
   诊断方法：`sum(rowid)`（期望 17,997,000）+ `row_number() OVER ()` 定位错位区间；
   **不要**依赖 count(rowid)/count(*)（DuckDB 会用 Cardinality 统计优化掉扫描）。
2. **DuckDB COPY TO PARQUET 的 ROW_GROUP_SIZE 不精确**：请求 1000 实际写出
   [0,2048)+[2048,3000)（writer 按 vector 大小 2048 flush）；分析 RG 布局以
   `parquet_metadata` 实测为准，别信 manifest 声明。
3. **PS 5.1 `-c` 传参无法携带引号标识符**：`"` 和 `` ` `` 都会被 mangle（DuckDB 1.5.4
   也不支持反引号标识符）。测试脚本里带引号标识符的 SQL 走临时文件 +
   `cmd /c "exe -csv -noheader < file"`（`Run-DuckDB-File`）。
4. **行级 filter 路径**：`TableFilterState::Initialize(context, filter)` 一次；
   `ColumnSegment::FilterSelection(sel, vec, vdata, filter, state, scan_count, approved)`
   尊重传入 selection 且 `scan_count` 是当前 selection 长度（链式 AND 直接传 approved）。

### Phase 4 关键经验

1. **表函数默认单线程**：`GlobalTableFunctionState::MaxThreads()` 默认返回 1 ——
   不 override 的话 `Pipeline::ScheduleParallel` 永远走 `ScheduleSequentialTask`，
   线程数再多也只跑 1 个任务。返回 `GlobalTableFunctionState::MAX_THREADS` 即放开并行。
2. **parquet 流按整向量（2048 行）前进，与"实际放置的行数"解耦**：RG 窗口复用
   必须满足"流位置 == 期望起点"（`local_start - rg_window_start == 流下一行 win 坐标`），
   否则行会丢失（下一个 chunk 从向量边界开始，缺中间行 → "scan ended early"）。
   不连续时重新 InitializeScan 并 discard 到目标行。旧代码靠 `parquet_pos >= plan_rows`
   在窗口尾"过冲"触发重算才没炸 —— 只对 RG ≤ 2048（单向量）的数据成立。
3. **copy 重叠必须双向 clamp**：`copy_from = max(w_start, win_pos)`、
   `copy_to = min(w_end, win_pos + valid_len)`；`copy_from >= copy_to` 即丢弃。
   只 clamp 一端会让 `copy_count = min(...) - copy_from` 下溢成巨大数 → 越界写 → 堆损坏
   （症状非确定：有时 0xC0000374 有时 0xC0000005，Debug 下变死循环）。
4. **并行 claim 粒度必须是"连续 Range"而非单 chunk**：每 chunk 一个 claim 时，线程间的
   claim 不连续 → 每次都要重定位（InitializeScan + discard 重读 RG）→ 并行开销吃掉全部
   收益（实测 8 线程无加速反而 user 时间翻倍）。按 16 chunks（32768 行）发 Range，
   线程在 Range 内顺序消费 → 8 线程 4.2×。
5. **诊断流程**：Release 堆损坏 + Debug 死循环 → 在 scan 循环里打每迭代
   `cursor/placed/need/segpos/ppos/async/chunk_rows`，一眼定位"placed=0 但 ppos 爬升"=
   向量被全部丢弃、范围不重叠。Windows 无调试器时这是最快路径。
6. **性能测量用 `.timer on`（文件输入）**，`-c` 的进程启动开销 ~0.8s 会淹没扫描时间；
   EXPLAIN ANALYZE 的算子耗时是可信的（不含进程启动）。

> 规则：每完成一项，把本节的 `[ ]` 改为 `[x]` 并记录日期/要点；
> 新决策必须写回对应小节，禁止只存在于对话里。
