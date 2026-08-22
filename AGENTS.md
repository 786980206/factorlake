# AGENTS.md — FactorLake / AlignedTable

> 本文件是项目的**唯一权威架构文件**。只记录"需要实现成什么样"、"契约是什么样"
> 以及"需要注意的问题"。开发日志见 `release.md`。

---

## 0. 一句话定位

基于 **DuckDB Extension + Parquet** 的超宽表存储/查询引擎：

> 逻辑上是一张几万甚至上十万列、10^8~10^10 行的宽表；
> 物理上拆成多个 **Column Group**，每个 Group 独立用 Parquet 存储；
> **Key 列只保存一份**；所有 Group 在 **Logical Row Space** 上严格 position-aligned；
> 查询时由 DuckDB Extension 把多个 Parquet Reader 的 Vector **直接组装进同一个 DataChunk**，
> **不做 JOIN、不做横向 materialize**。

```sql
SELECT date, symbol, close, alpha001, alpha002, ma20, yoy_revenue
FROM cnstk_ixday
WHERE date = DATE '2026-08-17';
-- 底层实际只读三个目录：index/、factor/alpha101/、fieldset/ma/
```

**核心价值**：利用"业务数据天然 row-aligned"这一先验，把传统 OLAP 的 JOIN 成本、
Key duplication、不需要的列扫描、横向 concat 全部消灭，只剩 Projection + Partition
Pruning + Row Group Pruning + Vectorized Aligned Scan。

---

## 1. 核心需求

- **超宽**：10,000+ 列，10^8 ~ 10^10 行，禁止单一巨型 Parquet 作为唯一物理布局。
- **Sparse / NULL-heavy**：大量因子 90%~99% NULL，靠 Parquet encoding/compression 天然压缩。
- **Column Group**：`factor/alpha101`、`fieldset/ma` 等叶子目录各是一个 Group。
- **Key 只存一份**：`symbol`/`date` 只在 `index/` 里保存，其他 Group 不再保存。
- **绝不 JOIN**：`index[row N] == alpha[row N] == ma[row N]` 被直接视为同一行。

---

## 2. 目录设计

```
<data_root>/
├── cnstk_ixday/            ← Logical Table
│   ├── index/
│   ├── factor/
│   │   ├── alpha101/
│   │   └── alpha191/
│   ├── fieldset/
│   │   ├── ema/  ma/  qoq/  ttm/  yoy/
│   └── panel/
│       └── cnstk_icday/
└── cnstk_klm01/            ← Logical Table
```

`cnstk_ixday` 是 Logical Table；`cnstk_ixday/index`、`cnstk_ixday/factor/alpha101`
等是这张表的 **Column Groups**。

---

## 3. Logical Row Space 与 7 条核心 Invariant

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

## 4. 无 Manifest（不做 Catalog DB）

目录本身就是 Catalog，文件发现用 Hive layout。**没有 `_table.json`，没有
`_group.json`，没有 part sidecar，没有 commit marker**。所有元数据从目录结构和
Parquet footer 推导：

- **分区对齐（Partition Alignment）是唯一契约**：所有 Group（含 index）用
  **同一种一层分区段**（`year=` / `month=` / `date=` 三选一）；分区键 = 完整
  `name=value` 段串；Group 分区键集合 **⊆ index**（允许缺分区 → 该区行保留、
  该组列全 NULL；**绝不添加 index 没有的分区**，违反即 fail-fast）；共享分区
  **总行数**必须一致（末 part 行数可不同）。
- **Group 发现唯一路径 = 一次 glob**（`**/*.parquet` → 跳过 `name=value` 分区段
  → 最长非分区段后缀即 Group）。空表（无任何 part）不是有效表——Reader 报错，
  Writer 从 `mapping` 参数推导 Group 结构。
- **分区模板从目录结构推导**（仅识别 `year=%Y`/`month=%Y-%m`/`date=%Y-%m-%d`
  三种单层段；空表首写默认 `month=%Y-%m`）。
- **Canonical Key = index Group 的 schema 列**（从 Parquet footer 读取）。
- **主键契约**（v8）：index schema（rel_path 排序最后 1 个 part footer）前两列 =
  主键 `(symbol, date)`——col0 = **symbol**（字符串），col1 = **DATE/TIMESTAMP**
  （分区源列）。col1 非 DATE/TIMESTAMP 即 fail-fast；分区目录只由该日期列求值；
  分区内按 `(symbol, date)` 升序排列。
- **不持久化的信息**：表名（←目录名）、schema（←Parquet footer）、
  行数/行区间（←part 文件名 `{idx:04d}-{rows:10d}`）、Row Group 大小（←编译常量
  131072）、事务号、Column Group 列表（←glob）、分区模板（←目录结构）。
- **行区间契约**：part 顺序 = 组内相对路径字符串排序，part_id = 文件名解析出的
  `idx`；行区间由文件名累加推导（`start_row = S_i + Σ(更小索引 part 的行数)`，
  零 footer IO）；index 分区内索引必须 0000 起连续（缺号/重复 fail-fast），非 index
  组允许缺号；扫描时 OpenPart 防御校验 footer 行数 == 文件名行数；组 schema = 组内
  rel_path 排序最后 1 个 part 的 footer（每 Group 只读 1 个 footer）。

---

## 5. Row Group 对齐

统一 `row_group_size = 131072`。最好 `index RG17 == alpha RG17 == ma RG17` 表示
同一 Logical Row Range。优先级：**Row Group alignment > File size**。
文件目标 256MB~512MB（本地 NVMe 128MB~512MB，对象存储 256MB~1GB）。

---

## 6. Reader 架构

```
AlignedTableScan
└── AlignedScanState
    ├── Partition Resolver
    ├── Column Group Resolver
    ├── Parquet Scanner #1 / #2 / #3 ...
    └── Output Mapping
```

- **Projection Pushdown**：只打开被选中的 Group，Parquet 内只读被选中的列。
- **Filter Pushdown**：Hive Partition Pruning → Parquet Row Group Pruning
  → Column Projection → Vectorized Scan。
- **DataChunk Assembly**（核心）：多个 Parquet Column Reader 直接填充同一个
  DuckDB DataChunk，每列一个 Vector，不需要横向 materialize。缺分区区间 NULL 填充。
- **并行执行**：任务单位 = Aligned Row Group，共享游标按连续 Range 发放。
- **Metadata Cache**：复用 DuckDB ObjectCache（LRU），footer/schema/RG stats
  跨查询跨线程共享。

---

## 7. Writer（aligned_create / aligned_compact / aligned_drop / CREATE TABLE / DML）

```
aligned_scan(table, root=...)                    → (table columns)
aligned_create(table, group, columns, root=..., partition_template=...)  → (dirs_created, files_created, txid)
aligned_compact(table, group_name, root=...)     → (dirs_compacted, parts_before, parts_after)
aligned_drop(table, group_name, root=...)         → (dirs_removed, files_removed, txid)
```

- **标准 DML**：ATTACH 后直接用 `INSERT` / `UPDATE` / `DELETE` 操作
  `al.<table>`，内部通过 `AlignedUpsertFromCollection` /
  `AlignedDeleteFromCollection` 直写 parquet 列组。`aligned_upsert` /
  `aligned_delete` 表函数已删除——标准 DML 完全替代。

- **v8 mutator**：按主键 `(symbol, date)` 插入/更新/删除，只重写受影响 part。
  删除逻辑：删空单 part 分区 → 整分区移除；删空多 part 分区最高索引 part →
  直接移除该 part；删空多 part 分区中间 part → 原地重写为 0 行空文件
  （保留文件名索引，保持索引连续）。
- **Atomic Commit**：先写 `_tmp/transaction-<id>/`，全部成功后 move 成正式
  partition；崩溃则丢弃（`_tmp/` 对 Reader 不可见）。无 sidecar、无 marker。
- **映射列类型 = 组内已存类型**（组 schema），非源文件类型——跨 part 列类型必须
  一致；首写空表回退源类型。新分区只建在映射过的 Group。未知 mapping Group →
  BinderException fail-fast。mapping 可选（已存在表按列名自动推断）。
- **Schema Evolution**：新列在老 part 上读到 NULL，不重写历史。
- **Append-to-Last-Part**（`ALIGNED_DEFAULT_PART_ROWS = 1048576`）：upsert 追加
  到末 part 时若行数 < 阈值则重写末 part 并追加（减少碎片化）；跨组一致性预检，
  任一不满足则整分区回退新建 part。
- **并发写互斥**（`.aligned_write.lock`）：mutator/compactor 执行前创建 lock 文件
  （RAII），已有 lock 则拒绝。crash 残留用户手动删除。
- **Compaction**：`aligned_compact(table, 'all')` 单事务合并所有组，原子切换。
  同目录必须同列集（拒绝 schema-evolution 合并）。**两阶段提交**：所有组的合并
  part 先写入 `_tmp/`，全部成功后再统一 move 到目标目录 + 删除旧 part；任一组
  合并失败则清理 `_tmp`、表状态不变（旧 part 仍在原位）。
  **规范化重写**：每个分区的所有 part 按 `ALIGNED_DEFAULT_PART_ROWS` (1M)
  重新切分——前面的 part 满行（恰好 1M 行），末 part ≤ 1M 行。0 行占位 part
  被合并吸收。已规范化的分区（单 part ≤ 1M，或多 part 均满行）跳过不重写
  （`IsAlreadyNormalized` 检查）。

### CREATE TABLE DDL

```sql
-- 建表（0 行占位 parquet，footer 携带 schema）
CREATE TABLE al.<table> (symbol VARCHAR, date DATE, ...)
  WITH (groups='index:close;factor/alpha:alpha001', partition_template='month=%Y-%m');

-- 已有表创建空分区
CREATE TABLE al.<table> (cols...) WITH (partition='month=2026-10');

-- 列组扩展（N 行全 NULL 占位，N = index 分区行数）
CREATE TABLE al.<table> (ma5 DOUBLE, ma20 DOUBLE) WITH (groups='fieldset/ma:ma5,ma20');
```

- 前两列必须 (symbol VARCHAR, date DATE/TIMESTAMP)（v8 主键契约）。
- `groups` 指定列→Group 映射；不指定时所有非 key 列默认放 index；非 index 组名
  必须是 `lv1/lv2` 两级路径。
- `partition_template` 默认 `month=%Y-%m`，可选 `date=%Y-%m-%d` / `year=%Y`。
- 列组扩展：表已存在时指定至少一个非 index Group；新 Group 的每个已存在分区
  写 N 行全 NULL 占位 parquet（满足分区对齐契约）；已有 Group 不被触碰。

### 标准 DML（DuckLake 式逻辑 Attach）

`ATTACH '<root>' AS al (TYPE ALIGNED)` → 表保持逻辑表，SELECT 走 aligned 扫描，
标准 INSERT/UPDATE/DELETE 通过 catalog 的 PlanInsert/PlanUpdate/PlanDelete 钩子
直写 parquet 列组。`DETACH al;` 卸载。

**注意**：v1.5.4 的标准 DML 算子硬绑定 `DuckTableEntry`+`DataTable`，自定义存储
引擎无法作为扩展承接标准 DML 写。方案是通过 catalog 的 PlanInsert/PlanDelete/
PlanUpdate 钩子返回自定义 sink 算子，直接调 mutator 的 C++ API。

---

## 8. 不做

Tombstone/Delta、类型升级、聚合下推（依赖 DuckDB ≥ v1.6 API）。

---

## 9. 技术栈

- Query Engine：**DuckDB Extension**（v1.5.4，SQL/Planner/Table Function/Execution/DataChunk/Parallelism）
- Storage：**Parquet**（Encoding/Compression/Statistics/Column Storage/Row Groups）
- 语言：**C++ / DuckDB / Parquet**

---

## 10. 代码结构

```
extension/aligned/src/
├── extension.cpp
├── catalog/       manifest.cpp  aligned_catalog.cpp  aligned_create.cpp  aligned_create_fn.cpp
├── resolver/      partition_resolver.cpp  key_resolver.cpp
├── scan/          aligned_scan.cpp
├── mutator/       aligned_mutator.cpp
├── rewriter/      part_rewriter.cpp
├── compaction/    aligned_compactor.cpp  aligned_drop.cpp
├── io/            parquet_io.cpp
├── execution/     aligned_dml.cpp
└── transaction/   aligned_transaction.cpp
```

对应 `src/include/` 下头文件。

---

## 11. 环境与构建

### Windows（scoop + MSVC）
- DuckDB v1.5.4 源码 vendored 在 `duckdb/`（gitignored）
- 构建：`cmd /c "D:\proj\factorlake\duckdb\build3\build_al3.bat"`（vcvars64 + ninja）
- 产物：`duckdb/build3/duckdb_al3.exe`
- 测试：`python test/run_sqllogictest.py` + `scripts\test_*.ps1`

### Linux（brew + gcc + Ninja）
- DuckDB v1.5.4 源码在 `duckdb/`（gitignored）
- 构建：`cmake -G Ninja + ninja`（通过 `scripts/aligned_extension_config.cmake` 注册）
- 产物：`duckdb/build/duckdb`

### 测试
- SQLLogicTest：`python test/run_sqllogictest.py`（auto-discover `test/aligned/*.test`）
- PS 脚本：test_aligned 42/42、test_dml 10/10（含 1.1M 行批量 INSERT 测试）、test_compaction 16/16、test_parallel 8/8
- 当前总：SQLLogicTest 118/118 + 4 PS 套件全 PASS

### 扩展发布
- `extension/aligned/CMakeLists.txt` 加 `build_loadable_extension`
- 发布构建：`-DEXTENSION_STATIC_BUILD=1`，产物 24MB 自包含
- 无签名扩展需 `duckdb -unsigned`；`INSTALL '<url>'` + `LOAD aligned`
- 详见 `docs/EXTENSION_RELEASE.md`

---

## 12. 关键经验（必须记住的坑）

### DuckDB API 陷阱

- **`Copy(source, target, source_count, source_offset, target_offset)`**：
  `source_count` 是排他结束下标，拷贝行数 = `source_count - source_offset`。
  传"行数"会在 `source_offset > count` 时下溢 → 越界崩溃。
- **Parquet 字符串/字典是零拷贝**：`DictionaryDecoder::Read` 在 `result_offset==0`
  时把结果向量做成 DICTIONARY_VECTOR。**每个 RG 窗口必须用全新的
  `ParquetReaderScanState` 和 `DataChunk`**（跨窗口复用会 UAF）。
- **`table_function_t` 返回 void**，用 `output.SetCardinality(0)` 表示结束。
- **`ParquetReader::Scan` 切换 Row Group 时先返回空 chunk**（setup call），不算结束。
- **表函数默认单线程**：必须 override `MaxThreads()` 返回 `MAX_THREADS`。
- **表函数必须以「输出 0 行」结��**：否则 DuckDB 无限重复调用 → CLI 卡死。
- **parquet 流按整向量（2048 行）前进**，与"实际放置行数"解耦。窗口复用需 carry
  buffer 或重新 InitializeScan + discard 到目标行。
- **parquet 读取的分区列可能是 DICTIONARY_VECTOR**：必须 `ToUnifiedFormat`
  再 `vdata.sel->get_index(r)` 取值，不能直接 `FlatVector::GetData`。
- **`ParquetWriter` 无 WriteChunk**：用 `ColumnDataCollection` +
  `InitializeAppend/Append`，满 RGS 时 `Flush` + `Reset`，结束 `Finalize`。
- **`FileFlags::FILE_FLAGS_FILE_CREATE` 不截断**：重写文件需先 `handle->Truncate(0)`。
- **`FileSystem::CreateLocal()` 返回 unique_ptr**：不能 `auto &fs = *CreateLocal()`
  （悬垂引用），必须保持所有权。
- **catalog 表扫描的列请求 ≠ 表函数**：executor 按 `projection_ids` 分配输出；
  `column_ids` 可能含重复列和虚拟 rowid(-1)。
- **`Connection(DatabaseInstance::GetDatabase(context))`**：需要 include database.hpp。
- **`Parser::ParseColumnList` 返回 UNBOUND 类型**：解析列定义字符串后
  `ColumnDefinition::Type().id()` 是 `LogicalTypeId::UNBOUND`（非 VARCHAR/DATE），
  必须用 `TransformStringToLogicalType(type.ToString(), context)` 解析为具体类型
  后才能用于 schema 校验或 ParquetWriter。
- **`TableWriteLock` 在目录中创建 `.aligned_write.lock`**：递归计数目录文件时
  须跳过此文件（RAII 析构前它仍存在）。

### 构建陷阱

- **切分支后必须全量重编扩展**：`git checkout` 后 ninja 增量重编产物会损坏
  （新旧布局混链 → 崩溃）。删除所有 obj 后重编。
- **改共享头文件（结构体布局）后必须强制全量重编**：ninja 增量会漏重编依赖该
  头的 obj → 新旧布局混链 → 隐蔽崩溃。
- **`git stash` 会把子模块本地补丁也 stash 进子模块自己的 stash 队列**：主仓
  stash list 看不到。避免在含子模块补丁的仓库用 `git stash`。

### 数据正确性陷阱

- **`ScanGroupWindow` 的 src_offset**：窗口从 RG 中部开始时必须用
  `seg.flow_off + (copy_from - seg.win_start) - rg_off`，不是简单的
  `copy_from - win_pos`。
- **copy 重叠必须双向 clamp**：`copy_from = max(w_start, win_pos)`、
  `copy_to = min(w_end, win_pos + valid_len)`。只 clamp 一端会下溢成巨大数 → 堆损坏。
- **并行 claim 粒度必须是连续 Range**：每 chunk 一个 claim 会频繁重定位，吃掉
  全部收益。按 16 chunks 发 Range → 8 线程 4.2×。
- **不要依赖 count(rowid)/count(\*) 诊断数据错误**：DuckDB 会用 Cardinality
  统计优化掉扫描。用 `sum(rowid)` + `row_number() OVER ()` 定位。

### 工具链陷阱

- **PS 5.1 stderr 78 列折行**：错误消息会被拆行，断言 pattern 须用折行点之前的子串。
- **PS 5.1 `-c` 无法携带引号标识符**：走临时文件 + `cmd /c "exe < file"`。
- **DuckDB CLI `-csv` 输出 NULL 是字面 `NULL`**（不是空串）。
- **DuckDB COPY TO PARQUET 的 ROW_GROUP_SIZE 不精确**：writer 按 vector 大小 flush。
