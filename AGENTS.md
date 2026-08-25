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
  **TIMESTAMP 键**（v9）：当 col1 为 TIMESTAMP 时，键为完整 timestamp 值
  （微秒级），不截断为日期——同一天内同一标的的多个时间戳（如分钟 K 线）是不同
  键。KeyResolver 内部键类型为 `int64_t`（兼容 date_t 和 timestamp_t），分区目录
  求值时自动提取日期部分。
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

## 7. Writer（COPY TO / aligned_create / aligned_compact / aligned_drop / CREATE TABLE / DML）

```
aligned_scan(table, root=...)                    → (table columns)
aligned_groups(table, root=...)                  → (group_name, columns, partition_count)
  # 注意：aligned_groups 的 columns 输出用分号 `;` 分隔列名（避免与
  # SQLLogicTest 的逗号分隔输出格式混淆），而 groups 选项映射字符串和
  # aligned_create 的 columns 参数用逗号 `,` 分隔列名。两个分隔符不同
  # 是设计选择，不是 bug。
aligned_create(table, group, columns, root=..., partition_template=...)  → (dirs_created, files_created, txid)
aligned_create(table, group, root=..., partition_template=...)            → (dirs_created, files_created, txid)  -- 2-arg 空组
aligned_compact(table, group_name, root=...)     → (dirs_compacted, parts_before, parts_after)
aligned_drop(table, group_name, root=...)         → (dirs_removed, files_removed, txid)
```

- **COPY TO (FORMAT aligned)**：批量写入主路径，走 DuckDB CopyFunction 框架。

```sql
-- 新组首次写入（schema 从 query 推断，排除 index key 列）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');

-- 已有组写入（schema 从 last parquet footer 读取，列裁剪到组内列）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday' (FORMAT aligned, GROUP 'index');
```

  - **写入 pipeline 架构**（单向数据流，唯一写入路径）：

    ```
    input chunk (sorted by symbol, date)
        ↓
    Sink: run detection → project 到 group schema → 累积到 per-partition RG buffer
        ↓   (PARALLEL_COPY_TO_FILE，多线程并行 source reader + per-thread Sink)
        ↓   buffer 满 (131072 rows) → PushJob 到 background FlushWorker (FIFO queue)
    Combine: push 剩余 buffer + sentinel
        ↓
    FlushWorker (N threads): PopJob → ParquetWriter::Flush (1 RG)
        ↓   线程私有 PerPartitionState (无锁)，partition affinity (round-robin)
        ↓   满 row_groups_per_file (8) → 轮转 part 文件
    Finalize: join threads → 检查 error → 统计校验 (received == written)
    ```

  - **per-partition 覆盖**：每个分区目录首次写入时自动清理旧 parquet 文件
    （无需 `OVERWRITE true`）。
  - **自描述文件名**：先以 `0000-0000000000.parquet` 写入，Finalize 后 rename 为
    `{idx:04d}-{rows:10d}.parquet`（实际行数）。0 行空文件自动删除。
  - **RG / Part 切分**：Row Group flush size = 131072；part 文件上限 = 8 RG
    = 1048576 行（`ALIGNED_DEFAULT_PART_ROWS`）。满 8 RG 轮转新 part。
  - **排序**：**`PARALLEL_COPY_TO_FILE`（多线程并行 source reader）+ FlushWorker
    端全局 (symbol, date) 排序**——输入按 `(symbol, date)` 排序则分区内输出**严格**
    保持排序（0 乱序行）。`PARALLEL_COPY_TO_FILE` 让 parquet source reader 用 8 线程
    并行扫描（~2.5× 加速：15.9s→6.5s），多线程 Sink 各自维护 per-thread per-partition
    RG buffer（无锁），满 RG_SIZE 后推到 background FlushWorker。FlushWorker 在 sentinel
    到达后收集每个分区的所有 buffer，合并后按 (symbol, date) 全局排序再 flush——
    消除 morsel 乱序。partition affinity（round-robin）保证同一分区的数据只由一个
    FlushWorker 线程写入。`PartitionedColumnData` 的 hash 分区不保序，aligned COPY
    不使用它——Sink 直接做 run detection + project 到 group schema。
    **排序优化**：symbol 字符串→int32 字典编码（unordered_map + lexicographic
    remap）后用 `std::stable_sort`（对近排序数据 ~O(n) 比较次数），比 `std::sort`
    + 字符串比较快 ~3×（perm 46.9s→13.2s）。
  - **列裁剪**：只写 group schema 包含的列，按 group schema 顺序重排。
    输入列类型 ≠ 组 schema 类型时自动 cast（如 TIMESTAMP → DATE）。
  - **统计校验**：每个 PartitionWriter 跟踪 `received_rows` / `flushed_rows` /
    `written_rows`，Finalize 时校验 `received == written`，不匹配抛
    `InternalException`。
  - **新组推断**：`aligned_create('table', 'group')` 2-arg 形式创建空组目录，
    首次 COPY 时从 query 列推断 schema（排除 index key 列 symbol/date）。

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

**注意**：v1.5.5 的标准 DML 算子硬绑定 `DuckTableEntry`+`DataTable`，自定义存储
引擎无法作为扩展承接标准 DML 写。方案是通过 catalog 的 PlanInsert/PlanDelete/
PlanUpdate 钩子返回自定义 sink 算子，直接调 mutator 的 C++ API。

---

## 8. 不做

Tombstone/Delta、类型升级、聚合下推（依赖 DuckDB ≥ v1.6 API）。

---

## 9. 技术栈

- Query Engine：**DuckDB Extension**（v1.5.5，SQL/Planner/Table Function/Execution/DataChunk/Parallelism）
- Storage：**Parquet**（Encoding/Compression/Statistics/Column Storage/Row Groups）
- 语言：**C++ / DuckDB / Parquet**

---

## 10. 代码结构

```
extension/aligned/src/
├── extension.cpp
├── catalog/       manifest.cpp  aligned_catalog.cpp  aligned_create.cpp  aligned_create_fn.cpp  aligned_groups.cpp
├── resolver/      partition_resolver.cpp  key_resolver.cpp
├── scan/          aligned_scan.cpp
├── copy/          aligned_copy.cpp
├── mutator/       aligned_mutator.cpp
├── rewriter/      part_rewriter.cpp
├── compaction/    aligned_compactor.cpp  aligned_drop.cpp
├── io/            parquet_io.cpp
├── execution/     aligned_dml.cpp
└── transaction/   aligned_transaction.cpp
```

对应 `src/include/` 下头文件。

### 共享工具函数

| 模块 | 函数 | 说明 |
|------|------|------|
| `io/parquet_io` | `CreateParquetWriter` | 统一 ParquetWriter 构造参数（ZSTD/V1） |
| | `FormatPartName` / `ParsePartName` | `{idx:04d}-{rows:10d}.parquet` 唯一格式化/解析 |
| | `WriteEmptyParquet` / `WriteNullParquet` | 0 行占位 / N 行全 NULL 占位写入 |
| | `OpenPartReaderAllColumns` / `OpenPartReaderNamedColumns` | 打开 ParquetReader + 初始化扫描 |
| | `CountRecursive` | 递归计数目录/文件（跳过 `.aligned_write.lock`） |
| `catalog/manifest` | `ResolveDataRoot` | root 参数或 `aligned_data_root` 设置解析 |
| | `IndexGroup` | 返回 `plan.groups[0]`（index 组不变量） |
| | `NextPartIndexForPartition` | 跨组最大 partition_index + 1 |
| `resolver/partition_resolver` | `EvaluatePartitionTemplate` / `IsKnownTemplate` | 三种模板求值/校验 |
| | `DefaultPartitionKey` / `ValidatePartitionKey` | 默认分区键 / 分区键校验 |
| `copy/aligned_copy` | `GetAlignedCopyFunction` | 注册 FORMAT aligned CopyFunction（Sink/Combine/Finalize pipeline） |
| | `FlushWorker` | 后台线程 + 线程私有 PerPartitionState（无锁热路径） |
| | `AlignedCopyGlobalState` | 拥有 N 个 FlushWorker 线程池 + partition→worker affinity |
| | `AlignedCopySink` | 并行 run detection + project + push RG buffer 到 FlushWorker |
| | `FlushWorker::WorkerLoop` | PopJob → ParquetWriter::Flush（线程私有，无锁）→ 轮转 part 文件 |
| `mutator/aligned_mutator` | `StagedTransaction` | RAII 暂存事务（锁 + txid + `_tmp/` 清理） |
| | `NextTransactionId` | 共享事务号计数器 |
| | `ExtractSortedRows` | 向量化提取 (symbol, date) 排序键 |

---

## 11. 环境与构建

### Windows（scoop + MSVC）
- DuckDB v1.5.5 源码 vendored 在 `duckdb/`（gitignored）
- 构建整体：`.\scripts\build.ps1`（vcvars64 + ninja）
- 构建插件：`.\scripts\build_extension.ps1 -Copy`（Release + `-DEXTENSION_STATIC_BUILD=1`）
- 运行测试：`.\scripts\run_tests.ps1`
- 产物：`duckdb/build3/duckdb_al3.exe`、`release/aligned.duckdb_extension`

### Linux（brew + gcc + Ninja）
- DuckDB v1.5.5 源码在 `duckdb/`（gitignored）
- 构建：`cmake -G Ninja + ninja`（通过 `scripts/aligned_extension_config.cmake` 注册）
- 产物：`duckdb/build/duckdb`

### 目录职责
- `scripts/`：构建入口（`build.ps1`、`build_extension.ps1`、`run_tests.ps1`）+ cmake 配置
- `test/`：SQLLogicTest（`run_sqllogictest.py` + `aligned/*.test`）、PS 验收脚本（`test_*.ps1`）、测试数据生成（`gen_*.ps1/.sh`）、基准测试（`bench_*.ps1/.sh/.py`）

### 测试
- SQLLogicTest：`python test/run_sqllogictest.py`（auto-discover `test/aligned/*.test`）
- PS 脚本（位于 `test/`）：test_aligned 42/42、test_dml 10/10（含 1.1M 行批量 INSERT 测试）、test_compaction 16/16、test_parallel 8/8
- 当前总：SQLLogicTest 271/271 + 4 PS 套件全 PASS

### 扩展发布
- `extension/aligned/CMakeLists.txt` 加 `build_loadable_extension`
- 发布构建：`-DEXTENSION_STATIC_BUILD=1`，产物 23MB 自包含
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
- **`ParquetWriter::Flush` 会消耗 ColumnDataCollection 的行计数**：`Flush` 后
  `buffer.Count()` 返回 0，必须在 Flush 前保存 `idx_t rows = buffer.Count()`，
  不可在 Flush 后再读 `buffer.Count()` 作为行数。Combine 中按 partition 逐个 Flush
  时尤其注意。
- **CopyFunction `REGULAR_COPY_TO_FILE` 下 `write_empty_file` 默认 true**：
  `GetGlobalSinkState` 会立即调 `copy_to_initialize_global`；`write_empty_file=false`
  时延迟到 `Sink` 首个 chunk 才调。两种模式都必须支持 global state 为 null 直到
  首次 Sink——不要假设 global state 一定在 sink 前已初始化。
- **`PhysicalCopyToFile::CheckDirectory` 在 partition_output/per_thread_output/
  rotate 路径才会调**：`REGULAR_COPY_TO_FILE`（无 partition/per_thread/rotate）
  不调 `CheckDirectory`，所以 OVERWRITE 选项不会删除目标目录文件。自定义的
  per-partition 覆盖须在 sink/flush 内自行实现。
- **`ParquetWriteTransformData` 是 `class` 不是 `struct`**（parquet_writer.hpp）：
  前向声明必须写 `class`，否则 MSVC 链接器 mangling 不同（`U` vs `V`）→ LNK2019。
- **Windows `MoveFile` 宏污染**：`windows.h` 把 `MoveFile` 宏定义为 `MoveFileA`，
  即使 `file_system.hpp` 有 `#undef MoveFile`，parquet_writer.hpp 等头文件间接包含
  windows.h 会重新定义。必须在所有 `#include` 之后、使用 `fs.MoveFile()` 之前再
  `#undef MoveFile`。
- **DuckDB 表函数不支持可选位置参数**：`aligned_create` 2-arg 和 3-arg 必须
  注册为 `TableFunctionSet` 两个独立 overload，不能用单个函数 + 默认参数。
- **`generate_series(DATE, DATE, INTERVAL)` 返回 TIMESTAMP 不是 DATE**：sink 中
  分区列读取时必须检查 `part_vec.GetType().id() == TIMESTAMP`，用 `int64_t`
  读取；DATE 列用 `int32_t`。类型不匹配时 `VectorOperations::Copy` 会崩溃。
- **`PartitionedColumnData::ComputePartitionIndices` 必须在注册新分区时创建
  partition collection**：基类 `Append` 不负责创建 partition——`ComputePartitionIndices`
  调用 `RegisterPartition` 时必须同步初始化 `partitions[id]`、`partition_append_states[id]`、
  `partition_buffers[id]`（参照 `HivePartitionedColumnData::AddNewPartition`）。
  否则 `AppendInternal` 访问空 `partitions[id]` → "Attempted to access index 0
  within vector of size 0" 崩溃。
- **`PartitionedColumnDataAppendState::partition_indices` 容量 = STANDARD_VECTOR_SIZE
  (2048)**：构造为 `Vector(LogicalType::UBIGINT)`，默认 FLAT_VECTOR，buffer 有 2048
  槽。`ComputePartitionIndices` 可直接 `FlatVector::GetData<idx_t>` 写入，无需
  `SetVectorType` 或 `Flatten`。
- **`COPY TO PARQUET` 的 `PARTITION_BY` 列必须在 `expected_types` 中**：DuckDB 原生
  `HivePartitionedColumnData` 包含全量列（含分区列），flush 时用 `SetDataWithoutPartitions`
  剥离分区列。aligned COPY 不使用 `PartitionedColumnData`（Sink 直接 run detection
  + project 到 group schema），列映射/cast 在 Sink 中完成（排除 key 列后 flush
  到 ParquetWriter）。

### 构建陷阱

- **切分支后必须全量重编扩展**：`git checkout` 后 ninja 增量重编产物会损坏
  （新旧布局混链 → 崩溃）。删除所有 obj 后重编。
- **改共享头文件（结构体布局）后必须强制全量重编**：ninja 增量会漏重编依赖该
  头的 obj → 新旧布局混链 → 隐蔽崩溃。
- **`git stash` 会把子模块本地补丁也 stash 进子模块自己的 stash 队列**：主仓
  stash list 看不到。避免在含子模块补丁的仓库用 `git stash`。

- **`ColumnDataCollection` 不能按值返回**：`ColumnDataCollection` 含
  `vector<ColumnDataCopyFunction>` 成员，`ColumnDataCopyFunction` 在头文件中仅
  前向声明。按值返回/传参会触发 `vector` 的拷贝/移动构造，需要完整类型定义
  → MSVC C2036 编译错误。必须用输出参数（`ColumnDataCollection &output`）或
  `unique_ptr` 传递。

### 数据正确性陷阱

- **`PartitionedColumnData` 不保证分区内行序**：hash 分区的 `AppendInternal`
  按 partition 遍历（不是按行序），128-row buffer 跨 chunk 累积后 flush 顺序
  与输入行序无关。即使输入 `ORDER BY (symbol, date)`，并行模式下分区内仍会
  产生乱序。DuckDB 原生 `COPY TO PARQUET PARTITION_BY` 也有同样问题
  （`plan_copy_to_file.cpp` 强制 `preserve_insertion_order=false`，且禁止
  `PRESERVE_ORDER`）。**aligned COPY 的解法**：不使用 `PartitionedColumnData`
  做 hash 分区——Sink 直接做 run detection + project 到 group schema。
  `PARALLEL_COPY_TO_FILE` 让 source reader 多线程并行，多线程 Sink 各自维护
  per-thread per-partition buffer；FlushWorker 在 sentinel 到达后收集每个分区的
  所有 buffer，合并后按 (symbol, date) 全局排序再 flush——消除 morsel 乱序，
  实现并行 source reader + 0 乱序行。
  **`REGULAR_COPY_TO_FILE` vs `PARALLEL_COPY_TO_FILE` 线程模型**：
  `REGULAR` → `ParallelSink()` 返回 false → `ScheduleSequentialTask()` →
  整条 pipeline（含 source reader）单线程；`PARALLEL` → `ParallelSink()` 返回
  true → pipeline 并行（source reader + Sink 均多线程）。同一批 pipeline 线程
  同时做 source 读取和 Sink 处理，因此用 mutex 串行化 Sink 会连带串行化 source
  reader（实测 16s，与 REGULAR 相同），无法只并行 source 不并行 Sink。
  **FlushWorker 端排序的类型安全**：排序时提取 date 列值必须从**实际 Vector
  类型**判断 INT64（TIMESTAMP）还是 INT32（DATE），不能用 `bind_data.is_timestamp`
  （反映**输入**类型，Sink cast 后**输出**类型可能不同，如 TIMESTAMP→DATE cast）。

- **`ScanGroupWindow` 的 src_offset**：窗口从 RG 中部开始时必须用
  `seg.flow_off + (copy_from - seg.win_start) - rg_off`，不是简单的
  `copy_from - win_pos`。
- **copy 重叠必须双向 clamp**：`copy_from = max(w_start, win_pos)`、
  `copy_to = min(w_end, win_pos + valid_len)`。只 clamp 一端会下溢成巨大数 → 堆损坏。
- **并行 claim 粒度必须是连续 Range**：每 chunk 一个 claim 会频繁重定位，吃掉
  全部收益。按 16 chunks 发 Range → 8 线程 4.2×。
- **不要依赖 count(rowid)/count(\*) 诊断数据错误**：DuckDB 会用 Cardinality
  统计优化掉扫描。用 `sum(rowid)` + `row_number() OVER ()` 定位。
- **KeyResolver 定位粒度 = part 文件，不是 RowGroup**：重写的最小单位是 Parquet
  文件（part），所以用 part 级别的 symbol [min, max] 范围做二分查找即可定位受影响
  文件。不需要加载 part 内部数据在 RowGroup 级别做二分查找——一旦确定某个 part
  受影响，整个 part 就要被重写。`LoadSinglePart` 只在需要区分 insert vs update 时
  加载单个 part 的键数据（O(1 part)），而非 `LoadPartition`（O(N parts)）。
- **KeyResolver 缓存标志必须在加载成功后设置**：`loaded` / `boundary_loaded` /
  `part_loaded` 标志必须在 Scan 循环成功完成后才设为 true。如果在循环前设置，
  循环抛异常（corrupt parquet、IO error）后标志仍为 true，后续调用跳过加载，
  使用空/不完整统计 → 键定位错误（所有键被判为 insert，或错误的 part 定位）。

### 工具链陷阱

- **PS 5.1 stderr 78 列折行**：错误消息会被拆行，断言 pattern 须用折行点之前的子串。
- **PS 5.1 `-c` 无法携带引号标识符**：走临时文件 + `cmd /c "exe < file"`。
- **DuckDB CLI `-csv` 输出 NULL 是字面 `NULL`**（不是空串）。
- **DuckDB COPY TO PARQUET 的 ROW_GROUP_SIZE 不精确**：writer 按 vector 大小 flush。
- **`EvaluatePartitionTemplate` int64_t 版本的 date/timestamp 启发式**：
  `value >= 0 && value <= 200000` 判为 date_t，否则判为 timestamp_t。1970 年
  之前的日期（负 date_t）和 epoch 后 200000 微秒内的 timestamp_t 会被误判。
  **已修复**：新增 `is_timestamp` 类型安全重载，所有调用方（`key_resolver`、
  `aligned_mutator`、`aligned_copy`）都已改用类型安全版本。旧的启发式重载
  保留向后兼容但已弃用。
- **读路径列匹配必须大小写不敏感**：`OpenPart` 和 `ComputeRowGroupWindow` 中
  查找 parquet reader 列名必须用 `StringUtil::CIEquals`，与 `parquet_io.cpp`
  一致。跨 part 列名大小写不一致（如 `Symbol` vs `symbol`）不应被静默 NULL 填充。
- **`partition_col_pos` 必须初始化为 `INVALID_INDEX`**：`AlignedCopyBindData`
  的 `partition_col_pos` 默认值必须为 `DConstants::INVALID_INDEX`，guard 必须用
  `== INVALID_INDEX`。默认 0 会在分区列缺失时静默使用第 0 列。
- **`EnsureTablesLoaded` 只 catch `IOException`**：不应 catch `std::exception`，
  否则 `InternalException`、`PermissionException` 被静默吞掉，真实错误隐藏。
- **`Date::FromString` 可能 throw**：`partition_resolver` 的 `ExtractPartitionDate`
  调用 `Date::FromString` 处理分区目录名时必须 try/catch，畸形目录名应跳过剪枝
  而非 abort 整个 scan。
- **`ParquetReader::Scan` 返回 `BLOCKED` 不是错误**：对象存储异步 I/O 可能返回
  `BLOCKED`（数据未就绪），应 `continue` 重试而非 `break`/throw。本地文件不会
  触发此路径。**所有** Scan 循环都已修复：scan、compactor（`MergePartsToWriter`、
  多 part 分流）、`part_rewriter`（`FetchOldChunk`）、
  `key_resolver`（`LoadPartition`、`LoadSinglePart`）、`aligned_mutator`。
- **Compaction Phase 1 并行化**：各分区目录的暂存独立（各自有 reader/writer），
  可并行处理。线程池用 `std::thread` + `std::atomic` work queue，每个 worker
  创建自己的 `ParquetReader`/`ParquetWriter`/`ScanState`/`DataChunk`/`Collection`。
  Phase 2（move + delete）仍串行。worker 异常用 `std::exception_ptr` 捕获并在
  `join()` 后在主线程 rethrow。
- **`GlobFiles` 在 Windows 可能返回反斜杠路径**：对 glob 结果做字符串搜索时
  （如排除 `_tmp/`），必须先 `std::replace(norm.begin(), norm.end(), '\\', '/')`
  归一化为正斜杠再搜索。`aligned_create.cpp` 和 `aligned_create_fn.cpp` 的
  table-exists 检测都已修复。
