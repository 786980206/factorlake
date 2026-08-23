# INSERT 写入路径详解

> 本文档梳理 `INSERT INTO al.<table>` 从 SQL 到 Parquet 文件落盘的完整逻辑。
> 对应代码：`extension/aligned/src/execution/aligned_dml.cpp`、
> `extension/aligned/src/catalog/aligned_catalog.cpp`、
> `extension/aligned/src/mutator/aligned_mutator.cpp`、
> `extension/aligned/src/resolver/key_resolver.cpp`、
> `extension/aligned/src/rewriter/part_rewriter.cpp`

---

## 总览

```
SQL: INSERT INTO al.t (symbol, date, close, alpha001) VALUES (...)
  │
  ▼
┌─────────────────────────────────────────────┐
│ 1. PlanInsert (catalog)                      │
│    解析列列表 → 构建 explicit_mapping        │
└──────────────────┬──────────────────────────┘
                   ▼
┌─────────────────────────────────────────────┐
│ 2. PhysicalAlignedInsert::Sink+Finalize      │
│    收集行到 ColumnDataCollection             │
│    ResolveDefaultsProjection 填充未指定列     │
└──────────────────┬──────────────────────────┘
                   ▼
┌─────────────────────────────────────────────┐
│ 3. AlignedUpsertFromCollection (mutator API) │
│    BuildUpsertBindFromCollection:            │
│      BuildTablePlan (读目录发现 groups)      │
│      ParseMapping → group_mapping            │
│      构建 needed_names (只读需要的列)         │
└──────────────────┬──────────────────────────┘
                   ▼
┌─────────────────────────────────────────────┐
│ 4. AlignedUpsertFunction (核心写入逻辑)      │
│    a. 读取源数据 → ColumnDataCollection      │
│    b. 定位键列 (symbol, date)                │
│    c. ExtractSortedRows + SortAndDedupe      │
│    d. KeyResolver::Resolve (定位每个键)       │
│    e. Dispatch → MutateTarget 缓冲区          │
│    f. ExecuteAndCommit → RewritePart 并行     │
└─────────────────────────────────────────────┘
```

---

## 第 1 步：PlanInsert — 构建 explicit_mapping

**文件**：`catalog/aligned_catalog.cpp` → `AlignedCatalog::PlanInsert`

当用户执行 `INSERT INTO al.t (symbol, date, close, alpha001) VALUES (...)` 时：

1. DuckDB planner 生成 `LogicalInsert`，其中 `column_index_map` 记录了哪些物理列
   被用户显式指定（`!= INVALID_INDEX`），哪些用默认值填充。

2. `PlanInsert` 检查 `column_index_map` 是否非空（用户指定了列列表）：
   - 如果非空，调用 `BuildTablePlan` 获取表的 group 结构（哪些列属于哪个 group）。
   - 遍历每个 group，检查该 group 的列中哪些被用户显式指定。
   - 构建 mapping 字符串，如 `"index:symbol,date,close;factor/alpha:alpha001"`。
   - 只包含有显式列的 group —— 未被用户指定的 group 不出现在 mapping 中。

3. 将 mapping 传给 `PhysicalAlignedInsert` 构造函数，存为 `explicit_mapping` 字段。

**关键优化**：如果不做这一步，DML 层的 `ResolveDefaultsProjection` 会用 NULL 填充
未指定的列，导致源数据包含全部列。mutator 走自动推导路径时，会把所有列都映射到
各自的 group，导致没有数据变化的 group 也被重写。

---

## 第 2 步：PhysicalAlignedInsert — 收集数据

**文件**：`execution/aligned_dml.cpp` → `PhysicalAlignedInsert`

1. `ResolveDefaultsProjection` 在子计划上添加 Projection，用默认值填充未指定的列。
   输出 chunk 包含表的全部列（已填充默认值）。

2. `Sink()`：每个线程收集 chunk 到本地 `ColumnDataCollection`。

3. `Combine()`：合并本地 collection 到全局 collection。

4. `Finalize()`：
   - 如果行数 ≤ 1M：一次调用 `AlignedUpsertFromCollection(mapping, collection, row_names)`。
   - 如果行数 > 1M：分批调用，每批 1M 行，每批一个独立事务。

---

## 第 3 步：BuildUpsertBindFromCollection — 绑定

**文件**：`mutator/aligned_mutator.cpp` → `BuildUpsertBindFromCollection`

1. **BuildTablePlan**：读目录结构，发现所有 column groups 及其 parts/partitions。
   每个 group 的 `column_order` 列出它拥有的列（从 Parquet footer 读取）。

2. **ParseMapping**：解析 mapping 字符串为 `group → [columns]` 映射。
   - 如果 mapping 为空（用户未指定列列表，如 `INSERT INTO t SELECT * FROM`）：
     走自动推导路径 —— 遍历源 schema 的每列，找到拥有该列的 group。
   - 如果 mapping 非空（用户指定了列列表）：
     每个 group 的 `col_names` = mapping 中该 group 的列。

3. **验证**：index group 的 mapping 必须包含键列 (symbol, date)。

4. **needed_names**：收集所有 group_mapping 中引用的列名（去重）。
   这是后续读取源数据时只读的列 —— 不读未映射的列。

---

## 第 4 步：AlignedUpsertFunction — 核心写入

**文件**：`mutator/aligned_mutator.cpp` → `AlignedUpsertFunction`

### 4a. 读取源数据

```
ReadSourceFromCollection(context, source_collection, needed_names, source_col_names)
```

从源 ColumnDataCollection 中只读取 `needed_names` 指定的列，构建一个新的 collection。
`source_col_names` 是源 collection 的实际列名（= 表的全部列），用于列名→位置查找。

### 4b. 定位键列

在 `needed_names` 中找到 `date_col` 和 `symbol_col` 的位置。
这两个列一定在 needed_names 中（index group 的 mapping 必须包含键列）。

### 4c. ExtractSortedRows + SortAndDedupe

向量化提取每行的 `(partition_key, symbol, date, src_row_index)`，然后按
`(partition_key, symbol, date)` 排序并去重。重复键保留最后一个（后到为准）。

- `partition_key`：由分区模板（如 `month=%Y-%m`）从 date 值求值得到。
- `src_row_index`：该行在源 collection 中的行号，用于后续读取值列。

排序后的 `rows` 数组是后续所有处理的基础。

### 4d. KeyResolver::Resolve — 定位每个键

**文件**：`resolver/key_resolver.cpp`

对每个源行，`Resolve(date, symbol)` 返回 `KeyLocation`，包含：
- `found`：true=更新已有行，false=插入新行
- `partition_key`：分区键
- `part_index`：目标 part 文件索引
- `part_local_row`：在 part 内的行位置
- `append_to_last`：追加到末 part（末 part 未满）
- `append_new_part`：创建新 part（末 part 已满或新分区）

**定位流程**（三层快速路径）：

```
┌──────────────────────────────────────────────────────────┐
│ 第 1 层：分区级快速拒绝（零数据 IO）                       │
│                                                            │
│ LoadPartitionBoundaries: 读每个 part 的 RG stats          │
│ (symbol min/max, 从 Parquet footer, 不读数据)              │
│                                                            │
│ 如果 symbol < partition.part_sym_min[0]                   │
│    或 symbol > partition.part_sym_max[last]               │
│ → 键不在该分区，走 append 路径                             │
│ → 零数据读取！                                            │
└──────────────────────┬───────────────────────────────────┘
                       ▼ symbol 在分区内
┌──────────────────────────────────────────────────────────┐
│ 第 2 层：Part 文件级二分查找（零数据 IO）                  │
│                                                            │
│ 每个 part 有 symbol [min, max] 范围（来自第 1 层）         │
│ 对 symbol_value 做 part 级别二分查找：                    │
│   找到唯一受影响的 part 文件                               │
│                                                            │
│ 关键：重写最小单位 = part 文件，找到文件就够了             │
│ 不需要在 RowGroup 级别做查找                              │
└──────────────────────┬───────────────────────────────────┘
                       ▼ 找到目标 part
┌──────────────────────────────────────────────────────────┐
│ 第 3 层：单 part 数据加载 + 逐行二分查找                    │
│                                                            │
│ LoadSinglePart: 只读这 1 个 part 的 symbol+date 数据       │
│ (O(1 part)，不是 O(N parts))                             │
│                                                            │
│ 在该 part 内部对 (symbol, date) 做二分查找：               │
│   找到 → found=true (更新)                                │
│   没找到 → found=false (插入)                             │
└──────────────────────────────────────────────────────────┘
```

**Fallback**：如果 part stats 缺失或 symbol 落在 part 间隙，
走 `LoadPartition`（加载全分区数据）做全量二分查找。

**分区缓存**：每个分区的数据最多加载一次，缓存在 `PartitionCache` 中。

### 4e. Append-to-last 跨组一致性验证

**文件**：`mutator/aligned_mutator.cpp`，dispatch 循环之前

`Resolve` 可能乐观地设置 `append_to_last=true`（追加到末 part）。但这个决策必须
对所有共享该分区的 group 一致：
- 所有 group 的末 part 必须有相同的 `partition_index` 和 `row_count`
- 末 part 行数必须 < `ALIGNED_DEFAULT_PART_ROWS` (1M)
- 如果 group 有映射列，末 part 必须包含所有映射列（schema evolution 检查）

如果任一条件不满足，该分区的所有 `append_to_last` 被改为 `append_new_part`。

### 4f. Dispatch — 两阶段批量路由

**优化**：dispatch 分为两阶段执行，消除逐行 Append 开销。

**第一阶段（分类）**：遍历所有排序后的行，确定每行的路由目标：
- 哪个 group 需要写入
- 哪个 MutateTarget（按 partition_key + part_index 定位）
- update_buffer 还是 insert_buffer
- part-local position

结果存入 `DispatchEntry` 数组，不读取源值——只做路由判断。

**第二阶段（批量追加）**：按 `(target, is_update)` 分组，用 `BatchAppender` 累积行：
- 每个 `(target, is_update)` 对有一个 `BatchAppender`
- BatchAppender 内部维护一个 `DataChunk scratch`，累积到 `STANDARD_VECTOR_SIZE`（2048）行后 flush 到 `ColumnDataCollection`
- 将 `buffer.Append` 调用次数从 N 次减少到 N/2048 次

```
对每个行 (已排序) → 分类到 DispatchEntry
    │
    ├── 跳过条件检查：
    │   1. loc.found=true 且该组映射列仅含键列 (symbol, date) → 跳过
    │   2. gi>0 且 loc.found=true 且该组无映射列 → 跳过
    │
    ├── 路由决策：
    │   ├── gi==0 → index 组：FindIndexPart
    │   ├── append_to_last → 追加末 part
    │   ├── append_new_part → 新建 part
    │   ├── FindPartByPosition → 按 position 定位 part
    │   ├── fresh + 有映射列 → 新分区
    │   └── found + 有映射列 + 无该分区 → synth
    │
    └── 结果 → DispatchEntry {row_idx, gi, target, is_update, pos}

第二阶段：按 (target, is_update) 分组
    │
    └── BatchAppender.AppendRow(pos, src_pos, reader, src_row)
        → 累积到 scratch chunk (最多 2048 行)
        → 达到 2048 行时 flush 到 ColumnDataCollection
```

### 4g. ExecuteAndCommit — 并行重写 + 原子提交

**文件**：`mutator/aligned_mutator.cpp` → `ExecuteAndCommit`

1. **收集 RewriteTask**：遍历所有 group 的 targets，有 insert/update/delete 的 part
   生成一个 `RewriteTask`。

2. **并行执行 RewritePart**：每个 RewriteTask 独立（自己的 reader/writer/buffer）。
   线程数 = min(tasks, hardware_concurrency)。

3. **RewritePart 逻辑**：

   **快速路径（全量覆盖）**：如果 `input.part` 存在且 `updates->Count() == old_count`
   且无 inserts/deletes，直接从 update buffer 写新 part，**完全不读旧数据**。
   → `RewritePartFullOverwrite`：向量化批量拷贝 mapped 列，未映射列写 NULL。

   **常规路径（读-改-写合并）**：
   ```
   打开旧 part（如果存在）→ 读全列
   创建新 ParquetWriter → 写到 _tmp/transaction-<id>/
   合并循环：
     - BulkCopyOld：连续未修改的旧行，批量拷贝
     - EmitInsertRow：在指定位置插入新行
     - 跳过删除行
     - EmitOldRow：旧行可能有 update 覆盖值
   Finalize → 关闭 writer
   ```

4. **原子提交**：所有 RewritePart 成功后，move `_tmp/` 文件到正式位置，
   删除旧 part。失败则丢弃 `_tmp/`。

---

## 如何判断哪些文件需要重写

总结判断逻辑：

```
源数据到达
  │
  ▼
ExtractSortedRows → 得到 (partition_key, symbol, date) 排序键列表
  │
  ▼
对每个键调用 KeyResolver::Resolve
  │
  ├── symbol 在分区范围外？ → append（新 part 或追加末 part）
  │   └── 零数据 IO，只需 plan 中的 part 行数
  │
  ├── symbol 在分区内？ → Part 级二分查找（用 RG stats 的 symbol min/max）
  │   ├── 找到目标 part → LoadSinglePart → 逐行二分查找
  │   │   ├── 找到 → 更新（该 part 需要重写）
  │   │   └── 没找到 → 插入（该 part 需要重写）
  │   └── 没找到目标 part → fallback LoadPartition
  │
  └── 新分区 → 新建 part 0000
  │
  ▼
对每个受影响的 (group, partition, part)：
  ├── 检查该 group 是否有映射列被修改
  │   ├── 有 → 创建 MutateTarget，写入 buffer
  │   └── 无（仅键列或无映射） → 跳过
  │
  ▼
ExecuteAndCommit → 并行 RewritePart
```

**核心原则**：
1. **重写最小单位 = Parquet 文件（part）**。一旦确定某个 part 受影响，整个 part 被重写。
2. **定位用 part 级别的 symbol [min, max] 范围**，不需要加载 part 内部数据。
   只在需要区分 insert vs update 时，加载单个 part 的 symbol+date 做二分查找。
3. **跳过无数据变化的 group**。通过 explicit_mapping 知道哪些列被修改，
   只重写包含这些列的 group。

---

## 三种高频写入场景的处理

### 场景 1：全量重写（覆盖整表所有列）

```
INSERT INTO al.t (symbol, date, close, volume, alpha001, alpha002)
SELECT ... -- 全部列，全部已存在的键
```

- explicit_mapping = `index:symbol,date,close,volume;factor/alpha:alpha001,alpha002`
- 所有键 found=true → 全部 update
- 两个 group 都有非键列被修改 → 都需要重写
- 每个分区的每个 part 都被重写

### 场景 2：单组全量重写（只覆盖 factor 列）

```
INSERT INTO al.t (symbol, date, alpha001, alpha002)
SELECT ... -- 只更新 factor/alpha 组的列
```

- explicit_mapping = `index:symbol,date;factor/alpha:alpha001,alpha002`
- 所有键 found=true → 全部 update
- index 组映射列 = [symbol, date] → **only_keys=true → 跳过 index 组重写！**
- 只重写 factor/alpha 组 → **减少约 28% 耗时**

### 场景 3：末分区重写（覆盖最后一个分区）

```
INSERT INTO al.t (symbol, date, close, volume, alpha001, alpha002)
SELECT ... -- 只覆盖最后一个分区的数据
```

- explicit_mapping = `index:symbol,date,close,volume;factor/alpha:alpha001,alpha002`
- KeyResolver 的第 1 层快速拒绝：只有最后一个分区的 symbol 落在范围内
- 其他分区的键在范围外 → 零数据 IO 快速判定为 append
- 只重写最后一个分区的 parts
