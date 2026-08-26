# Storage Contract — AlignedTable

> 本文件是**存储层唯一权威规格**。Reader / Writer / Compaction 的一切实现
> 必须与本文件一致；任何修改必须先改本文件再改代码。

**核心契约**：所有 Group 用**同一种一层分区段**（`year=`/`month=`/`date=` 三选一）；
分区键 = 完整目录段串；Group 分区键集合 ⊆ index 分区键集合（允许缺分区）；
共享分区**总行数**必须一致；part 文件名自描述 `{idx:04d}-{rows:10d}.parquet`；
index schema 前两列 = 主键 `(symbol, date)`。

---

## 1. 术语

| 术语 | 定义 |
|------|------|
| Logical Table | 一张逻辑宽表，如 `cnstk_ixday`，对应 `<data_root>/<table_name>/` |
| Column Group | Logical Table 的一个物理列子集，如 `index`、`factor/alpha101`，对应表内一个目录 |
| Canonical Row Space | 该表的逻辑行坐标集合 `{0, 1, ..., R-1}`，R = index 各分区行数之和 |
| Canonical Key | index Group 的 schema 列（从 Parquet footer 读取，不持久化） |
| Logical Partition | 按 Key 值划分的互斥行子集（如 `date = 2026-08-17` 的所有行） |
| Physical Partition | 某个 Group 内一个 Hive 风格目录（如 `month=2026-08/`），单层、各 Group 同一种段 |
| Part File | 一个 Parquet 文件，文件名自描述 `{idx:04d}-{rows:10d}.parquet`，是 Group 内最小读取单元 |
| Row Group | Parquet 内 Row Group；Writer flush 大小固定 131072 行（编译时常量） |
| Aligned Scan | 按行坐标直接组装多 Group 向量的扫描，绝不 JOIN |

---

## 2. 目录布局

```
<data_root>/
└── <table_name>/                  ← Logical Table
    ├── index/                     ← Column Group（必选）
    │   └── month=2026-08/         ← Physical Partition（单层）
    │       ├── 0000-0000002000.parquet
    │       └── 0001-0000002000.parquet
    ├── factor/alpha101/           ← 二级目录 Group
    │   └── month=2026-08/
    │       ├── 0000-0000002000.parquet
    │       └── 0002-0000002000.parquet  ← 非 index 组允许缺号
    ├── fieldset/ma/               ← 可以缺分区
    │   └── month=2026-08/
    │       └── 0000-0000004000.parquet
    └── ...
```

### 2.1 硬性规则

a. `<table_name>` 即表名。
b. **`<table>/index` 必须存在**：Key 列与基础列住在 index。
c. **其余 Group 必须以两级目录存在**：`<table>/<lv1>/<lv2>/`（如 `factor/alpha101`）。
d. **目录发现时忽略以 `.` 或 `_` 开头的目录**（如 `_tmp/`）。
e. **分区段唯一性**：一个 Group 内所有 part 的目录恰好一个 `name=value` 段；
   `year=` / `month=` / `date=` 三选一，所有 Group 必须用同一种。不允许多层嵌套。

### 2.2 列名规则

f. **重复列名处理**（按优先级）：
   1. 与 index 重复的列名：非 index Group 中的同名列忽略（index 列权威）。
   2. 非 index Group 间重复的列名：必须用限定名 `lv1.lv2.col` 引用
      （通过 `COLUMNS('lv1.lv2.col')` 正则引用）。
   3. 非重复列名照常以裸名注册，同时注册 `lv1.lv2.col` 限定别名。

---

## 3. Canonical Row Space 与分区对齐

### 3.1 定义

行坐标 `0..R-1`，R = index 各分区行数之和。每个 Group 的 part 拼接后**允许只覆盖
index 行空间的一部分**（缺分区 → 该区行保留、该组列全 NULL）。

**对齐契约**：

1. 所有 Group 使用同一层同一种分区段。
2. 任一 Group 的分区键集合 ⊆ index 的分区键集合。
3. 共享分区 i：各 Group 的总行数 == index 的总行数。
4. 共享分区内同一行坐标 N：各 Group 的第 N 行是同一 Logical Row。

### 3.2 分区行数公式与校验

- part 文件名自描述：`{idx:04d}-{rows:10d}.parquet`——`idx` = 分区内索引（0000 起），
  `rows` = 文件总行数（10 位定宽，上限 9,999,999,999）。
- **index 组**：分区内索引必须 0000 起连续（缺号/重复 → fail-fast）。
  非 index 组允许缺号——行区间按存在的 part 文件名累加推导。
- 分区 i 的行数：`R_i = Σ(该分区全部 part 的文件名行数)`。
- 分区内第 j 个存在 part 的 `start_row = S_i + Σ(索引更小的存在 part 的行数)`。
- 校验（fail-fast）：
  - 共享分区 R_i 跨 Group 一致。
  - 同分区同索引：两个 Group 都有该索引的 part → 行数必须相等。
  - 扫描时 OpenPart 防御校验：footer 实际行数 == 文件名行数。

### 3.3 全局行序

同一 Group 内的 part 按**规范化相对路径的字符串字典序**排列。
`part_id = 文件名解析出的 idx 字段`。

行坐标在写入时确定，**永不变更**。Compaction 必须保持行序与行坐标不变
（只能合并文件，不能重排行）。Writer 按 Key 排序写入（利于分区剪枝）；
Reader 不得假设有序。

### 3.4 不变量（7 条）

1. Logical Table 有唯一 Canonical Row Space。
2. Key Columns 只存一份（只在 index Group）。
3. 所有 Column Group 使用相同 Row Ordering。
4. 所有 Column Group 使用相同 Logical Row Coordinate。
5. Partition Scheme 必须相同：所有 Group 同一种一层分区段；Group 分区键 ⊆ index。
6. Physical Files 可以完全不同（文件名、数量、大小、切分方式）。
7. 查询阶段绝不通过 Key 做 JOIN。

---

## 4. 无 Manifest

**没有 `_table.json`，没有 `_group.json`，没有 sidecar，没有 commit marker。**
目录本身就是 Catalog，文件发现用 Hive layout。所有元数据从目录结构和 Parquet
footer 推导。

### 4.1 不持久化的信息

| 信息 | 来源 |
|------|------|
| 表名 | 目录名 |
| schema（列名+类型） | Parquet footer（每组最后 1 个 part） |
| 行数 / 行区间 | part 文件名 `{idx:04d}-{rows:10d}` |
| Row Group 大小 | 编译时常量 131072 |
| Part 行数软上限 | 编译时常量 1048576（`ALIGNED_DEFAULT_PART_ROWS`） |
| 事务号 | 不持久化（无 CAS、无并发控制） |
| Column Group 列表 | glob `<table>/**/*.parquet` |
| 分区模板 | 目录结构推导（`year=`/`month=`/`date=` 段） |

### 4.2 组发现

- glob `<table>/**/*.parquet`（跳过 `.`/`_` 开头目录），从路径尾部向前扫描目录段，
  跳过 `name=value` 分区段，剩余目录段的最长后缀即 Group。
- 分区推导：只识别 `year=YYYY`、`month=YYYY-MM`、`date=YYYY-MM-DD` 三种单层段。
  多层 → 报错。无识别段 → 该 Group 无分区（仅当 index 也无分区时合法）。
- 空表（glob 无任何 part）：Reader 报 "table directory does not exist" 或 0 groups；
  Writer 从 query 列推断 Group 结构。
  **例外**：`CREATE TABLE` DDL 写 0 行占位 parquet，glob 可发现 → 有效表。

### 4.3 Parquet 约定

- 支持类型：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、`HUGEINT`、
  `FLOAT`、`DOUBLE`、`DECIMAL`、`DATE`、`TIMESTAMP`、`TIME`、`VARCHAR`。
- 压缩：`zstd`；统计信息：全开（Row Group 裁剪依赖它）；Parquet 版本 V1。
- Row Group 大小固定 131072 行（编译时常量）。
- 同一列在所有 part 中类型必须一致。

### 4.4 Schema Evolution 与主键契约

- 列集合按 part 自描述（footer）。旧 part 没有的列 → 读为 NULL（不重写历史）。
- 同一列类型跨 part 必须一致。
- Canonical Key = index schema，一经创建不可变更。
- **主键契约（v8）**：index schema 前两列 = 主键 `(symbol, date)`——
  第 1 列（col0）= symbol（字符串），第 2 列（col1）= `DATE`/`TIMESTAMP`
  （分区源列）。col1 非 DATE/TIMESTAMP 即 fail-fast。表按 date 分区；
  分区内按 `(symbol, date)` 升序排列（同一 symbol 可多行/多日期）。
- **TIMESTAMP 键（v9）**：当 col1 为 TIMESTAMP 时，键为完整 timestamp 值
  （微秒级），不截断为日期——同一天内同一标的的多个时间戳（如分钟 K 线）是不同
  键。COPY TO 写入时按完整 timestamp 值排序；分区目录求值时自动提取日期部分
  （TIMESTAMP 列按 `int64_t` 读取，DATE 列按 `int32_t` 读取）。

---

## 5. 读取（Aligned Scan）

### 5.1 打开 / 组发现

1. 一次 glob → 推导 Group 与分区键。
2. 对每个 Group：按分区键分组 → 从文件名解析行区间 → 校验分区键 ⊆ index →
   校验共享分区 R_i 一致 → index 分区内索引连续 → 每组读最后 1 个 part 的 footer
   （组 schema + 日期列契约）→ ValidateRowSpace。
3. 跨 Group：index 的 R_i 表 = 权威；缺分区组不报错（扫描时 NULL 填充）。

### 5.2 执行

1. 按 `start_row` 把任务切成 Aligned Row Group 粒度。
2. 每个 task 打开涉及的各 Group part 文件，按列投影读出 Vector，
   直接填入同一个 DataChunk。**缺分区区间：NULL 填充**。
3. 禁止：JOIN、横向 concat、Key 比较、物化中间行。

### 5.3 剪枝

- **Partition Pruning**：`WHERE date = ...` → 按各 Group 的 partitioning 模板解析目录。
- **Row Group Pruning**：Parquet min/max 统计。
- **Projection Pushdown**：只读被选列、只开被选 Group。
- **Group Filter**：`aligned_scan(table, group_filter)` 仅扫描 index + 指定组。
- 跨 leaf 传播：各 leaf 的 partition pruning 结果映射到 Logical Row Space 坐标后
  **相交**为一个全局扫描区间（固定相交，仅限全覆盖组）。

### 5.4 并行

- 任务单位 = Aligned Row Group，共享游标按连续 Range 发放。
- 8 线程实测 ≈4.2× 加速。

### 5.5 Metadata Cache

- 缓存：Parquet footer / 统计。
- 键：`(绝对路径, mtime, size)`；LRU 淘汰；不缓存数据页。

---

## 6. 写入

> **标准 DML 已移除**。唯一写入路径是 `COPY TO (FORMAT aligned)`。
> `aligned_create` / `aligned_compact` / `aligned_drop` / `CREATE TABLE` 是
> 管理操作（建表、合并、删除）。

### 6.1 COPY TO (FORMAT aligned) — 批量写入

走 DuckDB CopyFunction 框架（`GetAlignedCopyFunction`，Sink/Combine/Finalize pipeline）。

```sql
-- 新组首次写入（schema 从 query 推断，排除 index key 列）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday'
  (FORMAT aligned, GROUP 'panel/ma');

-- 已有组写入（schema 从 last parquet footer 读取，列裁剪到组内列）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday'
  (FORMAT aligned, GROUP 'index');

-- 增量合并（MERGE true = 读旧数据 + 新数据 merge 后重写）
COPY (SELECT * FROM mock) TO 'cnstk_ixday'
  (FORMAT aligned, GROUP 'panel/ma', MERGE true);
```

#### 写入 pipeline

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

#### 关键行为

- **per-partition 覆盖**：每个分区目录首次写入时自动清理旧 parquet 文件
  （无需 `OVERWRITE true`）。
- **MERGE true**：增量合并模式——读取受影响分区的已有数据，与新数据按
  `(symbol, date)` 主键 merge（新数据覆盖旧 key），合并后按正常逻辑排序写入。
  非 index 组的 MERGE 依赖从 index 组读取 `(symbol, date)` 作为 hidden sort key，
  **必须在 MERGE index 组之前执行**。
- **自描述文件名**：先以 `0000-0000000000.parquet` 写入，Finalize 后 rename 为
  `{idx:04d}-{rows:10d}.parquet`（实际行数）；0 行空文件自动删除。
- **RG / Part 切分**：Row Group flush size = 131072；part 文件上限 = 8 RG
  = 1048576 行（`ALIGNED_DEFAULT_PART_ROWS`）。满 8 RG 轮转新 part。
- **排序**：`PARALLEL_COPY_TO_FILE` 让 source reader 多线程并行扫描；FlushWorker
  在 sentinel 到达后收集每个分区的所有 buffer，合并后按 `(symbol, date)` 全局
  排序再 flush——消除 morsel 乱序，0 乱序行。partition affinity（round-robin）
  保证同一分区的数据只由一个 FlushWorker 线程写入。
  非 index 组的 group schema 不含 symbol/date 列（key 只存一份），Sink 在 buffer
  末尾追加 hidden sort key 列，SortAndFlushPartition 排序后剥离。
- **列裁剪**：只写 group schema 包含的列，按 group schema 顺序重排；输入列类型
  ≠ 组 schema 类型时自动 cast（如 TIMESTAMP → DATE）。
- **统计校验**：每个 PartitionWriter 跟踪 `received_rows` / `flushed_rows` /
  `written_rows`，Finalize 时校验 `received == written`，不匹配抛
  `InternalException`。
- **新组推断**：`aligned_create('table', 'group')` 2-arg 形式创建空组目录，
  首次 COPY 时从 query 列推断 schema（排除 index key 列 symbol/date）。

### 6.2 CREATE TABLE DDL

通过标准 `CREATE TABLE ... WITH (...)` 语法在 AlignedTable catalog 上操作。

```sql
-- 新建表（写 0 行占位 parquet，footer 携带 schema）
CREATE TABLE al.<table> (symbol VARCHAR, date DATE, ...)
  WITH (groups='index:close;factor/alpha:alpha001', partition_template='month=%Y-%m');

-- 已有表创建空分区
CREATE TABLE al.<table> (cols...) WITH (partition='month=2026-10');

-- 已有表添加列组（写 N 行全 NULL 占位）
CREATE TABLE al.<table> (ma5 DOUBLE, ma20 DOUBLE) WITH (groups='fieldset/ma:ma5,ma20');
```

#### 新建表规则

- 前两列必须 (symbol VARCHAR, date DATE/TIMESTAMP)（v8 主键契约）。
- `groups` 指定列→Group 映射（`;` 分隔组，`:` 分隔组名/列名，`,` 分隔列名）；
  未列出的列默认放入 `index`。非 `index` 组名必须是 `lv1/lv2` 两级路径。
- `partition_template` 默认 `month=%Y-%m`，可选 `date=%Y-%m-%d` / `year=%Y`。
- 每个 Group 在默认分区（epoch：`month=1970-01` / `date=1970-01-01` / `year=1970`）
  下写一个 **0 行占位 parquet** `0000-0000000000.parquet`，footer 携带 schema →
  Reader 可通过 glob 发现表结构。

#### 分区创建规则

- 表必须已存在。分区键格式必须匹配现有模板（`date=YYYY-MM-DD` 15 字符 /
  `month=YYYY-MM` 13 字符 / `year=YYYY` 9 字符），且日期部分必须是有效的
  日历日期（如 `month=2026-13` 会 fail-fast）。
- 为每个已发现 Group 在新分区目录下写 0 行占位 parquet。
- 若 index 已有该分区 → fail-fast "partition already exists"。

#### 列组扩展规则

- 表必须已存在。`CREATE TABLE` 的列定义是新 Group 的列（不需要 symbol/date）。
- `groups` 必须指定至少一个非 `index` Group。
- 新 Group 名若已存在 → fail-fast。
- 新 Group 名必须是 `lv1/lv2` 两级路径（与 §2.1c 一致）。
- 每个 index 已有的分区，新 Group 写一个 **N 行全 NULL 占位 parquet**
  （N = index 分区行数，文件名 `0000-{rows:010d}.parquet`），满足分区对齐契约
  （共享分区总行数一致）。
- 已有的其他 Group 的 part 文件不被触碰（Schema Evolution：新列在老 part 上
  读为 NULL）。

#### 验证错误（全部 fail-fast）

- `at least 2 columns required` / `first two columns must be exactly one DATE/TIMESTAMP and one VARCHAR`
- `table already exists ... specify a non-index group`（表已存在但未指定新 Group）
- `group already exists`（扩展时组名重复）
- `must be 'index' or a two-level path 'lv1/lv2'`（组名格式错误）
- `references unknown column`（groups 引用了 CREATE TABLE 中不存在的列）
- `invalid partition_template` / `partition key does not match template`
- `partition already exists`（分区已存在）

### 6.3 aligned_compact — 合并 part 碎片

```
aligned_compact(table, group_name, root=...)  → (dirs_compacted, parts_before, parts_after)
```

- **规范化重写**：每个分区的所有 part 按 `ALIGNED_DEFAULT_PART_ROWS`（1M 行）
  重新切分——前面的 part 满行（恰好 1M 行），末 part ≤ 1M 行。0 行占位 part
  被合并吸收。已规范化的分区（单 part ≤ 1M，或多 part 均满行）跳过不重写
  （`IsAlreadyNormalized` 检查）。
- **并行暂存**：Phase 1 各分区目录的暂存独立（每个目录的读-写独立），可并行处理。
  Phase 2（move + delete）仍串行。
- **两阶段提交**：所有组的合并 part 先写入 `_tmp/`，全部成功后再统一 move 到
  目标目录 + 删除旧 part；任一组失败则清理 `_tmp`、表状态不变（旧 part 仍在原位）。
- 同目录必须同列集（拒绝 schema-evolution 合并）。
- `group_name = 'all'` 合并所有组（单事务原子切换）。

### 6.4 并发写互斥

- `.aligned_write.lock`：COPY / compactor 执行前创建 lock 文件（RAII），已有 lock
  则拒绝。crash 残留需手动删除。
- `_tmp/` 目录：compaction 的暂存区，Reader 永不访问。

---

## 7. 错误与校验语义

| 情形 | 行为 |
|------|------|
| 表目录不存在 | 报错 |
| 空表（无任何 part） | Reader 报错；Writer 从 query 列推断 Group |
| 无 index Group | 报错 "mandatory group 'index' was not found" |
| Group 分区键不在 index 分区键集合内 | 报错（fail-fast） |
| 共享分区 R_i != index 的 R_i | 报错（fail-fast） |
| part 文件名不匹配 `\d{4}-\d{10}\.parquet` | 报错（fail-fast） |
| index 分区内索引不连续 | 报错（fail-fast） |
| 同分区同索引两个 Group 的 part 行数不等 | 报错（fail-fast） |
| footer 行数 != 文件名 `rows` | 报错（fail-fast） |
| index schema 第二列不是 DATE/TIMESTAMP | 报错（fail-fast） |
| 非 index Group 不是 `lv1/lv2` | 报错 |
| 多层分区段 / 组间分区段不一致 | 报错 |
| 同列类型不一致 | 报错 |
| `_tmp/` 内文件 | Reader 永不访问 |

---

## 8. 明确不做

Tombstone/Delta、类型升级、聚合下推（依赖 DuckDB ≥ v1.6 API）、Bloom filter /
二级索引、稀疏专用存储、非日期列的 partition source、多层/多种分区段混合。
