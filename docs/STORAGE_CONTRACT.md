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
| Canonical Row Space | 该表的逻辑行坐标集合 `{0, 1, ..., R-1}`，R = 表行数（= index 各分区行数之和） |
| Canonical Key | 定义行语义的列集合 = index Group 的 schema 列（从 Parquet footer 读取，不写入 manifest） |
| Logical Partition | 按 Key 值划分的互斥行子集（如 `date = 2026-08-17` 的所有行） |
| Physical Partition | 某个 Group 内一个 Hive 风格目录（如 `month=2026-08/`），**单层、各 Group 同一种段** |
| Part File | 一个 Parquet 文件，**文件名自描述** `{idx:04d}-{rows:10d}.parquet`，是 Group 内最小读取单元 |
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
   2. 非 index Group 间重复的列名：必须用限定名 `lv1.lv2.col` 引用。
   3. 非重复列名照常以裸名注册。

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

### 3.4 不变量（7 条）

1. Logical Table 有唯一 Canonical Row Space。
2. Key Columns 只存一份（只在 index Group）。
3. 所有 Column Group 使用相同 Row Ordering。
4. 所有 Column Group 使用相同 Logical Row Coordinate。
5. Partition Scheme 必须相同：所有 Group 同一种一层分区段；Group 分区键 ⊆ index。
6. Physical Files 可以完全不同（文件名、数量、大小、切分方式）。
7. 查询阶段绝不通过 Key 做 JOIN。

---

## 4. Row Ordering

- 行坐标在写入时确定，**永不变更**。
- Compaction 必须保持行序与行坐标不变（只能合并文件，不能重排行）。
- Writer 按 Key 排序写入（利于分区剪枝）；Reader 不得假设有序。

---

## 5. 无 Manifest（`_table.json` 已删除）

### 5.1 设计

**没有 `_table.json`，没有 `_group.json`，没有 sidecar，没有 commit marker。**
目录本身就是 Catalog，文件发现用 Hive layout。所有元数据从目录结构和 Parquet
footer 推导。

**空表不是有效表**：`BuildTablePlan` 通过 glob 发现 Group，空表（无任何 part）
返回空 plan。Writer 的 `aligned_upsert` 第一次写入时，从 `mapping` 参数推导
Group 结构（哪些 Group、每 Group 写哪些列）；分区模板默认 `month=%Y-%m`。

### 5.2 不存在的字段

以下信息**不持久化**，全部从目录结构或 Parquet footer 读取：

| 信息 | 来源 |
|------|------|
| 表名 | 目录名 |
| schema（列名+类型） | Parquet footer（每组最后 1 个 part） |
| 行数 / 行区间 | part 文件名 `{idx:04d}-{rows:10d}` |
| Row Group 大小 | 编译时常量 131072 |
| 事务号 | 不持久化（无 CAS、无并发控制） |
| Column Group 列表 | glob `<table>/**/*.parquet` |
| 分区模板 | 目录结构推导（`year=`/`month=`/`date=` 段） |

### 5.3 组发现

- Group 发现：glob `<table>/**/*.parquet`（跳过 `.`/`_` 段），从路径尾部向前扫描目录段，
  跳过 `name=value` 分区段，剩余目录段的最长后缀即 Group。
- 分区推导：从已有 part 的目录结构推导分区 schema：
  - 只识别 `year=YYYY`、`month=YYYY-MM`、`date=YYYY-MM-DD` 三种段。
  - 只识别单层；多层 → 报错。
  - 无识别段 → 该 Group 无分区（仅当 index 也无分区时合法）。
- 空表（glob 无任何 part）：`BuildTablePlan` 返回空 plan，Reader 报 "table directory
  does not exist" 或 0 groups；Writer 从 `mapping` 参数推导 Group 结构。

---

## 6. Parquet 约定

- 支持类型：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、`HUGEINT`、
  `FLOAT`、`DOUBLE`、`DECIMAL`、`DATE`、`TIMESTAMP`、`TIME`、`VARCHAR`。
- 压缩：默认 `zstd`；统计信息：默认全开（Row Group 裁剪依赖它）。
- Row Group 大小固定 131072 行（编译时常量，不写入 manifest）。
- 同一列在所有 part 中类型必须一致。

---

## 7. Part 文件与行区间

- 文件名格式 `{idx:04d}-{rows:10d}.parquet`：`idx` = 分区内索引，`rows` = 文件总行数。
- 行区间由文件名累加推导，**计划阶段零 footer IO**。
- 列集合由 Parquet footer 自描述（schema evolution → 缺失列读 NULL）。
- 组 schema = 组内 rel_path 排序最后 1 个 part 的 footer（每组只读 1 个 footer）。
- 防御校验：扫描时 footer 实际行数 == 文件名 `rows`，违反 fail-fast。

---

## 8. Schema Evolution 与主键契约

- 列集合按 part 自描述（footer）。旧 part 没有的列 → 读为 NULL（不重写历史）。
- 同一列类型跨 part 必须一致。
- Canonical Key = index schema，一经创建不可变更。
- **主键契约（v8）**：index schema 前两列 = 主键 `(symbol, date)`——
  第 1 列（col0）= symbol（字符串），第 2 列（col1）= `DATE`/`TIMESTAMP`
  （分区源列）。col1 非 DATE/TIMESTAMP 即 fail-fast。表按 date 分区；
  分区内按 `(symbol, date)` 升序排列（同一 symbol 可多行/多日期）。

---

## 9. 写入与原子提交协议

**API**：

```
aligned_upsert(table, source_path [, mapping], root=...)  → (rows_inserted, rows_updated, parts_rewritten, txid)
aligned_delete(table, keys_source, root=...)              → (rows_deleted, parts_rewritten, txid)
```

- `mapping` 对已存在的表可省略（按列名自动推断所属 Group）；空表首写必须显式给出。
- 主键 = `(symbol, date)`；已存在 → 更新（只重写受影响 part），不存在 → 插入。
- 映射列类型 = 组内已存类型（组 schema），不是源文件类型。
- **提交协议**：
  1. 写入 `<table>/_tmp/transaction-<txid>/`，`_tmp/` 对 Reader 不可见。
  2. 提交时 part 以 v6 名 `{idx:04d}-{rows:10d}.parquet` 落位（rename 到正式位置）。
  3. 崩溃 → 丢弃 `_tmp/transaction-<txid>/`（读端从不读 `_tmp/`）。
- `aligned_delete`：删空最高索引 part → 直接移除；删空单 part 分区 → 整分区移除；
  删空内部 part → fail-fast（"run aligned_compact first"）。

---

## 10. 读取协议（Aligned Scan）

### 10.1 打开

1. 组发现：一次 glob → 推导 Group 与分区键。
2. 对每个 Group：按分区键分组 → 从文件名解析行区间 → 校验分区键 ⊆ index →
   校验共享分区 R_i 一致 → index 分区内索引连续 → 每组读最后 1 个 part 的 footer
   （组 schema + 日期列契约）→ ValidateRowSpace。
3. 跨 Group：index 的 R_i 表 = 权威；缺分区组不报错（扫描时 NULL 填充）。

### 10.2 执行

1. 按 `start_row` 把任务切成 Aligned Row Group 粒度。
2. 每个 task 打开涉及的各 Group part 文件，按列投影读出 Vector，
   直接填入同一个 DataChunk。**缺分区区间：NULL 填充**。
3. 禁止：JOIN、横向 concat、Key 比较、物化中间行。

### 10.3 剪枝

- Partition Pruning：`WHERE date = ...` → 按各 Group 的 partitioning 模板解析目录。
- Row Group Pruning：Parquet min/max 统计。
- Projection：只读被选列、只开被选 Group。
- 跨 leaf 传播：各 leaf 的 partition pruning 结果映射到 Logical Row Space 坐标后
  **相交**为一个全局扫描区间（固定相交，仅限全覆盖组）。

---

## 11. 错误与校验语义

| 情形 | 行为 |
|------|------|
| 表目录不存在 | 报错 |
| 空表（无任何 part） | Reader 报错；Writer 从 mapping 推导 Group |
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

## 12. Metadata Cache

- 缓存：Parquet footer / 统计。
- 键：`(绝对路径, mtime, size)`；LRU 淘汰；不缓存数据页。

---

## 13. 保留扩展点（明确不做）

- `canonical_order: "sorted"` + skip index
- Bloom filter / 二级索引
- Tombstone / Delta
- 并发写互斥
- 类型升级
- 稀疏专用存储
- 非日期列的 partition source
- 多层 / 多种分区段混合
