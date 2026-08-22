# FactorLake / AlignedTable — API 对接文档

> 本文档面向需要通过 DuckDB 对接 AlignedTable 存储引擎的项目。
> 涵盖 ATTACH 挂载、标准 SQL 增删改查、表函数 API 及全部参数说明。

---

## 目录

1. [快速开始](#1-快速开始)
2. [ATTACH 挂载](#2-attach-挂载)
3. [增删改查（标准 SQL）](#3-增删改查标准-sql)
4. [表函数 API](#4-表函数-api)
5. [配置项](#5-配置项)
6. [参数详解](#6-参数详解)
7. [C++ 内部 API（供扩展开发者）](#7-c-内部-api供扩展开发者)

---

## 1. 快速开始

```sql
-- 1. 加载扩展（静态构建无需此步）
LOAD aligned;

-- 2. 设置数据根目录（或通过 ATTACH 指定）
SET aligned_data_root = 'D:/data/factorlake';

-- 3. 建表
CREATE TABLE al.mytable (symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE)
  WITH (groups='index:close;factor/alpha:alpha001', partition_template='month=%Y-%m');

-- 4. 插入数据
INSERT INTO al.mytable VALUES ('000001', DATE '2026-01-15', 10.5, 0.32);

-- 5. 查询
SELECT symbol, date, close, alpha001 FROM al.mytable WHERE date = DATE '2026-01-15';

-- 6. 更新
UPDATE al.mytable SET close = 11.0 WHERE symbol = '000001' AND date = DATE '2026-01-15';

-- 7. 删除
DELETE FROM al.mytable WHERE symbol = '000001' AND date = DATE '2026-01-15';
```

---

## 2. ATTACH 挂载

### 语法

```sql
ATTACH '<data_root>' AS <alias> (TYPE ALIGNED);
```

### 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `<data_root>` | VARCHAR (路径字面量) | 数据根目录的绝对路径。目录下每个一级子目录代表一张逻辑表。 |
| `<alias>` | 标识符 | 逻辑数据库名别名，后续 SQL 中以 `alias.table_name` 引用表。 |
| `TYPE ALIGNED` | 固定关键字 | 指定使用 AlignedTable 存储引擎。 |

### 行为

- ATTACH 后，引擎扫描 `<data_root>` 下的所有一级子目录，自动发现已存在的表（从 Parquet footer 推导 schema）。
- 表的发现是**惰性**的：首次查询或 `EnsureTablesLoaded` 时触发。
- `DETACH <alias>` 卸载，不影响磁盘数据。

### 示例

```sql
ATTACH 'D:/data/factorlake' AS al (TYPE ALIGNED);

-- 查看有哪些表
SELECT table_name FROM information_schema.tables WHERE table_schema = 'al';

-- 查询
SELECT * FROM al.cnstk_ixday WHERE date = DATE '2026-08-17';

-- 卸载
DETACH al;
```

### 也可不 ATTACH，直接用表函数

```sql
SET aligned_data_root = 'D:/data/factorlake';
SELECT * FROM aligned_table('cnstk_ixday') WHERE date = DATE '2026-08-17';
```

---

## 3. 增删改查（标准 SQL）

ATTACH 后，对 `al.<table>` 的标准 `INSERT` / `UPDATE` / `DELETE` / `SELECT` 直接生效，
无需调用表函数。引擎通过 catalog 钩子（`PlanInsert` / `PlanUpdate` / `PlanDelete`）
将 DML 操作路由到 Parquet 列组的直写。

### 3.1 SELECT（查）

```sql
SELECT <columns> FROM al.<table> [WHERE <conditions>];
```

- 支持 Projection Pushdown（只读涉及的列组）。
- 支持 Filter Pushdown（分区裁剪 + Parquet Row Group 裁剪）。
- 支持并行扫描。

### 3.2 INSERT（增）

```sql
INSERT INTO al.<table> [(col1, col2, ...)] VALUES (...), (...), ...;
INSERT INTO al.<table> SELECT ... FROM ...;
```

- **Upsert 语义**：主键 `(symbol, date)` 已存在则更新对应列，不存在则插入。
- **大批量自动分批**：单次 INSERT 超过 1M 行时自动分批提交（每批 1M 行，各自独立事务），避免 OOM。
- 返回值：标准 `Count`（插入的行数）。

### 3.3 UPDATE（改）

```sql
UPDATE al.<table> SET <col> = <value> [, ...] WHERE <conditions>;
```

- **Upsert 语义**：匹配的行被原地重写（重写受影响的 part 文件）。
- `WHERE` 条件通过扫描索引组定位 rowid，再解析为 `(symbol, date)` 主键。
- 仅重写 SET 列所在的列组，未涉及的列组不读写。

### 3.4 DELETE（删）

```sql
DELETE FROM al.<table> WHERE <conditions>;
```

- `WHERE` 条件通过扫描索引组定位 rowid，再解析为 `(symbol, date)` 主键。
- 删除规则：
  - 删空单 part 分区 → 整个分区目录移除。
  - 删空多 part 分区的最高索引 part → 直接移除该 part。
  - 删空多 part 分区的内部 part → **fail-fast**（需先 `aligned_compact` 合并碎片）。

---

## 4. 表函数 API

除了标准 SQL DML，还提供 7 个表函数，适用于不 ATTACH 的场景或需要细粒度控制的场景。

### 4.0 `aligned_create` — 建表

```sql
SELECT * FROM aligned_create(table_name, columns [, groups => '...'] [, root => '...']
                             [, partition_template => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（将成为数据根目录下的子目录）。 |
| `columns` | 位置参数 2 | VARCHAR | 是 | 列定义字符串，如 `'symbol VARCHAR, date DATE, close DOUBLE'`。由 DuckDB SQL 解析器解析。 |
| `groups` | 命名参数 | VARCHAR | 否 | 列→组映射。格式同 `aligned_upsert` 的 `mapping`。省略时所有非 key 列默认放 index。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |
| `partition_template` | 命名参数 | VARCHAR | 否 | 分区模板，默认 `month=%Y-%m`。可选 `year=%Y` / `date=%Y-%m-%d`。 |

**返回**：单行 `(dirs_created BIGINT, files_created BIGINT, txid BIGINT)`。

**规则**：
- 前两列必须 `(symbol VARCHAR, date DATE/TIMESTAMP)`（v8 主键契约）。
- `groups` 格式：`"index:close;factor/alpha:alpha001"`。非 index 组名必须是 `lv1/lv2` 两级路径。
- 创建 0 行占位 parquet（footer 携带 schema），Reader 可自动发现。

```sql
-- 建表（含列组映射）
SELECT * FROM aligned_create('mytable', 'symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE',
                             groups => 'index:close;factor/alpha:alpha001');

-- 建表（所有列默认放 index）
SELECT * FROM aligned_create('simple_tbl', 'symbol VARCHAR, date DATE, close DOUBLE');

-- 指定分区模板
SELECT * FROM aligned_create('ptbl', 'symbol VARCHAR, date DATE, close DOUBLE',
                             groups => 'index:close',
                             partition_template => 'date=%Y-%m-%d');
```

### 4.1 `aligned_table` — 扫描逻辑表

```sql
SELECT * FROM aligned_table(table_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（数据根目录下的子目录名）。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root` 设置。 |

**返回**：表的全部列（schema 从 Parquet footer 推导）。

```sql
SET aligned_data_root = 'D:/data/factorlake';
SELECT symbol, date, close FROM aligned_table('cnstk_ixday')
  WHERE date >= DATE '2026-01-01' AND date <= DATE '2026-01-31';

-- 或指定 root
SELECT * FROM aligned_table('cnstk_ixday', root => 'D:/data/factorlake');
```

### 4.2 `aligned_scan` — 扫描逻辑表（root 为位置参数）

```sql
SELECT * FROM aligned_scan(root, table_name);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `root` | 位置参数 1 | VARCHAR | 是 | 数据根目录。 |
| `table_name` | 位置参数 2 | VARCHAR | 是 | 逻辑表名。 |

**返回**：同 `aligned_table`。

```sql
SELECT * FROM aligned_scan('D:/data/factorlake', 'cnstk_ixday') WHERE date = DATE '2026-08-17';
```

### 4.3 `aligned_upsert` — 插入/更新

```sql
SELECT * FROM aligned_upsert(table_name, source_path [, mapping, ...] [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `source_path` | 位置参数 2 | VARCHAR | 是 | 源数据 Parquet 文件路径。文件的列名需与表 schema 匹配。 |
| `mapping` | 位置参数 3+ | VARCHAR (变长) | 否 | 列→组映射字符串。格式见下。省略时按列名自动推断。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |

**mapping 格式**：`"group:col1,col2;group2:col3,..."`

```
index:close,volume;factor/alpha:alpha001,alpha002
```

- `index` 组必须包含主键列（symbol, date）。
- 非 index 组名必须是 `lv1/lv2` 两级路径。
- 未列出的列默认归入 index 组。

**返回**：单行 `(rows_inserted BIGINT, rows_updated BIGINT, parts_rewritten BIGINT, txid BIGINT)`。

```sql
SELECT * FROM aligned_upsert(
  'cnstk_ixday',
  'D:/data/new_rows.parquet',
  'index:symbol,date,close;factor/alpha:alpha001',
  root => 'D:/data/factorlake'
);
-- 结果: 1500, 300, 12, 7
```

### 4.4 `aligned_delete` — 按主键删除

```sql
SELECT * FROM aligned_delete(table_name, keys_source [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `keys_source` | 位置参数 2 | VARCHAR | 是 | 包含待删除主键的 Parquet 文件路径。文件需有 `(symbol VARCHAR, date DATE)` 两列。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。 |

**返回**：单行 `(rows_deleted BIGINT, parts_rewritten BIGINT, txid BIGINT)`。

```sql
SELECT * FROM aligned_delete('cnstk_ixday', 'D:/data/keys_to_delete.parquet');
```

### 4.5 `aligned_compact` — 合并 part 碎片

```sql
SELECT * FROM aligned_compact(table_name, group_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `group_name` | 位置参数 2 | VARCHAR | 是 | 要合并的列组名，或 `'all'` 合并所有组。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。 |

**返回**：单行 `(dirs_compacted BIGINT, parts_before BIGINT, parts_after BIGINT)`。

```sql
-- 合并单个组
SELECT * FROM aligned_compact('cnstk_ixday', 'factor/alpha001');

-- 合并所有组（单事务原子切换）
SELECT * FROM aligned_compact('cnstk_ixday', 'all');
```

### 4.6 `aligned_drop` — 删除列组或整表

```sql
SELECT * FROM aligned_drop(table_name, group_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `group_name` | 位置参数 2 | VARCHAR | 是 | 要删除的列组名。`'index'` = 删除整张表（所有列组 + 表目录）；其他值 = 仅删除该列组的目录树。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。 |

**返回**：单行 `(dirs_removed BIGINT, files_removed BIGINT, txid BIGINT)`。

**规则**：
- `group_name = 'index'`：删除整个表目录（`<root>/<table>/`），包括所有列组。
- `group_name = 其他`：仅删除该列组目录（`<root>/<table>/<group_path>/`），index 及其他列组不受影响。
- 删除前自动获取写锁（`TableWriteLock`），与并发写入互斥。
- 不存在的组名 → fail-fast（`BinderException`）。

```sql
-- 删除单个列组（保留 index 及其他组）
SELECT * FROM aligned_drop('cnstk_ixday', 'factor/alpha001');

-- 删除整张表（index = 删除所有内容）
SELECT * FROM aligned_drop('cnstk_ixday', 'index');
```

---

## 5. 配置项

### 5.1 `aligned_data_root`

| 属性 | 值 |
|------|-----|
| 类型 | VARCHAR |
| 默认值 | 无（未设置时使用 `root =>` 参数的函数会报错） |
| 作用域 | 全局 / Session |

设置数据根目录。当表函数的 `root` 参数省略时使用此值。

```sql
SET aligned_data_root = 'D:/data/factorlake';
-- 之后所有 aligned_table/upsert/delete/compact 调用无需传 root
```

### 5.2 `parquet_metadata_cache`

| 属性 | 值 |
|------|-----|
| 类型 | BOOLEAN |
| 默认值 | `true`（扩展加载时强制开启） |
| 作用域 | 全局 |

Parquet footer / schema / Row Group statistics 的 LRU 缓存。跨查询跨线程共享。
扩展加载时强制设为 `true`，通常无需手动调整。

---

## 6. 参数详解

### 6.1 `table_name`（表名）

- 类型：VARCHAR
- 含义：逻辑表名，对应数据根目录下的一个一级子目录。
- 示例：`'cnstk_ixday'` → `<root>/cnstk_ixday/`

### 6.2 `root`（数据根目录）

- 类型：VARCHAR
- 含义：所有逻辑表的顶层父目录。
- 路径格式：正斜杠或反斜杠均可（内部统一为正斜杠）。
- 可通过 `SET aligned_data_root` 设为默认值，避免每次传参。
- 在 ATTACH 模式下，`root` 就是 ATTACH 路径，无需单独指定。

### 6.3 `source_path`（源数据文件）

- 类型：VARCHAR
- 含义：`aligned_upsert` 的源数据 Parquet 文件路径。
- 要求：文件列名需与目标表的列名匹配（或通过 `mapping` 指定映射）。
- 主键列（symbol, date）必须存在且非 NULL。

### 6.4 `mapping`（列→组映射）

- 类型：VARCHAR（可传多个，分号分隔或作为变长参数）
- 格式：`"group1:col1,col2;group2:col3,col4"`
- 规则：
  - `index` 组必须包含主键列。
  - 非 index 组名格式为 `lv1/lv2`（如 `factor/alpha101`）。
  - 未列出的列默认归入 `index` 组。
  - 省略时按列名自动推断（已有表的场景）。
- 示例：
  ```sql
  -- 单个映射字符串
  SELECT * FROM aligned_upsert('mytable', 'data.parquet', 'index:symbol,date,close;factor/alpha:alpha001');

  -- 多个映射参数（自动拼接）
  SELECT * FROM aligned_upsert('mytable', 'data.parquet', 'index:symbol,date,close', 'factor/alpha:alpha001');
  ```

### 6.5 `keys_source`（删除主键文件）

- 类型：VARCHAR
- 含义：`aligned_delete` 的主键文件路径。
- 要求：Parquet 文件，恰好两列 `(symbol VARCHAR, date DATE)`。
- 不存在的主键被静默跳过（幂等）。

### 6.6 `group_name`（合并/删除目标组名）

- 类型：VARCHAR
- 含义：`aligned_compact` 和 `aligned_drop` 的目标列组名。
- `aligned_compact` 特殊值 `'all'`：合并表中所有列组（单事务原子切换）。
- `aligned_drop` 特殊值 `'index'`：删除整张表（所有列组 + 表目录）。
- 组名格式：`index` / `factor/alpha101` / `fieldset/ma` 等。

### 6.7 WITH 子句参数（CREATE TABLE）

CREATE TABLE 的 `WITH (...)` 子句支持以下选项：

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `groups` | VARCHAR | 空（所有列归 index） | 列→组映射。格式同 `mapping` 参数。 |
| `partition_template` | VARCHAR | `month=%Y-%m` | 分区模板。可选 `year=%Y` / `month=%Y-%m` / `date=%Y-%m-%d`。 |
| `partition` | VARCHAR | 空 | 创建空分区时指定分区键。如 `month=2026-10`。仅用于已有表。 |

---

## 7. C++ 内部 API（供扩展开发者）

以下 API 供 DuckDB 扩展开发者在 C++ 层直接调用，**不需要** 通过 SQL。

### 7.1 扫描绑定

```cpp
unique_ptr<FunctionData> AlignedBindForCatalog(
    ClientContext &context,
    const string &root,
    const string &table,
    vector<LogicalType> &return_types,
    vector<string> &names);
```

| 参数 | 说明 |
|------|------|
| `context` | DuckDB ClientContext |
| `root` | 数据根目录 |
| `table` | 逻辑表名 |
| `return_types` | 输出：列类型列表 |
| `names` | 输出：列名列表 |

用于在不经过 `TableFunctionBindInput` 的情况下绑定扫描（catalog 内部使用）。

### 7.2 内存 Upsert

```cpp
UpsertResult AlignedUpsertFromCollection(
    ClientContext &context,
    const string &table_name,
    const string &root,
    const string &mapping,
    ColumnDataCollection &source_collection,
    const vector<string> &source_col_names);
```

| 参数 | 说明 |
|------|------|
| `context` | DuckDB ClientContext |
| `table_name` | 逻辑表名 |
| `root` | 数据根目录 |
| `mapping` | 列→组映射字符串（可为空，自动推断） |
| `source_collection` | 内存中的行数据（DuckDB ColumnDataCollection） |
| `source_col_names` | collection 的列名列表 |

**返回**：`UpsertResult { idx_t rows_inserted, rows_updated, parts_rewritten }`

跳过临时 Parquet 文件的双写，直接从内存 collection 执行 upsert。`PhysicalAlignedInsert` 使用此接口。

### 7.3 内存 Delete

```cpp
DeleteResult AlignedDeleteFromCollection(
    ClientContext &context,
    const string &table_name,
    const string &root,
    ColumnDataCollection &keys_collection);
```

| 参数 | 说明 |
|------|------|
| `context` | DuckDB ClientContext |
| `table_name` | 逻辑表名 |
| `root` | 数据根目录 |
| `keys_collection` | 主键 collection，必须恰好两列 `(symbol VARCHAR, date DATE/TIMESTAMP)` |

**返回**：`DeleteResult { idx_t rows_deleted, parts_rewritten }`

### 7.4 建表

```cpp
void AlignedCreateTable(
    ClientContext &context,
    const string &root,
    const string &table_name,
    const vector<ColumnDefinition> &columns,
    const string &groups_option,
    const string &partition_template_option);

void AlignedCreatePartition(
    ClientContext &context,
    const string &root,
    const string &table_name,
    const string &partition_key);
```

### 7.5 写锁（RAII）

```cpp
TableWriteLock lock(fs, table_path);
```

在 `<table_path>/.aligned_write.lock` 创建锁文件；已存在则抛异常。析构时删除锁文件。
mutator 和 compactor 内部自动使用。崩溃残留需手动删除。

### 7.6 事务 ID

```cpp
idx_t NextTransactionId();
```

进程级共享原子计数器，用于 `_tmp/transaction-<id>/` 暂存目录命名。不持久化。

---

## 附录：返回值含义

| 函数 | 返回列 | 含义 |
|------|--------|------|
| `aligned_upsert` | `rows_inserted` | 新插入的主键行数 |
| | `rows_updated` | 更新的已有主键行数 |
| | `parts_rewritten` | 重写的 part 文件数 |
| | `txid` | 事务 ID（暂存目录名） |
| `aligned_delete` | `rows_deleted` | 删除的主键行数 |
| | `parts_rewritten` | 重写/移除的 part 文件数 |
| | `txid` | 事务 ID |
| `aligned_compact` | `dirs_compacted` | 合并的分区目录数 |
| | `parts_before` | 合并前 part 文件总数 |
| | `parts_after` | 合并后 part 文件总数 |
| `aligned_drop` | `dirs_removed` | 删除的目录数（含分区子目录） |
| | `files_removed` | 删除的 parquet 文件数 |
| | `txid` | 事务 ID |
| `aligned_create` | `dirs_created` | 创建的目录数（含表目录、组目录、分区目录） |
| | `files_created` | 创建的 parquet 文件数（占位文件） |
| | `txid` | 事务 ID |
| `INSERT` (标准 SQL) | `Count` | 插入的行数 |
| `UPDATE` (标准 SQL) | `Count` | 更新的行数 |
| `DELETE` (标准 SQL) | `Count` | 删除的行数 |
