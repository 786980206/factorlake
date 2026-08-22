# FactorLake / AlignedTable — API 对接文档

> 本文档面向需要通过 DuckDB 对接 AlignedTable 存储引擎的项目。
> 涵盖 ATTACH 挂载、标准 SQL 增删改查、表函数 API 及全部参数说明。

---

## 目录

1. [扩展安装与加载](#1-扩展安装与加载)
2. [快速开始](#2-快速开始)
3. [ATTACH 挂载](#3-attach-挂载)
4. [增删改查（标准 SQL）](#4-增删改查标准-sql)
5. [表函数 API](#5-表函数-api)
6. [配置项](#6-配置项)
7. [参数详解](#7-参数详解)
8. [C++ 内部 API（供扩展开发者）](#8-c-内部-api供扩展开发者)

---

## 1. 扩展安装与加载

AlignedTable 扩展是一个 unsigned 的 DuckDB 扩展二进制文件（`.duckdb_extension`），
基于 DuckDB **v1.5.4** 构建。加载时强校验 DuckDB 版本，必须与构建版本一致。

### 1.1 获取扩展二进制

- **GitHub Release 下载**：从项目的 GitHub Release 页面下载对应平台的
  `aligned.duckdb_extension` 文件。
- **本地构建**：见 `docs/EXTENSION_RELEASE.md`，使用
  `-DEXTENSION_STATIC_BUILD=1` 构建产出自包含的二进制（约 24MB）。

### 1.2 在 DuckDB CLI 中加载

扩展未签名，需通过 `-unsigned` 标志或 `allow_unsigned_extensions` 设置启用。

**方式 A：直接 LOAD 本地文件（无需 INSTALL）**

```bash
# 方式 A1：通过 -unsigned 标志（CLI 专用）
duckdb -unsigned -c "LOAD '/path/to/aligned.duckdb_extension';"
duckdb -unsigned -c "LOAD '/path/to/aligned.duckdb_extension'; SELECT * FROM aligned_scan('mytable');"
```

```sql
-- 方式 A2：在 SQL 会话中直接 LOAD 完整路径
LOAD '/path/to/aligned.duckdb_extension';
SET aligned_data_root = '/data';
SELECT * FROM aligned_scan('mytable');
```

**方式 B：INSTALL 到本地缓存 + LOAD 短名**

```bash
# 安装（下载或复制到 ~/.duckdb/extensions/v1.5.4/<platform>/）
duckdb -unsigned -c "INSTALL '/path/to/aligned.duckdb_extension';"
# 之后可用短名加载（无需再传路径）
duckdb -unsigned -c "LOAD aligned; SELECT * FROM aligned_scan('mytable');"
```

```sql
-- 在 SQL 会话中
INSTALL '/path/to/aligned.duckdb_extension';
LOAD aligned;   -- 之后扩展名 = 文件 base name（即 "aligned"）
SET aligned_data_root = '/data';
SELECT * FROM aligned_scan('mytable');
```

> **注意**：
> - `INSTALL` 会将扩展复制到 DuckDB 本地缓存目录
>   （`~/.duckdb/extensions/<version>/<platform>/`）。
> - 更新扩展时用 `FORCE INSTALL '/path/to/aligned.duckdb_extension';` 覆盖旧版本。
> - CLI 版本必须为 v1.5.4（与扩展构建版本一致），否则加载时报版本不匹配错误。

### 1.3 在 Python（duckdb 库）中加载

```python
import duckdb

# 必须在连接时设置 allow_unsigned_extensions（运行中不可更改）
con = duckdb.connect(config={'allow_unsigned_extensions': True})

# 方式 A：直接 LOAD 完整路径
con.execute("LOAD '/path/to/aligned.duckdb_extension';")

# 方式 B：INSTALL 到缓存 + LOAD 短名
con.execute("INSTALL '/path/to/aligned.duckdb_extension';")
con.execute("LOAD aligned;")

# 使用
con.execute("SET aligned_data_root = '/data';")
con.execute("SELECT * FROM aligned_create('mytable', 'index', 'symbol VARCHAR, date DATE, close DOUBLE');").fetchall()

# ATTACH + 标准 DML
con.execute("ATTACH '/data' AS al (TYPE ALIGNED);")
con.execute("INSERT INTO al.mytable VALUES ('000001', DATE '2026-01-15', 10.5);")
print(con.execute("SELECT * FROM al.mytable;").fetchall())
con.execute("DETACH al;")
```

> **版本要求**：Python duckdb 库版本必须与扩展构建版本一致（v1.5.4）。
> 使用 `pip install duckdb==1.5.4` 安装匹配版本。

---

## 2. 快速开始

```sql
-- 1. 加载扩展（静态构建无需此步）
LOAD aligned;

-- 2. 设置数据根目录
SET aligned_data_root = 'D:/data/factorlake';

-- 3. 建表（通过表函数，group='index' 表示新建表）
SELECT * FROM aligned_create('mytable', 'index', 'symbol VARCHAR, date DATE, close DOUBLE');

-- 4. 挂载为逻辑数据库（ATTACH 后可用标准 SQL 增删改查）
ATTACH 'D:/data/factorlake' AS al (TYPE ALIGNED);

-- 5. 插入数据
INSERT INTO al.mytable VALUES ('000001', DATE '2026-01-15', 10.5);

-- 6. 查询
SELECT symbol, date, close FROM al.mytable WHERE date = DATE '2026-01-15';

-- 7. 更新
UPDATE al.mytable SET close = 11.0 WHERE symbol = '000001' AND date = DATE '2026-01-15';

-- 8. 删除
DELETE FROM al.mytable WHERE symbol = '000001' AND date = DATE '2026-01-15';
```

---

## 3. ATTACH 挂载

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
SELECT * FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-08-17';
```

---

## 4. 增删改查（标准 SQL）

ATTACH 后，对 `al.<table>` 的标准 `INSERT` / `UPDATE` / `DELETE` / `SELECT` 直接生效，
无需调用表函数。引擎通过 catalog 钩子（`PlanInsert` / `PlanUpdate` / `PlanDelete`）
将 DML 操作路由到 Parquet 列组的直写。

### 4.1 SELECT（查）

```sql
SELECT <columns> FROM al.<table> [WHERE <conditions>];
```

- 支持 Projection Pushdown（只读涉及的列组）。
- 支持 Filter Pushdown（分区裁剪 + Parquet Row Group 裁剪）。
- 支持并行扫描。

### 4.2 INSERT（增）

```sql
INSERT INTO al.<table> [(col1, col2, ...)] VALUES (...), (...), ...;
INSERT INTO al.<table> SELECT ... FROM ...;
```

- **Upsert 语义**：主键 `(symbol, date)` 已存在则更新对应列，不存在则插入。
- **大批量自动分批**：单次 INSERT 超过 1M 行时自动分批提交（每批 1M 行，各自独立事务），避免 OOM。
- 返回值：标准 `Count`（插入的行数）。

### 4.3 UPDATE（改）

```sql
UPDATE al.<table> SET <col> = <value> [, ...] WHERE <conditions>;
```

- **Upsert 语义**：匹配的行被原地重写（重写受影响的 part 文件）。
- `WHERE` 条件通过扫描索引组定位 rowid，再解析为 `(symbol, date)` 主键。
- 仅重写 SET 列所在的列组，未涉及的列组不读写。

### 4.4 DELETE（删）

```sql
DELETE FROM al.<table> WHERE <conditions>;
```

- `WHERE` 条件通过扫描索引组定位 rowid，再解析为 `(symbol, date)` 主键。
- 删除规则：
  - 删空单 part 分区 → 整个分区目录移除。
  - 删空多 part 分区的最高索引 part → 直接移除该 part。
  - 删空多 part 分区的内部 part → **原地重写为 0 行空文件**（保留文件名索引，保持 index 分区内索引连续）。

---

## 5. 表函数 API

除了标准 SQL DML，还提供 5 个表函数，适用于不 ATTACH 的场景或需要细粒度控制的场景。

### 5.0 `aligned_create` — 建表 / 扩展列组

```sql
SELECT * FROM aligned_create(table_name, group_name, columns [, root => '...']
                             [, partition_template => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（将成为数据根目录下的子目录）。 |
| `group_name` | 位置参数 2 | VARCHAR | 是 | 列组路径：`'index'`（建表）或 `'lv1/lv2'`（扩展列组，如 `'factor/alpha'`）。 |
| `columns` | 位置参数 3 | VARCHAR | 是 | 该组的列定义字符串，如 `'symbol VARCHAR, date DATE, close DOUBLE'`。由 DuckDB SQL 解析器解析。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |
| `partition_template` | 命名参数 | VARCHAR | 否 | 分区模板，默认 `month=%Y-%m`。可选 `year=%Y` / `date=%Y-%m-%d`。仅建表时生效。 |

**返回**：单行 `(dirs_created BIGINT, files_created BIGINT, txid BIGINT)`。

**两种模式**：
- **`group_name='index'`**：**建表**。前两列必须 `(symbol VARCHAR, date DATE/TIMESTAMP)`（v8 主键契约）。所有列写入 index 组。创建 0 行占位 parquet。
- **`group_name='lv1/lv2'`**：**扩展列组**。表必须已存在。新组的列定义不需含主键。每个已有分区写 N 行全 NULL 占位 parquet（N = index 分区行数），满足分区对齐契约。已有列组不受影响。

```sql
-- 建表（index 组含所有列）
SELECT * FROM aligned_create('mytable', 'index', 'symbol VARCHAR, date DATE, close DOUBLE');

-- 扩展列组（向已有表添加 factor/alpha 组）
SELECT * FROM aligned_create('mytable', 'factor/alpha', 'alpha001 DOUBLE, alpha002 DOUBLE');

-- 建表 + 指定分区模板
SELECT * FROM aligned_create('ptbl', 'index', 'symbol VARCHAR, date DATE, close DOUBLE',
                             partition_template => 'date=%Y-%m-%d');
```

### 5.1 `aligned_scan` — 扫描逻辑表

```sql
SELECT * FROM aligned_scan(table_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（数据根目录下的子目录名）。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root` 设置。 |

**返回**：表的全部列（schema 从 Parquet footer 推导）。

```sql
SET aligned_data_root = 'D:/data/factorlake';
SELECT symbol, date, close FROM aligned_scan('cnstk_ixday')
  WHERE date >= DATE '2026-01-01' AND date <= DATE '2026-01-31';

-- 或指定 root
SELECT * FROM aligned_scan('cnstk_ixday', root => 'D:/data/factorlake');
```

### 5.2 `aligned_groups` — 查看列组

```sql
SELECT * FROM aligned_groups(table_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |

**返回**：每组一行 `(group_name VARCHAR, columns VARCHAR, partition_count BIGINT)`。

| 列 | 说明 |
|------|------|
| `group_name` | 列组路径（`'index'`、`'factor/alpha101'`、`'fieldset/ma'` 等）。 |
| `columns` | 该组的列名列表，分号分隔（如 `alpha001;alpha002`）。 |
| `partition_count` | 该组的分区数。 |

```sql
SET aligned_data_root = 'D:/data/factorlake';
SELECT * FROM aligned_groups('cnstk_ixday');
-- 结果示例：
--   index      symbol;date;close;volume   3
--   factor/alpha   alpha001;alpha002       3
--   fieldset/ma    ma5;ma20                3
```

### 5.3 `aligned_compact` — 合并 part 碎片

```sql
SELECT * FROM aligned_compact(table_name, group_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `group_name` | 位置参数 2 | VARCHAR | 是 | 要合并的列组名，或 `'all'` 合并所有组。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。 |

**返回**：单行 `(dirs_compacted BIGINT, parts_before BIGINT, parts_after BIGINT)`。

**行为**：
- **规范化重写**：每个分区的所有 part 按 `ALIGNED_DEFAULT_PART_ROWS`（1M 行）重新切分——前面的 part 满行（恰好 1M 行），末 part ≤ 1M 行。0 行占位 part 被合并吸收。
- 已规范化的分区（单 part ≤ 1M，或多 part 均满行）跳过不重写。
- **两阶段提交**：所有组的合并 part 先写入 `_tmp/`，全部成功后再统一 move 到目标目录 + 删除旧 part；任一组失败则清理 `_tmp`、表状态不变。
- 同目录必须同列集（拒绝 schema-evolution 合并）。

```sql
-- 合并单个组
SELECT * FROM aligned_compact('cnstk_ixday', 'factor/alpha001');

-- 合并所有组（单事务原子切换）
SELECT * FROM aligned_compact('cnstk_ixday', 'all');
```

### 5.4 `aligned_drop` — 删除列组或整表

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

## 6. 配置项

### 6.1 `aligned_data_root`

| 属性 | 值 |
|------|-----|
| 类型 | VARCHAR |
| 默认值 | 无（未设置时使用 `root =>` 参数的函数会报错） |
| 作用域 | 全局 / Session |

设置数据根目录。当表函数的 `root` 参数省略时使用此值。

```sql
SET aligned_data_root = 'D:/data/factorlake';
-- 之后所有 aligned_scan/create/compact/drop 调用无需传 root
```

### 6.2 `parquet_metadata_cache`

| 属性 | 值 |
|------|-----|
| 类型 | BOOLEAN |
| 默认值 | `true`（扩展加载时强制开启） |
| 作用域 | 全局 |

Parquet footer / schema / Row Group statistics 的 LRU 缓存。跨查询跨线程共享。
扩展加载时强制设为 `true`，通常无需手动调整。

---

## 7. 参数详解

### 7.1 `table_name`（表名）

- 类型：VARCHAR
- 含义：逻辑表名，对应数据根目录下的一个一级子目录。
- 示例：`'cnstk_ixday'` → `<root>/cnstk_ixday/`

### 7.2 `root`（数据根目录）

- 类型：VARCHAR
- 含义：所有逻辑表的顶层父目录。
- 路径格式：正斜杠或反斜杠均可（内部统一为正斜杠）。
- 可通过 `SET aligned_data_root` 设为默认值，避免每次传参。
- 在 ATTACH 模式下，`root` 就是 ATTACH 路径，无需单独指定。

### 7.3 `group_name`（列组路径）

- 类型：VARCHAR
- 含义：列组路径，用于 `aligned_create`、`aligned_compact`、`aligned_drop`。
- 特殊值：
  - `'index'`：index 组（Key 列 symbol/date 所在组）。`aligned_create` 中表示新建表；`aligned_drop` 中表示删除整张表。
  - `'all'`：`aligned_compact` 专用，合并表中所有列组（单事务原子切换）。
- 非 index 组名格式：必须是 `lv1/lv2` 两级路径，如 `factor/alpha101`、`fieldset/ma`。
- **各函数用法**：
  - `aligned_create`：`group='index'` → 建表（所有列写入 index 组）；`group='factor/alpha'` → 向已有表扩展列组。
  - `aligned_compact`：`group='factor/alpha'` → 合并该组；`group='all'` → 合并所有组。
  - `aligned_drop`：`group='factor/alpha'` → 删除该列组目录；`group='index'` → 删除整张表。

### 7.4 WITH 子句参数（CREATE TABLE）

CREATE TABLE 的 `WITH (...)` 子句支持以下选项：

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `groups` | VARCHAR | 空（所有列归 index） | 列→组映射。格式 `"index:close;factor/alpha:alpha001"`。 |
| `partition_template` | VARCHAR | `month=%Y-%m` | 分区模板。可选 `year=%Y` / `month=%Y-%m` / `date=%Y-%m-%d`。 |
| `partition` | VARCHAR | 空 | 创建空分区时指定分区键。如 `month=2026-10`。仅用于已有表。 |

---

## 8. C++ 内部 API（供扩展开发者）

以下 API 供 DuckDB 扩展开发者在 C++ 层直接调用，**不需要** 通过 SQL。

### 8.1 扫描绑定

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

### 8.2 内存 Upsert

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

### 8.3 内存 Delete

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

### 8.4 建表

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

### 8.5 写锁（RAII）

```cpp
TableWriteLock lock(fs, table_path);
```

在 `<table_path>/.aligned_write.lock` 创建锁文件；已存在则抛异常。析构时删除锁文件。
mutator 和 compactor 内部自动使用。崩溃残留需手动删除。

### 8.6 事务 ID

```cpp
idx_t NextTransactionId();
```

进程级共享原子计数器，用于 `_tmp/transaction-<id>/` 暂存目录命名。不持久化。

---

## 附录：返回值含义

| 函数 | 返回列 | 含义 |
|------|--------|------|
| `aligned_compact` | `dirs_compacted` | 合并的分区目录数 |
| | `parts_before` | 合并前 part 文件总数 |
| | `parts_after` | 合并后 part 文件总数 |
| `aligned_drop` | `dirs_removed` | 删除的目录数（含分区子目录） |
| | `files_removed` | 删除的 parquet 文件数 |
| | `txid` | 事务 ID |
| `aligned_create` | `dirs_created` | 创建的目录数（含表目录、组目录、分区目录） |
| | `files_created` | 创建的 parquet 文件数（占位文件） |
| | `txid` | 事务 ID |
| `aligned_groups` | `group_name` | 列组路径 |
| | `columns` | 该组的列名列表（分号分隔） |
| | `partition_count` | 该组的分区数 |
| `INSERT` (标准 SQL) | `Count` | 插入的行数 |
| `UPDATE` (标准 SQL) | `Count` | 更新的行数 |
| `DELETE` (标准 SQL) | `Count` | 删除的行数 |
