# FactorLake / AlignedTable — API 对接文档

> 本文档面向需要通过 DuckDB 对接 AlignedTable 存储引擎的项目。
> 涵盖 ATTACH 挂载、COPY TO 批量写入、表函数 API 及全部参数说明。

---

## 目录

1. [扩展安装与加载](#1-扩展安装与加载)
2. [快速开始](#2-快速开始)
3. [ATTACH 挂载](#3-attach-挂载)
4. [标准 DML 不支持](#4-标准-dml-不支持)
5. [COPY TO (FORMAT aligned) — 批量写入](#5-copy-to-format-aligned-批量写入)
6. [表函数 API](#6-表函数-api)
7. [配置项](#7-配置项)
8. [参数详解](#8-参数详解)
9. [C++ 内部 API（供扩展开发者）](#9-c-内部-api供扩展开发者)

---

## 1. 扩展安装与加载

AlignedTable 扩展是一个 unsigned 的 DuckDB 扩展二进制文件（`.duckdb_extension`），
基于 DuckDB **v1.5.5** 构建。加载时强校验 DuckDB 版本，必须与构建版本一致。

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
# 安装（下载或复制到 ~/.duckdb/extensions/v1.5.5/<platform>/）
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
> - CLI 版本必须为 v1.5.5（与扩展构建版本一致），否则加载时报版本不匹配错误。

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

# ATTACH + 批量写入（COPY TO）
con.execute("ATTACH '/data' AS al (TYPE ALIGNED);")
con.execute("COPY (SELECT '000001' AS symbol, DATE '2026-01-15' AS date, 10.5 AS close) TO 'mytable' (FORMAT aligned, GROUP 'index');")
print(con.execute("SELECT * FROM al.mytable;").fetchall())
con.execute("DETACH al;")
```

> **版本要求**：Python duckdb 库版本必须与扩展构建版本一致（v1.5.5）。
> 使用 `pip install duckdb==1.5.5` 安装匹配版本。

---

## 2. 快速开始

```sql
-- 1. 加载扩展（静态构建无需此步）
LOAD aligned;

-- 2. 设置数据根目录
SET aligned_data_root = 'D:/data/factorlake';

-- 3. 建表（通过表函数，group='index' 表示新建表）
SELECT * FROM aligned_create('mytable', 'index', 'symbol VARCHAR, date DATE, close DOUBLE');

-- 4. 挂载为逻辑数据库（ATTACH 后可用标准 SQL 查询）
ATTACH 'D:/data/factorlake' AS al (TYPE ALIGNED);

-- 5. 批量写入数据（COPY TO 是唯一的写入路径）
COPY (SELECT '000001' AS symbol, DATE '2026-01-15' AS date, 10.5 AS close)
  TO 'mytable' (FORMAT aligned, GROUP 'index');

-- 6. 查询
SELECT symbol, date, close FROM al.mytable WHERE date = DATE '2026-01-15';
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

## 4. 标准 DML 不支持

标准 DML（INSERT/UPDATE/DELETE）不支持。使用 COPY TO (FORMAT aligned) 进行写入，MERGE true 进行增量合并。

ATTACH 后仅支持 `SELECT` 查询（详见 §3）：

```sql
ATTACH 'D:/data/factorlake' AS al (TYPE ALIGNED);
SELECT symbol, date, close FROM al.<table> WHERE date = DATE '2026-01-15';
DETACH al;
```

写入请使用 [COPY TO (FORMAT aligned)](#5-copy-to-format-aligned-批量写入)（详见 §5）。

---

## 5. COPY TO (FORMAT aligned) — 批量写入

`COPY TO (FORMAT aligned)` 是**批量写入主路径**（也是唯一的写入路径），走 DuckDB
CopyFunction 框架，适用于大批量数据的一次性灌入（如因子批算、历史回填）。它走单向
数据流 pipeline（Sink → Combine → Flush → Finalize），吞吐高，尤其适合整组覆盖写入场景。

### 5.1 语法

```sql
COPY (SELECT ...) TO '<table_name>' (FORMAT aligned, GROUP '<group_name>');
```

| 选项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `FORMAT aligned` | 固定关键字 | 是 | 指定使用 AlignedTable 批量写入。`TO` 后的字符串是**逻辑表名**（数据根目录下的子目录），不是文件路径。 |
| `GROUP '<group_name>'` | VARCHAR | 是 | 目标列组：`'index'` 或 `'panel/ma'`、`'factor/alpha'` 等 `lv1/lv2` 两级路径。 |

> **注意**：`GROUP` 选项是**必填**的——COPY TO 必须明确写入哪个列组。`TO` 后的字符串是
> **逻辑表名**，不是文件路径。

### 5.2 新组首次写入（schema 从 query 推断）

目标列组不存在时，需先用 `aligned_create` 的 **2-arg 形式**（详见 §6.0）创建空组目录，
再执行首次 COPY：

```sql
-- 1. 创建空组目录（不写任何 parquet，schema 留待首次 COPY 推断）
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');

-- 2. 首次 COPY：从 query 列推断组 schema（排除 index key 列 symbol/date）
COPY (SELECT symbol, date, ma5, ma20, ma60 FROM mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');
```

- 首次 COPY 时，引擎从 query 的输出列中**排除 index key 列（symbol、date）**，
  剩余列即新组的 schema，按 query 列顺序写入 Parquet。
- Key 列（symbol/date）只在 `index/` 组保存一份，非 index 组不再重复存储。

### 5.3 已有组写入（schema 从 footer 读取 + 列裁剪）

组已存在（已有 parquet 文件）时，schema 从组内最后一个 part 的 Parquet footer 读取，
COPY 时只写组 schema 包含的列，按组 schema 顺序重排：

```sql
-- index 组写入（schema 已由建表确定）
COPY (SELECT * FROM mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'index');
```

- 输入列类型 ≠ 组 schema 类型时自动 cast（如 TIMESTAMP → DATE）。
- query 中不在组 schema 内的列会被忽略；缺少组 schema 需要的列则报错。

### 5.4 写入行为

- **per-partition 覆盖**：每个分区目录首次写入时**自动清理旧 parquet 文件**，
  无需 `OVERWRITE true` 选项。同一 COPY 语句内多次命中同一分区则追加。
- **自描述文件名**：先以 `0000-0000000000.parquet` 临时名写入，Finalize 后 rename 为
  `{idx:04d}-{rows:10d}.parquet`（`idx` = 组内 part 序号，`rows` = 该 part 实际行数）。
  0 行空文件自动删除。
- **RG / Part 切分**：Row Group flush size = 131072；单个 part 文件上限 = 8 RG
  = 1048576 行（`ALIGNED_DEFAULT_PART_ROWS`）。满 8 RG 轮转新 part 文件。
- **排序**：用户须在 query 中 `ORDER BY (symbol, date)` 保证分区内有序；COPY 在
  `REGULAR_COPY_TO_FILE` 执行模式下保留输入顺序。
- **统计校验**：每个 PartitionWriter 跟踪 `received_rows` / `flushed_rows` /
  `written_rows`，Finalize 时校验 `received == written`，不匹配抛 `InternalException`。

### 5.5 并行写入

默认单线程写入。开启多线程：

```sql
SET preserve_insertion_order = false;
COPY (SELECT * FROM huge_mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'factor/alpha');
```

`preserve_insertion_order = false` 允许 DuckDB 并行执行写入 pipeline，多线程同时写不同
分区。**注意**：关闭插入顺序后仍须在 query 内 `ORDER BY (symbol, date)`，以保证每个
分区**内部**有序（分区内有序是 AlignedTable 行对齐契约的要求）。

### 5.6 完整示例

```sql
SET aligned_data_root = 'D:/data/factorlake';

-- 场景 A：向新组 panel/ma 批量灌入（组不存在）
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');   -- 创建空组
COPY (SELECT symbol, date, ma5, ma20, ma60
      FROM mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');

-- 场景 B：向已有 index 组批量覆盖写入
COPY (SELECT symbol, date, close, volume
      FROM mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'index');
```

---

## 6. 表函数 API

提供 6 个表函数，适用于不 ATTACH 的场景或需要细粒度控制的场景。

### 6.0 `aligned_create` — 建表 / 扩展列组

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
- **`group_name='index'`**：**建表**。前两列的**类型**必须满足主键契约：col0 = VARCHAR（symbol 列），col1 = DATE/TIMESTAMP（date 列，分区源列）。列名可自定义（详见 §8.4）。所有列写入 index 组。创建 0 行占位 parquet。
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

#### 2-arg 形式：创建空组（供 COPY TO 批量灌入）

省略 `columns` 位置参数，仅传 `table_name` + `group_name`（及可选命名参数）：

```sql
SELECT * FROM aligned_create(table_name, group_name
                             [, root => '...']
                             [, partition_template => '...']);
```

**创建一个空组目录**（不写任何 parquet 文件，无 footer）。组的 schema 留待首次
`COPY TO (FORMAT aligned)`（详见 §5）时从 query 输出列推断——引擎自动排除 index key
列（symbol、date），剩余列即新组 schema，按 query 列顺序写入。

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（表必须已存在）。 |
| `group_name` | 位置参数 2 | VARCHAR | 是 | 列组路径，必须是 `lv1/lv2` 两级路径（**不能是 `'index'`**）。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |
| `partition_template` | 命名参数 | VARCHAR | 否 | 分区模板，默认 `month=%Y-%m`。 |

**返回**：单行 `(dirs_created BIGINT, files_created BIGINT, txid BIGINT)`，其中
`files_created = 0`（空组不创建 parquet）。

> **这是为 `COPY TO` 批量灌入创建非 index 组的推荐方式**。相比 3-arg 形式（需预先给出
> 完整列定义 + 每个已有分区写 N 行全 NULL 占位），2-arg 形式零开销创建空目录，schema
> 由首次灌入的数据自然确定。
>
> 2-arg 与 3-arg 注册为 `TableFunctionSet` 的两个独立 overload（DuckDB 表函数不支持
> 可选位置参数），按实参数量自动分发。

```sql
-- 创建空组，schema 留待首次 COPY 推断
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');

-- 首次 COPY 时从 query 列推断 schema（排除 symbol/date key 列）
COPY (SELECT symbol, date, ma5, ma20, ma60 FROM mock ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');
```

### 6.1 `aligned_scan` — 扫描逻辑表

```sql
-- 全列扫描（投影下推自然限制打开的 parquet 文件）
SELECT * FROM aligned_scan(table_name [, root => '...']);

-- 列组过滤扫描（仅打开 index + 指定组的 parquet）
SELECT * FROM aligned_scan(table_name, group_filter [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名（数据根目录下的子目录名）。 |
| `group_filter` | 位置参数 2 | VARCHAR | 否 | 列组过滤：单个组名或逗号分隔列表（如 `'factor/alpha101'` 或 `'factor/alpha101,fieldset/ma'`）。指定后仅扫描匹配的列组 + index 组（index 始终包含，提供主键列 symbol/date）。未指定时所有列组可用。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root` 设置。 |

> **注意**：DuckDB 表函数不支持可选位置参数，`aligned_scan` 注册为 `TableFunctionSet`
> 两个独立 overload（1-arg 和 2-arg），而非单个函数 + 默认参数。

**返回**：表的列（schema 从 Parquet footer 推导）。不指定 `group_filter` 时返回
所有列组的列；指定时仅返回 index 组 + 匹配组的列。

#### 列名命名规则

`aligned_scan` 的输出列名遵循以下规则：

| 列类型 | 输出列名 | 可查询方式 | 说明 |
|--------|---------|-----------|------|
| index 组列 | 裸名（如 `symbol`, `date`, `close`） | 直接标识符 | index 组列始终用裸名 |
| 非 index 唯一列 | 裸名（如 `alpha001`） | 直接标识符 | 仅出现在一个非 index 组中的列 |
| 非 index 唯一列的限定别名 | `lv1.lv2.col`（如 `factor.alpha.alpha001`） | `COLUMNS('factor.alpha.alpha001')` | 同一列的第二个输出别名 |
| 跨组重名列 | `lv1.lv2.col`（如 `factor.alpha.vwap`） | `COLUMNS('factor.alpha.vwap')` | 出现在 2+ 个非 index 组中的列，**只能**通过限定名引用 |
| index shadow 列（非 index 组中与 index 同名） | 不输出 | — | 被扫描路径忽略，不作为独立输出列 |

> **DuckDB SQL 限制**：DuckDB SQL 解析器将 `.` 视为 `schema.table.column` 分隔符，
> 因此包含 `.` 的列名**不能用标识符语法直接引用**（如 `SELECT "factor.alpha.alpha001"` 
> 会被解析为表引用而非列引用）。必须使用 `COLUMNS()` 正则函数：
>
> ```sql
> -- 引用单个限定列
> SELECT COLUMNS('factor.alpha.alpha001') FROM aligned_scan('my_table');
> 
> -- 正则匹配整组列
> SELECT COLUMNS('factor.alpha.*') FROM aligned_scan('my_table');
> 
> -- 正则匹配多个组
> SELECT COLUMNS('(factor|fieldset).*') FROM aligned_scan('my_table');
> ```

```sql
SET aligned_data_root = 'D:/data/factorlake';

-- 全列扫描（投影下推自然只打开需要的组）
SELECT symbol, date, close FROM aligned_scan('cnstk_ixday')
  WHERE date >= DATE '2026-01-01' AND date <= DATE '2026-01-31';

-- 列组过滤：仅打开 index + factor/alpha101
SELECT * FROM aligned_scan('cnstk_ixday', 'factor/alpha101');

-- 列组过滤：多组逗号分隔
SELECT * FROM aligned_scan('cnstk_ixday', 'factor/alpha101,fieldset/ma');

-- 限定列名查询（COLUMNS 正则）
SELECT COLUMNS('factor.alpha101.alpha001') FROM aligned_scan('cnstk_ixday')
  WHERE symbol = '000001';

-- 限定列名正则匹配整组
SELECT COLUMNS('factor.alpha101.*') FROM aligned_scan('cnstk_ixday')
  WHERE date = DATE '2026-08-17';

-- 或指定 root
SELECT * FROM aligned_scan('cnstk_ixday', root => 'D:/data/factorlake');
```

### 6.2 `aligned_groups` — 查看列组

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

### 6.3 `aligned_compact` — 合并 part 碎片

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
- **并行暂存**：Phase 1 暂存阶段并行处理各分区目录（每个目录的读-写独立），Phase 2 提交仍串行。
- 已规范化的分区（单 part ≤ 1M，或多 part 均满行）跳过不重写。
- **两阶段提交**：所有组的合并 part 先写入 `_tmp/`，全部成功后再统一 move 到目标目录 + 删除旧 part；任一组失败则清理 `_tmp`、表状态不变。
- 同目录必须同列集（拒绝 schema-evolution 合并）。

```sql
-- 合并单个组
SELECT * FROM aligned_compact('cnstk_ixday', 'factor/alpha001');

-- 合并所有组（单事务原子切换）
SELECT * FROM aligned_compact('cnstk_ixday', 'all');
```

### 6.4 `aligned_drop` — 删除列组或整表

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

### 6.5 `aligned_meta` — 查看表元数据

```sql
SELECT * FROM aligned_meta(table_name [, root => '...']);
```

| 参数 | 位置 | 类型 | 必填 | 说明 |
|------|------|------|------|------|
| `table_name` | 位置参数 1 | VARCHAR | 是 | 逻辑表名。 |
| `root` | 命名参数 | VARCHAR | 否 | 数据根目录。省略时使用 `aligned_data_root`。 |

**返回**：单行 11 列元数据。

| 列 | 类型 | 说明 |
|----|------|------|
| `table_name` | VARCHAR | 表名（目录名）。 |
| `table_path` | VARCHAR | 表的相对路径（`./<table>`）。 |
| `partition_template` | VARCHAR | 分区模板（`year=%Y` / `month=%Y-%m` / `date=%Y-%m-%d`），从 index 组目录结构推导。 |
| `total_rows` | BIGINT | 总行数（index 组所有分区行数之和）。 |
| `group_count` | BIGINT | 列组数量（含 index）。 |
| `partition_count` | BIGINT | 分区数（index 组的分区数）。 |
| `part_count` | BIGINT | parquet 文件总数（所有组之和）。 |
| `groups` | VARCHAR | 列组详情，格式 `group:col1,col2;group2:col3,col4`。 |
| `partitions` | VARCHAR | 分区键列表（逗号分隔，如 `year=2024,year=2025`）。 |
| `schema` | VARCHAR | 完整表 schema，格式 `queryable_name:type,...`（见下方命名规则）。 |
| `column_mapping` | VARCHAR | 列名映射，格式 `queryable_name:lv1.lv2.col;...`（见下方命名规则）。 |

#### `schema` 字段命名规则

`schema` 字段列出表的全部可查询列，每列格式 `queryable_name:type`：

| 列类型 | `schema` 中的名称 | 示例 |
|--------|-----------------|------|
| index 组列 | 裸名 | `symbol:VARCHAR`, `close:DOUBLE` |
| 非 index 唯一列 | 裸名 | `alpha001:DOUBLE` |
| 跨组重名列（2+ 个非 index 组含同名列） | 限定名 `lv1.lv2.col`（每组各一行） | `factor.alpha.vwap:DOUBLE`, `panel.ma.vwap:DOUBLE` |
| index shadow（非 index 组中与 index 同名） | 跳过 | — |

#### `column_mapping` 字段命名规则

`column_mapping` 字段映射每个非 index 列的可查询名到全限定名，格式
`queryable_name:lv1.lv2.col`：

| 列类型 | `column_mapping` 中的条目 | 示例 |
|--------|--------------------------|------|
| 非 index 唯一列 | `bare_name:lv1.lv2.col` | `alpha001:factor.alpha.alpha001` |
| 跨组重名列 | `lv1.lv2.col:lv1.lv2.col`（自别名） | `factor.alpha.vwap:factor.alpha.vwap` |
| index shadow 列 | 跳过 | — |
| index 列本身 | 跳过（已在 schema 中用裸名） | — |

> **用途**：`column_mapping` 帮助确定每个列的限定名，用于通过 `COLUMNS()` 
> 正则引用列。跨组重名列只能通过限定名访问，因此 `column_mapping` 中它们的
> 可查询名就是限定名本身。

```sql
SET aligned_data_root = 'D:/data/factorlake';
SELECT * FROM aligned_meta('cnstk_ixday');

-- 仅查看列名映射
SELECT column_mapping FROM aligned_meta('cnstk_ixday');
-- 示例结果：
--   alpha001:factor.alpha101.alpha001;alpha002:factor.alpha101.alpha002;
--   ma5:fieldset.ma.ma5;factor.alpha101.vwap:factor.alpha101.vwap;
--   fieldset.ma.vwap:fieldset.ma.vwap

-- 仅查看 schema
SELECT schema FROM aligned_meta('cnstk_ixday');
-- 示例结果：
--   symbol:VARCHAR,date:DATE,close:DOUBLE,alpha001:DOUBLE,alpha002:DOUBLE,
--   ma5:DOUBLE,factor.alpha101.vwap:DOUBLE,fieldset.ma.vwap:DOUBLE
```

---

## 7. 配置项

### 7.1 `aligned_data_root`

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

### 7.2 `parquet_metadata_cache`

| 属性 | 值 |
|------|-----|
| 类型 | BOOLEAN |
| 默认值 | `true`（扩展加载时强制开启） |
| 作用域 | 全局 |

Parquet footer / schema / Row Group statistics 的 LRU 缓存。跨查询跨线程共享。
扩展加载时强制设为 `true`，通常无需手动调整。

---

## 8. 参数详解

### 8.1 `table_name`（表名）

- 类型：VARCHAR
- 含义：逻辑表名，对应数据根目录下的一个一级子目录。
- 示例：`'cnstk_ixday'` → `<root>/cnstk_ixday/`

### 8.2 `root`（数据根目录）

- 类型：VARCHAR
- 含义：所有逻辑表的顶层父目录。
- 路径格式：正斜杠或反斜杠均可（内部统一为正斜杠）。
- 可通过 `SET aligned_data_root` 设为默认值，避免每次传参。
- 在 ATTACH 模式下，`root` 就是 ATTACH 路径，无需单独指定。

### 8.3 `group_name`（列组路径）

- 类型：VARCHAR
- 含义：列组路径，用于 `aligned_create`、`aligned_compact`、`aligned_drop`。
- 特殊值：
  - `'index'`：index 组（Key 列所在组，详见 §8.4 主键契约）。`aligned_create` 中表示新建表；`aligned_drop` 中表示删除整张表。
  - `'all'`：`aligned_compact` 专用，合并表中所有列组（单事务原子切换）。
- 非 index 组名格式：必须是 `lv1/lv2` 两级路径，如 `factor/alpha101`、`fieldset/ma`。
- **各函数用法**：
  - `aligned_create`：`group='index'` → 建表（所有列写入 index 组）；`group='factor/alpha'` → 向已有表扩展列组。
  - `aligned_compact`：`group='factor/alpha'` → 合并该组；`group='all'` → 合并所有组。
  - `aligned_drop`：`group='factor/alpha'` → 删除该列组目录；`group='index'` → 删除整张表。

### 8.4 主键契约（symbol 列与 date 列）

AlignedTable 的主键是 `(symbol, date)` 复合键，但 **列名不固定**——引擎按 index 组
前两列的**类型**动态推断哪列是 symbol、哪列是 date，而非按列名匹配。

#### 类型约束

| 位置 | 约束 | 角色 |
|------|------|------|
| index 组 col0 | 必须 VARCHAR | symbol 列（标的标识） |
| index 组 col1 | 必须 DATE 或 TIMESTAMP | date 列（分区源列） |

- 前两列中**恰好一列 VARCHAR + 一列 DATE/TIMESTAMP**，否则建表报错。
- 两列都是 VARCHAR 或两列都是 DATE/TIMESTAMP → 报错。
- col0 非 VARCHAR 或 col1 非 DATE/TIMESTAMP → 读取端报错。
- **建议**统一使用 `(symbol VARCHAR, date DATE)` 顺序，col0=symbol、col1=date。

#### 列名可自定义

建表时可用任意列名，只要类型满足约束：

```sql
-- 合法：列名不叫 symbol / date，但类型符合
SELECT * FROM aligned_create('mytable', 'index',
    'ticker VARCHAR, trade_date DATE, close DOUBLE, volume BIGINT');
-- col0 = ticker (VARCHAR → symbol 列)
-- col1 = trade_date (DATE → date 列)
-- col2+ = close, volume (数据列)
```

后续所有查询 / scan 操作均使用建表时的列名，引擎从 Parquet footer 动态提取实际列名
传递给内部链路（scan、COPY writer），**绝不硬编码 `"symbol"` 或 `"date"`**。

```sql
-- 写入时用实际列名（COPY TO 是唯一的写入路径）
SET aligned_data_root = 'D:/data';
COPY (SELECT '000001' AS ticker, DATE '2026-01-15' AS trade_date, 10.5 AS close, 1000 AS volume)
  TO 'mytable' (FORMAT aligned, GROUP 'index');

-- 查询时同样用实际列名
ATTACH 'D:/data' AS al (TYPE ALIGNED);
SELECT ticker, trade_date, close FROM al.mytable WHERE trade_date = DATE '2026-01-15';
```

#### 分区源列

date 列（col1）是**分区源列**——其值决定行属于哪个分区目录（`month=2026-01`、
`date=2026-01-15` 等）。分区模板（`partition_template`）定义了分区目录的命名格式，
但分区值始终从 date 列的值求值得出。

### 8.5 列名与限定别名（lv1.lv2.col）

AlignedTable 的列分布在多个 Column Group 中。不同 Group 可能包含同名列（如 `close` 
同时出现在 `index` 和 `factor/alpha` 中）。`aligned_scan` 的输出列名和 
`aligned_meta` 的 `schema` / `column_mapping` 字段遵循统一的命名规则：

#### 列名分类

| 分类 | 条件 | 可查询名 | 可查询方式 |
|------|------|---------|-----------|
| **index 列** | index 组中的列 | 裸名（如 `symbol`） | 标识符 `SELECT symbol FROM ...` |
| **非 index 唯一列** | 仅出现在一个非 index 组中的列 | 裸名（如 `alpha001`） | 标识符 `SELECT alpha001 FROM ...` |
| **非 index 唯一列的限定别名** | 同上列的第二个输出 | 限定名 `lv1.lv2.col` | `COLUMNS('lv1.lv2.col')` |
| **跨组重名列** | 出现在 2+ 个非 index 组中的同名列 | 限定名 `lv1.lv2.col`（每组各一） | `COLUMNS('lv1.lv2.col')` |
| **index shadow 列** | 非 index 组中与 index 组同名的列 | 不输出（被忽略） | — |

> **唯一列同时注册裸名和限定名**：非 index 唯一列有两个输出位置——裸名用于
> 方便的直接引用，限定名用于避免歧义或正则匹配整组列。两者数据完全相同，
> parquet 列只读一次后复制到两个位置。

#### 为什么需要 `COLUMNS()` 正则引用

DuckDB SQL 解析器将 `.` 视为 `schema.table.column` 分隔符。因此包含 `.` 的列名
（如 `factor.alpha.alpha001`）**不能用标识符语法直接引用**：

```sql
-- ✗ 错误：被解析为表 factor 的列 alpha.alpha001
SELECT "factor.alpha.alpha001" FROM aligned_scan('my_table');

-- ✗ 错误：被解析为表 factor 的 schema alpha 的列 alpha001
SELECT factor.alpha.alpha001 FROM aligned_scan('my_table');

-- ✓ 正确：COLUMNS() 正则匹配
SELECT COLUMNS('factor.alpha.alpha001') FROM aligned_scan('my_table');

-- ✓ 正确：正则匹配整组
SELECT COLUMNS('factor.alpha.*') FROM aligned_scan('my_table');
```

#### 使用 `aligned_meta` 查询映射

```sql
-- 查看列名映射（裸名 → 限定名）
SELECT column_mapping FROM aligned_meta('cnstk_ixday');
-- alpha001:factor.alpha101.alpha001;factor.alpha101.vwap:factor.alpha101.vwap;
-- fieldset.ma.vwap:fieldset.ma.vwap;...

-- 查看 schema（可查询名:类型）
SELECT schema FROM aligned_meta('cnstk_ixday');
-- symbol:VARCHAR,date:DATE,close:DOUBLE,alpha001:DOUBLE,...,
-- factor.alpha101.vwap:DOUBLE,fieldset.ma.vwap:DOUBLE

-- 从 mapping 提取某组的所有限定名
SELECT split_part(column_mapping, ';', 1);  -- 第一条映射
```

### 8.6 WITH 子句参数（CREATE TABLE）

CREATE TABLE 的 `WITH (...)` 子句支持以下选项：

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `groups` | VARCHAR | 空（所有列归 index） | 列→组映射。格式 `"index:close;factor/alpha:alpha001"`。 |
| `partition_template` | VARCHAR | `month=%Y-%m` | 分区模板。可选 `year=%Y` / `month=%Y-%m` / `date=%Y-%m-%d`。 |
| `partition` | VARCHAR | 空 | 创建空分区时指定分区键。如 `month=2026-10`。仅用于已有表。 |

---

## 9. C++ 内部 API（供扩展开发者）

以下 API 供 DuckDB 扩展开发者在 C++ 层直接调用，**不需要** 通过 SQL。

### 9.1 扫描绑定

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

### 9.2 建表

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

### 9.3 写锁（RAII）

```cpp
TableWriteLock lock(fs, table_path);
```

在 `<table_path>/.aligned_write.lock` 创建锁文件；已存在则抛异常。析构时删除锁文件。
COPY writer 和 compactor 内部自动使用。崩溃残留需手动删除。

### 9.4 事务 ID

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
| `aligned_meta` | `table_name` | 表名（目录名） |
| | `table_path` | 表相对路径 |
| | `partition_template` | 分区模板（`year=%Y` / `month=%Y-%m` / `date=%Y-%m-%d`） |
| | `total_rows` | 总行数 |
| | `group_count` | 列组数（含 index） |
| | `partition_count` | 分区数 |
| | `part_count` | parquet 文件总数 |
| | `groups` | 列组详情（`group:col1,col2;...`） |
| | `partitions` | 分区键列表（逗号分隔） |
| | `schema` | 完表 schema（`queryable_name:type,...`） |
| | `column_mapping` | 列名映射（`queryable_name:lv1.lv2.col;...`） |
