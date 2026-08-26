# FactorLake / AlignedTable

基于 **DuckDB Extension + Parquet** 的超宽表存储/查询引擎。

> 逻辑上是一张几万甚至上十万列、10^8~10^10 行的宽表；
> 物理上拆成多个 **Column Group**，每个 Group 独立用 Parquet 存储；
> **Key 列只保存一份**；所有 Group 在 **Logical Row Space** 上严格 position-aligned；
> 查询时由 DuckDB Extension 把多个 Parquet Reader 的 Vector **直接组装进同一个 DataChunk**，
> **不做 JOIN、不做横向 materialize**。

## 用法

```sql
LOAD aligned;
SET aligned_data_root = '/data';

-- 逻辑上是一张超宽表：
SELECT date, symbol, close, alpha001, alpha002, ma20
FROM aligned_scan('cnstk_ixday')
WHERE date = DATE '2026-08-17';

-- 或者显式指定根目录：
SELECT * FROM aligned_scan('cnstk_ixday', root => '/data');
```

底层可能只打开三个目录：`index/`、`factor/alpha101/`、`fieldset/ma/`。
`WHERE date = ...` 会做分区剪枝，`WHERE symbol = ...` 会做 Parquet min/max
Row Group 剪枝；跨 Group 的所有行天然对齐，无需 JOIN。

```sql
-- 建表（aligned_create 表函数，0 行占位 parquet 携带 schema）：
SELECT * FROM aligned_create('cnstk_ixday', 'index', 'symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE',
                             partition_template => 'month=%Y-%m');

-- 扩展列组（向已有表添加新列组，N 行全 NULL 占位满足分区对齐契约）：
SELECT * FROM aligned_create('cnstk_ixday', 'factor/alpha101', 'alpha001 DOUBLE, alpha002 DOUBLE');

-- 合并 part（单事务合并所有组，按分区目录，原子切换）：
SELECT * FROM aligned_compact('cnstk_ixday', 'all', root => '/data');

-- 删除列组或整表：
SELECT * FROM aligned_drop('cnstk_ixday', 'factor/alpha101', root => '/data');

-- Attach（DuckLake 式逻辑 Attach，推荐）：把数据根挂载为 catalog 数据库，
-- 表保持「逻辑表」——SELECT 走 aligned 扫描（ATTACH 仅用于读访问，写入请用 COPY TO）：
ATTACH '/data' AS al (TYPE ALIGNED);

-- 建表（DDL 方式，写 0 行占位 parquet，footer 携带 schema）：
CREATE TABLE al.cnstk_ixday (symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE)
  WITH (groups='index:close;factor/alpha101:alpha001', partition_template='month=%Y-%m');

-- 在已有表上创建空分区：
CREATE TABLE al.cnstk_ixday (cols...) WITH (partition='month=2026-10');

-- 列组扩展：
CREATE TABLE al.cnstk_ixday (ma5 DOUBLE, ma20 DOUBLE) WITH (groups='fieldset/ma:ma5,ma20');

SELECT * FROM al.cnstk_ixday;
-- 注意：ATTACH + 标准 DML（INSERT/UPDATE/DELETE）不再支持。
-- 唯一写入路径是 COPY TO (FORMAT aligned)，见下方示例。
-- DETACH al; 即可卸载（数据始终在 parquet 列组里）。

-- 批量写入（COPY TO，推荐大批量数据加载）：
-- 1. 先创建空组（2-arg 形式，schema 首次 COPY 时从 query 推断）：
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');
-- 2. 批量写入（per-partition 自动覆盖，无需 OVERWRITE）：
SET preserve_insertion_order = false;  -- 启用并行写入
COPY (SELECT symbol, date, ma5, ma20 FROM source ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');
-- 已有组写入时自动列裁剪（只写组内列）+ 类型转换（如 TIMESTAMP → DATE）。

-- 限定列名（lv1.lv2.col）查询：
-- 非 index 唯一列同时有裸名和限定名，重名列只有限定名。
-- DuckDB SQL 解析器将 . 视为 schema.table.column 分隔符，带点列名需用 COLUMNS() 引用：
SELECT COLUMNS('factor.alpha101.alpha001') FROM aligned_scan('cnstk_ixday') WHERE symbol='000001';
SELECT COLUMNS('factor.alpha101.*') FROM aligned_scan('cnstk_ixday') WHERE date = DATE '2026-08-17';

-- 列组过滤扫描（仅打开 index + 指定组）：
SELECT * FROM aligned_scan('cnstk_ixday', 'factor/alpha101');
SELECT * FROM aligned_scan('cnstk_ixday', 'factor/alpha101,fieldset/ma');

-- 查看表元数据（含列名映射）：
SELECT column_mapping FROM aligned_meta('cnstk_ixday');
-- alpha001:factor.alpha101.alpha001;factor.alpha101.vwap:factor.alpha101.vwap;...
```

## 核心概念

- **Column Group**：`factor/alpha101`、`fieldset/ma` 等叶子目录各是一个 Group，
  独立 Parquet 存储。`index/` 保存 Key 列（symbol, date），其他 Group 不重复存 Key。
- **Position-Aligned**：所有 Group 在 Logical Row Space 上严格对齐——
  `index[row N] == alpha[row N] == ma[row N]` 是同一行，查询时直接组装，
  不做 JOIN。
- **Partition-Aligned**：所有 Group 用同一套分区段（`year=` / `month=` / `date=`），
  共享分区的总行数必须一致。
- **无 Manifest**：目录即 Catalog，schema 从 Parquet footer 推导，行数从文件名
  `{idx:04d}-{rows:10d}` 推导，无 `_table.json`、无 sidecar、无 commit marker。

## 已实现功能

| 能力 | 状态 | 说明 |
|------|------|------|
| 读取 | ✅ | `aligned_scan()`，多 Group 并行读、跨 part/RG 行窗口、Schema Evolution（缺失列补 NULL）、跨 Group 重复列遮蔽、Row Space 校验 |
| 列组过滤 | ✅ | `aligned_scan(table, group_filter)`：仅扫描 index + 指定组（逗号分隔），减少 IO |
| 限定列名 | ✅ | 非 index 唯一列同时注册裸名和 `lv1.lv2.col` 限定别名；跨组重名列用限定名。通过 `COLUMNS('lv1.lv2.col')` 正则引用 |
| 投影下推 | ✅ | 只打开被选中的 Group、只读被选中的列（`SELECT alpha001` 只碰 alpha101） |
| 过滤下推 | ✅ | Hive 分区剪枝 + Parquet Row Group stats 剪枝 + 行级 filter（`WHERE date=...` 剪到 1/4 数据时约 2-3× 收益） |
| 并行扫描 | ✅ | Aligned Row Group 为任务单元，8 线程实测 ≈4.2× 加速 |
| 元数据缓存 | ✅ | 复用 DuckDB ObjectCache（LRU 8GiB），footer/schema/RG stats 跨查询共享 |
| 元数据查询 | ✅ | `aligned_meta()`：表名/路径/分区模板/行数/列组数/分区数/part 数/groups/schema/column_mapping |
| 写入 | ✅ | `COPY TO (FORMAT aligned, GROUP '...')`：per-partition 覆盖/MERGE，自描述文件名，RG 131072 / part 8 RG，ZSTD/V1 |
| 批量写入 | ✅ | `COPY TO ... (FORMAT aligned, GROUP '...')`：走 DuckDB CopyFunction 框架，per-partition 覆盖，自描述文件名，RG 131072 / part 8 RG，ZSTD/V1，`preserve_insertion_order=false` 启用并行写入（5M 行 7 列 0.8s） |
| 建表 | ✅ | `aligned_create()` 表函数 + `CREATE TABLE ... WITH (groups=..., partition_template=...)` DDL |
| 合并 | ✅ | `aligned_compact()`：单事务合并**所有组**，并行暂存各分区目录（Phase 1 多线程），规范化重写（1M rows/part），原子切换 |
| 删除 | ✅ | `aligned_drop()`：删除列组（`factor/alpha`）或整表（`index`） |
| catalog 集成 | ✅ | `ATTACH '/data' AS al (TYPE ALIGNED)`（DuckLake 式逻辑 attach）：表保持逻辑表，裸名 SELECT 走 aligned 扫描；`ATTACH` 仅用于读访问，`INSERT/UPDATE/DELETE` 抛 `NotImplementedException`，写入请用 `COPY TO (FORMAT aligned)` |
| 扩展发布 | ✅ | 独立 `aligned.duckdb_extension`（24MB 自包含），`INSTALL` + `LOAD` 即用 |

不支持（明确不做）：Tombstone/Delta、类型升级、聚合下推（依赖 DuckDB ≥ v1.6 API）。

## 构建

### 前置要求

- DuckDB **v1.5.5** 源码（vendored 在 `duckdb/`，gitignored）
- Windows：scoop + MSVC（vcvars64）+ Ninja
- Linux：brew/gcc + Ninja

### 构建整体（含调试 CLI）

```powershell
.\scripts\build.ps1          # Windows：vcvars64 + ninja → duckdb\build\duckdb_al3.exe
```

### 构建可加载扩展（Release，自包含）

```powershell
.\scripts\build_extension.ps1 -Copy   # -DEXTENSION_STATIC_BUILD=1
# 产物：release/aligned.duckdb_extension（~24MB，静态链接 DuckDB 核心 + parquet + mbedtls + zstd）
```

扩展是 **unsigned**（无官方签名），加载时需 `duckdb -unsigned` 或
`SET allow_unsigned_extensions=true`。扩展二进制内嵌 DuckDB engine version（v1.5.5），
加载时强校验与 CLI 版本一致，版本不匹配会报错。

### 运行测试

```powershell
.\scripts\run_tests.ps1           # 全部测试
python test/run_sqllogictest.py    # SQLLogicTest（auto-discover test/aligned/*.test）
```

## 扩展安装

### 方式 A：本地 LOAD（开发用）

```bash
# CLI 专用（-unsigned 标志）
duckdb -unsigned -c "LOAD '/path/to/aligned.duckdb_extension'; SELECT * FROM aligned_scan('mytable');"

# 或在 SQL 会话中直接 LOAD 完整路径
LOAD '/path/to/aligned.duckdb_extension';
SET aligned_data_root = '/data';
SELECT * FROM aligned_scan('mytable');
```

### 方式 B：INSTALL 从 URL 下载（发布后）

```sql
-- 一次性安装（下载到 ~/.duckdb/extensions/v1.5.5/<platform>/）
INSTALL 'https://github.com/<org>/<repo>/releases/download/v0.1.0/aligned-windows_amd64.duckdb_extension';

-- 使用（扩展名 = URL 文件 base name）
LOAD aligned-windows_amd64;
SET aligned_data_root = '/data';
SELECT * FROM aligned_scan('cnstk_ixday');

-- 更新版本
FORCE INSTALL 'https://github.com/<org>/<repo>/releases/download/v0.2.0/aligned-windows_amd64.duckdb_extension';
```

> **注意**：资产名带平台后缀时，`LOAD` 使用的扩展名 = 文件 base name
> （`aligned-windows_amd64`）。若想保持 `LOAD aligned`，可仅发布单平台资产
> `aligned.duckdb_extension`。CLI 版本必须 v1.5.5（与构建版本一致）。

### INSTALL 输入判定

| 输入 | 行为 |
|------|------|
| `INSTALL 'http://...'` | 直接 HTTP 下载（不需要 httpfs） |
| `INSTALL 'https://...'` | 直接 HTTPS 下载（自动 autoload httpfs） |
| `INSTALL '本地路径'` | 文件复制安装 |
| `INSTALL name FROM 'repo-url'` | 仓库模板 URL（不适合 GitHub Release） |
| `INSTALL name`（无 repo） | 默认 core 仓库 `extensions.duckdb.org` |

GitHub Release 资产平铺无目录结构，**直接 URL 安装是正解**（repository 方式
需要 `v1.5.5/<platform>/` 布局，GitHub Release 不支持）。

### 三个硬约束

1. **签名**：未开启 `allow_unsigned_extensions` 时无签名扩展被拒绝。
   → CLI 必须 `duckdb -unsigned` 或 `SET allow_unsigned_extensions=true`。
2. **版本强校验**：扩展内嵌 `DUCKDB_NORMALIZED_VERSION`，加载时
   `engine_version != duckdb_version` 直接拒绝。必须用相同 DuckDB 版本构建。
3. **依赖自包含**：`EXTENSION_STATIC_BUILD=1` 将 DuckDB 核心 + parquet + mbedtls +
   zstd 静态链入扩展 → 单文件，用户**无需预装 parquet**。

## 架构文档

- **AGENTS.md** — 唯一权威架构文件：核心需求、契约、代码结构、关键经验/坑
- **docs/API.md** — API 对接文档：ATTACH、COPY TO、全部表函数参数说明
