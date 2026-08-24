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
-- 表保持「逻辑表」——SELECT 走 aligned 扫描，标准 DML 直接读写底层 parquet 列组：
ATTACH '/data' AS al (TYPE ALIGNED);

-- 建表（DDL 方式，写 0 行占位 parquet，footer 携带 schema）：
CREATE TABLE al.cnstk_ixday (symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE)
  WITH (groups='index:close;factor/alpha101:alpha001', partition_template='month=%Y-%m');

-- 在已有表上创建空分区：
CREATE TABLE al.cnstk_ixday (cols...) WITH (partition='month=2026-10');

-- 列组扩展：
CREATE TABLE al.cnstk_ixday (ma5 DOUBLE, ma20 DOUBLE) WITH (groups='fieldset/ma:ma5,ma20');

SELECT * FROM al.cnstk_ixday;
INSERT INTO al.cnstk_ixday (date, symbol, close, alpha001, ma5)
  VALUES (DATE '2026-09-01', '009999', 99.5, 1.5, 2.5);
UPDATE al.cnstk_ixday SET close = 123.4 WHERE symbol = '009999';
DELETE FROM al.cnstk_ixday WHERE symbol = '009999';
-- INSERT/UPDATE 按 (symbol, date) 主键 upsert，只重写受影响 part；原子提交。
-- DETACH al; 即可卸载（数据始终在 parquet 列组里）。

-- 批量写入（COPY TO，推荐大批量数据加载）：
-- 1. 先创建空组（2-arg 形式，schema 首次 COPY 时从 query 推断）：
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');
-- 2. 批量写入（per-partition 自动覆盖，无需 OVERWRITE）：
SET preserve_insertion_order = false;  -- 启用并行写入
COPY (SELECT symbol, date, ma5, ma20 FROM source ORDER BY symbol, date)
  TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');
-- 已有组写入时自动列裁剪（只写组内列）+ 类型转换（如 TIMESTAMP → DATE）。
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
| 投影下推 | ✅ | 只打开被选中的 Group、只读被选中的列（`SELECT alpha001` 只碰 alpha101） |
| 过滤下推 | ✅ | Hive 分区剪枝 + Parquet Row Group stats 剪枝 + 行级 filter（`WHERE date=...` 剪到 1/4 数据时约 2-3× 收益） |
| 并行扫描 | ✅ | Aligned Row Group 为任务单元，8 线程实测 ≈4.2× 加速 |
| 元数据缓存 | ✅ | 复用 DuckDB ObjectCache（LRU 8GiB），footer/schema/RG stats 跨查询共享 |
| 写入 | ✅ | 标准 DML（INSERT/UPDATE/DELETE）通过 `ATTACH ... TYPE ALIGNED` 使用（v8 mutator）：按 (symbol, date) 主键插入 / 更新 / 删除；只重写受影响 part；`_tmp` 暂存 + 原子提交 |
| 批量写入 | ✅ | `COPY TO ... (FORMAT aligned, GROUP '...')`：走 DuckDB CopyFunction 框架，per-partition 覆盖，自描述文件名，RG 131072 / part 8 RG，ZSTD/V1，`preserve_insertion_order=false` 启用并行写入（5M 行 7 列 0.8s） |
| 建表 | ✅ | `aligned_create()` 表函数 + `CREATE TABLE ... WITH (groups=..., partition_template=...)` DDL |
| 合并 | ✅ | `aligned_compact()`：单事务合并**所有组**，按分区目录合并 part，规范化重写（1M rows/part），原子切换 |
| 删除 | ✅ | `aligned_drop()`：删除列组（`factor/alpha`）或整表（`index`） |
| catalog 集成 | ✅ | `ATTACH '/data' AS al (TYPE ALIGNED)`（DuckLake 式逻辑 attach）：表保持逻辑表，裸名 SELECT 走 aligned 扫描；标准 INSERT/UPDATE/DELETE 通过 catalog 的 PlanInsert/PlanUpdate/PlanDelete 钩子**直写 parquet 列组** |
| 扩展发布 | ✅ | 独立 `aligned.duckdb_extension`（24MB 自包含），`INSTALL` + `LOAD` 即用（见 `docs/EXTENSION_RELEASE.md`） |

不支持（明确不做）：Tombstone/Delta、类型升级、聚合下推（依赖 DuckDB ≥ v1.6 API）。

## 构建
