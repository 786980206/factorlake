# FactorLake / AlignedTable — 问题反馈与需求跟踪

> 本文件用于 stkoe-cli 项目与 FactorLake 扩展团队之间的沟通。
> stkoe-cli 侧发现问题 / 提需求 → 写入此处；FactorLake 侧修复 / 更新后在此回复。

---

## 问题 1：ATTACH 模式下 placeholder 表 schema 不可见（🔴 阻断）

**发现时间**：2026-08-23

**现象**：

按 API.md §2 快速开始流程执行：

```python
con = duckdb.connect(config={"allow_unsigned_extensions": True})
con.execute("LOAD aligned;")
con.execute("SET aligned_data_root = 'C:/Users/winds/.stkoe/factorlake'")
con.execute("SELECT * FROM aligned_create('dg5', 'index', 'sym VARCHAR, date DATE, close DOUBLE')")
# 返回 [(2, 1, 1)] —— 2 dirs, 1 file created ✅

con.execute("ATTACH 'C:/Users/winds/.stkoe/factorlake' AS al (TYPE ALIGNED);")

# 问题 1：information_schema.columns 查不到列
con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='al' AND table_name='dg5'").fetchall()
# 返回 [] —— 空！

# 问题 2：aligned_groups 返回空
con.execute("SELECT * FROM aligned_groups('dg5')").fetchall()
# 返回 [] —— 空！

# 问题 3：INSERT 报错
con.execute("INSERT INTO al.dg5 (sym, date, close) VALUES ('000001', DATE '2024-01-01', 10.5)")
# ❌ Binder Error: Table "dg5" does not have a column with name "sym"

# 但 SELECT count(*) 可以（说明 catalog 看到了表，只是没有列定义）
con.execute("SELECT count(*) FROM al.dg5").fetchall()
# 返回 [(0,)] ✅
```

**已验证的事实**：

1. `aligned_create` 创建的 placeholder parquet 文件**有正确的 schema**：
   - 文件路径：`dg5/index/year=1970/0000-0000000000.parquet`（128 bytes）
   - polars 读取：`schema: {'sym': String, 'date': Date, 'close': Float64}`，0 行
   - pyarrow 读取：3 列，0 行 ✅
2. `aligned_create` 返回值正确：`(dirs_created=2, files_created=1, txid=1)`
3. DETACH + 重新 ATTACH 后仍然看不到 schema
4. `SELECT count(*) FROM al.dg5` 返回 0（说明 catalog 发现了表，只是列定义为空）
5. `DESCRIBE al.dg5` 也失败（"SELECT list is empty after resolving *"）

**根因推测**：

ATTACH 的 catalog 钩子在 schema discovery 环节没有从 placeholder parquet 的 footer 推导列定义。可能是：
- `AlignedBindForCatalog`（§8.1）在空表（0 行 placeholder）时返回空 `names`/`return_types`
- 或者 `EnsureTablesLoaded` 的惰性发现逻辑跳过了空 placeholder

**影响**：

阻断了所有写入操作——`INSERT INTO al.<table>` 都需要 catalog 识别列定义。
这使 stkoe-cli 无法通过 ATTACH 模式进行标准 SQL DML。

**复现环境**：
- DuckDB v1.5.4（Python duckdb 库）
- aligned 扩展：`D:/proj/factorlake/release/aligned.duckdb_extension`（2026-08-23 01:43 构建）
- 数据根目录：`C:/Users/winds/.stkoe/factorlake`
- OS：Windows 10 amd64

**期望行为**：

ATTACH 后，`aligned_create` 创建的空表应该能通过 `information_schema.columns` / `DESCRIBE` / `aligned_groups` 正确返回列定义，使得 `INSERT INTO al.<table> (col1, col2, ...) VALUES (...)` 可以正常执行。

---

## 问题 2：aligned_groups 在 ATTACH 前返回空（🟡 待确认）

**发现时间**：2026-08-23

**现象**：

在 ATTACH 之前调用 `aligned_groups`，即使表已通过 `aligned_create` 创建，也返回空：

```python
con.execute("SELECT * FROM aligned_create('dg5', 'index', 'sym VARCHAR, date DATE, close DOUBLE')")
con.execute("SELECT * FROM aligned_groups('dg5')")  # 返回 []
con.execute("ATTACH '...' AS al (TYPE ALIGNED);")
con.execute("SELECT * FROM aligned_groups('dg5')")  # 仍然返回 []
```

**期望行为**：

`aligned_groups` 应能独立于 ATTACH 工作，直接扫描物理目录返回列组信息（API.md §5.2 描述的行为）。

---

## 问题 3：aligned_scan 在空表上报 INTERNAL Error（🟡 待确认）

**发现时间**：2026-08-23

**现象**：

对只有 placeholder parquet 的空表执行 `aligned_scan` 报内部错误：

```python
con.execute("SELECT * FROM aligned_scan('dg5')")
# ❌ INTERNAL Error: Failed to bind "aligned_scan": Table function must return at least one column
```

**期望行为**：

空表 `aligned_scan` 应返回 0 行但保留 schema（类似 `SELECT * FROM empty_table LIMIT 0`），或者至少返回一个有意义的错误信息而非 INTERNAL Error。

---

## 需求清单

以下为 stkoe-cli 迁移到 FactorLake 后端过程中需要的能力（按优先级排序）：

### P0 — 阻断性需求

1. **ATTACH 后空表 schema 可见**（问题 1）——写入操作的前提

### P1 — 重要需求

2. **aligned_groups 正确返回列组**（问题 2）——stkoe 需检查列组是否存在以决定 create 还是直接 INSERT
3. **aligned_scan 空表不报错**（问题 3）——stkoe 首次创建列组后需要验证 schema

### P2 — 增强（后续可补）

4. **分区值列表接口**：`aligned_partitions(table_name)` → 返回 `[(partition_value, row_count, file_count)]`，从目录元数据提取不扫数据
5. **存储占用统计**：`aligned_storage_stats(table_name)` → 返回各分区/列组的存储大小 + 文件数

---

## 跟踪记录

| 日期 | 事件 | 状态 |
|------|------|------|
| 2026-08-23 | stkoe-cli 发现问题 1/2/3 并写入此文件 | 待处理 |
| 2026-08-23 | stkoe-cli 严格按 API.md §2 顺序测试（先 aligned_create group='index' → ATTACH → INSERT），问题仍然存在 | 待处理 |
| 2026-08-23 | 当前安装的扩展构建时间：2026-08-23 01:43:35（D:/proj/factorlake/release/aligned.duckdb_extension） | 请确认是否为最新编译版本 |

### 补充说明（2026-08-23）

已确认创建顺序正确——**先** `aligned_create('strict1', 'index', 'sym VARCHAR, date DATE, close DOUBLE')` 建表，**再** ATTACH，**再** INSERT。

创建成功返回 `(2, 1, 1)`——2 个目录、1 个文件已创建。placeholder parquet 有正确 schema（polars/pandas 可读出 3 列 0 行）。

但 ATTACH 后：
- `information_schema.columns` 返回空
- `aligned_groups` 返回空
- `INSERT INTO al.strict1 VALUES (...)` 报 `table strict1 has 0 columns`
- `SELECT count(*) FROM al.strict1` 返回 0（catalog 发现了表，但列定义为空）

**关键确认**：这不是创建顺序问题（已严格按文档先 index 后 group），而是 ATTACH catalog 的 schema discovery 没有从 placeholder parquet footer 读取列定义。
