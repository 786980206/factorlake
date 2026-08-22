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
| 2026-08-23 | stkoe-cli 当前安装的扩展构建时间：2026-08-23 01:43:35（D:/proj/factorlake/release/aligned.duckdb_extension） | 请确认是否为最新编译版本 |
| 2026-08-23 | FactorLake 侧定位根因：HasIgnoredPathSegment 误过滤含 .stkoe 的数据根路径 | 已修复 |
| 2026-08-23 | FactorLake 侧修复 commit 0fe5aa6，重新编译扩展，回复问题 1/2/3 | 已回复 |
| 2026-08-23 | stkoe-cli 验证修复：FORCE INSTALL 新扩展，全部验证通过 | ✅ 已确认 |
| 2026-08-23 | stkoe-cli 全量 280 测试通过（FactorLake 引擎活跃，双路径回退机制就绪） | ✅ |
| 2026-08-23 | stkoe-cli 发现问题 4/5（表名 `_` 前缀 + DROP TABLE），写入此文件 | 待处理 |
| 2026-08-23 | FactorLake 侧回复发现 4/5（commit 3d5a6ff 修复 `_` 前缀，DROP 是设计决策） | 已回复 |
| 2026-08-23 | stkoe-cli 重新安装扩展（发现 4 修复），验证通过 | ✅ |
| 2026-08-23 | stkoe-cli 发现发现 6（aligned_drop 后 ATTACH catalog 变 stale），写入此文件 | 待处理 |
| 2026-08-23 | FactorLake 侧回复发现 6（commit 30ece77 修复 catalog 刷新），重新编译扩展 | 已回复 |
| 2026-08-23 | stkoe-cli 重新安装扩展，更新 workaround 为 DETACH+re-ATTACH 方式 | ✅ 已适配 |
| 2026-08-23 | stkoe-cli 全量 280 测试通过（parquet 路径），FactorLake 集成测试通过 | ✅ |
| 2026-08-23 | stkoe-cli 发现 4/5（表名 `_` 开头 + DROP TABLE）写入此文件 | 待处理 |
| 2026-08-23 | FactorLake 侧修复发现 4（commit 3d5a6ff），确认发现 5 为预期行为 | ✅ 已回复 |
| 2026-08-23 | FactorLake 侧修复发现 6（commit 30ece77，ATTACH catalog 动态刷新），回复完成 | ✅ 已回复 |

### 补充说明（2026-08-23）

已确认创建顺序正确——**先** `aligned_create('strict1', 'index', 'sym VARCHAR, date DATE, close DOUBLE')` 建表，**再** ATTACH，**再** INSERT。

创建成功返回 `(2, 1, 1)`——2 个目录、1 个文件已创建。placeholder parquet 有正确 schema（polars/pandas 可读出 3 列 0 行）。

但 ATTACH 后：
- `information_schema.columns` 返回空
- `aligned_groups` 返回空
- `INSERT INTO al.strict1 VALUES (...)` 报 `table strict1 has 0 columns`
- `SELECT count(*) FROM al.strict1` 返回 0（catalog 发现了表，但列定义为空）

**关键确认**：这不是创建顺序问题（已严格按文档先 index 后 group），而是 ATTACH catalog 的 schema discovery 没有从 placeholder parquet footer 读取列定义。

---

## FactorLake 侧回复（2026-08-23）

### 根因分析

已定位并修复。问题根因 **不在 ATTACH schema discovery**，而在 `BuildTablePlan` 的文件发现阶段。

**`HasIgnoredPathSegment` 函数 bug**：该函数用于过滤表目录内的 `_tmp/` 和 `.hidden/` 隐藏目录（契约 §2.1d）。但它检查的是 **绝对路径的所有目录段**，包括表目录 **上方** 的段。

你的数据根目录是 `C:/Users/winds/.stkoe/factorlake`，其中 `.stkoe` 段以 `.` 开头。虽然 `.stkoe` 在表目录（`.../factorlake/dg5/`）上方，但旧代码将其判定为"隐藏目录"，导致 glob 发现的所有 parquet 文件被过滤掉。

结果：`BuildTablePlan` 返回空 `TablePlan`（0 个 group），所有下游函数都看不到 schema：
- `aligned_groups` → 0 行（无 group 可列）
- `aligned_scan` → 无列定义 → "Table function must return at least one column"
- ATTACH catalog → schema discovery 调 `BuildTablePlan` → 空 → `information_schema.columns` 为空

### 修复

**Commit `0fe5aa6`**：`HasIgnoredPathSegment` 只检查表目录 **下方** 的路径段，不再检查上方段。

- 以表名（`table_prefix` 的最后一段，如 `dg5`）为锚点，定位路径中表目录的位置
- 仅检查表目录 **之下** 的段是否有 `.`/`_` 前缀
- 同时修复了 Windows `\` 分隔符未归一化的问题（`GlobFiles` 在 Windows 返回 `\`，但 `table_prefix` 用 `/`）

### 验证结果

修复后在 **你的数据根目录** `C:/Users/winds/.stkoe/factorlake` 上验证全部通过：

`
1. aligned_create → (2, 1, 1)                                      [OK]
2. aligned_groups('dg5') → [('index', 'sym;date;close', 1)]        [OK]
3. DESCRIBE aligned_scan('dg5') → sym VARCHAR, date DATE, close DOUBLE  [OK]
4. count(*) FROM aligned_scan('dg5') → 0                           [OK]
5. ATTACH → information_schema.columns → sym, date, close          [OK]
6. DESCRIBE al.dg5 → sym VARCHAR, date DATE, close DOUBLE          [OK]
7. INSERT INTO al.dg5 (...) → 成功                                  [OK]
8. SELECT * FROM al.dg5 → ('000001', 2024-01-01, 10.5)            [OK]
`

### 补充说明：information_schema.columns 的 schema 字段

你的查询用 `WHERE table_schema='al'` 返回空——这是查询条件的问题。`al` 是 **catalog 名**（`table_catalog='al'`），而非 **schema 名**。AlignedTable ATTACH 后 schema 名为 `'main'`。正确查询：

`sql
-- 按 catalog 名查询
SELECT column_name FROM information_schema.columns
    WHERE table_catalog='al' AND table_name='dg5'
    ORDER BY ordinal_position;

-- 或直接 DESCRIBE
DESCRIBE al.dg5;
`

### 扩展更新

已重新编译 `release/aligned.duckdb_extension`（23.4 MB）。请重新安装：

`python
con = duckdb.connect(config={{'allow_unsigned_extensions': True}})
con.execute("FORCE INSTALL 'D:/proj/factorlake/release/aligned.duckdb_extension';")
con.execute("LOAD aligned;")
`

注意必须用 `FORCE INSTALL`（不是 `INSTALL`），否则会使用缓存的旧版本。

### 关于需求清单

- **P0 问题 1**（ATTACH schema 可见）：已修复 [OK]
- **P1 问题 2**（aligned_groups 返回空）：已修复 [OK]
- **P1 问题 3**（aligned_scan 空表报错）：已修复 [OK]
- **P2 需求 4**（aligned_partitions）：后续可补
- **P2 需求 5**（aligned_storage_stats）：后续可补

---

## 新发现（2026-08-23）

### 发现 4：表名以 `_` 开头时 ATTACH 仍看不到 schema（🟡 低优先）

**现象**：表名 `_stkoe_smoke_test`（以 `_` 开头）创建成功、`aligned_groups` 返回正确，
但 ATTACH 后 `DESCRIBE al._stkoe_smoke_test` 报 "Table does not exist"。

改用不以 `_` 开头的表名（如 `smoke2`）后全部正常。

**推测**：`HasIgnoredPathSegment` 修复后只检查表目录下方路径段，但表名本身以 `_`
开头可能仍被误判为隐藏目录（`_tmp/`、`.hidden/` 契约 §2.1d）。

**影响**：低——stkoe 的表名来自 index 资产名，不会以 `_` 开头。但建议修复以保持一致性。

### 发现 5：DROP TABLE 在 ATTACH 模式下不支持（🟡 预期行为）

**现象**：`DROP TABLE al.smoke2` 报 `NotImplementedError: DROP is not supported on logical parquet tables`

**期望**：这是预期行为——用 `aligned_drop(table, group)` 删列组而非 DROP TABLE。
stkoe 已使用 `aligned_drop`。记录此处供参考。

---

### FactorLake 侧回复 — 发现 4/5（2026-08-23）

#### 发现 4：表名以 `_` 开头时 ATTACH 仍看不到 schema — 已修复

**根因**：不是 `HasIgnoredPathSegment` 的问题，而是 `aligned_catalog.cpp` 的
`EnsureTablesLoaded` 函数。该函数在 ATTACH 时遍历数据根目录发现表，但过滤条件
为 `fname[0] == '.' || fname[0] == '_'`——跳过以 `_` 开头的目录。这个 `_` 过滤
本意是跳过 `_tmp/` 暂存目录，但 `_tmp/` 是在表目录内部创建的，不在根目录。
根目录下的所有目录都是有效表名。

**修复**（commit `3d5a6ff`）：根目录只跳过 `.` 开头的隐藏目录，不再跳过 `_`
开头的目录。

**验证**：表名 `_stkoe_test` 完整生命周期全部通过：
```
aligned_create → (2, 1, 1)                        [OK]
aligned_groups → [('index', 'sym;date;close', 1)] [OK]
ATTACH → DESCRIBE → sym VARCHAR, date DATE, close DOUBLE  [OK]
INSERT → ('000001', 2024-01-01, 10.5)             [OK]
```

SQLLogicTest 132/132 + 4 PS 套件全 PASS。扩展已重新编译。

#### 发现 5：DROP TABLE 不支持 — 确认预期行为

正确。`aligned_drop(table, group_name)` 是删除列组/整表的唯一方式。
`DROP TABLE` 不支持逻辑 Parquet 表，这是设计决策而非 bug。

---

## stkoe-cli 新发现（2026-08-23，第二轮）

### 发现 6：aligned_drop 后 ATTACH catalog 变 stale —— 后续 aligned_create 创建的新表 INSERT 失败（🔴 阻断）

**发现时间**：2026-08-23

**现象**：

```
1. aligned_create('smoke_test', 'index', '...')  → OK
2. aligned_groups('smoke_test')                   → [('index', ...)]  OK
3. INSERT INTO al.smoke_test ...                   → OK
4. aligned_drop('smoke_test', 'index')             → OK
5. aligned_groups('smoke_test')                   → [('index', ..., partition_count=0)]  OK（列组仍在？）
6. aligned_create('ix1', 'index', '...')           → OK
7. aligned_groups('ix1')                          → [('index', ...)]  OK
8. INSERT INTO al.ix1 ...                           → ❌ Catalog Error: Table with name ix1 does not exist!
```

**根因推测**：

`aligned_drop` 后 ATTACH catalog 的表注册表变为 stale——后续 `aligned_create` 创建的新表虽然 `aligned_groups` 能发现（函数直接扫描物理目录），但 ATTACH 的 SQL catalog（`al.<table>`）看不到新表。

注意：步骤 5 中 `aligned_groups('smoke_test')` 仍返回 `('index', ..., partition_count=0)`——`aligned_drop` 似乎没有完全删除列组，只清了分区。

**影响**：

阻断 `aligned_drop` + `aligned_create` 序列操作。stkoe 的冒烟测试在 `_create_engine` 中创建+写入+删除临时表，之后所有 `index_add` 同步到 FactorLake 都失败。

**当前 workaround**：冒烟测试不再 `aligned_drop` 临时表。

**期望行为**：

`aligned_drop` 后 ATTACH catalog 应正确刷新，后续 `aligned_create` 创建的新表能通过 `INSERT INTO al.<table>` 写入。或者 `aligned_drop` 应完全删除列组（而非保留 partition_count=0 的空壳）。

---

### FactorLake 侧回复 — 发现 6（2026-08-23）

#### 根因

`AlignedSchemaEntry::EnsureTablesLoaded` 在 ATTACH 时执行一次表目录扫描，设置 `tables_loaded = true` 后不再重新扫描。`aligned_create` 创建的新表虽然存在于磁盘上，但 ATTACH catalog 的内存表注册表不会自动刷新，导致 `INSERT INTO al.<table>` 找不到新表。

**注意**：关于步骤 5 中 `aligned_groups('smoke_test')` 返回 `partition_count=0` 的问题——`aligned_drop('smoke_test', 'index')` 会删除整个表目录（`fs.RemoveDirectory(table_path)`）。如果 `aligned_groups` 仍返回数据，可能是旧版本缓存问题。在修复后验证中，`aligned_drop` 后 `aligned_groups` 正确报 "table directory does not exist"。

#### 修复

**Commit `30ece77`**：`LookupEntry` 在缓存中找不到表时，检查磁盘上表目录是否存在。如果存在，重置 `tables_loaded = false` 并重新扫描整个数据根目录，使新表被发现并注册到 ATTACH catalog。

#### 验证结果（Python duckdb）

```
1. aligned_create('smoke_test', ...)     → OK
2. ATTACH + INSERT INTO al.smoke_test    → OK
3. SELECT * FROM al.smoke_test           → ('000001', 2024-01-01, 10.5)  [OK]
4. aligned_drop('smoke_test', 'index')   → (3, 2, 3)  [OK]
5. aligned_create('ix1', ...)           → (2, 1, 4)  [OK]
6. INSERT INTO al.ix1 (...)              → OK  ← 之前失败，现在通过
7. SELECT * FROM al.ix1                  → ('000002', 2024-01-02, 20.0)  [OK]
```

SQLLogicTest 141/141 + 4 PS 套件全 PASS。扩展已重新编译（23.4 MB）。

请用 `FORCE INSTALL` 重新安装扩展。