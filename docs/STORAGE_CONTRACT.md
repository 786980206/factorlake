# Storage Contract — AlignedTable（Phase 0 定稿）

> 本文件是**存储层唯一权威规格**。Reader / Writer / Compaction 的一切实现
> 必须与本文件一致；任何修改必须先改本文件再改代码。
> 状态：**Phase 0 定稿，v1**（2026-08）

---

## 1. 术语

| 术语 | 定义 |
|------|------|
| Logical Table | 一张逻辑宽表，如 `cnstk_ixday`，对应 `<data_root>/<table_name>/` |
| Column Group | Logical Table 的一个物理列子集，如 `index`、`factor/alpha101`，对应表内一个目录 |
| Canonical Row Space | 该表的逻辑行坐标集合 `{0, 1, ..., R-1}`，R = 表行数 |
| Canonical Key | 定义行语义的列集合，如 `["date", "symbol"]`（写在 `_table.json`） |
| Logical Partition | 按 Key 值划分的互斥行子集（如 `date = 2026-08-17` 的所有行） |
| Physical Partition | 某个 Group 内一个 Hive 风格目录（如 `date=2026-08-17/` 或 `year=2026/month=08/day=17/`），含一组 part 文件 |
| Part File | 一个 Parquet 文件（`part-<6位序号>.parquet`），是 Group 内最小读取单元 |
| Row Group | Parquet 内 Row Group；本契约要求统一大小 `row_group_size`（默认 131072） |
| Aligned Scan | 按行坐标直接组装多 Group 向量的扫描，绝不 JOIN |

---

## 2. 目录布局（唯一合法布局）

```
<data_root>/
└── <table_name>/                  ← Logical Table
    ├── _table.json                ← 表级 Manifest（必选）
    ├── index/                     ← Column Group 目录
    │   └── date=2026-08-17/       ← Physical Partition 目录（Hive 风格）
    │       ├── .aligned-commit.json   ← 提交标记（必选，见 §9）
    │       ├── part-000000.parquet
    │       ├── part-000000.aligned.json  ← part 级 sidecar（必选，见 §7）
    │       ├── part-000001.parquet
    │       └── ...
    ├── factor/alpha101/
    │   ├── _group.json            ← Group 级 Manifest（必选）
    │   └── year=2026/month=08/day=17/
    │       ├── .aligned-commit.json
    │       ├── part-000000.parquet
    │       └── ...
    └── ...
```

**硬性规则**：
1. 每个 Group 目录必须有 `_group.json`；每个 Logical Table 必须有 `_table.json`。
2. `_tmp/` 是唯一临时区（交易目录），**Reader 永不扫描 `_tmp/`**。
3. 保留前缀/后缀：`_table.json`、`_group.json`、`.aligned-commit.json`、`part-`、`.aligned.json`、`_tmp/`。数据列名、目录名不得与保留名冲突。
4. Physical Partition 目录是**叶子目录**：直接包含 part 文件，不再有子目录（partitioning 模板的最后一层）。
5. 目录里除本契约列出的文件外不允许其他文件（便于 glob 与校验）。

### 2.1 目录硬性规则（v1.2，2026-08 用户补充）

a. `<table_name>` 即表名（如 `cnstk_ixday`）。
b. **`<table>/index` 必须存在**：每张表必须有 index Group（物理目录 + `_group.json`），
   缺失即报错。Key 列与基础列都住在 index。
c. **其余 Group 必须以两级目录存在**：`<table>/<lv1>/<lv2>/`（如 `factor/alpha101`、
   `fieldset/ma`、`panel/cnstk_klday`）。manifest 的 `groups` 条目必须形如 `lv1/lv2`，
   否则报错。Physical Partition 目录在 group 目录之下，不受此限制。
d. **目录发现时忽略任何以 `.` 或 `_` 开头的目录**（如 `_tmp/`、`.hidden/`）：
   glob 结果中路径段以 `.`/`_` 开头的 part 一律跳过；`_table.json`、`_group.json`、
   sidecar 等以 `.`/`_` 开头的**文件**不受影响。

### 2.2 列名规则（v1.2，2026-08 用户补充）

e. **重复列名的处理**（按优先级）：

   1. **与 index 中重复的列名，全部忽略**：非 index Group 中的同名列不进入表 schema
      （index 列是权威的）；reader 扫描时跳过这些列（不读、不填）。
   2. **非 index Group 之间重复的列名，必须用限定名引用**：`lv1_name.lv2_name.col_name`
      （如 `factor.alpha101.close`）。表 schema 中这类列只以限定名注册（裸名不注册）；
      引用裸名 → 报"列不存在"；引用限定名（DuckDB 中需反引号）→ 正常。
   3. 非重复列名照常以裸名注册。

   物理映射：Group 内部仍按裸名与 sidecar/parquet 列匹配；`lv1`/`lv2` 来自 group 路径。
   Writer 侧：非 index Group 不应写与 index 重复的列（写了会被 Reader 忽略）。

---

## 3. Canonical Row Space（"row N 对齐"的精确定义）

### 3.1 定义

设表 T 行数为 R，行坐标 `0..R-1`。每个 Column Group G 的所有 part 文件
（按 partition key 排序、part 序号升序）拼接后，恰好覆盖 `[0, R)` 且互不重叠。
G 的第 N 行 = 该拼接序列的第 N 行。

**对齐（Alignment）**：表 T 是 aligned 的，当且仅当：

> 对任意两个 Group A、B 与任意行坐标 N ∈ [0, R)：
> **A 的第 N 行与 B 的第 N 行是同一个 Logical Row（拥有相同的 Canonical Key 值）**。

### 3.2 保证方式（不对称责任）

- **Writer 必须构造式保证**：一个提交事务内，所有 Group 写入**完全相同的行区间
  （相同 start_row + row_count）**；提交原子化（§9）；未完成的交易对 Reader 不可见。
- **Reader 必须盲信**：Aligned Scan **永不**通过 Key 比较 / JOIN / 查表验证对齐。
  对齐验证只允许在显式校验模式（`aligned_validate()`，Phase 1 之后）与测试中使用。
- 每 part 的 `start_row`/`row_count` 由 sidecar（§7）声明；扫描开始时
  Reader 校验已打开 Group 的 sidecar 行数之和 == `_group.json.row_count` == `_table.json.row_count`，
  不一致立即报错（fail-fast）。

### 3.3 不变量（Invariant，7 条）

1. Logical Table 有唯一 Canonical Row Space。
2. Key Columns 只存一份（只在 `index/` Group，其他 Group 不得包含 Key 列）。
3. 所有 Column Group 使用相同 Row Ordering。
4. 所有 Column Group 使用相同 Logical Row Coordinate（即 §3.1）。
5. Partition Scheme 可以不同（各 Group 自定）。
6. Physical Files 可以完全不同（文件名、数量、大小、切分方式）。
7. 查询阶段绝不通过 Key 做 JOIN。

---

## 4. Row Ordering

- 本版本 `canonical_order` 仅支持 `"fixed"`：行坐标在写入时确定，**永不变更**。
  （保留未来 `"sorted"` 枚举位，但不实现。）
- Compaction 必须保持行序与行坐标不变（只能合并文件，不能重排行）。
- Writer 可选择按 Key 排序写入（利于未来 skip index），但 Reader **不得假设有序**。

---

## 5. Manifest 规格

### 5.1 `_table.json`

```json
{
  "name": "cnstk_ixday",
  "version": 1,
  "schema_version": 1,
  "key": ["date", "symbol"],
  "canonical_order": "fixed",
  "row_count": 0,
  "row_group_size": 131072,
  "groups": [
    "index",
    "factor/alpha101",
    "factor/alpha191",
    "fieldset/ema",
    "fieldset/ma",
    "fieldset/qoq",
    "fieldset/ttm",
    "fieldset/yoy",
    "panel/cnstk_icday",
    "panel/cnstk_ixday",
    "panel/cnstk_klday"
  ]
}
```

- `key`：Canonical Key 列名（必须在 `index/` Group 中存在）。
- `row_count`：表权威行数，写入时由 Writer 维护；Reader 以此为扫描基数。
- `row_group_size`：全表默认 Row Group 大小，Group 可覆盖。

### 5.2 `_group.json`

```json
{
  "group": "factor/alpha101",
  "row_count": 1234567890,
  "row_group_size": 131072,
  "partitioning": [
    {"template": "year=%Y", "source": "date"},
    {"template": "month=%m", "source": "date"},
    {"template": "day=%d", "source": "date"}
  ]
}
```

- `partitioning`：**有序**目录模板列表（顶层→底层）。`template` 用 strftime 占位符
  引用 `source` 列（Key 或 index 列）的值生成目录名。例：
  - `{"template": "date=%Y-%m-%d", "source": "date"}` → `date=2026-08-17/`
  - `{"template": "year=%Y", ...}`、`{"template": "month=%m", ...}` → `year=2026/month=08/`
  - 也可用常量/其他列（如 `symbol=%s`，Phase 3+ 再放开）。
- `partitioning` 为空 `[]` 或缺失 → 该 Group 无分区，part 文件直接放 Group 目录。
- 模板生成值**必须**与 Hive 风格 `name=value` 一致（`<name>=<strftime结果>`）。
- `row_group_size` 缺省继承 `_table.json`。

### 5.3 Manifest 职责边界

- Manifest **不记录** part 文件清单（文件发现 = glob + sidecar，见 §7）。
- `_table.json` 只由表创建 / Schema 变更 / Compaction 重写；`_group.json` 同。
- 所有 Manifest 都是 JSON（UTF-8），写时先写临时文件再原子 rename。

---

## 6. Parquet 约定

- **类型**（Phase 1）：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、
  `HUGEINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、`DATE`、`TIMESTAMP`、`TIME`、`VARCHAR`。
  不用嵌套类型。
- `date` 列：Parquet `DATE`（int32 days）；`symbol` 列：`VARCHAR`（字典编码默认开启）。
- 压缩：默认 `zstd`（可配）；统计信息：默认全开（Row Group 裁剪依赖它）。
- 每个 part 文件内 Row Group 大小 = 该 Group 的 `row_group_size`，**文件内连续铺满**
  （最后一个 RG 可不满）。
- 同一列在**所有 part 中类型必须一致**（类型严格，不隐式转换）。

---

## 7. Part 文件与 sidecar（行区间声明）

每个 `part-XXXXXX.parquet` 必须有一个同名 sidecar `part-XXXXXX.aligned.json`：

```json
{
  "table": "cnstk_ixday",
  "group": "factor/alpha101",
  "part": "part-000000",
  "start_row": 0,
  "row_count": 131072,
  "row_group_size": 131072,
  "columns": ["alpha001", "alpha002"]
}
```

- `start_row`：本 part 在 Canonical Row Space 中的**全局起始行**；`row_count`：本 part 行数。
- `columns`：本 part 包含的列名（**每个 part 的 schema 自描述**，Schema Evolution 的基础）。
- **不变量**：同一 Group 内，所有 part 的 `[start_row, start_row+row_count)` 两两不相交，
  且并集 == `[0, group.row_count)`。违反即算数据损坏，扫描报错。
- sidecar 在 part 写入完成后、提交标记之前写入。
- （可选未来：把同样内容塞进 Parquet key-value metadata，sidecar 仍为权威来源。）

---

## 8. Schema Evolution（第一版规则）

- 列集合按 part 自描述（§7 `columns`）。Reader 对打开的行区间取**并集**。
- 旧 part 没有的列 → 读为 `NULL`（不重写历史）。
- 同一列类型跨 part 必须一致；不一致 = 错误（Phase 1 不做类型升级）。
- Key 列集合 `_table.json.key` 一经创建**不可变更**（除非重建表）。

---

## 9. 写入与原子提交协议（Phase 5 实现，契约现在定死）

1. Writer 以 **Logical Partition 为提交单位**（如 `date=2026-08-17` 的全部行）。
2. 写入路径：`<table>/_tmp/transaction-<txid>/<group>/<partition-dirs...>/`，
   所有 Group 的 part 与 sidecar 都在这里写完整并 fsync。
3. **提交 = 两步**：
   a. 把每个 Group 的 partition 目录 rename 到正式位置（同文件系统，单次 rename 原子）；
   b. **最后**在**每个** partition 目录写 `.aligned-commit.json`（先写临时文件再原子 rename）：
   ```json
   {
     "txid": 123,
     "parts": ["part-000000", "part-000001"]
   }
   ```
   `parts` 列出**该目录当前全部已提交**的 part 文件名（basename）。
4. **Reader 可见性规则**（v1.1，2026-08 修订）：
   - 一个 part 可见 ⟺ sidecar 存在 **且** 所在目录有合法 commit marker **且** marker 的 `parts` 包含该 part。
   - 目录可以累积多个事务的 part（如 `fieldset/ma` 按 `year/month` 分区、逻辑分区按天）：
     Writer 提交新事务时**追加** part 名并重写 marker（marker 是原子提交记录，每目录一个，
     文件数有界——这是 §5.3「不记录文件清单」的唯一例外，因为它不是全局 manifest）。
   - 崩溃发生在 (a) 与 (b) 之间 → 新 part 已 rename 但 marker 未更新 → 不可见；
     老 marker 仍在 → 旧数据照常可查。原子性成立。
5. 同一 Logical Partition 的重复提交 = 覆盖（从 marker 移除旧 part 并删除文件，再追加新 part），
   由 Writer 序列化保证。
6. `_table.json` / `_group.json` 的 `row_count` 更新在提交标记写完后执行（最终一致，
   以 sidecar 汇总为校验依据）。
7. 第一版 **Immutable + Append-only**：已有 part 永不修改；UPDATE/DELETE/Tombstone/Delta
   不在 Phase 1~6 范围。

---

## 10. 读取协议（Aligned Scan）

### 10.1 打开

1. 读 `<data_root>/<table>/_table.json`；校验 `version` 支持、`groups` 非空。
2. 按投影（Phase 2+）选出需要的 Group（MVP：全部）。
3. 对每个需要的 Group：读 `_group.json`；校验 row_count 与表一致（fail-fast）。
4. 对每个 Group：glob partition 目录 → 校验 `.aligned-commit.json` → glob `part-*.parquet`
   → 读 sidecar 汇总行区间；校验覆盖 `[0, row_count)` 无重叠无空洞（fail-fast）。
5. 结果：`(group, [part + 行区间])` 列表 = 扫描计划。该计划可缓存（§12）。

### 10.2 执行

1. 按 `start_row` 把任务切成 **Aligned Row Group 粒度**（默认 131072 行一个 task）。
2. 每个 task 打开涉及的各 Group part 文件（DuckDB ParquetReader），
   读出需要的列（Parquet 列投影），把 Vector 按输出列位置**直接填入同一个 DataChunk**。
3. **禁止**：JOIN、横向 concat、Key 比较、物化中间行。
4. 行数以 `_table.json.row_count` 为准（截断多出的行属于数据损坏，报错）。

### 10.3 剪枝（Phase 2/3 启用，MVP 不做）

- Partition Pruning：`WHERE date = ...` → 按各 Group 的 `partitioning` 模板解析目录，
  跳过无关 partition。
- Row Group Pruning：Parquet min/max 统计。
- Projection：只读被选列、只开被选 Group。

---

## 11. 错误与校验语义

| 情形 | 行为 |
|------|------|
| `_table.json` / `_group.json` 缺失或 JSON 非法 | 报错，指明路径 |
| Group 的 part 区间不覆盖 `[0,row_count)` 或有重叠 | 报错"alignment violation" |
| sidecar 缺失 | 报错 |
| 同列类型不一致 | 报错 |
| Key 列出现在非 index Group | 写入时报错（Reader 不查） |
| `_tmp/` 内文件 | Reader 永不访问 |

---

## 12. Metadata Cache（Phase 4）

- 缓存：`_table.json`、`_group.json`、sidecar、Parquet footer/统计。
- 键：`(绝对路径, mtime, size)`；LRU 淘汰；第一版不缓存数据页。
- 缓存命中不得跳过 §10.1 的 key/row_count 一致性校验（校验成本 = 内存比较）。

---

## 13. 保留扩展点（明确不做，禁止提前实现）

- `canonical_order: "sorted"` + skip index
- Bloom filter / 二级索引
- Tombstone / Delta / UPDATE / DELETE
- 类型升级（int→bigint 等）
- 稀疏专用存储（`sparse/` Group 形态）

---

## 14. 验收清单（Phase 0 完成条件）

- [x] 术语与 7 条不变量定义（§1~§3）
- [x] "row N 对齐"的精确含义与 Writer/Reader 责任（§3）
- [x] 目录布局、Manifest、sidecar、提交协议（§2, §5, §7, §9）
- [x] Schema Evolution 规则（§8）
- [ ] 由 Writer（Phase 5）与 Reader（Phase 1）实现并互测通过
