# Storage Contract — AlignedTable（最终定稿）

> 本文件是**存储层唯一权威规格**。Reader / Writer / Compaction 的一切实现
> 必须与本文件一致；任何修改必须先改本文件再改代码。
> 状态：**v2**（2026-08）。v2 相对 v1 的核心变更：
> `_group.json`、part sidecar、`.aligned-commit.json` commit marker 全部删除；
> `aligned` 字段语义固定为 true；partitioning 移入 `_table.json`（或由目录推导）；
> txid 记录在 `_table.json.last_txid`；行区间由 Parquet footer 声明。

---

## 1. 术语

| 术语 | 定义 |
|------|------|
| Logical Table | 一张逻辑宽表，如 `cnstk_ixday`，对应 `<data_root>/<table_name>/` |
| Column Group | Logical Table 的一个物理列子集，如 `index`、`factor/alpha101`，对应表内一个目录 |
| Canonical Row Space | 该表的逻辑行坐标集合 `{0, 1, ..., R-1}`，R = 表行数 |
| Canonical Key | 定义行语义的列集合，如 `["date", "symbol"]`（写在 `_table.json`） |
| Logical Partition | 按 Key 值划分的互斥行子集（如 `date = 2026-08-17` 的所有行） |
| Physical Partition | 某个 Group 内一个 Hive 风格目录（如 `date=2026-08-17/` 或 `year=2026/month=2026-08/date=2026-08-17/`），含一组 part 文件 |
| Part File | 一个 Parquet 文件（`part-<6位序号>.parquet`），是 Group 内最小读取单元 |
| Row Group | Parquet 内 Row Group；本契约要求统一大小 `row_group_size`（默认 131072） |
| Aligned Scan | 按行坐标直接组装多 Group 向量的扫描，绝不 JOIN |

---

## 2. 目录布局（唯一合法布局）

```
<data_root>/
└── <table_name>/                  ← Logical Table
    ├── _table.json                ← 表级 Manifest（唯一 manifest，必选）
    ├── index/                     ← Column Group 目录（必选，见 §2.1b）
    │   └── date=2026-08-17/       ← Physical Partition 目录（Hive 风格）
    │       └── part-000000.parquet
    ├── factor/alpha101/           ← 二级目录 Group（必选，见 §2.1c）
    │   └── year=2026/month=2026-08/date=2026-08-17/
    │       └── part-000000.parquet
    └── ...
```

**硬性规则**：
1. 每个 Logical Table 必须有 `_table.json`（Group 级 manifest 不存在）。
2. `_tmp/` 是唯一临时区（交易目录），**Reader 永不扫描 `_tmp/`**（见 §2.1d）。
3. 保留前缀/后缀：`_table.json`、`part-`、`_tmp/`。数据列名、目录名不得与保留名冲突。
4. Physical Partition 目录是**叶子目录**：直接包含 part 文件，不再有子目录。
5. 目录里除本契约列出的文件外不允许其他文件（便于 glob 与校验）。

### 2.1 目录硬性规则（v1.2 起保留）

a. `<table_name>` 即表名（如 `cnstk_ixday`）。
b. **`<table>/index` 必须存在**：每张表必须有 index Group（物理目录 + `_table.json.groups` 条目），
   缺失即报错。Key 列与基础列都住在 index。
c. **其余 Group 必须以两级目录存在**：`<table>/<lv1>/<lv2>/`（如 `factor/alpha101`、
   `fieldset/ma`、`panel/cnstk_klday`）。manifest 的 `groups` 条目必须形如 `lv1/lv2`，
   否则报错。Physical Partition 目录在 group 目录之下，不受此限制。
d. **目录发现时忽略任何以 `.` 或 `_` 开头的目录**（如 `_tmp/`、`.hidden/`）：
   glob 结果中路径段以 `.`/`_` 开头的 part 一律跳过；`_table.json` 以 `.`/`_` 开头
   的**文件**不受影响。

### 2.2 列名规则（v1.2 起保留）

e. **重复列名的处理**（按优先级）：

   1. **与 index 中重复的列名，全部忽略**：非 index Group 中的同名列不进入表 schema
      （index 列是权威的）；reader 扫描时跳过这些列（不读、不填）。
   2. **非 index Group 之间重复的列名，必须用限定名引用**：`lv1_name.lv2_name.col_name`
      （如 `factor.alpha101.close`）。表 schema 中这类列只以限定名注册（裸名不注册）；
      引用裸名 → 报"列不存在"；引用限定名（DuckDB 中需反引号）→ 正常。
   3. 非重复列名照常以裸名注册。

   Writer 侧：非 index Group 不应写与 index 重复的列（写了会被 Reader 忽略）。

---

## 3. Canonical Row Space（"row N 对齐"的精确定义）

### 3.1 定义

设表 T 行数为 R，行坐标 `0..R-1`。每个 Column Group G 的所有 part 文件
（按 §3.3 的全局行序排列）拼接后，恰好覆盖 `[0, R)` 且互不重叠。
G 的第 N 行 = 该拼接序列的第 N 行。

**对齐（Alignment）**：表 T 是 aligned 的，当且仅当：

> 对任意两个 Group A、B 与任意行坐标 N ∈ [0, R)：
> **A 的第 N 行与 B 的第 N 行是同一个 Logical Row（拥有相同的 Canonical Key 值）**。

### 3.2 保证方式（不对称责任）

- **Writer 必须构造式保证**：一个提交事务内，所有 Group 写入**完全相同的行区间
  （相同 start_row + row_count）**；提交原子化（§9）；未完成的交易对 Reader 不可见。
- **Reader 必须盲信**：Aligned Scan **永不**通过 Key 比较 / JOIN / 查表验证对齐。
  对齐验证只允许在显式校验模式与测试中使用。
- 行区间由 Parquet footer 声明（§7）：每 part 的 `row_count` = footer 行数；
  全局行序与 start_row 按 §3.3 排序累加。Reader 校验所有已打开 Group 的
  总行数完全一致（== footer 汇总），不一致立即报错（fail-fast）。

### 3.3 全局行序（v2 精确定义）

同一 Group 内的 part 按如下顺序排列（= 该 Group 的行顺序）：

1. **分区目录**：目录相对路径（group 根之下）的**字符串字典序**。
   对固定格式 `year=YYYY`、`month=YYYY-MM`、`date=YYYY-MM-DD`，字符串序即时间序。
   非识别格式段不参与排序依据的判定（按字符串整体比较即可）。
2. **part 序号**：`part-<数字>` 中数字的数值升序（写入顺序）。

`start_row` 按该顺序累加 footer row_count 得到；part 区间两两不相交且并集
== `[0, R)`（`ValidateRowSpace` 校验）。

### 3.4 不变量（Invariant，7 条）

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

### 5.1 `_table.json`（唯一 manifest）

```json
{
  "name": "cnstk_ixday",
  "version": 1,
  "schema_version": 1,
  "key": ["date", "symbol"],
  "canonical_order": "fixed",
  "row_count": 1234567890,
  "row_group_size": 131072,
  "part_rows": 4194304,
  "last_txid": 42,
  "groups": ["index", "factor/alpha101", "fieldset/ma"],
  "partitioning": {
    "index":          [{"template": "date=%Y-%m-%d", "source": "date"}],
    "factor/alpha101": [{"template": "year=%Y", "source": "date"},
                        {"template": "month=%Y-%m", "source": "date"},
                        {"template": "date=%Y-%m-%d", "source": "date"}],
    "fieldset/ma":    [{"template": "year=%Y", "source": "date"},
                        {"template": "month=%Y-%m", "source": "date"}]
  }
}
```

字段语义：

- `name` / `version` / `schema_version`：表名与 schema 版本（读端校验 name 与请求一致）。
- `key`：Canonical Key 列名（必须在 `index/` Group 中存在；**不可变更**，除非重建表）。
- `canonical_order`：仅支持 `"fixed"`（§4）。
- `row_count`：**仅记账**（Writer 维护）。Reader 忽略该字段——总行数以
  Parquet footer 汇总为准（§3.2/§7）。
- `row_group_size`：默认 Row Group 大小（Writer 按此 flush）。
- `part_rows`：可选，Writer 的目标 part 大小提示（默认 4194304）。
- `last_txid`：最近一次成功提交的事务号（§9）。无 marker 文件，这是事务的
  唯一记录。Writer/Compactor 每次成功 commit 后 +1。读端忽略未知字段。
- `groups`：Column Group 列表（`"index"` 必含，其余形如 `lv1/lv2`，§2.1b/c）。
- `partitioning`（**可选**）：`group → 有序目录模板列表`（顶层→底层）。规则：
  - 模板只有三种合法形式：`year=%Y`、`month=%Y-%m`、`date=%Y-%m-%d`；
    `source` 固定为逻辑 `date` 列（本版本不支持其他 source/列）。
  - 显式 partitioning 优先于目录推导；**空表（尚无 part）起步时必须有显式
    partitioning**，否则 Writer 无法决定目录布局。
  - **Writer 重写 `_table.json` 时必须原样写回 partitioning**，否则显式配置丢失
    （退化为目录推导，可能改变未来写入的目录布局）。

### 5.2 目录推导（无显式 partitioning 时）

Group 无显式 partitioning 时，Reader/Writer 从已有 part 的目录结构推导分区 schema：

- 只识别三种固定段：`year=YYYY` → `{template: year=%Y}`、
  `month=YYYY-MM` → `{template: month=%Y-%m}`、`date=YYYY-MM-DD` → `{template: date=%Y-%m-%d}`；
  `source` 一律为 `"date"`。
- 其他 `name=value` 段（如 `day=01`、`symbol=xxx`）**忽略**（不识别、不影响推导）。
- 同一 Group 的 part 之间某段的格式不一致 → 报错（如一部分 `month=2026-08`、
  另一部分 `month=08`）。
- 无任何识别段 → 该 Group 无分区（part 直接在 Group 目录下）。

### 5.3 Manifest 职责边界

- Manifest **不记录** part 文件清单（文件发现 = glob + Parquet footer）。
- 只有 `_table.json` 一个 manifest 文件；无 `_group.json`。
- Manifest 是 JSON（UTF-8），写时先写临时文件再原子 rename（§9）。

---

## 6. Parquet 约定

- **类型**（Phase 1）：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、
  `HUGEINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、`DATE`、`TIMESTAMP`、`TIME`、`VARCHAR`。
  不用嵌套类型。
- `date` 列：Parquet `DATE`（int32 days）；`symbol` 列：`VARCHAR`（字典编码默认开启）。
- 压缩：默认 `zstd`（可配）；统计信息：默认全开（Row Group 裁剪依赖它）。
- 每个 part 文件内 Row Group 大小 = `_table.json.row_group_size`，**文件内连续铺满**
  （最后一个 RG 可不满）。
- 同一列在**所有 part 中类型必须一致**（类型严格，不隐式转换）。

---

## 7. Part 文件与行区间声明（v2：footer 自描述）

v1 的 `*.aligned.json` sidecar 已删除。每 part 的行数与列集合由 **Parquet
footer** 自描述：

- part `row_count` = footer 行数（`NumRows`）；列集合 = footer 列名（文件 schema 顺序）。
- `start_row` 由 §3.3 排序后累加得到；**没有 sidecar，没有 start_row 声明文件**。
- 不变量：同一 Group 内所有 part 的 `[start_row, start_row+row_count)` 两两不相交，
  且并集 == `[0, R)`（R = 该 Group footer 汇总行数）。违反即算数据损坏，扫描报错
  （`ValidateRowSpace`）。
- 因没有 sidecar/start_row 声明，**重命名/移动 part 会改变其在全局行序中的位置**；
  Writer/Compactor 的提交顺序必须保证最终目录 + part 序号满足 §3.3。

---

## 8. Schema Evolution（第一版规则）

- 列集合按 part 自描述（footer）。Reader 对打开的行区间取**并集**（first-seen 顺序）。
- 旧 part 没有的列 → 读为 `NULL`（不重写历史）。
- 同一列类型跨 part 必须一致；不一致 = 错误（Phase 1 不做类型升级）。
- Key 列集合 `_table.json.key` 一经创建**不可变更**（除非重建表）。

---

## 9. 写入与原子提交协议（Phase 5 实现，契约现在定死）

1. Writer 以 **Logical Partition 为提交单位**（如 `date=2026-08-17` 的全部行）。
2. 写入路径：`<table>/_tmp/transaction-<txid>/<group>/<partition-dirs...>/`，
   所有 Group 的 part 都在这里写完整并 fsync。
3. **提交 = 两步**：
   a. 把每个 Group 的 partition 目录 rename 到正式位置（同文件系统，单次 rename 原子）；
   b. 重写 `_table.json`：`row_count`（记账）加上新增行数、`last_txid = <txid>`，
      **原样保留 partitioning**；临时文件写 + 原子 rename。
4. **Reader 可见性规则**（v2）：
   - 一个 part 可见 ⟺ 存在于正式位置（`_tmp/` 内的 part 永不可见，§2.1d）。
   - 崩溃发生在 (a) 与 (b) 之间 → 新 part 已 rename 但 `_table.json.last_txid` 未更新：
     部分 Group 可见部分不可见。这是 v2 的已知取舍：读取一致性依赖"写入方
     先写全再 move"的顺序与 §3.3 行序契约；崩溃残留的 `_tmp/` 由下一次事务清理
     （读端从不读 `_tmp/`）。
   - 跨 Group 行数一致性在**扫描时**强校验（§3.2 fail-fast）：索引有数据、alpha
     没写完的非法状态会被扫描直接报错而不是返回错行。
5. 同一 Logical Partition 的重复提交 = 追加新 part（append-only），由 Writer
   序列化保证。
6. 事务号：`txid = last_txid + 1`；任何失败（写、move、manifest 重写）→ 删除
   `_tmp/transaction-<txid>/`（及 best-effort 清空 `_tmp/`），事务丢弃。
7. 第一版 **Immutable + Append-only**：已有 part 永不修改；UPDATE/DELETE/Tombstone/Delta
   不在 Phase 1~6 范围。

---

## 10. 读取协议（Aligned Scan）

### 10.1 打开

1. 读 `<data_root>/<table>/_table.json`；校验 `version` 支持、`name` 匹配、`groups` 非空、
   index 存在（§2.1b）、非 index Group 为 `lv1/lv2`（§2.1c）。
2. 按投影（Phase 2+）选出需要的 Group（MVP：全部）。
3. 对每个 Group：glob `part-*.parquet`（忽略 `_`/`.` 目录）→ 读每个 part 的 footer
   （行数 + 列名）→ 推导/读取 partitioning → §3.3 排序 → 累加 start_row →
   校验覆盖 `[0, R)` 无重叠无空洞（fail-fast）。
4. 跨 Group 校验：所有 Group 的 footer 汇总行数**完全一致**（§3.2），否则报
   "alignment violation"。
5. 结果：`(group, [part + 行区间])` 列表 = 扫描计划。该计划可缓存（§12）。

### 10.2 执行

1. 按 `start_row` 把任务切成 **Aligned Row Group 粒度**（默认 131072 行一个 task）。
2. 每个 task 打开涉及的各 Group part 文件（DuckDB ParquetReader），
   读出需要的列（Parquet 列投影），把 Vector 按输出列位置**直接填入同一个 DataChunk**。
3. **禁止**：JOIN、横向 concat、Key 比较、物化中间行。
4. 行数以 footer 汇总为准（截断多出的行属于数据损坏，报错）。

### 10.3 剪枝（Phase 2/3 启用，MVP 不做）

- Partition Pruning：`WHERE date = ...` → 按各 Group 的 partitioning 模板解析目录，
  跳过无关 partition。等值走目录路径匹配；范围走 part 目录重建的日期比较。
- Row Group Pruning：Parquet min/max 统计。
- Projection：只读被选列、只开被选 Group。
- **跨 leaf 传播（v2 固定开启）**：各 leaf 的 partition pruning 结果映射到统一
  Logical Row Space 坐标后**相交**为一个全局扫描区间（`IntersectIntervals`）。
  `aligned=false`（UnionIntervals）模式已删除，读端忽略 `_table.json` 中的
  `aligned` 字段（一律按 true 处理）。

---

## 11. 错误与校验语义

| 情形 | 行为 |
|------|------|
| `_table.json` 缺失或 JSON 非法 | 报错，指明路径 |
| manifest `name` 与请求表名不一致 | 报错 |
| `groups` 不含 `index` 或含非 `lv1/lv2` 条目 | 报错 |
| Group 的 part 区间不覆盖 `[0,R)` 或有重叠 | 报错 "alignment violation" |
| 各 Group footer 汇总行数不一致 | 报错 "alignment violation"（跨 Group） |
| 同一 Group 推导出的分区段格式冲突 | 报错 |
| 同列类型不一致 | 报错 |
| Key 列出现在非 index Group | 写入时报错（Reader 不查） |
| `_tmp/` 内文件 | Reader 永不访问 |

---

## 12. Metadata Cache（Phase 4）

- 缓存：`_table.json`、Parquet footer/统计。
- 键：`(绝对路径, mtime, size)`；LRU 淘汰；第一版不缓存数据页。
- 缓存命中不得跳过 §10.1 的行数一致性校验（校验成本 = 内存比较）。

---

## 13. 保留扩展点（明确不做，禁止提前实现）

- `canonical_order: "sorted"` + skip index
- Bloom filter / 二级索引
- Tombstone / Delta / UPDATE / DELETE
- 类型升级（int→bigint 等）
- 稀疏专用存储（`sparse/` Group 形态）
- 非 `date` 的 partition source 列

---

## 14. 验收清单（Phase 0 完成条件）

- [x] 术语与 7 条不变量定义（§1~§3）
- [x] "row N 对齐"的精确含义与 Writer/Reader 责任（§3）
- [x] 目录布局、Manifest、footer 行区间、提交协议（§2, §5, §7, §9）
- [x] Schema Evolution 规则（§8）
- [x] 由 Writer（Phase 5）与 Reader（Phase 1）实现并互测通过
- [x] v2 简化：删除 `_group.json`/sidecar/marker，partitioning 入 `_table.json`，
      行区间 footer 自描述，`last_txid` 事务记录，aligned 固定 true
