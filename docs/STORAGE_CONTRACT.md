# Storage Contract — AlignedTable（v7：partition-aligned + 自描述 part 名 + 主键契约）

> 本文件是**存储层唯一权威规格**。Reader / Writer / Compaction 的一切实现
> 必须与本文件一致；任何修改必须先改本文件再改代码。
> 状态：**v7**（2026-08）。要点：**分区对齐（partition-aligned）是唯一契约**——
> 所有 Group 用**同一种一层分区段**（`year=`/`month=`/`date=` 三选一，组间必须
> 相同）；分区键 = 完整目录段串（含 `name=` 前缀，因此不同分区方式自动无法对齐）；
> Group 的分区键集合必须是 index 分区键集合的**子集**（允许缺分区 → 该区行保留、
> 该组列全 NULL）；共享分区**总行数**必须一致（末 part 行数可不同）；**part 文件名
> 自描述** `{idx:04d}-{rows:10d}.parquet`（分区内索引 + 文件行数），行区间由文件名
> 累加推导（零 footer IO）；**index 组分区内索引必须 0000 起连续**（非 index 组允许
> 缺号）；Canonical Key = index Group 的 schema；**v7 主键契约**：index schema 前两列
> = 主键 `(date, symbol)`——**恰一列 DATE/TIMESTAMP**（分区源列，用于目录模板求值）
> + **一列 symbol**，两日期列或无日期列均 fail-fast；`_table.json` **可选**；`groups`
> 字段读端永不解析（唯一例外：空表起步给 Writer 提供 Group 骨架）；旧字段
> （`aligned`、`key`、`canonical_order`、`row_count`、`row_group_size`）向后兼容忽略。
> 写入侧（v7）：`aligned_upsert` / `aligned_delete` 取代 `aligned_write`，见 §9。

---

## 1. 术语

| 术语 | 定义 |
|------|------|
| Logical Table | 一张逻辑宽表，如 `cnstk_ixday`，对应 `<data_root>/<table_name>/` |
| Column Group | Logical Table 的一个物理列子集，如 `index`、`factor/alpha101`，对应表内一个目录 |
| Canonical Row Space | 该表的逻辑行坐标集合 `{0, 1, ..., R-1}`，R = 表行数（= index 各分区行数之和） |
| Canonical Key | 定义行语义的列集合（v5：**= index Group 的 schema 列**，不再写入 manifest） |
| Logical Partition | 按 Key 值划分的互斥行子集（如 `date = 2026-08-17` 的所有行） |
| Physical Partition | 某个 Group 内一个 Hive 风格目录（如 `month=2026-08/`），**单层、各 Group 同一种段** |
| Part File | 一个 Parquet 文件，**文件名自描述** `{idx:04d}-{rows:10d}.parquet`（v6），是 Group 内最小读取单元 |
| Row Group | Parquet 内 Row Group；本契约约定统一大小 `rg_rows`（默认 16384） |
| Aligned Scan | 按行坐标直接组装多 Group 向量的扫描，绝不 JOIN |
| Partition Alignment | 唯一支持的对齐契约：所有 Group 用同一层同一种分区段；Group 分区键 ⊆ index 分区键；共享分区总行数一致（§3.2） |

---

## 2. 目录布局（唯一合法布局）

```
<data_root>/
└── <table_name>/                  ← Logical Table
    ├── _table.json                ← 表级 Manifest（可选，缺失时用默认值，见 §5.1）
    ├── index/                     ← Column Group 目录（必选，见 §2.1b）
    │   └── month=2026-08/         ← Physical Partition 目录（单层 Hive 风格）
    │       ├── 0000-0000002000.parquet
    │       └── 0001-0000002000.parquet
    ├── factor/alpha101/           ← 二级目录 Group（必选，见 §2.1c）
    │   └── month=2026-08/         ← 与 index 同一种分区段（非 index 组可缺号：0000,0002）
    │       ├── 0000-0000002000.parquet
    │       └── 0002-0000002000.parquet
    ├── fieldset/ma/               ← 可以缺分区（这里缺 month=2026-07）
    │   └── month=2026-08/
    │       └── 0000-0000004000.parquet
    └── ...
```

**硬性规则**：
1. `_table.json` **可选**：缺失时使用全部默认值（rg_rows=16384、part_rows=4194304），
   Group 列表从文件布局发现（§5.2）。**分区对齐是唯一契约**：数据不满足即 fail-fast。
2. `_tmp/` 是唯一临时区（交易目录），**Reader 永不扫描 `_tmp/`**（见 §2.1d）。
3. 保留前缀/后缀：`_table.json`、`_tmp/`。数据列名、目录名不得与保留名冲突。
   **part 文件必须自描述命名** `{idx:04d}-{rows:10d}.parquet`（v6，§7）；不再支持
   `part-` 前缀。
4. Physical Partition 目录是**叶子目录**：直接包含 part 文件，不再有子目录。
   分区目录**恰好一层**：不允许 `year=/month=/date=` 嵌套多层。
5. 目录里除本契约列出的文件外不允许其他文件（便于 glob 与校验）。

### 2.1 目录硬性规则

a. `<table_name>` 即表名（如 `cnstk_ixday`）。
b. **`<table>/index` 必须存在**：每张表必须有 index Group（物理目录；空表起步时
   显式 manifest 的 `groups` 条目也必须含 `index`），缺失即报错。Key 列与基础列
   都住在 index。
c. **其余 Group 必须以两级目录存在**：`<table>/<lv1>/<lv2>/`（如 `factor/alpha101`、
   `fieldset/ma`）。manifest 的 `groups` 条目必须形如 `lv1/lv2`，否则报错。
   Physical Partition 目录在 group 目录之下，不受此限制。
d. **目录发现时忽略任何以 `.` 或 `_` 开头的目录**（如 `_tmp/`、`.hidden/`）：
   glob 结果中路径段以 `.`/`_` 开头的 part 一律跳过；`_table.json` 以 `.`/`_` 开头
   的**文件**不受影响。
e. **分区段唯一性**：一个 Group 内所有 part 的目录**恰好一个 `name=value` 段**；
   `year=` / `month=` / `date=` 三选一，**所有 Group（含 index）必须用同一种段**
   （否则分区键串不同 → 无法对齐，fail-fast）。不允许多层嵌套分区。

### 2.2 列名规则

f. **重复列名的处理**（按优先级）：

   1. **与 index 中重复的列名，全部忽略**：非 index Group 中的同名列不进入表 schema
      （index 列是权威的）；reader 扫描时跳过这些列（不读、不填）。
   2. **非 index Group 之间重复的列名，必须用限定名引用**：`lv1_name.lv2_name.col_name`
      （如 `factor.alpha101.close`）。表 schema 中这类列只以限定名注册（裸名不注册）；
      引用裸名 → 报"列不存在"；引用限定名（DuckDB 中需反引号）→ 正常。
   3. 非重复列名照常以裸名注册。

   Writer 侧：非 index Group 不应写与 index 重复的列（写了会被 Reader 忽略）。

---

## 3. Canonical Row Space 与分区对齐

### 3.1 定义

设表 T 行数为 R = index 各分区行数之和，行坐标 `0..R-1`。每个 Column Group G 的
part 文件（§3.3 排序）拼接后，**允许只覆盖 index 行空间的一部分**（缺分区 → 该区
行保留、G 的列全 NULL）。G 的第 N 行（若 G 覆盖 N）= 拼接序列中对应行。

**对齐（Partition Alignment）**：表 T 是 aligned 的，当且仅当：

> 1. 所有 Group（含 index）使用**同一层同一种分区段**；
> 2. 任一 Group 的分区键集合 ⊆ index 的分区键集合（Group 可以缺分区，**绝不添加
>    index 没有的分区**）；
> 3. 对任意共享分区 i：**Group 的 `month=...` 覆盖的行数与 index 的完全相同**
>    （总行数一致；末 part 行数允许不同，只有总和是契约）；
> 4. 对任意两个共享分区键的 Group A、B 与行坐标 N ∈ 该分区：**A 的第 N 行与 B 的
>    第 N 行是同一个 Logical Row**。

### 3.2 分区行数公式与校验（v6：文件名自描述，零 footer IO）

- part 文件名自描述：`{idx:04d}-{rows:10d}.parquet`——`idx` = 分区内索引
  （0000 起），`rows` = 文件总行数（十进制、10 位定宽，上限 9,999,999,999）。
  文件名不匹配该格式 → fail-fast（"does not match the self-describing v6 name"）。
- **index 组**：分区内索引必须 **0000 起连续**（定义行空间，缺号/重复 → fail-fast）。
  非 index 组允许缺号（如 `0000,0002`）——该分区行区间按**存在的 part 的文件名
  累加**推导（缺号只是文件名索引空隙，不影响数据行连续性）。
- 分区 i 的行数：`R_i = Σ(该分区全部 part 的文件名行数)`。
- 分区内第 j 个存在 part 的 `start_row = S_i + Σ(索引更小的存在 part 的行数)`，
  `row_count = 该 part 文件名行数`，其中 S_i = index 同键分区的起始行
  （= 更早分区的 R 之和）。**不读任何 footer 推导行区间**。
- 校验（fail-fast）：
  - 共享分区：Group 的 R_i == index 的 R_i（§3.1.3，两边都按文件名累加）。
  - 同分区同索引：两个 Group 都有该索引的 part → 行数必须相等。
  - **扫描时 OpenPart 防御校验**：footer 实际行数 == 文件名行数（所有 part，
    违反 fail-fast）。
  - 组内同键分区的 part 数不要求 == index 同键分区 part 数。
- 组间总行数：各 Group 覆盖分区的行数之和不一定相等（缺分区）；**index 的总行数
  定义 R**；扫描时缺分区区间填充 NULL（§10.2）。

### 3.3 全局行序（v6 精确定义）

同一 Group 内的 part 按**规范化相对路径（group 根之下，含文件名）的字符串
字典序**排列（= 该 Group 的行顺序）：

1. 对固定格式 `year=YYYY`、`month=YYYY-MM`、`date=YYYY-MM-DD`，目录段字符串序
   即时间序；非识别格式段按字符串整体比较。
2. **part_id = 文件名解析出的 `idx` 字段**（分区内索引，`{idx:04d}`）。不再按
   文件名数值整体排序、不再有 `part-<6位序号>` 命名。

`start_row` 按 §3.2 的文件名累加推导；同一分区内 part 区间两两不相交，且该分区
覆盖区间 == `[S_i, S_i+R_i)`（`ValidateRowSpace` 校验，始终执行）。

### 3.4 不变量（Invariant，7 条）

1. Logical Table 有唯一 Canonical Row Space。
2. Key Columns 只存一份（只在 `index/` Group，其他 Group 不得包含 Key 列）。
3. 所有 Column Group 使用相同 Row Ordering。
4. 所有 Column Group 使用相同 Logical Row Coordinate（即 §3.1）。
5. Partition Scheme 必须相同：所有 Group 同一种一层分区段；Group 分区键 ⊆ index。
6. Physical Files 可以完全不同（文件名、数量、大小、切分方式）。
7. 查询阶段绝不通过 Key 做 JOIN。

---

## 4. Row Ordering

- 本版本 `canonical_order` 仅支持 `"fixed"`（v2 及更早的 manifest 字段，v5 读端
  忽略）：行坐标在写入时确定，**永不变更**。（保留未来 `"sorted"` 枚举位，但不实现。）
- Compaction 必须保持行序与行坐标不变（只能合并文件，不能重排行）。
- Writer 可选择按 Key 排序写入（利于未来 skip index），但 Reader **不得假设有序**。

---

## 5. Manifest 规格

### 5.1 `_table.json`（可选；v5）

**`_table.json` 是可选文件**。缺失时全部字段取默认值，读取行为
与"显式写出默认值"完全一致。示例（全部显式）：

```json
{
  "name": "cnstk_ixday",
  "version": 1,
  "rg_rows": 16384,
  "part_rows": 4194304,
  "last_txid": 42,
  "groups": ["index", "factor/alpha101", "fieldset/ma"],
  "partitioning": {
    "index":           [{"template": "month=%Y-%m", "source": "date"}],
    "factor/alpha101": [{"template": "month=%Y-%m", "source": "date"}],
    "fieldset/ma":     [{"template": "month=%Y-%m", "source": "date"}]
  }
}
```

字段语义：

- `name` / `version`：表名与 manifest 版本（可选；写时保留、读时校验 name 与请求一致）。
- `rg_rows`（可选）：默认 Row Group 大小，Writer 按此 flush（默认 16384）。
- `part_rows`（可选）：目标 part 大小提示（默认 4194304）。**读端不使用该值**
  ——part 行数与行区间全部由 part 文件名自描述（v6，§3.2/§7）。
- `last_txid`：最近一次成功提交的事务号（§9）。无 marker 文件，这是事务的
  唯一记录。Writer/Compactor 每次成功 commit 后 +1。

- `groups`（**可选**，v5 起**读端永不解析**）：Column Group 列表（`"index"` 必含，
  其余形如 `lv1/lv2`）。**唯一例外**：表为空（glob 无任何 part）且 `groups` 非空时，
  用它给 Writer 提供 Group 骨架（空表起步）；除此之外读端一律从文件布局发现（§5.2）。
  Writer/Compactor 重写 manifest 时必须原样写回 `groups`（round-trip）。
- `partitioning`（**可选**）：`group → 目录模板列表`。规则：
  - **每个 Group 恰好一个模板**（v5：分区单层）；模板只有三种合法形式：
    `year=%Y`、`month=%Y-%m`、`date=%Y-%m-%d`；`source` 为**逻辑日期列名**。
  - **v7 主键契约**：index Group 的 schema 前两列 = 主键 `(date, symbol)`——
    **恰一列 `DATE`/`TIMESTAMP`**（组 schema = rel_path 排序最后 1 个 part 的
    footer，§8）且**另一列为 symbol**（两日期列 / 无日期列均 fail-fast）；该日期
    列即分区源列。模板的 `source` 必须 == 该实际列名（无显式 partitioning 时目录
    推导自动绑定，显式声明不匹配 → fail-fast）。**分区目录只由日期列求值**（v6
    起不再写死 `date`）。
  - **所有 Group（含 index）必须用同一种模板**（分区对齐 §3.1.1）。
  - 显式 partitioning 优先于目录推导；**空表（尚无 part）起步时必须有显式
    partitioning**，否则 Writer 无法决定目录布局。
  - **Writer 重写 `_table.json` 时必须原样写回 partitioning**，否则显式配置丢失
    （退化为目录推导，可能改变未来写入的目录布局）。
- **旧字段向后兼容**：`aligned`（v3 三模式）、v2 的 `key`、`schema_version`、
  `canonical_order`、`row_count`、`row_group_size` 被读端**忽略**（不报错、不参与
  任何计算）。**对齐模式不存在可配置字段**：分区对齐是唯一契约，数据不满足即
  fail-fast（§3.2）。v5 的 Canonical Key = index Group 的 schema 列（§1）。

### 5.2 目录/组发现（无显式 groups 时）

- Group 发现：glob `<table>/**/*.parquet`（跳过 `.`/`_` 段），对每个 part 从路径
  尾部向前扫描目录段，跳过 `name=value` 分区段，剩余目录段的最长后缀即 Group
  （`factor/alpha101/month=...` → `factor/alpha101`；`index/month=...` → `index`）。
  part 直接位于表根下 → 报错。
- 分区推导（无显式 partitioning 时）：从已有 part 的目录结构推导分区 schema：
  - 只识别三种固定段：`year=YYYY` → `{template: year=%Y}`、
    `month=YYYY-MM` → `{template: month=%Y-%m}`、`date=YYYY-MM-DD` →
    `{template: date=%Y-%m-%d}`；`source` 一律为 `"date"`。
  - **v5：只识别单层**；出现多层 `name=value` 段 → 报错（不迭代推导）。
  - 同一 Group 的 part 之间某段的格式不一致 → 报错（如一部分 `month=2026-08`、
    另一部分 `month=08`）。
  - 无任何识别段 → 该 Group 无分区（part 直接在 Group 目录下，分区键 = `""`；
    仅当 index 也无分区时合法）。

### 5.3 Manifest 职责边界

- Manifest **不记录** part 文件清单（文件发现 = glob + Parquet footer）。
- 只有 `_table.json` 一个 manifest 文件；无 `_group.json`。
- Manifest 是 JSON（UTF-8），写时先写临时文件再原子 rename（§9）。

---

## 6. Parquet 约定

- **类型**：`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、
  `HUGEINT`、`FLOAT`、`DOUBLE`、`DECIMAL`、`DATE`、`TIMESTAMP`、`TIME`、`VARCHAR`。
  不用嵌套类型。
- `date` 列：Parquet `DATE`（int32 days）；`symbol` 列：`VARCHAR`（字典编码默认开启）。
- 压缩：默认 `zstd`（可配）；统计信息：默认全开（Row Group 裁剪依赖它）。
- 每个 part 文件内 Row Group 大小 = `_table.json.rg_rows`（默认 16384），**文件内
  连续铺满**（最后一个 RG 可不满）。
- 同一列在**所有 part 中类型必须一致**（类型严格，不隐式转换）。

---

## 7. Part 文件与行区间声明（v6：文件名自描述）

无 sidecar：part 文件名本身就是行区间声明：

- **文件名格式** `{idx:04d}-{rows:10d}.parquet`：`idx` = 分区内索引（0000 起），
  `rows` = 文件总行数（10 位定宽十进制）。`idx` 决定分区内顺序，`rows` 决定
  `start_row` 与 `row_count`（§3.2 累加推导，**计划阶段零 footer IO**）。
- 列集合由 **Parquet footer** 自描述：各 part 实际列集以扫描时打开的 reader 为准
  （schema evolution → 缺失列读 NULL）。
- 组 schema（列+类型）= **组内 rel_path 排序最后 1 个 part** 的 footer schema
  （每 Group 只读 1 个 footer）。
- **防御校验（扫描时 OpenPart）**：footer 实际行数 == 文件名 `rows` 字段，
  违反 fail-fast（"declared <rows> but the file contains <n> rows"）。
- 不变量（始终成立）：同一 Group 内**同一分区**的所有 part 的
  `[start_row, start_row+row_count)` 两两不相交，且并集 == `[S_i, S_i+R_i)`。
  违反即算数据损坏，扫描报错（`ValidateRowSpace`）。
- **重命名/移动 part 会改变其在全局行序中的位置**（`idx` 变了）或破坏行区间
  （`rows` 变了）；Writer/Compactor 的提交顺序必须保证最终目录 + 文件名满足 §3.3。

---

## 8. Schema Evolution（第一版规则）

- 列集合按 part 自描述（footer）。Reader 对打开的行区间取**并集**（first-seen 顺序）。
- 旧 part 没有的列 → 读为 `NULL`（不重写历史）。
- 同一列类型跨 part 必须一致；不一致 = 错误（Phase 1 不做类型升级）。
- Canonical Key = index schema（v5），一经创建**不可变更**（除非重建表）。
- **v7 主键契约**：组 schema（= rel_path 排序最后 1 个 part 的 footer）的**前两列 =
  主键 `(date, symbol)`**——恰一列是 `DATE`/`TIMESTAMP`（分区源列）且另一列为
  symbol 列，两日期列或无日期列均 fail-fast；该日期列即分区源列
  （partitioning `source`，§5.1），Writer 用它求值分区目录模板。

---

## 9. 写入与原子提交协议（v7：aligned_upsert / aligned_delete 取代 aligned_write）

**API**（v7）：

```
aligned_upsert(table, source_path, mapping, root=...)   → (rows_inserted, rows_updated, parts_rewritten, txid)
aligned_delete(table, keys_source, root=...)            → (rows_deleted, parts_rewritten, txid)
```

- 主键 = `(date, symbol)`（§8 v7 主键契约）；`source_path` 内所有行按此定位：
  已存在 → 更新（只重写受影响 part，行数不变）；不存在 → 插入（追加到分区内
  最后一个 part 之后；若键排序于分区末尾 → 创建新 part `idx = max+1`；若分区
  不存在 → 为所有 mapped Group 创建新分区目录，每个 Group 一个 fresh part）。
- **映射列类型 = 组内已存类型**（组 schema），不是源文件类型：跨 part 的列类型
  必须一致（§8），新分区/新 part 不得改变列类型（首写空表时回退到源类型）。
- 新分区只出现在映射了的 Group；未映射 Group 的该分区行读为 NULL（缺分区
  契约，§3.2）。
- `aligned_delete`：keys source 只需 `(date, symbol)`；删空单 part 分区 →
  整分区移除（所有 Group）；删空多 part 分区的 part → fail-fast
  （"run aligned_compact first"）。

**提交协议**（与 v5/v6 相同）：

1. Writer 以 **Logical Partition 为提交单位**（如 `month=2026-08` 的全部行）。
2. 写入路径：`<table>/_tmp/transaction-<txid>/<group>/<partition-dirs...>/`，
   所有 Group 的 part 都在这里写完整并 fsync（staged 名可任意，如 `part-%06llu`，
   `_tmp/` 对 Reader 不可见）；**提交时** part 必须以 v6 名
   `{idx:04d}-{rows:10d}.parquet` 落位（`idx` = 目标分区目录内 NextPartIndex 递增，
   `rows` = 该 part 实际行数；行数超 10 位 → fail-fast，不自动切分）。
3. **提交 = 两步**：
   a. 把每个 Group 的 partition 目录 rename 到正式位置（同文件系统，单次 rename 原子）；
   b. 重写 `_table.json`：`last_txid = <txid>`，**原样保留 partitioning 与 groups**；
      临时文件写 + 原子 rename。v5 的 `_table.json` 不含行数/Key 字段
      （行数由 footer 得出 + 分区公式推导行区间）。
4. **Reader 可见性规则**：
   - 一个 part 可见 ⟺ 存在于正式位置（`_tmp/` 内的 part 永不可见，§2.1d）。
   - 崩溃发生在 (a) 与 (b) 之间 → 新 part 已 rename 但 `_table.json.last_txid`
     未更新：部分 Group 可见部分不可见。这是 v2 起保留的已知取舍：读取一致性
     依赖"写入方先写全再 move"的顺序与 §3.3 行序契约；崩溃残留的 `_tmp/` 由
     下一次事务清理（读端从不读 `_tmp/`）。
   - 跨 Group 分区行数一致性在**扫描时**强校验（§3.2 fail-fast）：index 有分区、
     alpha 没写完的非法状态会被扫描直接报错而不是返回错行。
5. 同一 Logical Partition 的重复提交 = 追加新 part（append-only），由 Writer
   序列化保证。
6. 事务号：`txid = last_txid + 1`；任何失败（写、move、manifest 重写）→ 删除
   `_tmp/transaction-<txid>/`（及 best-effort 清空 `_tmp/`），事务丢弃。
7. **追加校验（v5 分区对齐）**：写前模拟——index 组必须覆盖追加区间
   `[start_row, start_row+rows)`（`ValidateRowSpace` 强制）；分区子集组只要求其
   末 part 结束 == start_row（或空组要求 start_row==0，首写必须从行 0 覆盖全表）。
   upsert 同理：更新/插入的位置必须落在现有行空间或紧邻其后的追加区间。

---

## 10. 读取协议（Aligned Scan）

### 10.1 打开

1. 尝试读 `<data_root>/<table>/_table.json`；不存在则用默认值（§5.1），
   校验 `name`（若写）匹配、显式 `groups`（若写）含 index（§2.1b）、
   非 index Group 为 `lv1/lv2`（§2.1c）。
2. 组发现：**一次 glob**（`**/*.parquet`，跳过 `.`/`_` 段）→ 推导 Group 与分区键
   （§5.2）；**绝不**使用 manifest `groups`（空表例外，§5.1）。
3. 对每个 Group：按分区键分组（rel_path 字符串序）→ 从**文件名**解析
   `(idx, rows)` 推导行区间（§3.2，零 footer IO）→ 校验分区键 ⊆ index（§3.1.2，
   违反 fail-fast）→ 校验共享分区 R_i 一致（§3.2）→ index 分区内索引 0000 起连续
   （§3.2，违反 fail-fast）→ 每组只读 **rel_path 排序最后 1 个 part** 的 footer
   （组 schema + 日期列契约，§8）→ `ValidateRowSpace`（分区内无重叠无空洞）。
4. 跨 Group：index 的 R_i 表 = 权威分区表；缺分区组不报错（扫描时 NULL 填充）。
5. 结果：`(group, [part + 行区间])` 列表 = 扫描计划。该计划可缓存（§12）。

### 10.2 执行

1. 按 `start_row` 把任务切成 **Aligned Row Group 粒度**（默认 131072 行一个 task）。
2. 每个 task 打开涉及的各 Group part 文件（DuckDB ParquetReader），
   读出需要的列（Parquet 列投影），把 Vector 按输出列位置**直接填入同一个 DataChunk**。
   **缺分区区间：该组列整段 NULL 填充（行保留，总行数由 index 决定）**。
3. **禁止**：JOIN、横向 concat、Key 比较、物化中间行。
4. 行数以 index footer 汇总为准（截断多出的行属于数据损坏，报错）。

### 10.3 剪枝

- Partition Pruning：`WHERE date = ...` → 按各 Group 的 partitioning 模板解析目录，
  跳过无关 partition。等值走目录路径匹配；范围走 part 目录重建的日期比较。
- Row Group Pruning：Parquet min/max 统计。
- Projection：只读被选列、只开被选 Group。
- **跨 leaf 传播（固定相交，仅限全覆盖组）**：各 leaf 的 partition pruning 结果
  映射到统一 Logical Row Space 坐标后**相交**为一个全局扫描区间
  （`IntersectIntervals`，固定行为）。分区子集组不参与相交，其缺失区由 §10.2 的
  NULL 填充分支处理。

---

## 11. 错误与校验语义

| 情形 | 行为 |
|------|------|
| 表目录不存在 | 报错，指明路径（"table directory does not exist"） |
| `_table.json` JSON 非法 | 报错，指明路径 |
| manifest `name`（若写）与请求表名不一致 | 报错 |
| `groups`（若写）不含 `index` 或含非 `lv1/lv2` 条目 | 报错 |
| 无 index Group（无 part 且无显式 groups，或 glob 无 index） | 报错 "mandatory group 'index' was not found" |
| Group 分区键不在 index 分区键集合内 | 报错 "group has partition ... index group does not have"（fail-fast） |
| 共享分区 Group 的 R_i != index 的 R_i | 报错（fail-fast） |
| part 文件名不匹配 `\d{4}-\d{10}\.parquet` | 报错 "does not match the self-describing v6 name"（fail-fast） |
| index 分区内索引不连续（非 0000 起或重复） | 报错 "part indexes must be consecutive from 0000"（fail-fast） |
| 同分区同索引两个 Group 的 part 行数不等 | 报错（fail-fast） |
| footer 行数 != 文件名 `rows` | 报错（扫描时 OpenPart 防御校验，fail-fast） |
| index schema 前两列不是主键 (date, symbol)（恰一 DATE/TIMESTAMP + 一 symbol） / 显式 partitioning source 不匹配 | 报错（v7 主键契约，fail-fast） |
| 分区内 part 区间不覆盖 `[S_i, S_i+R_i)` 或有重叠 | 报错 "alignment violation" |
| 非 index Group 不是 `lv1/lv2` | 报错 "must be a two-level path" |
| 多层 `name=value` 分区段 / 组间分区段不一致 | 报错（v5 single-level contract） |
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
- Tombstone / Delta
- 并发写互斥（last_txid CAS，未做）
- 类型升级（int→bigint 等）
- 稀疏专用存储（`sparse/` Group 形态）
- 非日期列（DATE/TIMESTAMP 之外）的 partition source
- 多层 / 多种分区段混合

---

## 14. 验收清单

- [x] 术语与 7 条不变量定义（§1~§3）
- [x] "row N 对齐"的精确含义与 Writer/Reader 责任（§3）
- [x] 目录布局、Manifest、footer 行区间、提交协议（§2, §5, §7, §9）
- [x] Schema Evolution 规则（§8）
- [x] 由 Writer（Phase 5）与 Reader（Phase 1）实现并互测通过
- [x] v2 简化：删除 `_group.json`/sidecar/marker，partitioning 入 `_table.json`，
      行区间 footer 自描述，`last_txid` 事务记录
- [x] v3：`_table.json` 可选 + 默认值；aligned 三模式（all/group/none，声明
      fail-fast / 未声明探测降级链）；Canonical Key = index schema；part 顺序
      = 相对路径字符串序（索引即 part_id）；行区间公式推导（校验 footer）；
      旧字段向后兼容忽略
- [x] v4：删除三模式与探测降级链，只保留全对齐（all）——`aligned` 字段读端
      忽略；组间 part 数/大小必须一致、组内非最后 part == part_rows，违反即
      fail-fast（"full alignment required"）；行区间一律公式 `i*part_rows`；
      数据生成器/测试/文档全部改为全对齐布局；Compaction 改为单事务处理所有组
- [x] v5：**分区对齐（partition-aligned）唯一契约**——所有 Group 单层同一种分区段；
      分区键 = 完整 `name=value` 段串；Group 分区键 ⊆ index（缺分区 → 行保留、
      列全 NULL）；共享分区总行数一致（末 part 行数可不同）；行区间按分区公式
      `start_row = S_i + j*part_rows`；`groups` 读端永不解析（空表例外）；组 schema
      = 每分区最后 1 个 part footer；bench/测试/文档全部改为分区对齐布局
