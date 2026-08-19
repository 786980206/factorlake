# AGENTS.md — FactorLake / AlignedTable

> 本文件是项目的**唯一权威记忆文件**。任何 agent 开始工作前必须先读本文件；
> 任何架构决策、Phase 进度更新必须写回本文件（文末「当前进度」节）。

---

## 0. 一句话定位

基于 **DuckDB Extension + Parquet** 的超宽表存储/查询引擎：

> 逻辑上是一张几万甚至上十万列、10^8~10^10 行的宽表；
> 物理上拆成多个 **Column Group**，每个 Group 独立用 Parquet 存储；
> **Key 列只保存一份**；所有 Group 在 **Logical Row Space** 上严格 position-aligned；
> 查询时由 DuckDB Extension 把多个 Parquet Reader 的 Vector **直接组装进同一个 DataChunk**，
> **不做 JOIN、不做横向 materialize**。

最终用户完全无感知：

```sql
SELECT date, symbol, close, alpha001, alpha002, ma20, yoy_revenue
FROM cnstk_ixday
WHERE date = DATE '2026-08-17';
```

底层实际可能只读三个目录：`index/`、`factor/alpha101/`、`fieldset/ma/`。

**项目真正的价值**：利用"业务数据天然 row-aligned"这一先验，把传统 OLAP 的
JOIN 成本、Key duplication、不需要的列扫描、横向 concat **全部消灭**，
最终只剩 Projection + Partition Pruning + Row Group Pruning + Vectorized Aligned Scan。

---

## 1. 核心需求

- **超宽**：10,000+ 列，10^8 ~ 10^10 行，禁止单一巨型 Parquet 作为唯一物理布局。
- **Sparse / NULL-heavy**：大量因子 90%~99% NULL。上层看到普通 column，
  下层自动靠 Parquet encoding/compression 省空间，用户不需要知道 Sparse Column。
- **Column Group**：`factor/alpha101`、`fieldset/ma`、`panel/cnstk_icday` 等叶子目录各是一个 Group。
- **Key 只存一份**：`date`/`symbol` 只在 `index/` 里保存，其他 Group 不再保存。
- **绝不 JOIN**：`index[row N] == alpha[row N] == ma[row N]` 被直接视为同一行。

---

## 2. 目录设计

```
<data_root>/
├── cnidx_klday/            ← Logical Table
├── cnstk_ixday/            ← Logical Table
│   ├── _table.json
│   ├── index/
│   ├── factor/
│   │   ├── alpha101/
│   │   └── alpha191/
│   ├── fieldset/
│   │   ├── ema/  ma/  qoq/  ttm/  yoy/
│   └── panel/
│       ├── cnstk_icday/  cnstk_ixday/  cnstk_klday/
└── cnstk_klm01/            ← Logical Table
```

**语义**：`cnstk_ixday` 是 Logical Table；`cnstk_ixday/index`、
`cnstk_ixday/factor/alpha101` 等是这张表的 **Column Groups**。
这是整个设计最重要的语义。

---

## 3. Logical Row Space 与 7 条核心 Invariant

每张 Logical Table 定义一个 **Canonical Row Space**（如 row 0 → 2026-08-17/000001，
row 1 → 2026-08-17/000002 …）。所有 Group 必须满足：

| # | Invariant |
|---|-----------|
| 1 | Logical Table 有唯一 Canonical Row Space |
| 2 | Key Columns 只存一份 |
| 3 | 所有 Column Group 使用相同 Row Ordering |
| 4 | 所有 Column Group 使用相同 Logical Row Coordinate |
| 5 | Partition Scheme 可以不同 |
| 6 | Physical Files 可以完全不同 |
| 7 | 查询阶段绝不通过 Key 做 JOIN |

必须一致：`Logical Row N`；可以不同：Partition、File、File size、File name、
Physical partition key、Parquet 文件数量。

---

## 4. 物理 Partition 可不同（Logical vs Physical Partition）

不同 Group 的物理目录结构**允许不同**：

```
index/            date=2026-08-17/
factor/alpha101/  year=2026/month=08/day=17/
fieldset/ma/      year=2026/month=08/
```

插件维护 **Logical Row Space → Physical Partition Resolver**，把
`WHERE date = '2026-08-17'` 分别解析到各 Group 的文件，最终映射到同一个
**Logical Row Range**。

---

## 5. Manifest（轻量，不做 Catalog DB）

目录本身就是 Catalog，文件发现用 Hive layout。每张 Logical Table **一个** `_table.json`
（**没有 `_group.json`、没有 part sidecar、没有 commit marker**，v2 契约）：

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
  "groups": ["index", "factor/alpha101", "factor/alpha191", "fieldset/ema",
             "fieldset/ma", "fieldset/qoq", "fieldset/ttm", "fieldset/yoy",
             "panel/cnstk_icday", "panel/cnstk_ixday", "panel/cnstk_klday"],
  "partitioning": {
    "index":          [{"template": "date=%Y-%m-%d", "source": "date"}],
    "factor/alpha101": [{"template": "year=%Y", "source": "date"},
                        {"template": "month=%Y-%m", "source": "date"},
                        {"template": "date=%Y-%m-%d", "source": "date"}]
  }
}
```

- `row_count` 只记账，**Reader 以 Parquet footer 汇总为准**（所有 Group 必须一致）。
- `last_txid`：最近成功提交的事务号（Writer/Compactor 每次 commit +1；无 marker 文件）。
- `partitioning`（可选）：`group → 有序目录模板`，模板只允许 `year=%Y` / `month=%Y-%m` /
  `date=%Y-%m-%d`，source 固定 `date`。**空表起步必须显式给出**；否则从目录结构推导
  （仅识别这三种段，未知 `name=value` 段忽略）；Writer 重写 manifest 必须原样写回。
- 行区间契约：同一 Group 的 part 按 (分区目录字符串序, part 序号数值序) 排序，
  start_row 由 footer 行数累加；跨 Group 总行数必须完全一致（否则 fail-fast）。

---

## 6. Row Group 对齐（比文件对齐更重要）

统一 `row_group_size = 131072`（可配）。不要求 `index/part-000 == alpha/part-000`，
但最好 `index RG17 == alpha RG17 == ma RG17` 表示同一 Logical Row Range：

```
RG17 → index vectors + alpha vectors + ma vectors → 同一个 DataChunk
```

**文件大小策略**：目标 256MB~512MB（本地 NVMe 可 128MB~512MB，对象存储 256MB~1GB）。
优先级：**Row Group alignment > File size**，不要为了凑文件大小破坏 Row Group。

---

## 7. Reader 架构

```
AlignedTableScan
└── AlignedScanState
    ├── Partition Resolver
    ├── Column Group Resolver
    ├── Parquet Scanner #1 / #2 / #3 ...
    └── Output Mapping
```

例：`SELECT close, alpha001, ma20` → 只打开 `index/`、`factor/alpha101/`、`fieldset/ma/` 三个 Group。

### 7.1 Projection Pushdown（第一优先级优化）
`SELECT alpha001` 时：Parquet 内只读 alpha001 一列；index/fieldset/panel 完全不打开。

### 7.2 Filter Pushdown
`WHERE date = '2026-08-17'` → Hive Partition Pruning → Parquet Row Group Pruning
→ Column Projection → Vectorized Scan。`WHERE symbol = '000001'` 可用 Parquet min/max stats 进一步裁剪。

### 7.3 DataChunk Assembly（插件最核心代码）
不是把几个表 concat，而是：**多个 Parquet Column Reader 直接填充同一个 DuckDB DataChunk**。
每列一个 Vector，不需要横向 materialize。

---

## 8. 最大程度复用 DuckDB

**不自己实现**：Parquet parser、compression、dictionary encoding、RLE、statistics、
filesystem、S3、HTTP range request。
**复用**：Parquet Reader、FileSystem、MultiFileReader、Parquet metadata、
Row Group statistics、Projection Pushdown、Filter Pushdown、Vectorized execution、Buffer Manager。

Extension 只负责：`Logical Table → Column Group resolution → Partition resolution
→ Aligned Scanner → DataChunk`。

---

## 9. Writer

```
AlignedTableWriter: Arrow RecordBatch → 按 Column Group 拆多个 Parquet Writer
                    所有 Writer 共享同一 Row Boundary
```

- 第一版 **Immutable + Append-only**：`part-001/002/003...`，新数据只追加，不改旧文件。
- **Atomic Commit**：先写 `_tmp/transaction-<id>/`，全部成功后 move 成正式 partition，
  再原子重写 `_table.json`（row_count 记账 + last_txid+1，partitioning 原样写回）；
  崩溃则丢弃 transaction（`_tmp/` 对 Reader 永不可见），扫描时跨 Group 行数
  fail-fast 兜底。无 sidecar、无 marker 文件。
- **Compaction**（后续）：后台合并 old parts → 新 part，保持 Canonical Row Space /
  Row Ordering / Row Group Boundary 不变，atomic switch，查询不中断。
- **Schema Evolution**（后续）：新列在老 partition 上读到 NULL，不重写历史。
- **Sparse**：第一版不自己实现，靠 Parquet RLE/Dictionary/Compression 天然压缩；
  未来极端 sparse 列再考虑独立 Group（如 `sparse/alpha999`）。
- **第一版不支持 UPDATE/DELETE**，不要提前做 Tombstone/Delta。

---

## 10. 并行执行

执行单位 = **Aligned Row Group**：

```
Task 1 → rows 0..131071      (index RG + alpha RG + ma RG → DataChunk)
Task 2 → rows 131072..262143
Task 3 → rows 262144..393215
```

每个 Task 组装 DataChunk 后交给 DuckDB execution pipeline，自然利用多核。

---

## 11. Metadata Cache

缓存 Parquet Footer、Schema、Row Group Metadata、Min/Max Statistics，用 LRU。
第一版**不缓存实际数据**。工作负载反复查 index/alpha101/ma，收益会很高。

---

## 12. 技术栈

- Query Engine：**DuckDB Extension**（SQL/Planner/Table Function/Execution/DataChunk/Parallelism）
- Storage：**Parquet**（Encoding/Compression/Statistics/Column Storage/Row Groups）
- Writer 第一版：**C++ / DuckDB / Arrow + Parquet**
  （先打通 extension execution integration，比引入 Rust FFI 更重要；
  未来 Writer/Compaction 复杂化再考虑 Rust Storage Engine）

---

## 13. Extension API

- 第一版：`SELECT * FROM aligned_table('cnstk_ixday');` 或
  `SELECT * FROM aligned_scan('/data', 'cnstk_ixday');`
- 最终目标：Catalog Integration，用户直接 `SELECT * FROM cnstk_ixday;`

---

## 14. 代码结构（位于 extension/aligned/ 内）

```
src/
├── extension.cpp
├── catalog/       logical_table.cpp  schema.cpp  manifest.cpp
├── resolver/      group_resolver.cpp  partition_resolver.cpp  row_space.cpp
├── scan/          aligned_scan.cpp  aligned_scan_state.cpp  group_scan.cpp  scheduler.cpp
├── parquet/       duckdb_parquet_adapter.cpp
├── writer/        aligned_writer.cpp  group_writer.cpp  transaction.cpp
├── compaction/    compactor.cpp
└── optimizer/     projection.cpp  filter.cpp
```

---

## 15. 开发阶段

| Phase | 内容 | 验收标准 |
|-------|------|----------|
| 0 | **Storage Contract**：Logical Table / Canonical Key / Row Ordering / Row Group / Partition / Column Group / Schema Evolution 全部写死，尤其定义**什么叫 row N 对齐** | docs/STORAGE_CONTRACT.md |
| 1 | **Read MVP**：`aligned_table()` 跑通，支持 Logical Table + Column Group + Parquet + Row alignment + DataChunk，不做复杂优化 | `SELECT * FROM aligned_table('cnstk_ixday')` |
| 2 | **Projection**：只扫描被选中的 Group | `SELECT close, alpha001, ma20` 只开 3 个 Group |
| 3 | **Partition / Predicate Pushdown**：Hive pruning + Parquet stats + RG pruning + projection | 有实用价值 |
| 4 | **Parallel Scan**：Aligned Row Group 为任务单元，多核 | 多核利用率 |
| 5 | **Writer**：RecordBatch → Column Group split → Aligned Parquet Writer → Atomic Commit（append-only） | 写入+读取闭环 |
| 6 | **Benchmark**：10^8/10^9 rows × 1K/10K/50K cols；查 5/100/1K/10K 列；1%/10%/100% scan；并发 1/4/16/32/64；对比普通 Parquet / JOIN / DuckDB 宽表 | 报告 |
| 7 | **Compaction / Evolution**：最后再做，不要提前 | — |

---

## 16. 环境与构建（Windows + Linux）

> 项目在 Windows（PowerShell + scoop + MSVC）与 Linux（brew + gcc + Ninja）两条
> 开发链路上均可构建。源码、扩展代码、`scripts/` 共享同一套逻辑；仅环境/构建命令、
> 测试脚本后缀（`.ps1` vs `.sh`）不同。

### 16.1 Windows（scoop + MSVC）

#### 已确认环境（2026-08）
- scoop（buckets: main/extras/versions/nerf-fonts/java/dbx），aria2 加速可用
- 已装：`git 2.44.0`、`cmake 4.0.3`、`duckdb 1.5.4`(CLI)、`python310`、`vcredist2022`、`7zip`
- **MSVC**：VS2022 BuildTools 装在 `C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools`
  - `vcvars64.bat` → `...\BuildTools\VC\Auxiliary\Build\vcvars64.bat`
  - cl.exe 版本 14.44.35207（MSVC 14.44）
- **ninja**：`scoop install ninja`（2026-08 安装中/已完成）
- 目标 DuckDB 源码版本：**v1.5.4**（与 CLI 严格一致，extension ABI 版本锁定）

#### 构建命令（MSVC 环境必须在 PATH 里）
PowerShell 里通过 cmd 导入 vcvars64 再跑 cmake：

```powershell
cmd /c "call ""C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"" && cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release ..."
```

测试用 `duckdb -unsigned -c "LOAD 'build/release/extension/aligned/aligned.duckdb_extension'; ..."`
（自定义扩展需 `-unsigned`；或直接用同源码树构建出的 duckdb CLI，避免 platform 校验问题）。

### 16.2 Linux（brew + gcc + Ninja）

#### 已确认环境（2026-08）
- **DuckDB v1.5.4 CLI**：`/home/windsing/.duckdb/cli/1.5.4/duckdb`（已加入 `~/.bashrc` PATH，验证为 v1.5.4 Variegata）
- 工具链：cmake 3.28、gcc/g++ 13.3、git 2.43、python3、`brew install ninja`（ninja 1.13.2）
- **DuckDB v1.5.4 源码**：从 gitee 镜像 clone 到 `/home/windsing/proj/factorlake/duckdb/`
  （v1.5.4，commit `08e34c4`，与 CLI 一致，gitignored）

#### 构建命令
扩展通过 `scripts/aligned_extension_config.cmake` 注册进 DuckDB 构建：

```bash
cd /home/windsing/proj/factorlake/duckdb
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DDUCKDB_EXTENSION_CONFIGS=/home/windsing/proj/factorlake/scripts/aligned_extension_config.cmake
ninja -C build duckdb
```

改完单个源文件后增量重编（只重编该文件并链接）：

```bash
cd /home/windsing/proj/factorlake/duckdb
rm -f build/extension/aligned/CMakeFiles/aligned_extension.dir/src/scan/aligned_scan.cpp.o
ninja -C build duckdb
```

产物：`duckdb/build/duckdb`（已静态链接 aligned 扩展，可直接运行，无需 `-unsigned`）。

#### 测试数据与验收脚本（bash 版）
- `scripts/gen_testdata.sh` — 生成测试数据到 `testdata/`（布局与 PS 版一致，6000 行）
- `scripts/test_aligned.sh` — `test_aligned.ps1` 的 bash 版，**28/28 PASS**（含 Phase 3 分区剪枝断言）
- 共享 helper：`scripts/lib_aligned.sh`（环境变量 `DUCKDB` / `ALIGNED_DATA_ROOT` 可覆盖）

```bash
cd /home/windsing/proj/factorlake
bash scripts/gen_testdata.sh    # 首次或需要重建数据时
bash scripts/test_aligned.sh
```

> 注：`duckdb/` 为 gitignored（vendored 源码，重新拉取见 §16.2）；测试数据 `testdata/` 同样 gitignored。
> 运行示例 SQL 时用 `SET aligned_data_root='<abs-path>/testdata';`（见 §17 各 Phase 的基线命令）。

#### 多场景基准（Linux，Phase 6+）
- Group Settings 事实来源：`bench/multi_bench_config.sh`（引擎组 D-WIDE/D-JOIN/
  P-CONCAT/P-JOIN/A-ALIGNED/A-NORMAL、规模 R1..R4×W1..W3、稀疏、Q1..Q5、F1..F5、
  S0..S3、环境、Tier A/B/C 组合）
- 参数化数据生成：`scripts/gen_multi_bench.sh`（`--rows --width --sparsity --aligned`
  `--out --tag`；所有引擎读同一逻辑源，保证 key/顺序/NULL 一致；写 `.gen-meta` 标记）
- **一键递增基准**：`scripts/bench_scenarios.sh`（g-250k→g-1m→g-1m-q→g-10m→g-w2→
  g-sparse→g-thread，逐级递增 + 内存监控 + 每阶段保存 `bench/out/g-*.csv` + SUMMARY）
- 低层 runner：`scripts/run_multi_bench.sh`（`REPEATS` env 均值、`QS_OVERRIDE` 等
  覆盖查询/过滤/选择率、`SELF-CHECK` 分区剪枝 + `CONSISTENCY` 跨引擎行数校验）
- **polars 引擎（P-CONCAT/P-JOIN）需 polars**：`uv venv --python 3.13 .venv-bench` +
  `uv pip install --python .venv-bench/bin/python polars`；P-CONCAT 用
  `DataFrame.hstack` 做位置对齐横向拼接（见 §Phase 6 经验 4）。缺失时 P-* 自动跳过。
- 一键复现 & 结论见 `docs/BENCHMARK_MULTI_ANALYSIS.md`（权威结论文档）。


---

## 17. 当前进度

- [x] 完整 Plan 定稿（本文件）
- [x] scoop 安装 ninja（1.13.2）
- [x] duckdb v1.5.4 源码 clone 到 `duckdb/`（vendored，gitignored；重新拉取命令见 §16）
- [x] Phase 0：`docs/STORAGE_CONTRACT.md` 定稿（v1.1：commit marker 含 part 名单，见 §9）
- [x] **Phase 1 Read MVP 完成（2026-08）**：`aligned_table()` / `aligned_scan()` 跑通
  - 多 Group 并行读取、RG 窗口调度、跨 part/RG 行窗口、Schema Evolution（缺失列补 NULL）、
    跨 Group 重复列遮蔽、Row Space 校验、commit marker 过滤
  - 验收：`scripts/test_aligned.ps1` 11/11 PASS（6000 行 × 3 组、对齐断言 misaligned=0、
    边界行、错误场景）
  - 构建产物：`duckdb/build/duckdb_aligned.exe`（shell 输出名已改为可配置 `DUCKDB_SHELL_OUTPUT_NAME`，
    原因见下）
- [x] **Phase 2 Projection Pushdown 完成（2026-08）**：
  - `projection_pushdown = true`；bind 仍返回全量 schema；`init_global` 消费 `input.column_ids`
    构建 全量列→投影输出位 的映射；扫描只填被请求列、只开被请求 Group
  - **实测验证**：`SELECT rowid_alpha` 只打开 alpha101 的 2 个 part（index/ma 零打开）；
    `SELECT date, ma20` 只打开 index+ma（alpha101 零打开）
  - `count(*)`（空 column_ids）走 0 向量 chunk、只报 cardinality 的路径 ✓
  - 验收：`scripts/test_aligned.ps1` **14/14 PASS**
- [x] `scripts/gen_testdata.ps1` 测试数据生成器
- [x] git 仓库（提交历史见 git log）
- [x] **Phase 3 Partition / Predicate Pushdown 完成（2026-08）**：
  - Partition pruning：`PrunePartsByFilter`（等值走目录路径；范围走 part 路径反向重建 part 日期，
    有界、不迭代日期）
  - Row Group stats pruning：`ComputeRowGroupWindow` 用 parquet 列统计 `CheckStatistics`，
    FILTER_ALWAYS_FALSE/FALSE_OR_NULL 的 RG 不进扫描窗口、整段 NULL 填充（行级 filter 必拒）
  - Row-level filter：`TableFilterState::Initialize` + `ColumnSegment::FilterSelection` 链式
    （尊重传入 selection，支持 AND 组合），经 scratch chunk 组装/Slice 后 `Reference` 输出
  - §3.1/§4 用户约束全部落地：index 必须存在（§2.1b）、非 index 必须两级目录 lv1/lv2（§2.1c）、
    路径段以 `.`/`_` 开头一律忽略（§2.1d）、重复列规则（§2.2e：与 index 重复→忽略非 index 副本；
    非 index 组间重复→仅限定名 `lv1.lv2.col`；裸名→not found）
  - 验收：`scripts/test_aligned.ps1` **21/21 PASS**（新增 §2.1b/c、§2.2e1/e2/e3 自动化测试）
- [x] **Phase 4 Parallel Scan + Metadata Cache 完成（2026-08）**：
  - **并行扫描**：共享游标（mutex 保护 `{interval_idx, next_row}`）按**连续 Range**（16 chunks =
    32768 行）发放给各 pipeline 线程；线程在自身 Range 内顺序扫描（窗口复用零重定位），
    Range 边界才重新定位（InitializeScan + discard 到目标行）
  - 行级 filter state 改为**每线程一份**（全局只存 filter 定义；FilterSelection 可能改 state）
  - **元数据缓存**：复用 DuckDB ObjectCache（LRU，8GiB，带 IsValid 校验）——把
    `parquet_metadata_cache` 默认置 ON（`DBConfig::SetOptionByName`），footer/schema/RG stats
    跨查询跨线程共享
  - 验收：`scripts/test_aligned.ps1` 21/21 + `scripts/test_parallel.ps1` 全 PASS；
    bench_ixday 1M 行 × 127 列：1 线程 0.89s → 8 线程 0.21s（**≈4.2× 加速**）
- [x] **Phase 5 Writer 完成（2026-08）**：
  - `aligned_write(table, source_path, mapping, root=..., start_row=...)` 表函数：
    mapping `"group:col1,col2;group2:col3"` 指定每个 group 的写入列（文件列序）
  - 逐 chunk 读源 parquet；每 group 按**分区值变化**切行段（字典编码列必须走
    ToUnifiedFormat！），组装切片 DataChunk → `ColumnDataCollection` 缓冲 →
    `ParquetWriter`（ZSTD，按 group manifest RGS flush）
  - 分区目录由 manifest templates 对源 DATE 列求值（组间分区粒度可不同）；
    part 名 = 目标目录下一个空闲 `part-%06llu`
  - **原子提交**：全部写入 `<table>/_tmp/transaction-<txid>/`，成功后逐 part
    move 到目标目录 + 写 sidecar + 更新 `.aligned-commit.json`（临时名+rename），
    最后 bump `_table.json`/`_group.json` row_count；失败删除暂存树
  - 写前模拟 ValidateRowSpace（所有 group 必须覆盖追加区间——对齐契约强制全组写入）
  - 验收：`scripts/test_writer.ps1` 全 PASS（空表首写 → 追加 → alpha999 进化列 →
    粗分区共享目录追加 part → 读回 6000 行全对 + 错误路径）
- [x] **Phase 6 Benchmark 完成（2026-08）**：
  - `bench_aligned.ps1`（aligned / wide / join 三种 DuckDB 引擎）+ `bench_polars.py`
    （polars 按行位置横向 concat——传统宽表装配路径，正是本引擎要消灭的）
  - 维度：投影 5/25/120 列 × 扫描 25%/100% × 线程 1/4/8；p5 正确性跨引擎交叉校验
  - 数据：bench_ixday 1M 行 × 127 列；结果见 `docs/BENCHMARK.md`、`scripts/bench_output.csv`
- [x] **Phase 7 Compaction / Evolution 完成（2026-08）**：
  - `aligned_compact(table, group)`：按分区目录合并多 part → 单 part（同目录必须同列集，
    拒绝 schema-evolution 合并）→ 暂存 → move+sidecar → 替换 commit marker（临时名+rename）→
    删旧文件；失败清理 `_tmp`
  - 验收：`test_compaction.ps1` 全 PASS（2→1 part，前后 count/sum/misalign 全对，index 未动）
- [x] **新增需求：元数据 `aligned` 开关（2026-08，已随 v2 契约回滚）**：
  - 曾加入 `_table.json.aligned`(bool) 与 UnionIntervals 并集剪枝路径；
    **v2 契约已删除该语义**：读端忽略 `aligned` 字段、`UnionIntervals`/aligned=false
    分支从代码删除，固定为相交剪枝（`IntersectIntervals`）——见"v2 契约收尾"条目
  - 历史验收：`test_aligned_flag.ps1` 当时全 PASS（该脚本已删除）
- [x] **v2 契约收尾：20 步改造全部落地（2026-08）**：
  - **删除**：`_group.json`、part sidecar（`*.aligned.json`）、commit marker
    （`.aligned-commit.json`）、`aligned` 字段语义（固定 true）；`aligned_scan.cpp`
    删除 `UnionIntervals` 与 aligned=false 分支；`OpenPartReader` 删 sidecar 列序校验
  - **`_table.json` 升级**：新增 `last_txid`（事务记录，每次 commit +1）、可选
    `partitioning` map（`group → [{template, source}]`，模板仅 `year=%Y`/`month=%Y-%m`/
    `date=%Y-%m-%d`，source 固定 `date`，空表起步必须显式，writer 重写必须原样写回）；
    `row_count` 只记账、Reader 以 footer 为准
  - **行区间**：part 按 (分区目录字符串序, part 序号数值序) 排序，start_row 由 footer
    行数全局累加；跨 Group 总行数必须完全一致（fail-fast）——`BuildTablePlan` 重写
  - **目录推导**：`DerivePartitioningFromPaths` 从目录推导分区（仅识别 year/month/date
    段，格式冲突报错，未知 `name=value` 段忽略）
  - **Writer/Compactor**：txid = `last_txid+1`；commit 段干净（move + 重写 `_table.json`）；
    rgs 用 `_table.json.row_group_size`（兜底 131072）
  - **验收**：test_aligned.ps1 25 项、test_writer.ps1 17 项、test_compaction.ps1 17 项、
    test_parallel.ps1 全 PASS（1M 行 bench 数据，8 线程 0.055s vs 1 线程 0.12s）；
    bench 生成器 gen_bench.ps1/sh、gen_multi_bench.sh 已去除旧契约产物（`day=`→`date=`）；
    清理 build2/build-debug 临时构建目录；`docs/STORAGE_CONTRACT.md` 重写为 v2
  - 注意：A-NORMAL 引擎（run_multi_bench.sh 翻转 aligned 字段的兄弟表）行为现与
    A-ALIGNED 相同，仅保留作历史对照
- [x] **Linux 迁移 + 迁移暴露的两个真实 Bug 修复（2026-08）**：
  - Linux 环境/构建链路见 §16.2：gitee clone `duckdb/`（v1.5.4）、`cmake -G Ninja` +
    `scripts/aligned_extension_config.cmake` 注册扩展、产物 `duckdb/build/duckdb`
  - bash 版脚本 `scripts/{gen_testdata,lib_aligned,test_aligned}.sh`（`test_aligned.sh` **28/28 PASS**，见上方新增 3 个分区剪枝断言）
  - **Bug A（整块被过滤后误判扫描结束）**：`AlignedScanFunction` 改为内部 `while(true)`
    跳过被 row filter 全部拒绝的空 chunk，直到产出非空 chunk 或真正耗尽才返回 0 行；
    否则 DuckDB 把 0 行 chunk 当扫描结束中断后续 chunk（`WHERE rowid>=2048` 曾 0 行，应 3952）
  - **Bug B（stats-skipped 行组的 `rg_skip` 只记录部分区间）**：`ComputeRowGroupWindow`
    被跳过的 RG 由"当前 wanted 范围的重叠段"改记为**整 RG 范围**
    （`rg_skip.emplace_back(rg_start - window_start, rg.count)`），避免下一块想要同 RG
    其它部分时 `rg_skip` 未覆盖 → `read_need>0` 访问 NULL `scan_state`/`chunk` 崩溃
    （`WHERE rowid=2048` 投影 symbol 曾崩溃，现返回 `002049`）
  - 清除了 `aligned_scan.cpp` 内全部 `getenv("ALIGNED_DEBUG"/"ALIGNED_TRACE_INIT")` 调试打印
    与 Windows 写死路径 `D:/proj/factorlake/...`（`init_trace.txt`/`filter_trace.txt`）
- [x] **Phase 3 分区剪枝游标 Bug（非首分区崩溃）修复 + Linux 基准工具（2026-08）**：
  - **Bug C（共享游标起点未对齐剪枝区间）**：`AlignedScanGlobalState::next_row` 初值 0，
    但剪枝后首个 active interval 起点可 >0（如按日期过滤只保留第 2/3/4 个分区）。
    首次 claim 用 `range_next = next_row = 0` 去扫起始行 >0 的 part →
    `local_start = cursor - part.start_row` 无符号下溢 → 抛巨大行号（≈ -分区起点），
    真实症状：`WHERE date` 命中**非首分区**的任何查询都联错
    （`count(*)`/`count(col)` 报 "no row groups cover rows [184467440737XXX,...)"）。
  - **修复**：claim 时把 `next_row` 钳到当前 interval 起点
    （`if (next_row < interval_start) next_row = interval_start`）；首分区（start=0）因
    与初值 0 重合而未暴露。验证：testdata/b 的数据第 2/3/4 分区均正确返回。
  - **验收**：`scripts/test_aligned.sh` 新增 3 个分区剪枝断言（day18->rows[3000,6000)、
    剪枝+行滤错分区=0、匹配分区=1），**28/28 PASS**。
  - **Linux 基准工具**：`scripts/gen_bench.sh`（gen_bench.ps1 的 bash 版，`-n` 或位置参数
    控制行数）+ `scripts/bench_aligned.sh`（aligned/wide/join × p5/p25/p100/s25/s100 ×
    线程 1/4/8，含**非首分区剪枝自检**：断言 date='2026-09-02' 返回 rowsPerDay）。
    修复了 `alpha*()/ma20()` 末指令 `[ ... ] && printf` 在 `set -e` 下让命令替换返回非零导致
    脚本秒退的问题；计时改用整数纳秒避免偶发负值。
  - **Phase 6 结论修正**：此前"分区剪枝完全失效（s25≈s100）"的判断不成立——剪枝在首分区一直
    生效，只是**非首分区直接崩溃**、且小数据全缓存下固定开销掩盖剪枝收益。修复后实测
    (400K, aligned) s25(1/4 扫描) threads=1 ≈ 0.06s vs s100(全扫) ≈ 0.14-0.21s，**≈2-3× 收益**；
    aligned 相对 wide/join 的优势需在更大/冷缓存数据上才显现（见 docs/BENCHMARK.md）。
- [x] **Phase 6+ 多场景基准 + 一键复现框架（2026-08）**：
  - **Group Settings 事实来源** `bench/multi_bench_config.sh`（6 引擎组 D-WIDE/D-JOIN/
    P-CONCAT/P-JOIN/A-ALIGNED/A-NORMAL、R1..R4×W1..W3、DENSE/SPARSE-90/99、Q1..Q5、
    F1..F5、S0..S3、COLD/WARM、线程/文件数/分区、Tier A/B/C）
  - **参数化生成器** `scripts/gen_multi_bench.sh`（`--rows/--width/--sparsity/--aligned`，
    所有引擎读同一逻辑源保证一致；写 `.gen-meta` 供 `--skip-regen` 校验规模）
  - **一键递增基准** `scripts/bench_scenarios.sh`（g-250k→g-1m→g-1m-q→g-10m→g-w2→
    g-sparse→g-thread，逐级递增 + 内存监控 + bench/out/g-*.csv + SUMMARY）
  - 低层 runner `scripts/run_multi_bench.sh`：`REPEATS` 同进程均值、`QS/FS/SS_OVERRIDE`
    覆盖查询/过滤/选择率、`SELF-CHECK` 分区剪枝 + `CONSISTENCY` 跨引擎行数校验
  - polars 引擎 `scripts/bench_polars_multi.py`（P-CONCAT 用 `DataFrame.hstack`、
    P-JOIN 用 rowid hash join），经 `uv venv .venv-bench`（py3.13+polars）运行
  - **实测结论**（详见 `docs/BENCHMARK_MULTI_ANALYSIS.md`）：A-ALIGNED 相对 D-JOIN 在
    1M×128≈5×、10M×128≈40×，相对 P-JOIN 10M≈152×（position 组装近似线性 vs JOIN/hstack
    超线性）；A-ALIGNED≈A-NORMAL（差异是测量噪声）；D-WIDE 在窄表单 reader 仍最快、
    随列数(W2 1024)→1.4×收窄；并行小数据~1.6×。
  - **测试驱动又修 4 个 bug**：A-NORMAL 兄弟表重建、`.gen-meta` 规模校验、超宽 SQL
    stdin 绕 ARG_MAX、`$TMPDIR` 未设置秒崩（`${TMPDIR:-/tmp}`）。
- [x] **master vs alpha（v1 vs v2 契约）同机基准对比（2026-08）**：
  - 两分支分别编译、同一 `bench_aligned.ps1`、各自格式数据（v1：sidecar/marker/`day=`；
    v2：footer/`_table.json`/`date=`）各跑一轮；**对照引擎（wide/join/polars）两轮数值几乎
    完全一致** → 环境稳定、对比可信
  - **结论：v2 全 workload 无回归且小幅更快（1%~10%）**：p5 t1 0.176→0.163、p100 t1
    2.008→1.986、s100 t1 0.522→0.511、s100 t8 0.215→0.203；原因 = 计划构建免去
    sidecar/marker 读取、footer 元数据被 metadata cache 吸收。对比表已写入
    `docs/BENCHMARK.md`
  - **关键教训：git checkout 切分支后 ninja 增量重编产物会损坏**（实测症状：扩展查询
    崩溃，WER 偏移 0x3fa9=PE 头区域 = 跳转到坏地址；`SELECT 1` 正常；旧 exe 读同一数据
    正常 → 排除数据/源码问题）。**每次切换分支后必须删扩展 obj 全量重编**：
    `Remove-Item build3/extension/aligned/CMakeFiles/aligned_extension.dir -Recurse -Filter '*.obj'`
    再 `ninja -C build3 duckdb_al3.exe`（12:50 增量链接连崩 3/3 → 全量重编后 3/3 稳定）
  - 排查路径备忘：`Get-WinEvent -FilterHashtable @{LogName='Application'; Id=1000}` 看崩溃
    偏移；dumpbin `/headers` 查段表判断偏移属哪个段；obj 内容特征字符串比对（master 版
    "sidecar declares" vs alpha 版 "last_txid"）确认混编

### Phase 1 关键经验（必须记住，避免重踩）：`Copy(source, target, source_count, source_offset, target_offset)`
   中 `source_count` 是**排他结束下标**，拷贝行数 = `source_count - source_offset`。
   传"行数"会在 `source_offset > count` 时下溢 → 字典向量 selection 越界读 → 崩溃。
   正确调用：`Copy(src, dst, src_offset + copy_count, src_offset, dst_offset)`。
2. **Parquet 字符串/字典是零拷贝**：`DictionaryDecoder::Read` 在 `result_offset==0` 时把结果向量
   做成 DICTIONARY_VECTOR（`Vector::Dictionary`），引用解码器内部的可复用字典。**每个 RG 窗口必须用
   全新的 `ParquetReaderScanState` 和 `DataChunk`**（勿跨窗口复用；复用会在第二次数据读取时 UAF）。
3. **1.5 的 `table_function_t` 返回 void**，用 `output.SetCardinality(0)` 表示结束。
4. **`ParquetReader::Scan` 每次切换 Row Group 会先返回一个空 chunk（setup call）**，不算结束；
   只有 `AsyncResultType::FINISHED/BLOCKED` 才是结束。
5. **`TableFunctionInput::bind_data` 是 `optional_ptr<const FunctionData>`**——扫描回调里对 bind data
   的引用是 const 的。
6. **C++11 标准**：`string::data()` 返回 `const char*`，写缓冲用 `&s[0]`。
7. **扩展的 include 路径要自己加**：`DUCKDB_EXTENSION_*_INCLUDE_PATH` 只是元数据，
   必须在扩展 CMakeLists 里 `include_directories` 自己的 `src/include` + parquet 头文件目录。
8. **调试经历**（2026-08）：UAF 崩溃排查用 SetUnhandledExceptionFilter + 寄存器转储 +
   `dumpbin /disasm` 定位（RVA 0x17236B = ValidityMask 位测试）。崩溃进程会锁住 duckdb.exe
   （taskkill 拒绝）→ shell 输出名改为 `duckdb_aligned`。
9. **测试数据里的 `rowid`/`rowid_alpha`/`rowid_ma` 是对齐断言列**（每 Group 独立命名，
   值 = 全局行号），`misaligned = 0` 即证明跨 Group 对齐。

### Phase 2 关键经验

1. **1.5 投影下推模型**：bind 永远返回全量 schema（bind 输入没有 column_ids）；
   `init_global`/`init_local` 的 `TableFunctionInitInput::column_ids` 才是被请求列
   （全量 schema 下标子集）。输出 chunk 向量数 = `column_ids.size()`（executor 按
   `op.types` = 投影后类型初始化）。`column_ids` 为空 = 无列请求（如 count(*)）→
   扫描只设 cardinality 不填向量。`COLUMN_IDENTIFIER_ROW_ID`（= -1）是虚拟列，不支持。
2. **全量列→投影位映射**放在 `AlignedScanGlobalState`（init_global 构建一次）：
   `projected_pos[full_id] = chunk 位置`；Group 级 `group_active` 决定是否打开。
   OpenPart 只读被请求列（`column_ids` 传给 ParquetReader 实现文件内列投影）。

### Phase 3 关键经验

1. **`ScanGroupWindow` 的 src_offset 计算（最隐蔽的数据错误，2026-08）**：
   `src_offset = copy_from - win_pos` **只在窗口起点 == RG 起点时正确**（flow_off == rg_off）。
   窗口从 RG 中部开始（flow_off > 0，如 chunk 2 请求 index day-18 part-000000 的 local
   [1096,2048)，RG 是 [0,2048)）时，必须用：
   `src_offset = seg.flow_off + (copy_from - seg.win_start) - rg_off`
   （窗口行 w ↔ RG 行 flow_off + (w - win_start) ↔ chunk 行 = 该值 - rg_off）。
   错误症状极具迷惑性：`count(rowid)` 显示 6000（被 count(*) 优化掩盖）、
   `misaligned = 0`（index 与 alpha 被**同样方式**破坏 → 互相"对齐"）、
   只有 count(alpha001)/count(alpha099) 各差 +1（952 行错位恰好各多 1 个命中）。
   诊断方法：`sum(rowid)`（期望 17,997,000）+ `row_number() OVER ()` 定位错位区间；
   **不要**依赖 count(rowid)/count(*)（DuckDB 会用 Cardinality 统计优化掉扫描）。
2. **DuckDB COPY TO PARQUET 的 ROW_GROUP_SIZE 不精确**：请求 1000 实际写出
   [0,2048)+[2048,3000)（writer 按 vector 大小 2048 flush）；分析 RG 布局以
   `parquet_metadata` 实测为准，别信 manifest 声明。
3. **PS 5.1 `-c` 传参无法携带引号标识符**：`"` 和 `` ` `` 都会被 mangle（DuckDB 1.5.4
   也不支持反引号标识符）。测试脚本里带引号标识符的 SQL 走临时文件 +
   `cmd /c "exe -csv -noheader < file"`（`Run-DuckDB-File`）。
4. **行级 filter 路径**：`TableFilterState::Initialize(context, filter)` 一次；
   `ColumnSegment::FilterSelection(sel, vec, vdata, filter, state, scan_count, approved)`
   尊重传入 selection 且 `scan_count` 是当前 selection 长度（链式 AND 直接传 approved）。

### Phase 4 关键经验

1. **表函数默认单线程**：`GlobalTableFunctionState::MaxThreads()` 默认返回 1 ——
   不 override 的话 `Pipeline::ScheduleParallel` 永远走 `ScheduleSequentialTask`，
   线程数再多也只跑 1 个任务。返回 `GlobalTableFunctionState::MAX_THREADS` 即放开并行。
2. **parquet 流按整向量（2048 行）前进，与"实际放置的行数"解耦**：RG 窗口复用
   必须满足"流位置 == 期望起点"（`local_start - rg_window_start == 流下一行 win 坐标`），
   否则行会丢失（下一个 chunk 从向量边界开始，缺中间行 → "scan ended early"）。
   不连续时重新 InitializeScan 并 discard 到目标行。旧代码靠 `parquet_pos >= plan_rows`
   在窗口尾"过冲"触发重算才没炸 —— 只对 RG ≤ 2048（单向量）的数据成立。
3. **copy 重叠必须双向 clamp**：`copy_from = max(w_start, win_pos)`、
   `copy_to = min(w_end, win_pos + valid_len)`；`copy_from >= copy_to` 即丢弃。
   只 clamp 一端会让 `copy_count = min(...) - copy_from` 下溢成巨大数 → 越界写 → 堆损坏
   （症状非确定：有时 0xC0000374 有时 0xC0000005，Debug 下变死循环）。
4. **并行 claim 粒度必须是"连续 Range"而非单 chunk**：每 chunk 一个 claim 时，线程间的
   claim 不连续 → 每次都要重定位（InitializeScan + discard 重读 RG）→ 并行开销吃掉全部
   收益（实测 8 线程无加速反而 user 时间翻倍）。按 16 chunks（32768 行）发 Range，
   线程在 Range 内顺序消费 → 8 线程 4.2×。
5. **诊断流程**：Release 堆损坏 + Debug 死循环 → 在 scan 循环里打每迭代
   `cursor/placed/need/segpos/ppos/async/chunk_rows`，一眼定位"placed=0 但 ppos 爬升"=
   向量被全部丢弃、范围不重叠。Windows 无调试器时这是最快路径。
6. **性能测量用 `.timer on`（文件输入）**，`-c` 的进程启动开销 ~0.8s 会淹没扫描时间；
   EXPLAIN ANALYZE 的算子耗时是可信的（不含进程启动）。

### Phase 5 关键经验（Writer）

1. **parquet 读取的分区列可能是 DICTIONARY_VECTOR**（常量/低基数列必然字典化）：
   `FlatVector::GetData` 直接读字典数据数组 → 行 1 起全是越界垃圾（实测读到
   1970-01-01）。必须 `vec.ToUnifiedFormat(rows, vdata)` +
   `vdata.sel->get_index(r)` 取值。
2. **`FileFlags::FILE_FLAGS_FILE_CREATE` 不截断已有文件**：重写 `_table.json`/
   `_group.json`/commit marker 时旧内容尾部残留 → JSON 损坏。打开后先
   `handle->Truncate(0)`。注意 flag 名是 `FILE_FLAGS_FILE_CREATE`（不是 CREATE）。
3. **`ParquetWriter` 无 WriteChunk**：缓冲用 `ColumnDataCollection` +
   `InitializeAppend/Append`，满 `row_group_size` 时 `writer->Flush(buffer,
   transform)` + `buffer.Reset()` + `InitializeAppend`，结束 `Finalize()`；
   `ColumnDataAppendState`（不是 CollectionAppendState）；构造参数抄
   parquet_extension.cpp 的默认值（ZSTD、ChildFieldIDs()、ShreddingType()、
   dict limit 1GiB、bloom on）。
4. **原子提交**：先全部写 `<table>/_tmp/transaction-<txid>/`，commit 时
   `fs.MoveFile`（同卷原子）+ sidecar + marker（临时名写 + rename）；
   `fs.RemoveDirectory` 递归删除但留下空的父 `_tmp`，需再删一次（best-effort）。
5. **写前模拟校验**：用占位 PartInfo 扩展 parts 列表后调 `ValidateRowSpace`，
   自动强制"每个 group 都必须覆盖追加区间"（对齐契约），漏写 group 直接报错。

### Phase 6 关键经验（性能 + Benchmark）

1. **窗口复用必须靠 carry，不能靠精确流位置匹配**：parquet 流按整向量（2048 行）
   前进，与"实际放置行数"解耦。窗口起始于非向量边界时，最后一个向量必然 over-read；
   丢弃多余行会让下一个 chunk 期望起点 ≠ 流位置 → 每次都要重新 `InitializeScan`
   （CreateReader，~1.3ms）→ 1 列全表 373 次 recompute ≈ 500ms。
   **修正：把 over-read 的多余行深拷贝进独立 `carry_chunk`**（自己数据，不随 g.chunk
   被覆盖），下一个 chunk 先 drain carry 再继续读流 → recompute 373→16，1 列
   count 522→115ms。carry 必须存独立 buffer，不能指望 g.chunk 保留（read 循环会
   再调 Scan 覆盖它）。
2. **这次诊断流程**：先 `sum(rowid)`/`count(col)` 定位数据错误，再数 recompute
   （373）确认热点，再修 carry；顺手发现 exe 被 zombie 进程锁住 → 用 build2/build3
   全新输出名绕过编译锁（zombie 无法 taskkill，"Access is denied"）。
3. **Benchmark 测量要确定性**：`.timer` 走 `cmd` 管道 + `2>&1` 时 `real` 行顺序
   不稳定（会把 SET 的 0.001 当成冷启动）；改为 Stopwatch 包 fresh-process 单次
   执行（跑 2 次取 warm）最稳。PS `$M` hashtable 键在脚本内丢失（scope 怪异）→
   改为执行时就往 `$all` 列表 append，报告/CSV 全从 `$all` 生成，避免键查找。
4. **polars 横向 concat 基线**：用 **`df.hstack(...)`（DataFrame.hstack，polars 1.x，
   接受 DataFrame 或 Series）** 做纯位置对齐横向拼接，语义最准确（P-CONCAT 引擎，
   见 `scripts/bench_polars_multi.py`）。`pl.concat([..], how="horizontal")` 虽结果相近，
   但 `hstack` 才是明确"按物理行位置拼、不靠 key"的写法，正是本引擎要消灭的路径。

### Phase 7 关键经验（Compaction）

1. **同分区目录才能合并**：schema-evolution 在目录内（不同 part 列集不同）无法
   合并（合并后的列集不统一）→ 直接报错拒绝；跨目录（la分区）各自独立合并。
2. **原子切换**：新 part 先写暂存 → move 进目录 + sidecar → 写 `.aligned-commit.json`
   （临时名+rename）→ 最后删旧文件。marker 切换后旧文件已不可见，删除失败只留
   orphan（下次 compact 清理）。
3. **TEMP/DEBUG 不要碰主路径的 assert**：`FastRerun` 外，读写用 file 重定向
   `cmd /c "exe < in > out 2>err"`，别用 PS `& exe <`（PS 不支持 `<`）。

### 新增需求（aligned 开关）关键经验

1. `_table.json` 的 `aligned` 字段曾是**叶子间剪枝是否可统一传播**的开关：
   true → 各 leaf kept-part 区间**相交**（`IntersectIntervals`，全局一个扫描区间）；
   false → **并集**（`UnionIntervals`）。**v2 契约已删除该开关**：读端忽略 `aligned`
   字段，固定为相交剪枝（`IntersectIntervals`）；`UnionIntervals` 与 aligned=false
   分支已从代码删除。yyjson 判布尔用 `yyjson_is_bool`/`yyjson_get_bool`。
2. writer 重写 `_table.json` 时务必保留 `partitioning` 字段，否则显式分区配置
   丢失（退化为目录推导，可能改变未来写入的目录布局）。

> 规则：每完成一项，把本节的 `[ ]` 改为 `[x]` 并记录日期/要点；
> 新决策必须写回对应小节，禁止只存在于对话里。
