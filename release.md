# release.md — FactorLake / AlignedTable 开发日志

> 本文件记录所有开发进度、契约演进、bug 修复历史。从 AGENTS.md 剥离，保持
> AGENTS.md 只含当前架构契约。

---

## 2026-08 完成项

### Phase 0–7

- **Phase 0**：`docs/STORAGE_CONTRACT.md` 定稿（v1.1）。
- **Phase 1 Read MVP**：`aligned_table()` / `aligned_scan()` 跑通。多 Group 并行
  读取、RG 窗口调度、跨 part/RG 行窗口、Schema Evolution（缺失列补 NULL）、
  跨 Group 重复列遮蔽、Row Space 校验。验收：test_aligned.ps1 11/11 PASS。
  构建产物：`duckdb/build/duckdb_aligned.exe`（shell 输出名可配
  `DUCKDB_SHELL_OUTPUT_NAME`）。
- **Phase 2 Projection Pushdown**：`projection_pushdown = true`；bind 返回全量
  schema，`init_global` 消费 `input.column_ids` 构建全量列→投影输出位映射；扫描只
  填被请求列、只开被请求 Group。`count(*)` 走 0 向量 chunk。验收 14/14 PASS。
- **Phase 3 Partition / Predicate Pushdown**：`PrunePartsByFilter`（等值走目录
  路径；范围走 part 路径反向重建 part 日期）、`ComputeRowGroupWindow` 用 parquet
  列统计 `CheckStatistics`、行级 filter `TableFilterState::Initialize` +
  `ColumnSegment::FilterSelection` 链式。§3.1/§4 用户约束全部落地。验收 21/21 PASS。
- **Phase 4 Parallel Scan + Metadata Cache**：共享游标（mutex 保护）按连续 Range
  （16 chunks = 32768 行）发放；行级 filter state 每线程一份；元数据缓存复用
  DuckDB ObjectCache（LRU，8GiB，`parquet_metadata_cache` 默认 ON）。验收 21/21 +
  test_parallel PASS；bench_ixday 1M×127 列：1t 0.89s → 8t 0.21s（≈4.2×）。
- **Phase 5 Writer**（aligned_write，已被 v7 mutator 取代）：mapping 指定每个 group
  的写入列；逐 chunk 读源 parquet，按分区值变化切行段（字典编码列必须走
  `ToUnifiedFormat`）；`ParquetWriter`（ZSTD）；part 名
  `{idx:04d}-{rows:10d}`；原子提交（`_tmp/transaction-<txid>/` → move + 重写
  `_table.json`）。验收 test_writer.ps1 全 PASS。
- **Phase 6 Benchmark**：`bench_aligned.ps1`（aligned/wide/join）+
  `bench_polars.py`（polars `hstack`）。维度：投影 5/25/120 列 × 扫描 25%/100% ×
  线程 1/4/8；p5 正确性跨引擎交叉校验。数据 bench_ixday 1M×127 列。结果见
  `docs/BENCHMARK.md`。
- **Phase 7 Compaction / Evolution**：`aligned_compact`：按分区目录合并多 part →
  单 part（同目录必须同列集）→ 暂存 → move → 删旧文件；失败清理 `_tmp`。验收
  test_compaction.ps1 全 PASS。

### 契约演进

#### v2 契约收尾
- 删除：`_group.json`、part sidecar、commit marker、`aligned` 字段语义。
- `_table.json` 升级：`last_txid`、可选 `partitioning` map。
- 行区间：part 按 (分区目录字符串序, part 序号数值序) 排序，start_row 由 footer
  行数全局累加；跨 Group 总行数必须完全一致（fail-fast）。
- 验收：test_aligned 25、test_writer 17、test_compaction 17、test_parallel 全 PASS。

#### v3 契约（_table.json 可选 + aligned 三模式 + 探测降级链）
- `_table.json` 缺失时全部默认值 + glob 组发现；`aligned` 三模式
  `all`/`group`/`none`，显式声明 fail-fast 不降级，未声明探测降级链
  all→group→none。
- 探测语义："group" 判据 = 组内全量校验（不是 "1st==2nd" 弱猜测）。
- 验收：test_aligned 28/28 PASS。
- 事故教训：`git stash` 会把 duckdb/ 子模块的本地补丁也 stash 进子模块自己的
  stash 队列（主仓 stash list 看不到）。

#### v4 契约（all-only，全对齐唯一契约）
- 只有一种对齐模式 all：所有 Group 必须同构——part 数 == index、组内非最后 part
  行数 == index 首 part 行数、总行数一致，违反 fail-fast。
- Compactor 同步化：per-group compact 在 all-only 下造成组间不一致 →
  `aligned_compact` 一次原子处理所有组。API 语义变更：
  `aligned_compact(table, group)` 实际总是合并所有组。
- 删除：gen_bench_modes.ps1、bench_modes.ps1、BENCHMARK_MODES.md。
- 验收：test_aligned 28/28、test_writer 17/17、test_compaction 16/16、
  test_parallel 全 PASS。

#### v5 契约（partition-aligned，分区对齐）
- 所有 Group 用同一种一层分区段（year=/month=/date= 三选一）；分区键 = 完整
  name=value 段串；Group 分区键集合 ⊆ index（允许缺分区）；共享分区总行数必须
  一致（末 part 行数可不同）。无全对齐（part 数相同）要求。
- 行区间：part_rows = index 首分区首 part footer 行数；分区行数
  R_i = (分区 part 数-1)*part_rows + 分区最后 part 行数。
- 组 schema = 组内 rel_path 排序最后 1 个 part 的 footer。
- 修 2 个 bug：index 组 manifest.partitioning 未赋值 → 分区剪枝对 index 失效；
  ScanGroupWindow rewind 后游标落入 part 间 gap → 无符号下溢，新增 gap 分支
  NULL 填充。
- 验收：test_aligned 33/33、test_writer 17/17、test_compaction 16/16、
  test_parallel 全 PASS。

#### v6 契约（自描述 part 文件名 + 日期列契约）
- part 文件名自描述 `{idx:04d}-{rows:10d}.parquet`；行区间由文件名累加推导
  （零 footer IO）；index 分区内索引必须 0000 起连续，非 index 组允许缺号。
- 日期列契约：index schema 前两列必须至少有一列 DATE/TIMESTAMP（v7 收紧为
  恰好一列）；该列即 partitioning source。
- 验收：test_aligned 42/42、test_writer 17/17、test_compaction 16/16、
  test_parallel 全 PASS。

#### v7 契约 + Mutator（aligned_upsert / aligned_delete）
- 主键契约：index schema 前两列 = (date, symbol) → v8 改为 (symbol, date)。
  恰一列 DATE/TIMESTAMP + 一列 symbol（两日期列或无日期列均 fail-fast）。
- aligned_upsert/aligned_delete 取代 aligned_write（已删除）。按主键
  (date, symbol) 插入/更新/删除，只重写受影响 part。
- 删除逻辑：删空单 part 分区 → 整分区移除；多 part 分区删空 → fail-fast
  run aligned_compact first。
- _tmp/transaction-<txid>/ 暂存 + 原子提交；mapping 里未知 Group →
  BinderException fail-fast。
- 验收：test_upsert 29/29、test_aligned 42/42、test_compaction 14/14、
  test_parallel 8/8 全 PASS。

#### v8 契约（主键列序改为 (symbol, date)）
- 主键从 v7 的 (date, symbol) 改为 (symbol, date)——col0 = symbol（字符串），
  col1 = DATE/TIMESTAMP（分区源列）。不再搜索前两列找日期列。
- key_resolver.cpp：复合二分搜索（先比 symbol，再比 date）；TIMESTAMP 用
  `Timestamp::GetDate` 转换。
- 数据/脚本全部从 date,symbol 改为 symbol,date。
- 验收：SQLLogicTest 84/84 PASS；test_aligned、test_upsert、test_compaction、
  test_parallel、test_dml 全 PASS。

### Bug 修复记录

#### Linux 迁移暴露的 bug
- **Bug A（整块被过滤后误判扫描结束）**：`AlignedScanFunction` 改为内部
  `while(true)` 跳过被 row filter 全部拒绝的空 chunk，直到产出非空 chunk 或真正
  耗尽才返回 0 行；否则 DuckDB 把 0 行 chunk 当扫描结束中断后续 chunk。
- **Bug B（stats-skipped 行组的 rg_skip 只记录部分区间）**：被跳过的 RG 改记为
  整 RG 范围，避免下一块想要同 RG 其它部分时 rg_skip 未覆盖 → 崩溃。
- **Bug C（共享游标起点未对齐剪枝区间）**：claim 时把 `next_row` 钳到当前
  interval 起点。非首分区崩溃修复。

#### aligned 开关历史
- `_table.json` 的 `aligned` 字段曾是叶子间剪枝是否可统一传播的开关。v2 契约已
  删除该语义：读端忽略 `aligned` 字段，固定为相交剪枝（`IntersectIntervals`）。

#### Mutator bug 修复（被真实 DML 暴露）
- **映射列类型 = 组内已存类型**：源 VALUES 字面量 111.1→DECIMAL、111→INTEGER，
  写出的新 part 类型与老 part（DOUBLE/BIGINT）不一致 → Copy 类型不匹配崩溃。
  修复：组已存在时用组 schema 类型，仅首写空表回退源类型。
- **manifest 丢弃声明组**：BuildTablePlan 用「仅发现的组」覆盖
  plan.table.groups → 重写的 _table.json 永久丢失声明但无 part 的组。修复：
  保留声明 ∪ 发现的并集。
- **mutator 不合成缺失分区的 part**：UPDATE 路径（键已存在）合成该组的第一个
  对齐 part（R_i 行镜像 index 分区）。
- **Append 跨组 part 索引碰撞**：append part 索引原只取 index 组 max+1，当某非
  index 组 max 更大时新 part 与该组现有同号 part 行数冲突。改为跨所有组取分区内
  max 索引 +1。
- **DataChunk 复用损坏**：Initialize() 追加向量（D_ASSERT 在 release 中是
  noop）→ 复用已用过的 chunk 静默增长并留下旧类型向量。修复：当目标 buffer
  变化时原地重建 chunk（析构 + placement new）。
- **DELETE 删空最高索引 part 直接移除**：v7 契约原语义「多 part 分区删空任一
  part → fail-fast」对标准 SQL UX 不友好。修正：删空的 part 若是该组在该分区的
  最高索引 part → 直接移除。仅内部 part 仍 fail-fast。
- **重要教训**：不要用 aligned_compact 做 DELETE 回退——compact 在
  schema-evolution 目录会失败，且失败时已 compact 的组不回滚（原子性缺口）。

### 其他完成项

#### 三模式性能基准 + 扩展发布
- 扩展发布：`build_loadable_extension` + `EXTENSION_STATIC_BUILD=1`，
  产物 24.3MB 自包含。INSTALL 机制实证（v1.5.4）。
  详见 `docs/EXTENSION_RELEASE.md`。
- 三模式基准结论：扫描性能无实质差异（±5%）；part 粒度影响固定开销
  （group 的 s25 比 all/none 慢 20~27%）。

#### master vs alpha（v1 vs v2 契约）同机基准对比
- 结论：v2 全 workload 无回归且小幅更快（1%~10%）。原因 = 计划构建免去
  sidecar/marker 读取、footer 元数据被 metadata cache 吸收。
- 关键教训：git checkout 切分支后 ninja 增量重编产物会损坏 → 必须全量重编。

#### 整体审视 + 文档补齐
- `README.md`（新增）：项目定位/用法/核心概念/功能矩阵/构建/性能结论。
- 读取链路优化：filter_prune=true、P1-A 列类型解析复用、P2-B NULL 填充向量化。
  未实施：P2-C reader 缓存（DuckDB MultiFileLocalColumnIds 无 clear 接口）、
  P1-B 批量 footer 读取（ObjectCache 已缓解）、P1-C 聚合 stats 快速路径
  （依赖 DuckDB ≥ v1.6）。

#### 读+写基准（2026-08-22）
- 读基准：aligned vs join 在 p100 1 线程 1.15× 慢（位置组装固定开销），4 线程
  差距收窄；aligned 在 s25 分区剪枝上快于 join。wide 在此规模最快。
- 写基准：append 1k 新键 aligned 4× 快；append 100k native 追平；update 300k/600k
  aligned 慢（per-part 重写 O(part_size)）。aligned 写优势 = 多分区 part 级粒度。

#### SQLLogicTest 基础设施
- `test/run_sqllogictest.py`（Python 运行器）+ `test/aligned/*.test`。
  占位符 `{DATA_ROOT}`/`{TEST_DIR}`、`mkdir`/`writejson`/`writefile` 指令、
  `statement error` 用 `----` 分隔。PS 5.1 的 78 列 stderr 折行、`-c` 引号
  mangle 等痛点全部消除。

#### 删除 `_table.json`（无 manifest 契约）
- `_table.json` 原仅用于空表 bootstrap。现完全删除——空表不是有效表，
  Writer 从 `mapping` 参数推导 Group 结构。分区模板默认 `month=%Y-%m`。
- manifest.hpp/cpp 删全部 JSON helpers + `yyjson.hpp` include。

#### 删除 `aligned_attach()` / `aligned_detach()` 物化快照函数
- Phase 8 早期的会话内物化快照辅助函数。DML 写入 DuckDB 自身存储、不回写
  parquet 列组（语义混淆），且大表全量物化很慢。已被 DuckLake 式逻辑 attach
  取代。删除 aligned_attach.cpp/hpp/test/scripts。

#### Append-to-Last-Part 优化
- `ALIGNED_DEFAULT_PART_ROWS = 1048576`。KeyLocation 加 `append_to_last`
  字段。Mutator 预检所有组末 part，任一不满足则整分区回退 `append_new_part`。
  单 batch 可小幅超限（不做硬截断）。

#### 写入路径 7 项优化
1. 向量化 Source 读取（key building）
2. 分区 symbol 边界索引（RG stats）
3. 多 part 并行重写（std::thread 池）
4. 标准 INSERT 消除双写（AlignedUpsertFromCollection C++ API）
5. 并发写 lock 文件（TableWriteLock RAII）
6. 向量化 Synth 填充（STANDARD_VECTOR_SIZE 行批量）
7. 消除 worker thread + Connection（直接 pipeline 线程调用，DmlWorkerPool 删除）

#### Phase 8：catalog 集成 + 标准 DML（DuckLake 式逻辑 attach）
- `ATTACH '<root>' AS al (TYPE ALIGNED)` 创建 AlignedCatalog。
- PlanInsert/PlanDelete/PlanUpdate 返回自定义 sink 算子，DDL 在独立线程 + 独立
  Connection 执行避免嵌套查询死锁。
- 标准 DELETE/UPDATE：收集 WHERE 选中的 rowid → 侧扫 index 组解析 keys →
  调 aligned_delete/aligned_upsert。
- 扫描侧修复：虚拟 rowid 须映射到有效输出位（projection_ids 秩）；重复列请求
  复制填充；get_bind_info 返回归属 entry。

#### CREATE TABLE 标准 DDL + 分区创建 + 列组扩展
- `CREATE TABLE al.<table> (...) WITH (groups=..., partition_template=...)`
  建表；`WITH (partition=...)` 创建空分区；列组扩展为已有表添加新列组。
- 实现：aligned_create.cpp（CreateParquetWriter + WriteEmptyParquet +
  WriteNullParquet + AlignedCreateTable + AlignedCreatePartition +
  ValidateGroupName）；aligned_catalog.cpp CreateTable + SupportsCreateTable
  override + LookupEntry 返回 nullptr + EnsureTablesLoaded 清缓存。
- 代码审查优化：3 critical（C1 分区检查、C2 列校验、C3 quote 剥离）+
  3 medium（M1 ParquetWriter 去重、M5 O(1) 列查找、M6 组名校验）+
  2 minor（m6 _tmp 过滤、m8 undef 位置）。
- 验收：SQLLogicTest 122/122、test_aligned 42/42、test_upsert 50/50、
  test_dml 7/7、test_compaction 16/16、test_parallel 8/8 全 PASS。

### 后续加固（2026-08）

- **M2: ValidatePartitionKey 日期格式校验**：`ValidatePartitionKey` 原先只检查
  长度（如 `date=1970-13-99` 能通过），现使用 `Date::TryConvertDate` 校验日期
  有效性（`month=2026-13` → fail-fast "invalid date"）。新增 2 个 SQLLogicTest
  错误用例（122→124）。
- **M4: tables map 线程安全**：`AlignedSchemaEntry` 的 `tables` map + `tables_loaded`
  原先无锁，`Scan` (no-context overload) 迭代 `tables` 不调用 `EnsureTablesLoaded`
  可能与并发 `CreateTable` reload 竞争。新增 `std::mutex tables_mutex`，保护
  `EnsureTablesLoaded`、`Scan`、`LookupEntry`、`CreateTable` reload 路径。
- **Compactor 跨组原子性加固**：原先 per-group commit（合并组 A → 删旧 part →
  合并组 B 失败 → 组 A 已提交、组 B 未变 → 不一致）。改为**两阶段提交**：
  Phase 1 所有组的合并 part 先写入 `_tmp/`；Phase 2 全部成功后统一 move + 删旧。
  任一组失败 → 清理 `_tmp`、表状态不变。
- **DML UPDATE 向量化**：`PhysicalAlignedUpdate::Finalize` 原先用
  `std::map<int64_t, vector<Value>>` 存 set 值 + 逐行 `GetValue`/`SetValue` join。
  改为 sorted vector + merge-join（两端按 rowid 排序，单遍扫描），消除
  `std::map` 开销。
- **DML INSERT 大批量分批提交**：`PhysicalAlignedInsert::Finalize` 原先将全部
  行物化到单个 `ColumnDataCollection` 后一次调用 mutator，10M+ 行可能 OOM。改为
  阈值 1M 行分批：超过 1M 行时扫描 collection 按批调用
  `AlignedUpsertFromCollection`，每批独立事务（重获写锁 + 重读 plan）。小 INSERT
  （≤1M 行）走原单次路径（零开销）。新增 test_dml.ps1 1.1M 行批量 INSERT 测试
  （count/distinct/date filter 全验证通过）。

## v0.11-audit-cleanup — 代码结构审查修复

基于全量代码审查报告，系统性修复所有发现项：

### 死代码清理
- **删除 `resolver/row_space.{cpp,hpp}`**：`ValidateRowSpace` 零调用点（49 行死代码），
  `BuildTablePlan` 已内联等价校验。连带清理 compactor/manifest 中的无用 include。
- **删除 `compactor::NextPartIndex`**：零调用点（~30 行），compactor 把合并 part
  硬编码为 0000。
- **删除 6 个死结构体字段**：`AlignedInsertGlobalState::{rows_inserted,rows_updated,
  error}`、`AlignedDeleteGlobalState::{error,staged_path}`、
  `AlignedUpdateGlobalState::error` — 全部 write-only 或 never-touched。
- **删除 12 个无用 include**：跨 6 个文件的 `<fstream>`/`<future>`/
  `vector_operations.hpp`/`database.hpp`/`expression_executor.hpp`/
  `create_table_info.hpp`/`not_null_constraint.hpp`/`file_system.hpp`(header)/
  `row_space.hpp` 等，全部 grep 确认无符号引用后移除。

### 正确性修复
- **修复硬编码主键列名**：`aligned_dml.cpp` 的 `ResolveKeysForRowids` 和 UPDATE
  `Finalize` 原先硬编码 `"date"`/`"symbol"` 列名查找，改用 plan 权威字段
  `plan.groups[0].partition_source`/`symbol_column`（与 `key_resolver.cpp` 一致）。
  若表使用了不同的主键列名（如 `dt`/`sym`），旧代码会失败。

### 重复代码消除
- **提取 `io/parquet_io.cpp`**：`CreateParquetWriter` 共享函数，消除
  `aligned_create.cpp`/`part_rewriter.cpp`/`aligned_compactor.cpp` 三处 14 行
  ParquetWriter 构造参数的逐字复制粘贴。
- **合并 `ResolveKeysForRowids`**：DELETE 和 UPDATE 各一份 ~80 行几乎逐行相同
  的函数，合并为单一 helper + `op_name` 参数 + `ProjectRowidFromKeys` 零拷贝
  投影辅助。
- **统一 `NextTransactionId`**：mutator 和 compactor 各自维护独立 `static idx_t`
  计数器，txid 可能撞号导致 `_tmp/transaction-1` 冲突。改为共享 `std::atomic` 计数器
  （声明在 `aligned_mutator.hpp`）。

### 文档同步
- **AGENTS.md §10**：修正为真实文件列表（删除虚构的 `row_space.cpp`/`group_resolver.cpp`/
  `aligned_scan_state.cpp`/`group_scan.cpp`/`scheduler.cpp`/`logical_table.cpp`/
  `schema.cpp`/`optimizer/`，新增 `io/parquet_io.cpp`）。
- **删除 `_table.json` 过时注释**：`mutator.cpp` ExecuteAndCommit 注释提及
  "rewrite _table.json"，与 §4"无 manifest"契约矛盾。

### 后续待办

- BuildTablePlan 无缓存（UPDATE 触发 3 次全量 glob + footer 读取）— 性能优化，
  需设计缓存失效策略（按 part mtime/size），影响面较广，暂缓。
- StagedTransaction RAII（mutator + compactor 暂存逻辑统一）— 中等优先级重构。
- manifest → plan/table_plan 重命名 — 低优先级命名清理。

## v0.12-aligned-drop — 列组/整表删除

新增 `aligned_drop` 表函数，支持按列组或整表级别删除 AlignedTable 数据：

```sql
-- 删除单个列组（保留 index 及其他组）
SELECT * FROM aligned_drop('mytable', 'factor/alpha', root => '...');
-- 删除整张表（group_name='index'）
SELECT * FROM aligned_drop('mytable', 'index', root => '...');
```

- **规则**：`group_name='index'` 删除整个表目录（所有列组）；其他组名仅删除
  该列组的目录树，index 及其他组不受影响。
- **并发安全**：删除前获取 `TableWriteLock`，与并发 upsert/delete/compact 互斥。
- **返回值**：`(dirs_removed, files_removed, txid)` — 删除的目录数、文件数、事务 ID。
- **DuckDB 1.5.4 限制**：SQL 解析器不支持 `DROP TABLE ... WITH (...)` 语法
  （`ExtraDropInfo` 机制仅用于 SECRETS），因此使用表函数而非标准 DROP TABLE。
- 新增文件：`compaction/aligned_drop.{cpp,hpp}`（~140 行）。
- 新增测试：`test/aligned/aligned_drop.test`（11 个断言）。
- 更新 `docs/API.md`：新增 §4.6 aligned_drop + 参数详解 + 返回值表。

## v0.13-aligned-create — 表函数式建表

新增 `aligned_create` 表函数，作为 `CREATE TABLE` DDL 的表函数替代方案：

```sql
SELECT * FROM aligned_create('mytable', 'symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE',
                             groups => 'index:close;factor/alpha:alpha001');
```

- **设计理由**：CREATE TABLE 不是标准建表逻辑（WITH 子句解析 groups/partition_template
  非 SQL 标准），用表函数更自然，与其他 aligned_* 函数风格一致。
- **参数**：`table_name`（位置）、`columns`（位置，列定义字符串）、`groups`
  （命名，可选）、`root`（命名，可选）、`partition_template`（命名，可选）。
- **列定义解析**：用 `Parser::ParseColumnList` 解析列定义字符串。注意：解析器
  返回 `LogicalTypeId::UNBOUND` 类型，需用 `TransformStringToLogicalType` 解析
  为具体类型后才能用于 schema 校验和 ParquetWriter（已记入 AGENTS.md §12）。
- **返回值**：`(dirs_created, files_created, txid)` — 创建的目录数、parquet 文件
  数、事务 ID。递归计数时跳过 `.aligned_write.lock`（RAII 析构前仍存在）。
- 内部复用 `AlignedCreateTable`（新建表模式）。
- 新增文件：`catalog/aligned_create_fn.{cpp,hpp}`（~130 行）。
- 新增测试：`test/aligned/aligned_create_fn.test`（9 个断言）。
- 修复 `aligned_drop.cpp` 中 `CountRecursive` 未跳过 `.aligned_write.lock` 的 bug。

## v0.14-remove-upsert-delete — 删除 aligned_upsert / aligned_delete 表函数

标准 DML（ATTACH + INSERT/UPDATE/DELETE）已完全替代 `aligned_upsert` 和
`aligned_delete` 表函数，故将其删除：

- **删除 SQL 表函数注册**：`extension.cpp` 中 `aligned_upsert` / `aligned_delete`
  的 `TableFunction` 注册块已移除。
- **删除表函数入口**：`AlignedUpsertFunction` / `AlignedDeleteFunction` /
  `AlignedUpsertBind` / `AlignedDeleteBind` / `MutateInitGlobal` 从头文件和
  `.cpp` 中移除（或改为 file-local static，若被 `*FromCollection` 内部调用）。
- **保留内部 C++ API**：`AlignedUpsertFromCollection` / `AlignedDeleteFromCollection`
  仍保留——标准 DML 的物理算子（`PhysicalAlignedInsert` /
  `PhysicalAlignedDelete`）通过它们直写 parquet 列组。
- **删除测试**：`test_upsert.ps1`（50 断言）、`test/aligned/aligned_upsert.test`
  （SQLLogicTest）已删除——它们测的是表函数本身。
- **修改测试**：`test/aligned/aligned_dml.test` 的 setup 从 `aligned_upsert`
  改为 `aligned_create` + `INSERT ... SELECT`。
- **修改 benchmark**：`bench_write.ps1` 从 `aligned_upsert` 改为 `ATTACH` +
  标准 `INSERT`/`UPDATE`。
- **更新文档**：`docs/API.md` 删除 §4.3/§4.4（aligned_upsert/aligned_delete）、
  §6.3/§6.4/§6.5（source_path/mapping/keys_source 参数）、附录返回值表对应行，
  表函数数量从 7 改为 5；`AGENTS.md` §7 删除两个函数签名并标注"已删除"。
- 当前总：SQLLogicTest 144/144 + 4 PS 套件全 PASS。
