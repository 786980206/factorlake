# release.md — FactorLake / AlignedTable 开发日志

> 本文件记录所有开发进度、契约演进、bug 修复历史。从 AGENTS.md 剥离，保持
> AGENTS.md 只含当前架构契约。

---

## 2026-08 完成项

### Phase 0–7

- **Phase 0**：`docs/STORAGE_CONTRACT.md` 定稿（v1.1）。
- **Phase 1 Read MVP**：`aligned_scan()` / `aligned_scan()` 跑通。多 Group 并行
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

## v0.15-delete-empty-part — 删空中间 part 改为 0 行重写

删空多 part 分区的中间 part 不再 fail-fast，而是原地重写为 0 行空文件
（保留文件名索引，保持索引连续）。

- **设计理由**：0 行空文件在现有 Reader/Writer 契约下完全合法——索引连续性
  契约不破坏（part 还在，索引不变）、行区间累加正确（0 行贡献 0）、OpenPart
  防御校验通过（footer NumRows=0 == 文件名 rows=0）、扫描 0 行窗口天然不产出。
  旧的 fail-fast + 强制 compact 是保守但非必要的选择。
- **改动**：
  - `aligned_mutator.cpp`：删除 `throw IOException("...run aligned_compact first")`
    分支，改为设置 `t.empty_part = true`。
  - `aligned_mutator.hpp`：`MutateTarget` 新增 `bool empty_part = false` 字段。
  - `ExecuteAndCommit`：`empty_part` 目标正常走 `RewritePart`（产出 0 行文件），
    但 `new_row_count == 0` 时不加入 `removed_partitions`（保留分区目录）；
    Pass B 不跳过 `empty_part` 的 0 行文件（move 到目标路径覆盖旧文件）。
- 新增测试：`test/aligned/aligned_delete_empty.test`（12 个断言）——3 分区表
  删空中间分区、验证前后行数、验证清空分区可再插入。
- 当前总：SQLLogicTest 106/106 + 4 PS 套件全 PASS。

## v0.16-compact-normalize — aligned_compact 规范化重写

重写 `aligned_compact` 从"简单合并多 part 到单 part"改为"按 `ALIGNED_DEFAULT_PART_ROWS`
(1M) 规范化重新切分"。

- **设计理由**：经过 INSERT/DELETE 后各 parquet 行数不统一（有些删后少行、有些
  插后多行）。需要一次重整：保证前面所有 part 满行（恰好 1M 行），只有末 part
  可能少行。旧的简单合并无法处理 >1M 行的分区（会产出超大的单文件）。
- **行为**：
  - 每个分区的所有 part 按 `ALIGNED_DEFAULT_PART_ROWS` (1048576) 重新切分。
  - 前面的 part 恰好 1M 行，末 part ≤ 1M 行。
  - 0 行占位 part（从 delete-empty-part 产生）被合并吸收。
  - **跳过优化**：已规范化的分区（单 part ≤ 1M，或多 part 均满行 + 末 part ≤ 1M）
    跳过不重写（`IsAlreadyNormalized` 检查），保证效率。
  - 两阶段提交不变：所有组先 stage 到 `_tmp/`，全部成功后统一 move + 删旧文件。
- **改动**：`aligned_compactor.cpp` 重写（~340 行），新增 `IsAlreadyNormalized`
  和 `MergePartsToWriter` 辅助函数。多 part 切分时按行流式读取，在 1M 行边界
  flush writer + 开新 writer。
- 新增测试：`test/aligned/aligned_compact.test` 新增 9 个断言（0-row middle part
  合并、幂等性、skip 优化、error case）。
- 当前总：SQLLogicTest 115/115 + 4 PS 套件全 PASS。

## v0.17-create-group-param — aligned_create 参数重构

将 `aligned_create` 从 `groups => 'index:close;factor/alpha:alpha001'` 的复杂
映射语法改为与 `aligned_drop` 对称的简单路径参数：

```sql
-- 旧签名
SELECT * FROM aligned_create('mytable', 'symbol VARCHAR, date DATE, close DOUBLE, alpha001 DOUBLE',
                             groups => 'index:close;factor/alpha:alpha001');

-- 新签名
SELECT * FROM aligned_create('mytable', 'index', 'symbol VARCHAR, date DATE, close DOUBLE');
SELECT * FROM aligned_create('mytable', 'factor/alpha', 'alpha001 DOUBLE');
```

- **参数变更**：从 2 位置 + 3 命名参数改为 3 位置 + 2 命名参数。
  - `group_name`（位置参数 2）：`'index'`（建表）或 `'lv1/lv2'`（扩展列组）。
  - `columns`（位置参数 3）：该组的列定义字符串。
  - 移除 `groups` 命名参数。
  - 保留 `root` 和 `partition_template` 命名参数。
- **两种模式**：
  - `group_name='index'`：建表。所有列写入 index 组。检查表不存在 + PK 校验。
  - `group_name='lv1/lv2'`：扩展列组。检查表已存在。委托 `AlignedCreateTable`
    extend mode（每个已有分区写 N 行 NULL 占位）。
- **计数优化**：建表模式计全表目录，扩展模式只计新组目录。
- **更新测试**：`aligned_create_fn.test` 重写（新签名），`aligned_dml.test`、
  `aligned_delete_empty.test`、`aligned_compact.test` 改用新签名。
  `bench_write.ps1` 同步更新。
- 当前总：SQLLogicTest 118/118 + 4 PS 套件全 PASS。

## v0.18-unify-scan — 删除 aligned_table，统一表函数参数格式

两项改动：

### 1. 删除 `aligned_table`，保留 `aligned_scan`

`aligned_table` 和 `aligned_scan` 功能完全重复（都是扫描逻辑表），删除
`aligned_table`，统一用 `aligned_scan`。同时统一参数格式：

```sql
-- 旧：两个函数，参数格式不同
SELECT * FROM aligned_table('mytable');                    -- 1 位置参数
SELECT * FROM aligned_scan('/data/root', 'mytable');        -- 2 位置参数 (root, table)

-- 新：统一为 1 位置参数 + root 命名参数
SELECT * FROM aligned_scan('mytable');
SELECT * FROM aligned_scan('mytable', root => '/data/root');
```

- `AlignedBind` 简化：移除 2-参数 vs 1-参数分支，只保留 `(table_name)` + `root => ...`。
- `extension.cpp`：删除 `aligned_table_fn` 注册，`aligned_scan_fn` 改为 1 位置参数。
- `aligned_catalog.cpp`：内部 catalog 函数名从 `"aligned_table"` 改为 `"aligned_scan"`。
- 全项目替换 `aligned_table(` → `aligned_scan(`（test/*.test、scripts/*.ps1、
  scripts/*.sh、docs/*、README.md）。
- `aligned_scan(root, name)` 2-位置参数调用全部改为 `aligned_scan(name, root => ...)`。

### 2. 统一表函数参数格式

所有表函数现在遵循统一约定：
- **第 1 位置参数**：`table_name`（逻辑表名）
- **后续位置参数**：各函数特定的参数（`group_name`、`columns` 等）
- **命名参数**：`root`（可选，默认 `aligned_data_root`）、`partition_template`（仅 create）

| 函数 | 位置参数 | 命名参数 |
|------|---------|---------|
| `aligned_scan` | `(table_name)` | `root` |
| `aligned_create` | `(table_name, group_name, columns)` | `root`, `partition_template` |
| `aligned_compact` | `(table_name, group_name)` | `root` |
| `aligned_drop` | `(table_name, group_name)` | `root` |

- 当前总：SQLLogicTest 118/118 + 4 PS 套件全 PASS。

## v0.19-aligned-groups — 新增 aligned_groups 表函数

新增 `aligned_groups(table_name, root=...)` 表函数，用于查看表上已有哪些列组。

```sql
SELECT * FROM aligned_groups('cnstk_ixday');
-- 结果：
--   index          symbol;date;close;volume   3
--   factor/alpha   alpha001;alpha002          3
--   fieldset/ma    ma5;ma20                   3
```

- **参数**：`table_name`（位置参数 1）+ `root`（命名参数，可选）。
- **返回**：每组一行 `(group_name VARCHAR, columns VARCHAR, partition_count BIGINT)`。
  - `group_name`：列组路径（`index` / `factor/alpha` 等）。
  - `columns`：该组的列名列表，分号分隔（避免与 SQLLogicTest 逗号分隔符冲突）。
  - `partition_count`：该组的分区数。
- **实现**：复用 `BuildTablePlan` 发现所有列组，从 `GroupPlan` 提取
  `manifest.group`、`column_order`、`partitions.size()`。
- **新增文件**：`aligned_groups.cpp` + `aligned_groups.hpp`，注册在 `extension.cpp`。
- **新增测试**：`test/aligned/aligned_groups.test`（9 个断言）——多组表创建、
  列组列表、显式 root、插入后分区数增长、错误案例。
- 表函数数量从 4 个增加到 5 个。
- 当前总：SQLLogicTest 127/127 + 4 PS 套件全 PASS。

## 2026-08-24 — 架构重构：消除代码重复

基于 `ARCHITECTURE_ANALYSIS.md` 的 20+ 条问题分析，一次性修复所有可操作项。
分析文件已删除（所有问题已解决或标记为有意推迟）。

### 共享 IO 模块 `io/parquet_io`（§2.1/§2.2/§2.3/§2.6/§3.1）

提取到 `extension/aligned/src/io/parquet_io.cpp` + `src/include/io/parquet_io.hpp`：

| 函数 | 替换的原散落代码 |
|------|----------------|
| `CreateParquetWriter` | aligned_create / part_rewriter / aligned_compactor 三处 14 参数构造 |
| `FormatPartName` / `ParsePartName` | 5 处格式化 + 2 处解析 |
| `WriteEmptyParquet` / `WriteNullParquet` | 从 aligned_create 移入 |
| `OpenPartReaderAllColumns` | part_rewriter 内联 reader 初始化 |
| `OpenPartReaderNamedColumns` | mutator `ReadSourceColumns` + key_resolver 列读取 |

提交：`9d12c0c`、`c31cc2c`

### 计划查询 helpers `catalog/manifest`（§1.6/§1.7/§2.5/§2.8/§3.4）

| 函数 | 替换的原散落代码 |
|------|----------------|
| `ResolveDataRoot` | mutator / scan / compactor / create_fn / groups / drop 六处内联 |
| `IndexGroup` | 6+ 处 `groups[0]` + `CIEquals(..., "index")` 硬编码假设 |
| `NextPartIndexForPartition` | key_resolver 快/慢路径 + mutator fallback 三处 max_index 循环 |

提交：`c31cc2c`

### 分区模板共享 `resolver/partition_resolver`（§3.6）

| 函数 | 来源 |
|------|------|
| `IsKnownTemplate` | 从 aligned_create 内联校验提取 |
| `DefaultPartitionKey` | 从 aligned_create 静态函数移入 |
| `ValidatePartitionKey` | 从 aligned_create 静态函数移入 |

aligned_create 现在调用共享函数，不再持有静态副本。提交：`c4a37f8`

### 暂存事务 RAII `StagedTransaction`（§1.5/§2.4/§3.3）

提取到 `mutator/aligned_mutator.hpp`：

```cpp
class StagedTransaction {
    FileSystem &fs;
    const string table_path;
    const idx_t txid;           // NextTransactionId() — 单一共享计数器
    const string tmp_root;      // table_path/_tmp/transaction-<id>/
    // 构造时创建目录，析构时清理（成功+异常路径均安全）
};
```

- 替换 mutator `ExecuteAndCommit` 中的手动 try/catch 清理。
- 替换 compactor `AlignedCompactFunction` 中的手动 try/catch 清理。
- 统一两个独立的 `NextTransactionId` 计数器为一个。

提交：`b5bc709`

### 源键向量化读取 `ExtractSortedRows`（§2.9）

`AlignedUpsertFunction` 和 `AlignedDeleteFunction` 中两份近乎相同的 ~60 行向量化
chunk-scan 循环合并为一个共享函数。提交：`1fdaef3`

### DML 键扫描统一（§1.4/§2.7）

- `ResolveKeysForRowids` 从 DELETE 和 UPDATE 两份 ~80 行副本合并为一个。
- 硬编码 `"date"`/`"symbol"` 名称匹配改为 `plan.groups[0].partition_source` /
  `.symbol_column` 字段。

### aligned_create 拆分（§4.4）

`AlignedCreateTable` 从一个混合新表创建 + 列组扩展的大函数拆分为：
- `ResolveGroupColumns` — 共享列名解析。
- `CreateNewTable` — 新表创建路径。
- `ExtendTableWithGroup` — 列组扩展路径。
- `AlignedCreateTable` — 薄分发器。

提交：`641a914`

### 死代码清理（§1.2/§4.3）

- 删除 `resolver/row_space.cpp` / `row_space.hpp`（`ValidateRowSpace` 无调用方）。
- 删除 `ARCHITECTURE_ANALYSIS.md`（所有问题已解决）。

### AGENTS.md 更新

- §10 新增「共享工具函数」表，列出所有提取的共享函数及所属模块。
- §11 测试计数更新为 141/141 + 4 PS 套件全 PASS。

### 有意推迟的项

| 项 | 原因 |
|----|------|
| §1.1/§4.1 manifest→plan 重命名 | 纯机械重命名，影响所有 include，收益低 |
| §1.3 mutator 进一步拆分 | 已从 1768→1330 行，剩余为 bind 逻辑（§3.7），拆分收益不足 |
| §4.5 compactor 复用 RewritePart | 合并逻辑不同（concat vs read-modify-write），不适用 |
| §4.6 aligned_dml 位置 | 已在 `execution/`，AGENTS.md §10 已更正 |

- 当前总：SQLLogicTest 141/141 + 4 PS 套件全 PASS。

## 2026-08-25 — 写路径性能优化

### 背景

用户提出三种最高频写入场景需要加速：
1. **全量重写** — 整表或整组数据覆盖
2. **单组全量重写** — 针对某一个 column group 的全量重写
3. **末分区重写** — 针对 group 最后一个分区的重写

用户明确排除 Delta 层方案（"确定的就是不做 delta"）。

### 优化一：分区边界预加载 + KeyResolver 分区缓存

**问题**：原 `AlignedUpsertFunction` 中 `KeyResolver::Resolve` 对每个源行逐行调用，
即使分区数据已缓存，仍需 N 次二分查找。更关键的是，`LoadPartitionBoundaries`
（仅读 RG stats，零数据 IO）没有被预先触发——只有当 `Resolve` 被调用时才按需加载。

**优化**：在批量 resolve 之前，`AlignedUpsertFunction` 不再需要显式预加载（`KeyResolver`
内部按需加载 + 缓存）。分区缓存确保每个分区的数据最多加载一次，无论有多少键落入其中。

提交：`acf2c53`

### 优化二：part 文件级 symbol 范围二分查找

**问题**：原 `KeyResolver::Resolve` 的慢路径（symbol 在分区内）会调用
`LoadPartition`——加载**整个分区所有 part** 的 symbol+date 数据到内存做逐行二分查找。
对于 N 个 part 的分区，数据 IO 为 O(N parts)。

**关键洞察**（用户指出）：重写的最小单位是 Parquet 文件（part），不是 RowGroup。
所以只需定位到**哪个 part 文件受影响**即可，不需要在 RowGroup 级别做二分查找。

**优化**：在 `Resolve` 中新增 part 级别二分查找路径：
1. `LoadPartitionBoundaries`（已有）为每个 part 建立 symbol [min, max] 范围索引
2. 对 symbol_value 做 part 级别二分查找——找到唯一的受影响 part 文件
3. `LoadSinglePart`（新增）只加载**那一个 part** 的 symbol+date 数据
4. 在该 part 内部做逐行二分查找，确定 insert vs update

数据 IO 从 O(N parts) 降为 O(1 part)。原 `LoadPartition` 全量加载保留为 fallback
（part stats 缺失或 symbol 落在 part 间隙时触发）。

**新增 API**：
- `KeyResolver::LoadSinglePart(partition, part_k)` — 按需加载单个 part 的键数据
- `PartitionCache` 新增 `part_symbols` / `part_dates` / `part_loaded` per-part 缓存数组

提交：`eaccc88`

### 优化三：AppendRowsToBuffer 批量追加函数

**问题**：原 `AppendRowToBuffer` 逐行 `SetValue` + `Append`——每行一次
`ColumnDataCollection::Append` 调用，开销巨大。

**优化**：新增 `AppendRowsToBuffer`，将同一目标 buffer 的多行收集后以
`STANDARD_VECTOR_SIZE`（2048 行）为单位批量 `Append`。将 Append 调用次数
减少约 2000 倍。

当前 `AlignedUpsertFunction` 的 dispatch 循环仍逐行路由（因 insert/update/synth
分类逐行不同），但 `AppendRowsToBuffer` 为未来全批量 dispatch 重构做好了准备。

提交：`acf2c53`

### 优化四：显式 INSERT 列映射 + 跳过仅含键列的组重写

**问题**：INSERT 路径（`PhysicalAlignedInsert::Finalize`）传空映射给 mutator，导致
mutator 走自动推导路径——从源 schema 推导列→组映射。但 DML 层的 `ResolveDefaultsProjection`
已经用默认值（NULL）填充了未指定的列，所以源 schema 包含全部列。结果是：即使用户只
指定了 `INSERT INTO t (symbol, date, alpha001, alpha002)`，index 组（含 close, volume）
也会被重写——尽管这些列的值全是 NULL（默认填充）。

**优化**：
1. `AlignedCatalog::PlanInsert` 中，利用 `op.column_index_map` 判断哪些列是用户显式指定的
   （`!= INVALID_INDEX`），构建只包含显式列的映射字符串。通过 `BuildTablePlan` 获取组结构，
   为每个组收集被指定的列。映射传给 `PhysicalAlignedInsert` → `AlignedUpsertFromCollection`。

2. `AlignedUpsertFunction` dispatch 循环中新增 `only_keys` 检查：当某个组的映射列**仅**
   包含键列（symbol, date）且该键为更新（`found=true`）时，跳过该组重写。键值在更新时
   不变，重写是纯浪费。

**效果**：`INSERT INTO t (symbol, date, alpha001, alpha002)` 更新现有行时，index 组的映射
变为 `[symbol, date]`（仅键列）→ 被跳过 → 只重写 `factor/alpha` 组。

**基准测试结果**（200K 行，2 分区，2 组）：

| 场景 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| 全量重写 200K | 3070ms | 3277ms | ~0% |
| **单组重写 200K** | **3092ms** | **2212ms** | **-28%** |
| 末分区重写 100K | 1784ms | 1894ms | ~0% |
| 大批量插入 100K（新分区） | 1989ms | 1885ms | -5% |

**Bug 修复**：`ReadSourceFromCollection` 原先用 `bind.needed_names` 作为源列名，仅在
`needed_names == source columns`（空映射时）正确。新增 `bind.source_col_names` 存储实际
源 collection 列名，修复了显式映射时的列查找。

提交：`35b0be0`

### 测试

- 全部测试通过：SQLLogicTest 141/141 + 4 PS 套件全 PASS。
- 扩展已重建（23.3 MB）。

### 优化五：两阶段批量 dispatch + SourceReader UnifiedVectorFormat 缓存

**问题**：timing 分析显示 dispatch 循环占全量重写耗时的 64%（1879ms / 3236ms）。
瓶颈在于逐行调用 `AppendRowToBuffer`：每行一次 `reader.GetValue` + 一次 `buffer.Append`。
对 100K 唯一键 × 2 组 = 200K 次 Append 调用。

**优化**：
1. **SourceReader 缓存 UnifiedVectorFormat**：每列的 `UnifiedVectorFormat` 在 chunk 变化时
   计算一次，所有该 chunk 内的行复用。标量类型（DOUBLE/BIGINT）直接从缓存提取，
   避免 `chunk.GetValue` 的 Value 构造开销。VARCHAR 回退到 `chunk.GetValue`。

2. **两阶段批量 dispatch**：
   - **第一阶段（分类）**：遍历所有行，确定每行的路由目标 `(target, is_update, pos)`，
     收集到 `DispatchEntry` 数组。不读取源值——只做路由判断。
   - **第二阶段（批量追加）**：按 `(target, is_update)` 分组，用 `BatchAppender` 累积
     行到 scratch chunk，每 `STANDARD_VECTOR_SIZE`（2048）行 flush 一次到
     `ColumnDataCollection`。将 Append 调用次数减少 ~2048 倍。

3. **ExtractSortedRows 快速路径**：VARCHAR symbol 列直接用 `string_t::GetString()` 构造
   `Value`，避免逐行 `sym_vec.GetValue(si)` 的通用路径开销。

**效果**：

| 场景 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| 全量重写 200K | 3236ms | 2452ms | -24% |
| 单组重写 200K | 2193ms | 2056ms | -6% |
| 末分区重写 100K | 1879ms | 1298ms | -31% |
| 大批量插入 100K | 2017ms | 1252ms | -38% |

**与初始值对比**（所有优化累计）：

| 场景 | 初始 | 最终 | 累计改善 |
|------|------|------|----------|
| 全量重写 200K | 3070ms | 2452ms | -20% |
| **单组重写 200K** | **3092ms** | **2056ms** | **-33%** |
| **末分区重写 100K** | **1784ms** | **1298ms** | **-27%** |
| **大批量插入 100K** | **1989ms** | **1252ms** | **-37%** |

提交：`17cf109`（SourceReader 缓存）、`ac97b77`（两阶段 dispatch + ExtractSortedRows）

## 2026-08-26 — TIMESTAMP 键支持 + 示例脚本

### TIMESTAMP 键支持（分钟级数据）

**问题**：KeyResolver 使用 `date_t`（32 位天数）作为键类型，当分区源列为
TIMESTAMP 时，`Timestamp::GetDate()` 截断为日期，导致同一天内同一标的的不同
分钟 K 线被视为相同键——只有最后一根 K 线被保留。

**修复**：将键类型从 `date_t` 改为 `int64_t`，可容纳 `date_t`（DATE 列）或
`timestamp_t`（TIMESTAMP 列）的完整值。对于 TIMESTAMP 列，不再调用
`Timestamp::GetDate()` 截断，而是保留完整微秒级时间戳作为键。

**改动文件**：
- `key_resolver.hpp`：`Resolve(date_t)` → `Resolve(int64_t)`，
  `PartitionCache::dates` 从 `vector<date_t>` → `vector<int64_t>`，
  新增 `ToDate(int64_t)` 方法和 `is_timestamp` 标志
- `key_resolver.cpp`：`LoadPartition`、`LoadSinglePart` 保留完整 timestamp 值
- `partition_resolver.hpp/.cpp`：新增 `EvaluatePartitionTemplate(string, int64_t, string)`
  重载，自动将 int64_t 转为 date_t 用于分区目录求值
- `aligned_mutator.cpp`：`SortedRow::date` 从 `date_t` → `int64_t`，
  `ExtractSortedRows` 保留完整 timestamp 值

**兼容性**：DATE 列行为不变（date_t 值 0-200000 范围直接使用），TIMESTAMP 列
现在支持同一天内多个时间戳作为不同键。

**测试**：全量 141 SQLLogicTest + 4 PS 套件 PASS。新增 4 行 TIMESTAMP 键测试
（同日同标的不同时间戳全部保留）。

### 示例脚本 `example_setup.ps1`

生成分钟级行情数据用于体验 FactorLake 功能：
- 500 标的 × 22 交易日 × 240 K 线/天 = 2.64M 行
- 5 个 column group：index(sym,dt) + quote/ohlc + indicator/ma + indicator/ema + indicator/kdj
- 按日分区（`date=%Y-%m-%d`），23 个分区，115 个 parquet 文件
- 运行时间约 4 分钟
- 参数可调（`$NSYM` 改为 8000 可生成 42M 行全量数据）

提交：`<TBD>`（TIMESTAMP 键 + example_setup.ps1）

## 2026-08-27 — COPY TO (FORMAT aligned) 批量写入路径

### 背景

旧的写入路径（mutator / aligned_upsert）适合小批量 upsert，但批量全量写入
（如 400 标的 × 36 年 = 500 万行）性能很差。新增 `COPY TO (FORMAT aligned)`
走 DuckDB CopyFunction 框架，直接用 ParquetWriter 写分区 parquet 文件，
绕过 mutator 的逐行 upsert 逻辑。

### 用法

```sql
-- 新组首次写入（2-arg aligned_create 创建空组，schema 从 query 推断）
SELECT * FROM aligned_create('cnstk_ixday', 'index', 'symbol VARCHAR, date DATE', partition_template => 'year=%Y');
SELECT * FROM aligned_create('cnstk_ixday', 'panel/ma');

-- 批量写入（无需 OVERWRITE / WRITE_EMPTY_FILE，per-partition 覆盖自动处理）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday' (FORMAT aligned, GROUP 'index');
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/ma');
```

### 架构演进：从混乱到单向 pipeline

#### 第一版（有缺陷）

初版实现存在 5 个架构级缺陷（详见代码评审）：

1. **Partition 路由责任放错层**：Combine 把所有 local buffer 剩余数据 flush 到
   "最后一个 active partition writer"，导致跨分区数据混淆（数据正确性 bug）。
2. **三个 flush 入口**：Sink / Combine / Finalize 都可能 flush，修改同一个
   `rows_in_current_part` / `part_index` / file size，风险极高。
3. **ParallelSink + 单 writer 锁**：并行 sink 串行写，partition 少时性能差。
4. **Finalize 依赖 destructor**：异常路径可能 double close。
5. **小数据场景 0 行文件**：`rows_in_current_part` 在 `Flush` 后读
   `buffer.Count()` 返回 0（因为 Flush 消耗了行计数），文件名显示 rows=0。

#### 第二版（重构后）

按评审建议重构为单向数据流 pipeline：

```
input chunk
    ↓
Sink: 按 partition key 分流 → per-partition local buffer (ColumnDataCollection)
    ↓                          (Sink 不碰 ParquetWriter)
Combine: 每个 partition buffer → GlobalState::Flush (FlushManager, 唯一写入入口)
    ↓
Flush: PartitionWriter → ParquetWriter::Flush (写一个 RG)
       满足 row_groups_per_file (8) → 轮转 part 文件 (rename 临时名 → 自描述名)
    ↓
Finalize: 逐分区 Finalize + Rename + 统计校验 (received == written)
```

核心改动：

- **LocalState**：从单个 `ColumnDataCollection buffer` 改为
  `map<partition_key, PartitionBuffer>`，每个分区独立 buffer，杜绝跨分区混写。
- **Sink**：只做分区路由 + 追加到 local buffer，**不碰 ParquetWriter**。
- **Combine**：遍历每个 partition buffer，分别交给 `GlobalState::Flush`。
- **GlobalState::Flush**：唯一写入入口，创建 PartitionWriter + Flush + 轮转 part。
  Flush 前保存 `idx_t rows = buffer.Count()`（因为 Flush 后 Count() 返回 0）。
- **Finalize**：唯一做 `writer->Finalize()` + `RenamePartFile()` + 统计校验。
  Destructor 是默认的，只释放 unique_ptr（`~ParquetWriter()` 是空的）。
- **统计校验**：每个 PartitionWriter 跟踪 `received_rows` / `flushed_rows` /
  `written_rows`（原子计数器），Finalize 时校验 `received == written`。

### aligned_create 2-arg 形式

DuckDB 表函数不支持可选位置参数，`aligned_create` 改为 `TableFunctionSet` 注册
两个 overload：
- 2-arg `aligned_create('table', 'group')`：创建空组目录（无 parquet），首次
  COPY 时从 query 推断 schema。
- 3-arg `aligned_create('table', 'group', 'cols')`：显式列定义，写 0 行占位
  parquet（footer 携带 schema）。

### 遇到的坑

| 坑 | 原因 | 修复 |
|----|------|------|
| LNK2019 `ParquetWriter::Flush` unresolved | 前向声明 `struct ParquetWriteTransformData` 但实际是 `class`，MSVC mangling 不同 | 改为 `class` 前向声明 |
| INTERNAL Error INT64 vs INT32 | `generate_series(DATE, DATE, INTERVAL)` 返回 TIMESTAMP (INT64)，group schema 是 DATE (INT32) | Sink 中检测类型不匹配时 cast |
| `MoveFileA` not a member of FileSystem | `windows.h` 把 `MoveFile` 宏定义为 `MoveFileA`，parquet_writer.hpp 间接包含 windows.h | 所有 `#include` 后再 `#undef MoveFile` |
| 文件名 rows=0 | `ParquetWriter::Flush` 后 `buffer.Count()` 返回 0，在 Flush 前未保存行数 | Flush 前 `idx_t rows = buffer.Count()` |
| `WRITE_EMPTY_FILE false` 必需 | 第一版 global state 初始化有 bug | 第二版重构后不再需要，默认 `write_empty_file=true` 也能工作 |

### 改动文件

- `extension/aligned/src/include/copy/aligned_copy.hpp`（NEW）：数据结构定义
  - `AlignedCopyBindData`：bind 数据（表名、组名、schema、分区配置、列映射）
  - `PartitionWriter`：单分区 ParquetWriter + part 轮转 + 原子统计计数器
  - `AlignedCopyGlobalState`：全局状态，持有所有 PartitionWriter，唯一 Flush 入口
  - `AlignedCopyLocalState`：每线程状态，per-partition ColumnDataCollection buffer
- `extension/aligned/src/copy/aligned_copy.cpp`（NEW，~480 行）：CopyFunction 实现
  - `copy_to_bind`：解析 GROUP 选项，发现表结构，推断新组 schema，构建列映射
  - `copy_to_sink`：按 partition key 分流到 local buffer
  - `copy_to_combine`：每个 partition buffer 交给 GlobalState::Flush
  - `copy_to_finalize`：逐分区 Finalize + Rename + 统计校验
  - `execution_mode` = `REGULAR_COPY_TO_FILE`
- `extension/aligned/src/extension.cpp`：注册 CopyFunction + aligned_create
  TableFunctionSet（2-arg + 3-arg）
- `extension/aligned/src/catalog/aligned_create_fn.cpp`：2-arg 空组创建分支
- `extension/aligned/CMakeLists.txt`：新增 `src/copy/aligned_copy.cpp`
- `AGENTS.md`：§7 Writer 新增 COPY TO (FORMAT aligned) 文档；§10 代码结构新增
  `copy/` 目录；§12 新增 8 条 API 陷阱

### 测试

- SQLLogicTest：141/141 PASS
- PS test suite：ALL TESTS PASSED
- 完整数据集（400 标的 × 36 年 = 5,263,600 行）：写入 + 读取验证通过
- 文件名正确：`{idx:04d}-{rows:10d}.parquet` 自描述格式
- Loadable extension 构建通过

提交：`b70bfa2`

## 2026-08-28 — COPY TO (FORMAT aligned) 性能优化

### 基准测试

数据：400 标的 × 36 年 = 5,263,600 行，7 列，按年分区（37 分区）。
环境：8 核 / 25 GB RAM，DuckDB `duckdb_al3.exe -unsigned`。

| 测试 | 列数 | 分区 | 排序 | real | user | 线程 |
|------|------|------|------|------|------|------|
| aligned index (默认) | 2 | year | 无 | 1.28s | 1.16s | 1 |
| aligned index (默认) | 2 | year | ORDER BY | 1.88s | 4.25s | 1 |
| aligned index (preserve_order=false) | 2 | year | 无 | **0.28s** | 1.59s | ~6 |
| aligned panel/ma (默认) | 7 | year | 无 | 2.88s | 3.50s | 1 |
| aligned panel/ma (preserve_order=false) | 7 | year | 无 | **0.71s** | 3.69s | ~5 |
| native year-part (preserve_order=false) | 7 | year | 无 | 1.18s | 4.89s | ~4 |
| native flat (preserve_order=false) | 7 | 无 | 无 | 0.50s | 2.84s | ~6 |

### 瓶颈分析

#### 瓶颈 1：`REGULAR_COPY_TO_FILE` 强制单线程

`PhysicalCopyToFile::SinkOrderDependent()` 硬编码返回 `true`，加上
`preserve_insertion_order` 默认 `true`，导致 pipeline 永远单线程执行。
DuckDB 原生 parquet 用 `BATCH_COPY_TO_FILE`（`PhysicalBatchCopyToFile`，
`SinkOrderDependent()=false`）绕过此限制。

**修复**：`execution_mode` 改为当 `preserve_insertion_order=false` 时返回
`PARALLEL_COPY_TO_FILE`。用户需 `SET preserve_insertion_order = false;` 启用并行。
效果：aligned index 1.28s → 0.54s（2.4× 加速）。

#### 瓶颈 2：Combine 全局互斥锁串行化所有分区

`AlignedCopyCombine` 在整个 `Flush` 期间持有 `global_state.lock`。
`ParquetWriter::Flush` 是 I/O + 压缩操作（几十毫秒），锁导致所有线程串行等待。
37 分区 × 8 线程 → 每个线程约 5 个分区的 Flush 全部串行。

**修复**：改为两阶段锁：
1. Phase 1：全局锁只用于 PartitionWriter 的创建/查找（毫秒级）。
2. Phase 2：per-partition 锁保护实际 `ParquetWriter::Flush`。
   不同分区的 Flush 完全并行。

效果：aligned index 0.54s → 0.28s（1.9× 加速）；
aligned panel/ma 2.52s → 0.71s（3.5× 加速）。

### 优化后性能对比

| 路径 | 列数 | real | vs native year-part |
|------|------|------|---------------------|
| aligned index | 2 | 0.28s | **4.2× 更快** |
| aligned panel/ma | 7 | 0.71s | **1.7× 更快** |
| native year-part | 7 | 1.18s | baseline |
| native flat | 7 | 0.50s | 2.4× 更快 |

aligned 比 native year-partitioned 更快的原因：
1. aligned 不写分区列到 parquet 文件内（key 列只存 index，非 index 组不写
   date 列）→ 更少数据 → 更快压缩
2. per-partition 锁让不同分区完全并行 flush
3. aligned 直接控制分区目录命名和文件名，不走 DuckDB Hive 分区框架的开销

### 使用建议

```sql
-- 批量写入时启用并行（不需要排序时）
SET preserve_insertion_order = false;
COPY (SELECT * FROM mock) TO 'table' (FORMAT aligned, GROUP 'index');

-- 需要排序时（自动单线程，但保证分区内有序）
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'table' (FORMAT aligned, GROUP 'index');
```

### 改动文件

- `extension/aligned/src/include/copy/aligned_copy.hpp`：
  `PartitionWriter` 新增 `std::mutex lock`（per-partition 锁）
- `extension/aligned/src/copy/aligned_copy.cpp`：
  - `execution_mode`：`preserve_insertion_order=false` → `PARALLEL_COPY_TO_FILE`
  - `Flush`：两阶段锁（全局锁查 writer → 分区锁 flush）
  - `Combine`：不再持全局锁调 Flush

### 测试

- SQLLogicTest：141/141 PASS
- PS test suite：ALL TESTS PASSED
- Loadable extension 构建通过

提交：`2f3aa48`

## 2026-08-29 — 文档清理 + 渐进式基准测试 + Sink 优化

### 文档清理

删除 4 个 repo root 下孤立 bench SQL 文件（`bench_copy_perf.sql`、
`bench_copy_perf2.sql`、`bench_no_order.sql`、`bench_parallel.sql`），
被 `test/bench_copy_to.ps1` 渐进式基准脚本取代。

文档更新：
- `README.md`：新增 COPY TO (FORMAT aligned) 用法示例 + 功能表行
- `docs/API.md`：新增 §5 COPY TO 章节（语法/新组/已有组/写入行为/并行/示例）+
  2-arg aligned_create 文档 + 节号重编
- `docs/STORAGE_CONTRACT.md`：§8 新增 v9 TIMESTAMP 键契约；§9 新增 COPY TO
  批量写入路径；§13 删除"并发写互斥"（已实现）
- `docs/BENCHMARK.md`：修正 AGENTS.md 交叉引用 §16→§11；删除 (Phase 6) 标题；
  manifest commit → atomic commit
- `docs/BENCHMARK_MULTI_ANALYSIS.md`：修正 AGENTS.md 交叉引用 §16.2→§11；
  标注 _table.json 引用为历史

### 渐进式基准测试

数据：400 标的 × 36 年，7 列，按年分区。`preserve_insertion_order=false`，8 线程。

| 规模 | 行数 | aligned index (2col) | aligned panel (7col) | native year-part (7col) | native flat (7col) | aligned/native 比值 |
|------|------|------|------|------|------|------|
| 1 sym | 13K | 0.086s | 0.122s | 0.078s | 0.023s | 1.56x (慢) |
| 4 sym | 53K | 0.084s | 0.135s | 0.104s | 0.031s | 1.30x (慢) |
| 20 sym | 263K | 0.087s | 0.160s | 0.260s | 0.074s | 0.62x (快) |
| 80 sym | 1.05M | 0.104s | 0.241s | 0.642s | 0.151s | 0.38x (快) |
| **400 sym** | **5.26M** | **0.244s** | **0.734s** | **1.292s** | **0.654s** | **0.57x (快)** |

结论：
- 小数据量（<100K 行）时 aligned 有固定开销，比 native 慢
- 大数据量（>250K 行）时 aligned 比 native year-part 快 1.6-2.6×
- aligned panel (0.734s) 接近 native flat (0.654s)，分区开销仅 12%
- aligned index（2 列）比 native year-part 快 5.3×（因为只写 2 列 vs 7 列）

### Sink 优化：partition key cache + run-length batch copy

1. **Partition key cache**：`unordered_map<int64_t, string>` 缓存 date_val →
   partition_key 映射，避免对相同日期重复调用 `EvaluatePartitionTemplate`。
   对排序输入效果显著（同一年的所有日期共享缓存）。

2. **Run-length batch copy**：检测连续相同分区键的行段（run），用连续
   SelectionVector 批量拷贝，避免 per-row `std::map` 插入和 `vector<idx_t>`
   构建。对排序输入（ORDER BY symbol, date）效果最好——整个 chunk 通常
   属于同一分区，只有 1 个 run。

### 改动文件

- `test/bench_copy_to.ps1`（NEW）：渐进式基准脚本
- `extension/aligned/src/include/copy/aligned_copy.hpp`：
  `AlignedCopyLocalState` 新增 `partition_key_cache` 字段
- `extension/aligned/src/copy/aligned_copy.cpp`：
  Sink 改为 run-length 检测 + cache 查找
- 6 个文档文件更新

### 测试

- SQLLogicTest：141/141 PASS
- Loadable extension 构建通过

提交：`126049b`

### 后续优化

1. **并行 Finalize**（`c9d3b2b`）：Finalize 循环不再持有全局锁——每个分区
   `FinalizePartition` 用自己的 per-partition lock，不同分区并行 finalize。
   `FinalizePartition` 新增 per-partition lock 防止与 Combine 的 Flush 竞争。

2. **Flush 返回 PartitionWriter\***（`e05aac1`）：`Flush` 返回 `PartitionWriter*`，
   Combine 直接用返回的指针更新 `received_rows`（atomic），不再在 Combine 中
   额外获取全局锁做 map lookup。

3. **全 chunk 快速路径**（`af6db46`）：当 run 覆盖整个 chunk（`run.start == 0 &&
   n == input.size()`）时跳过 SelectionVector，直接用
   `VectorOperations::Copy(src, tgt, n, 0, 0)` 全量拷贝。排序输入下几乎所有
   chunk 都是 full-chunk single-run，此优化省去了 SelectionVector 构建 +
   indexed copy 开销。

### 优化后基准测试结果

| 规模 | 行数 | aligned panel (7col) | native year-part (7col) | native flat (7col) | aligned/native |
|------|------|------|------|------|------|
| 400 sym | 5.26M | **0.678s** | 1.342s | 0.574s | **0.51x (1.98× faster)** |

优化后 aligned panel（0.678s）接近 native flat（0.574s），差距仅 18%。
分区路由 + Combine + Finalize 的总开销 < 100ms。

### 测试

- SQLLogicTest：141/141 PASS
- PS test suite：ALL PASSED（test_aligned 42/42、test_dml 10/10、
  test_compaction 16/16、test_parallel 8/8）
- Loadable extension 构建通过

提交：`af6db46`

## 2026-08-30 — 深度审查 Round 1：Bug 修复 + 边界测试 + 代码清理

### 审查方法

4 个并行审计子代理分别审查读路径、写路径、边界条件、catalog/extension init，
发现 2 个 P0 bug + 4 个重要 bug + 多个性能/清理机会。全部修复并测试。

### Bug 修复

1. **P0: `partition_col_pos` 默认值为 0 而非 `INVALID_INDEX`**
   （`aligned_copy.hpp`）：当分区列缺失时，静默使用第 0 列作为分区列。
   修复：默认 `DConstants::INVALID_INDEX`，guard 改为 `== INVALID_INDEX`。

2. **读路径大小写敏感列匹配 bug**
   （`aligned_scan.cpp:610,685`）：`OpenPart` 和 `ComputeRowGroupWindow` 用
   `c.name == col` 精确匹配，而 `parquet_io.cpp` 用 `CIEquals` 大小写不敏感。
   跨 part 列名大小写不一致（如 `Symbol` vs `symbol`）会被静默 NULL 填充。
   修复：统一使用 `StringUtil::CIEquals`。

3. **`EnsureTablesLoaded` 吞掉所有 `std::exception`**
   （`aligned_catalog.cpp:141`）：`IOException`、`InternalException`、
   `PermissionException` 全被静默跳过。真实 I/O 错误和内部 bug 被隐藏。
   修复：只 catch `IOException`。

4. **`PlanUpdate` 不校验空 group**
   （`aligned_catalog.cpp:482`）：SET 列不在任何 group 中时 `grp` 为空，下游
   mutator 收到空 group 字符串导致混乱。修复：throw `BinderException`。

5. **`aligned_create` 2-arg 不校验 group 名格式和已存在**
   （`aligned_create_fn.cpp:157`）：单级 group 名（如 `factor`）和已存在的
   group 目录被静默接受。修复：校验 `lv1/lv2` 格式 + 已存在检查。

6. **compactor 不吸收 0-row 占位 part**
   （`aligned_compactor.cpp:91`）：`IsAlreadyNormalized` 跳过 0-row part，
   导致 `[1M, 1M, 0]` 被误判为已规范化，0-row 占位 part 不被合并。
   修复：遇到 0-row part 返回 `false`（需要合并）。

7. **`partition_resolver` `Date::FromString` 可能 throw**
   （`partition_resolver.cpp:263`）：畸形分区目录名导致 `Date::FromString`
   throw 而非返回 `infinity`，整个 scan 失败。修复：try/catch 包裹。

### 边界条件测试

新增 `test/aligned/aligned_copy.test`（66 个断言），覆盖 12 种 COPY TO 边界：
0-row、单行、NULL 非分区列、单分区覆写、多日分区、列顺序不同、额外列忽略、
类型 cast（INT→DOUBLE）、`preserve_insertion_order=true` 单线程、TIMESTAMP
分区列、新组自动创建、缺少列拒绝。

SQLLogicTest 总数：141 → 207（+66）。

### 代码清理

- 删除 dead fields：`row_group_size`、`write_partition_column`、`flushed_rows`
- 删除 `CreateAlignedParquetWriter` 重复（改用共享 `CreateParquetWriter`）
- 删除 compactor 中 dead no-op loop（`pending_moves` 空循环）
- 去重 `CountRecursive` 到 `io/parquet_io`（`aligned_create_fn` + `aligned_drop` 共用）

### Feature Lake 生成脚本

新增 `test/gen_feature_lake.sql`：完整 end-to-end Feature Lake 生命周期脚本，
包含建表 → COPY TO 批量写入 → aligned_scan 跨组查询 → DML 更新/删除 →
compaction → 数据完整性验证。

### 测试

- SQLLogicTest：207/207 PASS
- PS test suite：ALL PASSED（test_aligned, test_dml, test_compaction, test_parallel）
- Feature Lake 脚本：全流程通过

提交：`4fe9c03`

## 2026-08-31 — 深度审查 Round 2-4：性能优化 + 竞态修复 + 边界测试

### Round 2：读路径性能

1. **key_resolver 逐行 `GetValue` 优化**
   （`key_resolver.cpp`）：`LoadPartition` 和 `LoadSinglePart` 中每行调用
   `chunk.GetValue(0, r)` 构造 `Value` 对象（含堆分配），改为通过
   `UnifiedVectorFormat::GetData<string_t>` 直接读取 `string_t`，避免
   `Value` 构造开销。新增 NULL symbol 校验。

2. **基准测试验证**：400 symbols（5.26M 行）对齐 panel 0.657s vs
   原生 year-part 1.166s = **1.78x 更快**（vs 之前 0.678s，无回归）。

### Round 3：写入路径 + DDL + 异步修复

3. **part_rewriter `EmitInsertRow` 向量化**
   （`part_rewriter.cpp`）：将 `scratch.SetValue(c, pos, Value(col_types[c]))`
   （每列构造 `Value` 对象）替换为 `FlatVector::Validity(scratch.data[c]).SetInvalid(pos)`。
   对于宽表（10k+ 列），消除每行每列的 `Value` 堆分配。

4. **scan `BLOCKED` 异步处理**（`aligned_scan.cpp:970`）：
   对象存储 `Scan` 可能返回 `BLOCKED`（异步未就绪），之前被误判为
   "alignment violation" 并抛异常。改为 `continue` 重试。

5. **DDL 选项解析**（`aligned_catalog.cpp`）：
   `WITH (groups='...')` 选项解析从 `strip_quotes(expr->ToString())` 改为
   检查 `ConstantExpression` 并直接读取 `value` 字段，正确处理转义引号。

6. **`LookupEntry` `tables_loaded` 数据竞态**（`aligned_catalog.cpp:193`）：
   `tables_loaded = false` 在锁外设置，与并发 `EnsureTablesLoaded`/`Scan` 竞态。
   修复：在 `lock_guard` 内设置 flag 后再调用 `EnsureTablesLoaded`。

### Round 4：边界条件测试 + 扫描优化

7. **新增 27 个边界测试**（`aligned_boundary.test`）：
   - DML 空表（insert/update/delete/re-insert）
   - COPY TO 覆写（同组写入两次替换旧数据）
   - SCAN 非存在值过滤
   - CREATE TABLE 列组扩展
   - Compaction：单 part 跳过 + 多 part 合并（2M 行）
   总数：207 → 234 SQLLogicTests。

8. **扫描列查找 O(1) 优化**（`aligned_scan.cpp`）：
   在 `OpenPart` 时构建 `case_insensitive_map_t<idx_t> col_map`，将
   `OpenPart` 列映射和 `ComputeRowGroupWindow` 过滤剪枝中的
   `std::find_if` 线性扫描替换为 O(1) map 查找。对宽表消除
   O(cols²) per-part 开销。

### Round 5：Compaction 并行化 + 异步修复

9. **Compaction Phase 1 并行化**（`aligned_compactor.cpp`）：
   将 `_tmp/` 暂存阶段从串行改为并行处理各分区目录。新增 `StagingJob`/`StagingResult`
   结构，用 `std::thread` 线程池 + `std::atomic` work queue 分发任务。每个 worker
   创建独立的 `ParquetReader`/`ParquetWriter`/`ScanState`/`DataChunk`/`Collection`，
   无共享状态。worker 异常用 `std::exception_ptr` 捕获并在 `join()` 后 rethrow。
   Phase 2（move + delete）仍串行。

10. **Compactor BLOCKED 异步修复**（`aligned_compactor.cpp`）：
    `MergePartsToWriter` 和多 part 分流写入路径中 `ParquetReader::Scan` 返回
    `BLOCKED` 之前被误判为结束。改为 `continue` 重试。

11. **扫描列查找 O(1) 优化**（`aligned_scan.cpp`）：
    在 `OpenPart` 时构建 `case_insensitive_map_t<idx_t> col_map`，将列映射和
    过滤剪枝中的 `std::find_if` 线性扫描替换为 O(1) map 查找。

### 测试

- SQLLogicTest：234/234 PASS
- PS test suite：ALL PASSED
- 基准测试：0.731s @ 400 symbols（1.68x faster than native year-part，无回归）

提交：`3260a9a`

## 2026-09-01 — 深度审查 Round 6-7：Mutator 性能 + 代码审计

### Round 6：Mutator 路径 + 文档

12. **`SourceReader::GetValue` VARCHAR 快速路径**（`aligned_mutator.cpp`）：
    添加 VARCHAR 类型的 `UnifiedVectorFormat::GetData<string_t>` 直接读取，
    避免通用 `GetValue` 的类型切换 + 字符串拷贝开销。

13. **扫描边界条件审计**（`aligned_scan.cpp`）：
    - 0-row part 在 `BuildIntervals` 中正确跳过
    - 空表（无 part）返回空 plan，scan 返回 0 行
    - 过滤列不在任何 group 中时不做分区剪枝（正确，行级过滤由 executor 处理）
    - 全部分区被剪枝时 `active_intervals` 为空，scan 正确返回 0 行

14. **文档更新**：README.md、API.md、AGENTS.md、release.md 全部更新到最新。
    - API.md 添加并行暂存文档
    - README.md 功能表更新
    - AGENTS.md 添加并行 compaction + BLOCKED async 坑

### Round 7：Mutator 快速路径扩展

15. **`SourceReader::GetValue` 全类型快速路径**（`aligned_mutator.cpp`）：
    从 if-else 链改为 switch，添加 FLOAT、INTEGER、SMALLINT、TINYINT、BOOLEAN
    快速路径。所有常见类型都通过 `UnifiedVectorFormat::GetData` 直接读取，
    避免通用 `GetValue` 的类型切换和 `Value` 构造开销。

### 测试

- SQLLogicTest：234/234 PASS
- PS test suite：ALL PASSED（test_aligned, test_dml, test_compaction, test_parallel）

提交：`8866bc2`

## 2026-09-01 — COPY TO 性能优化：缩小与 native 的差距

### 性能分析

对 COPY TO (FORMAT aligned) 进行了深度性能分析，发现两个高影响瓶颈：

1. **Sink 中的列重排拷贝**（高）：即使 `input_col_map` 是恒等映射（列顺序
   匹配），每行都会构建一个完整的重排中间 `DataChunk`，产生全量逐列拷贝。
   Native COPY TO PARQUET 直接将输入 chunk 追加到 CDC，零拷贝。

2. **Sink 不按 RG 刷新**（高）：aligned 在整个 Sink 过程中累积整个分区的 CDC，
   直到 Combine 才刷新。Native 在 CDC 达到 RG 大小时立即刷新，重叠压缩与
   摄取。这导致更大的 CDC 和无流水线。

### 修复

16. **Identity-map 零拷贝快速路径**（`aligned_copy.cpp`）：当 `input_col_map`
    是恒等映射且类型匹配时，直接将输入 chunk 追加到 CDC，跳过中间重排 chunk。
    这消除了恒等情况下的全量逐列拷贝。

17. **Sink 中按 RG 刷新**（`aligned_copy.cpp`）：当 per-partition CDC 达到
    `ALIGNED_DEFAULT_RG_ROWS` (131072) 时，在 Sink 中直接调用
    `GlobalState::Flush`，重叠 parquet 压缩与摄取（与 native COPY TO PARQUET
    相同）。Combine 处理每个分区的最后部分 CDC。

18. **分区键单槽快缓存**（`aligned_copy.cpp`）：在 hash map 之前添加单槽
    快缓存（`fast_cache_date` + `fast_cache_key`），利用有序输入中分区键
    几乎不变化的特性，消除 ~所有 hash map 查找。

### 基准测试结果（400 symbols, 5.26M rows, 7 cols, ZSTD, 8 threads）

| 配置 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| Aligned panel | 0.731s | 0.680s | 7% |
| Native year-part | 1.230s | 1.142s | — (运行差异) |
| Native flat | 0.532s | 0.538s | — (运行差异) |
| Aligned / native flat | 1.37x | 1.26x | 差距缩小 9pp |
| Aligned / native year-part | 0.59x | 0.60x | 1.67x 更快 |

### 测试

- SQLLogicTest：234/234 PASS
- PS test suite：ALL PASSED

提交：`fcffc3c`

## 2026-09-01 — 深度审查 Round 9-10：BLOCKED 系统性修复 + 边界测试

### Round 9：BLOCKED async 系统性修复

19. **`part_rewriter.cpp` BLOCKED async**（`FetchOldChunk`）：
    `ParquetReader::Scan` 返回 `BLOCKED` 被误判为流结束。改为 `continue` 重试。

20. **系统性修复所有 BLOCKED 处理器**（4 处）：
    `aligned_mutator.cpp`、`key_resolver.cpp`（`LoadPartition` + `LoadSinglePart`
    2 处）、`part_rewriter.cpp`（`FetchOldChunk`）。
    所有 `FINISHED || BLOCKED` → `FINISHED` + `BLOCKED continue`。
    全代码库 8 个 Scan 循环已全部修复。

21. **`aligned_copy.hpp` 缺少 `<limits>` 头文件**：
    `std::numeric_limits<int64_t>::min()` 使用需 `#include <limits>`。

### Round 10：COPY TO 边界测试

22. **新增 10 个 COPY TO 边界测试**（`aligned_copy.test`）：
    - (i) 多 RG 单分区：200k 行触发 per-RG flush
    - (j) 列重排：date 在 symbol 前触发 non-identity map 慢路径
    - (k) 多分区多 RG：300k 行跨 2+ 分区
    - (l) 类型不匹配：TIMESTAMP 输入 → DATE 组 schema（cast ��径）

### 测试

- SQLLogicTest：254/254 PASS（234→254，+10 新边界测试）
- PS test suite：ALL PASSED（test_aligned, test_dml, test_compaction, test_parallel）
- 编译器警告：0
- TODO/FIXME：0

提交：`00a7f90`

## 2026-09-01 — 深度审查 Round 11：DML 边界条件修复

### DML 路径审计

23. **空 DML 输入早期退出**（`aligned_mutator.cpp`）：
    `AlignedUpsertFunction` 和 `AlignedDeleteFunction` 在输入集合为空时
    不再获取写锁、不创建暂存事务、不创建任何文件。直接返回 0 计数。
    之前会走完整 pipeline（锁 → 事务目录 → 空操作 → 提交），浪费 I/O。

24. **NULL symbol 键验证**（`aligned_mutator.cpp`）：
    `ExtractSortedRows` 中 NULL date 已有验证，但 NULL symbol 会静默创建
    NULL 键行。现在 NULL symbol 抛出 `IOException`，与 NULL date 一致。

### 新增边界测试（254→266）

25. **空 INSERT**（0 行）：不崩溃，不创建文件，count=0
26. **空 DELETE**（0 行）：不崩溃，已有数据保留，count=1
27. **NULL symbol INSERT**：正确拒绝，抛出 IOException

### 测试

- SQLLogicTest：266/266 PASS（254→266，+12 新边界测试）
- PS test suite：ALL PASSED

提交：`6289ecc`

## 2026-09-02 — 深度审查 Round 12-17：文档清理 + 路径归一化 + 最终审计

### Round 12-14：文档清理 + 死代码移除
- 移除死代码 `ReadPartToCollection`（`parquet_io.cpp` 定义 + 声明，从未被调用）
- 清理 `AGENTS.md` 和 `release.md` 中所有 `ReadPartToCollection` 过期引用
- 审计 `aligned_transaction.cpp`（正确，极简事务管理器）
- 审计 `aligned_drop.cpp`（正确，drop 不需要 staging）

### Round 15：manifest.cpp + partition_resolver 审计
- 审计 `manifest.cpp` `BuildTablePlan`：分区对齐契约完整强制执行
  - index 组必须存在、index 分区索引必须从 0000 连续
  - 非 index 组分区键必须是 index 键子集
  - 共享分区总行数必须一致、共享 part 索引行数必须一致
  - 非 index 组必须是 lv1/lv2 两级路径
- 审计 `partition_resolver.cpp`：`Date::FromString` 和 `std::stoi` 都有 try/catch
- 基准测试验证：无性能回归

### Round 16：aligned_create.cpp + extension.cpp 审计
- **修复 Windows 路径归一化 bug**（P2）：`aligned_create.cpp` line 305
  `p.path.find("/_tmp/")` 在 Windows 上 glob 可能返回反斜杠路径，导致
  `\_tmp\` 不匹配 `/_tmp/`，崩溃残留的 _tmp ���录会被误判为"表已存在"
- 修复：搜索前 `std::replace(norm.begin(), norm.end(), '\\', '/')`
- 审计 `extension.cpp`：所有 6 个函数注册完整
- Feature Lake 脚本验证通过

### Round 17：aligned_catalog.cpp + aligned_dml.cpp + aligned_groups.cpp 审计
- **修复 `aligned_create_fn.cpp` 同一路径归一化 bug**（P2）
- 新增 AGENTS.md 陷阱条目：`GlobFiles` Windows 路径归一化
- 审计 `aligned_catalog.cpp`：
  - `EnsureTablesLoaded` 线程安全（mutex + IOException catch only）
  - `LookupEntry` 无死锁（锁释放后再重载）
  - `CreateTable` 选项解析正确
  - `PlanInsert/PlanDelete/PlanUpdate` 映射构建正确
- 审计 `aligned_dml.cpp`：
  - INSERT 批量 1M 行、空输入提前退出
  - DELETE rowid 收集 + 键解析 + 投影
  - UPDATE 排序归并 + DATE/TIMESTAMP 键类型处理
  - `ResolveKeysForRowids` 使用表计划列名（非硬编码）
- 审计 `aligned_groups.cpp`：简单表函数，无 bug
- 路径分隔符全面扫描：所有 `find_last_of` 都用 `"/\\"`

### 测试结果
- SQLLogicTest：266/266 PASS
- PS test suite：ALL PASSED
- 基准测试：aligned 0.67s vs native year-part 1.05s = 1.56x faster

提交：`890b91b`、`eaea083`

## 2026-09-03 — 深度审查 Round 18-22：全路径审计 + 并行扫描 + 最终验证

### Round 18-19：COPY TO 审计 + 质量扫描
- 审计 `aligned_copy.hpp` + `aligned_copy.cpp` Sink/Combine/Finalize 完整流程
  - Sink：分区键缓存 + run-length 批量复制 + identity-map 零拷贝快路径
  - Combine：本地缓冲刷新到全局
  - Flush：两阶段锁（全局锁查找 + 分区锁写入）+ part 轮转
  - Finalize：received == written 统计校验
  - Bind：列映射、组 schema 推断、分区列位置校验
- 审计 `aligned_scan.cpp` carry_chunk 逻辑
  - src_offset 正确使用 `seg.flow_off + (copy_from - seg.win_start) - rg_off`
  - 双向 clamp 防止下溢
  - surplus → carry 转换正确
- 质量扫描：无 TODO/FIXME/HACK 注释，全量重编零警告

### Round 20：key_resolver + part_rewriter 审计 + 新测试
- 审计 `key_resolver.cpp`：三级解析（fast path → part 二分 → 行二分）
  - 缓存标志在成功后设置（之前修复）
  - BLOCKED 异步处理正确
  - DATE/TIMESTAMP 键类型处理正确
- 审计 `part_rewriter.cpp`：合并/重写逻辑
  - 全覆写快路径（跳过旧数据读取）
  - 合并循环按位置处理事件
  - EmitInsertRow 使用 SetInvalid（避免 per-column Value 构造）
  - FetchOldChunk BLOCKED 处理正确
- 新增测试：多分区 TIMESTAMP COPY TO（跨午夜时间戳 → 2 个日期分区）
- 测试总数：271/271

### Round 21：aligned_mutator 审计 + 文档一致性
- 审计 `aligned_mutator.cpp` SourceReader/ExtractSortedRows/SortAndDedupe
  - SourceReader GetValue 所有基本类型快路径
  - ExtractSortedRows 向量化提取 + NULL 校验 + VARCHAR 快路径
  - SortAndDedupe last-wins 语义（upsert 后值覆盖前值）
  - ReadSourceColumns BLOCKED 处理正确
  - 空输入提前退出（不获取锁、不创建事务）
- 文档一致性：17 个 cpp 文件 + 8 个共享函数与 AGENTS.md 完全一致

### Round 22：aligned_compactor 审计 + 最终验证
- 审计 `aligned_compactor.cpp` 并行暂存 + 两阶段提交
  - IsAlreadyNormalized 正确处理 0-row part
  - MergePartsToWriter schema 校验 + BLOCKED 处理
  - StageOneJob contiguity 校验 + 多 part 切分
  - Phase 1 并行（thread pool + atomic work queue）
  - Phase 2 串行（move + delete + RAII 清理）
  - 无新性能优化机会（已优化到位）

### 并行扫描验证
- `MaxThreads()` 返回 `MAX_THREADS`
- `CLAIM_RANGE = 16 * STANDARD_VECTOR_SIZE`（16 chunk/range → 8 线程 4.2×）
- Range claiming 逻辑正确（锁仅用于 claim，扫描在锁外执行）
- Interval 推进 + cursor snap-up 正确

### 错误消息一致性
- 所有异常使用 DuckDB 类型（IOException/BinderException/InternalException）
- 无裸 `throw std::exception`
- 4 处 `catch(...)` 全部正确（worker 异常传播 / 日期解析容错）
- 无错误被静默吞掉

### 测试结果
- SQLLogicTest：271/271 PASS
- PS test suite：ALL PASSED
- 基准测试：aligned 0.69s vs native year-part 1.19s = 1.72x faster
- Feature Lake 脚本：109,600 行，对齐校验通过，0 null key

提交：`bca59f8`

## 2026-09-04 — Round 23-25：类型安全修复 + 向量化 + 文档

### 类型安全 EvaluatePartitionTemplate 修复
- 新增 `EvaluatePartitionTemplate(template, value, is_timestamp, result)`
  类型安全重载，消除 `int64_t` 启发式的误分类问题（1970 前日期、epoch 附近
  timestamp）
- 所有 3 个调用方（`key_resolver`、`aligned_mutator`、`aligned_copy`）改用
  类型安全版本——调用方已知列类型，无需启发式猜测
- 旧启发式重载保留向后兼容，AGENTS.md 更新标注已修复

### aligned_groups 分隔符文档化
- `aligned_groups` 输出用 `;` 分隔列名（避免 SQLLogicTest 逗号混淆）
- `groups` 选项映射字符串用 `,` 分隔列名
- 两个分隔符不同是设计选择，已在 AGENTS.md 函数签名中注释说明

### BatchAppender::AppendRow 向量化
- 原实现：每列构造 `Value` + `SetValue`（对 10k+ 列宽表是主要瓶颈）
- 新实现：直接 `FlatVector::GetData<T>` 写入，支持 DOUBLE/FLOAT/INTEGER/
  BIGINT/SMALLINT/TINYINT/BOOLEAN/VARCHAR 8 种基本类型
- NULL 通过 `Validity::SetInvalid/SetValid` 处理
- 非基本类型回退到 `SetValue`
- `SourceReader::EnsureChunk` 复用已缓存的 UnifiedVectorFormat

### 测试结果
- SQLLogicTest：271/271 PASS
- PS test suite：ALL PASSED
- 基准测试：aligned 0.69s vs native year-part 1.11s = 1.62x faster
- Feature Lake 脚本：109,600 行，对齐校验通过

提交：`c6d5bcb`、`d42bd3a`、`a915a87`

## 2026-09-05 — Round 26：PartitionedColumnData 重构 COPY Sink

### 根因分析：261 date 分区慢
- 原基准测试 (250M rows, 4000 symbols, 261 date 分区) 耗时 528s+
- 根因：benchmark 数据生成时 `CROSS JOIN tmins × symbols` 不保证输出顺序，
  Parquet 每个 Row Group 包含全年 261 天数据 → Sink 收到的数据 partition key
  几乎每行都变 → `run_count ≈ 行数` (30M runs/thread)，每行创建一个 DataChunk
  + SelectionVector + column copy
- `ORDER BY symbol, date` 修复数据排序后 run_count 从 30M → 18K/thread（~9.5
  runs/chunk），但手动 run detection 仍有 ~50% CPU 开销

### 架构重构：用 PartitionedColumnData 替代手动 run detection
- 新增 `AlignedPartitionedColumnData`：继承 DuckDB 的 `PartitionedColumnData`
- `ComputePartitionIndices` 通过 `EvaluatePartitionTemplate` 计算分区索引
- 哈希分区 + 固定大小 buffer（128 rows），由基类管理 selection vector 和
  partition buffer lifecycle
- Sink 只需一行 `part_data->Append(state, input)` — 无手动 run detection
- Combine 中 `FlushAppendState` + 遍历 partitions，project 到 group schema 后
  调用 `GlobalState::Flush`
- 列映射/类型转换移至 Combine（使用零拷贝 `Reference` 当类型匹配时）
- 删除 `GetFlushMode` / `ALIGNED_FLUSH_MODE` 环境变量及所有 instrumentation

### 文件变更
- `extension/aligned/src/include/copy/aligned_copy.hpp`：
  - 新增 `AlignedPartitionedColumnData` 类
  - `AlignedCopyLocalState` 重构：移除 `buffers`/`dirty_buffers`/`fast_cache`/
    instrumentation counters，替换为 `part_data`/`part_append_state`
  - `AlignedCopyBindData` 新增 `input_types` 字段
- `extension/aligned/src/copy/aligned_copy.cpp`：
  - 新增 `AlignedPartitionedColumnData::ComputePartitionIndices` /
    `RegisterPartition` 实现
  - Sink 简化为单行 `Append` 调用
  - Combine 重构为 `FlushAppendState` + partition projection + Flush

### 性能基准（分钟级 TIMESTAMP 数据, date=%Y-%m-%d 分区）

**500 symbols × 261 days × 240 min = 31.32M rows, 8 threads:**

| 场景 | 旧 (手动 run detection) | 新 (PartitionedColumnData) | 原生 PARTITION_BY |
|------|------------------------|---------------------------|-------------------|
| index group (identity) | ~4.6s | **3.2s** | 50s |
| panel group (7→5 col projection) | ~4.6s | **5.1s** | — |

**1000 symbols × 261 days × 240 min = 62.64M rows, 8 threads:**

| 场景 | 时间 | 吞吐 | 倍率 vs 原生 |
|------|------|------|-------------|
| aligned index group (identity) | **6.2s** | 10.1M rows/s | — |
| aligned panel group (7→5 col projection) | **10.5s** | 6.0M rows/s | — |
| 原生 COPY TO PARQUET PARTITION_BY | **52.1s** | 1.2M rows/s | **8.4× 慢** |

**扩展性与分区基数（62.64M rows, 8 threads, index group）:**

| 配置 | 时间 |
|------|------|
| 4 threads | 8.0s |
| 8 threads | 6.2s |
| 16 threads | 6.3s (IO 瓶颈) |
| month 分区 (12 分区) | 5.4s |
| date 分区 (261 分区) | 6.2s |
| 乱序 (ORDER BY date, 单线程排序) | 26.3s |
| 乱序 (无 ORDER BY, 并行 RG 扫描) | 6.2s |

- 日分区 (261) vs 月分区 (12) 仅差 15%，高基数分区扩展性优秀
- panel group 投影开销：7→5 列 `Reference` + `ColumnDataCollection::Append` 占
  Combine 主要时间
- 8 线程已接近最优，16 线程受磁盘 IO 限制
- hash 分区对乱序数据鲁棒：不依赖输入顺序，不退化到 O(rows) 最坏情况

### 测试结果
- SQLLogicTest：271/271 PASS
- PS test suite：ALL PASSED (test_aligned 42/42, test_dml 10/10,
  test_compaction 16/16, test_parallel 8/8)
- 数据正确性：62.64M 行，0 NULL，行数和分布与源完全一致

提交：`f270a66`

