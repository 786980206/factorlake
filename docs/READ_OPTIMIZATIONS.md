# 读取链路现状分析与优化计划

> 2026-08 整体审视。读取链路主代码：`extension/aligned/src/scan/aligned_scan.cpp`
> （1106 行）+ `catalog/manifest.cpp`（plan 构建）+ `resolver/*`。
> 本文档按"问题 → 影响 → 建议方案 → 优先级"组织。

## 1. 当前读取链路（一图流）

```
Bind:    BuildTablePlan           → 打开每分区最后 1 个 part 的 footer（行数/列），分区对齐校验，
                                     校验 Row Space、派生分组/分区
         ResolveColumnTypes       → 每 group 开第一个 part 读列类型（schema evolution
                                    时逐 part 找）
InitG:   projection 映射 / 收集 filters / 分区剪枝 / 区间相交
InitL:   每线程 filter state
Scan:    AlignedScanFunction
         ├─ 共享游标 claim 连续 Range（16 chunks = 32768 行，mutex 保护）
         └─ 每 chunk：对每个活跃 group 调 ScanGroupWindow
            ├─ part 定位（可回退）→ OpenPart（建 ParquetReader + 读 RG stats）
            ├─ ComputeRowGroupWindow（stats 剪枝 → rg_plan/rg_skip）
            ├─ 流读取（carry 机制复用窗口，向量 Copy 进输出 chunk）
            └─ row-level filter → Reference 输出
```

## 2. 现有优化（已落地，不要再破坏）

- 投影下推：非活跃 group 零打开；part 内只读被请求列（Phase 2）
- 分区剪枝 + RG stats 剪枝 + 行级 filter 链式 AND（Phase 3）
- 连续 Range 并行 claim + 窗口复用 + carry 深拷贝（Phase 4，recompute 373→16）
- footer/schema/RG stats 全走 ObjectCache（LRU 8GiB，默认 ON）
- `count(*)`（空 column_ids）只报 cardinality 不扫描

## 3. 发现的问题与优化项（按优先级）

### ✅ 已实施（2026-08，实测验证）

**结论修正（重要）**：初版分析认为 `filter_prune=false` 导致"过滤列未投影时
无法剪枝"（P0-A）与"index 组不活跃导致 symbol stats 剪枝失效"（P0-B）——
**两者均为误判**。读 DuckDB v1.5.4 源码（`src/optimizer/remove_unused_columns.cpp`
`RemoveColumnsFromLogicalGet`）确认：

1. 过滤列**总是**被保留在 `column_ids` 中（filter 列被注入 column_references
   防止剪除，`SetColumnIds` 无条件执行）；
2. `filter_prune` 只控制 `projection_ids`（输出时剪掉过滤列），不影响 scan
   能看到的列集合。

实测验证（`SELECT alpha001 WHERE date=...`，date 未投影）：EXPLAIN 显示
`Projections: date, alpha001` + Filters 下推 → 分区剪枝/RG 剪枝/行级过滤
**全部已生效**（count=35714=142858/4、0.028s）；`WHERE symbol='000001'`
→ index 组活跃、stats 剪枝生效（count=1）。

**已落地两项改动**：

1. **`filter_prune = true`**（extension.cpp，两个函数）：过滤列不出现在 scan
   输出中 → EXPLAIN 里多余的 PROJECTION 算子消失（`SELECT alpha001 WHERE
   date=...` 的 plan 从 `PROJECTION → ALIGNED_TABLE` 变为只有
   `ALIGNED_TABLE(Projections: alpha001)`）。scan 仍读取过滤列（隐藏读取列，
   用于剪枝 + 行级过滤），输出经 scratch + `ReferenceColumns(projection_ids)`
   修剪——该路径原有代码已完整实现，本次只是打开开关。
    验收：`test_aligned.ps1` 42 项、`test_upsert.ps1` 29 项、`test_compaction.ps1`
    14 项、`test_parallel.ps1` 全 PASS。

2. **P1-A 列类型解析复用**（`PartInfo.types`）：`ReadPartFooter` 在 plan 阶段
   已读 footer，把每列类型存入 `PartInfo.types`；`ResolveColumnTypes` 不再
   逐个 part 构造 ParquetReader 找类型（此前 schema evolution 场景每缺失列
   开一个 reader）。删除了 `first_reader` 构造。零额外 IO（footer 已读）。

**教训**：改动 `PartInfo` 等共享结构体后，**必须强制全量重编扩展**
（ninja 增量漏重编了依赖 manifest.hpp 的 writer/compactor obj → 新旧布局
混链 → 0xC0000005 崩溃）。命令：
`Remove-Item build3\extension\aligned\CMakeFiles\aligned_extension.dir -Recurse -Filter *.obj`
再 `ninja -C build3 duckdb_al3.exe`。

### P1-B. bind 阶段每个 part 都构造 ParquetReader 读 footer

**现状**（`manifest.cpp` `ReadPartFooter`）：每个 part 文件一个
`ParquetReader`（构造含 footer 解析，metadata cache 缓解了重复查询，但
冷查询/首次查询仍要全量构造）。100 万行 × 32 part 的表 bind 一次约几十 ms 级。

**方案**：
1. 依赖 cache 是现状（可接受）；
2. 中期：`part_rows`/`rg_rows` 探测所需的 footer 信息可走
   `MultiFileReader`/parquet metadata 批量读取（DuckDB 有批量 metadata 读取
   路径，但要确认 ObjectCache 命中率）；
3. 长期：目录级 `_meta.json`（每 part 一行数/列/统计摘要）——**注意 v3 契约
   已砍掉 sidecar，新缓存文件属于"性能 cache"，缺失时必须可回退到 footer**，
   与契约不冲突（cache 不是权威数据）。

**影响**：冷启动/大表首次查询。

### P1-C. count(col)/min/max 聚合 stats 快速路径（依赖 DuckDB 升级）

**现状**：`count(alpha001)` 要走完整扫描（每行读列）。Parquet footer 里已有
每 RG 的 null count / min / max——`GetPartitionStats` 已有 rg_stats，但未用。

**方案**：对 `count(col)`/`min(col)`/`max(col)` 用 footer stats 直接累加，
零数据读取。**v1.5.4 无聚合下推 API（AggregatePushdown 是 v1.6+ 引入）**——
需要 table function 侧自行识别（不可行，scan 不知道上层算子），或升级
DuckDB。当前标记为"依赖升级"的待办，不在本版本实施。

**影响**：因子列 90%+ NULL 时 `count(alpha)` 从全扫 → 零 IO（升级后）。

### P2-A. 窗口复用对"同 part 内后退"的场景仍重算

并行 claim 的非单调性（一个线程 claim [N, N+32768) 后又 claim 更小的区间）
导致 `ScanGroupWindow` 里 `cursor < parts[g.part_idx].start_row` 整 part 回退
+ 窗口重算。实测已收敛（recompute 16 次），当前影响小；若未来 claim 粒度
变小或数据分片变碎，需重审。

### P2-B. NULL-fill 逐行 SetInvalid 可向量化

`rg_skip` 填充与 `missing_positions` 填充用 `for (r...) mask.SetInvalid(r)`，
2048 行 × 每列逐行。可改为 `FlatVector::Validity(vec)` 的整段操作
（构造 ValidityMask 段置位），热路径上省几次分支。

### P2-C. `OpenPart` 每 part 重建 reader

同一 query 内一个 (part, thread) 的 `ParquetReader` 在每次 OpenPart 时重建
（reader 里 footer/schema 已 cache，但构造+GetPartitionStats 有开销）。
同 query 内投影列集固定 → 可在 lstate 缓存 (part_idx → reader)，OpenPart
命中直接复用；scan_state 仍然每次新开（契约：每 RG 窗口全新 scan_state）。
注意 ParquetReader 复用需要确认无隐藏状态（metadata 只读，风险低）。

### P2-D. 表级 `count(*)` 已走 cardinality；但 `SELECT date FROM t WHERE date<...`
范围过滤时 `ExtractPartitionDate` 每月粒度重建日期失败 → 整组不剪枝（`partition_resolver.cpp:217-219`）。这是设计取舍（month 分区无法确定 day），保持现状并在文档说明。

## 4. 建议的实施顺序

| 阶段 | 内容 | 工作量 | 状态 |
|------|------|--------|------|
| S1 | filter_prune=true（隐藏读取列机制本身已存在，打开开关即可） | 小 | ✅ 已实施 |
| S1 | P1-A 列类型解析复用（PartInfo.types） | 小 | ✅ 已实施 |
| S2 | P2-B + P2-C：NULL 填充向量化 + reader 缓存 | 小 | 待做 |
| S3 | P1-B：批量 footer 读取 / meta 缓存文件（可回退） | 中 | 待做 |
| S4 | P1-C：聚合 stats 快速路径（**依赖 DuckDB ≥ v1.6 的 AggregatePushdown**） | 中 | 待做（升级后） |

> 原则：所有优化不得破坏 v4 契约（footer 是唯一权威、无 sidecar 是唯一权威
> 语义；性能 cache 必须可回退）。每步做完更新 AGENTS.md 进度与经验。