# 写入功能现状与开发计划

> 2026-08 整体审视。写入主代码：`extension/aligned/src/writer/aligned_writer.cpp`
> （627 行）+ `compaction/aligned_compactor.cpp`（390 行）。
> Phase 5（Writer）/ Phase 7（Compaction）已完成第一版并通过验收；
> 本文档是下一步写入能力建设的路线图。

## 1. 现状（已交付）

```
aligned_write(table, source_path, mapping, root=..., start_row=...)
  mapping: "group:col1,col2;group2:col3"   （文件列序 = 写入列序）
```

- 单线程、append-only、immutable part；写入必须从当前表尾开始
  （`start_row == plan.row_count`，防重复写）
- **对齐契约强制**：mapping 必须覆盖全部 group（写前模拟 ValidateRowSpace，
  漏写 group 直接报错）
- 按**分区值变化**切 part（字典编码列走 UnifiedFormat）；part 名 =
  目标目录下一个空闲 `part-%06llu`（`NextPartIndex` 扫描目录）
- 每 group 一个 `ColumnDataCollection` 缓冲，满 `rg_rows`（默认 16384）flush
  Row Group；ZSTD 压缩；写入 parquet 构造参数与 parquet 扩展默认值一致
- **原子提交**：全部写入 `<table>/_tmp/transaction-<txid>/` →
  成功后逐 part move 到目标目录 → 重写 `_table.json`
  （last_txid+1，partitioning 原样写回）→ 清理 `_tmp`；失败删除暂存树
  （`_tmp` 对 Reader 永不可见，`HasIgnoredPathSegment` 过滤 `.`/`_` 段）
- `aligned_compact(table, group)`：**单事务合并所有组**（保持组间 part 数一致，
  v4 全对齐契约要求）；group 参数（`'all'` 或任意已存在组名）仅作校验、不限制
  范围。按分区目录合并多 part → 单 part（同目录必须同列集，拒绝 schema-evolution
  合并），原子切换 + 删旧文件

## 2. 已知问题与缺口（按优先级）

### W-1. 只支持单文件 source（真实场景是多文件/目录）

`source_path` 只能是一个 parquet 文件。日频增量数据通常是
`stage/2026-08-17/part-*.parquet` 多文件或 Hive 布局目录。
**需求**：source 支持目录（glob `**/*.parquet` → 按路径排序拼接，逐文件
顺序扫描），文件间行序 = 相对路径字符串序（与读端 part 排序规则一致）。

### W-2. 单分区内 part 无大小上限（part_rows 未生效）

manifest 的 `part_rows`（默认 4194304）只是 hint，writer 按"分区值变化"切
part：一个分区数据量超过 part_rows 时**一个 part 无限增长**。bench 结论
"writer 应尽量大 part"是对的，但应有上限：
**需求**：同分区内按 `part_rows` 切多个 part（part 名递增 `part-000000`、
`part-000001`…，保持 RG 边界、part 间行连续）。注意与读端公式行区间的
兼容：`part_rows` 切分后组内 part 大小仍规则（除最后 part），不破坏
全对齐契约（组间 part 数必须一致）。

### W-3. 无并发写互斥（last_txid 竞争）

两个并发 `aligned_write` 同时从 `last_txid=5` 起算，都写 txid=6 的
`_tmp/transaction-6/` → 目录冲突/part 名冲突。当前失败模式是
"part 已存在"报错（fail-safe），但行为未定义，且后写覆盖先写状态。
**需求**：commit 前重读 `_table.json` 的 last_txid，不一致则 abort
（失败重试由调用方控制）；可选 `.lock` 文件互斥（原子 create，带 stale
检测）。**不引入分布式锁**。

### W-4. 无写后校验

写成功后不验证（row_count/列集/行序）。契约上读端有 fail-fast 兜底
（跨 group 行数不一致报错），但问题在写入侧暴露更晚。
**需求**：`aligned_write(..., validate=true)` 可选：commit 前读回 footer
（行数/列集），与预期比对；跨 group 行数一致性校验（模拟读端
BuildTablePlan 的 ValidateRowSpace）。

### W-5. 单线程写入（group 间可并行）

写入循环内 group 是串行的（每 chunk 逐 group 组装）。Parquet writer
本身顺序写；group 间无依赖 → 可并行。
**需求**：group 间并行（每个 group 一个 writer 线程，缓冲独立；
`ColumnDataCollection` 已每 group 一份）。并行度受 group 数限制；
单 group 内部保持顺序（part 内 RGS flush 依赖顺序）。

### W-6. 无增量（delta）写入路径：UPDATE/DELETE 明确不做

保持第一版决策：UPDATE/DELETE/Tombstone 不做，文档已声明。Append-only
+ Compaction 覆盖"新数据追加 + 历史合并"两个场景即可。

### W-7. 小问题

- 手拼 `_table.json` JSON（writer/compactor 各一份重复逻辑）→ 提取公共
  JSON 序列化 helper；
- `NextPartIndex` 每次目录扫描 O(parts)——可接受（目录内 part 数小）；
- `gs.rgs` 取 `plan.table.rg_rows`（默认 16384）而读端公式探测与 RGS 无关
  （footer 为准），但写入 RGS 与读端 RG 窗口性能相关（131072 更优）——
  默认值建议按基准结论调整为 131072，保持 manifest 显式值优先。

## 3. 开发计划（Phase 8 写入增强）

| 阶段 | 内容 | 工作量 | 验收 |
|------|------|--------|------|
| W-S1 | **W-1 目录源 + W-2 part_rows 上限切分** | 中 | `aligned_write('t', 'stage/2026-08/', mapping)` 多文件写入；单分区多 part 行连续、RG 边界正确、读回 count/mis 全对；test_writer 新增断言 |
| W-S2 | **W-3 并发写互斥（last_txid CAS）** | 小 | 两个并发 write 只有一个成功或先后成功（第二个重读 last_txid 后重算）；无 part 冲突 |
| W-S3 | **W-4 写后校验（validate=true）** | 小 | 校验开启时读回 footer 比对行数/列集；错误路径报错清晰 |
| W-S4 | **W-5 group 间并行写入** | 中 | 3 group 表写入 wall-time 下降（≥2×），结果与单线程逐字节一致 |
| W-S5 | **W-7 公共 JSON helper + rg_rows 默认值 131072** | 小 | 回归全 PASS |
| W-S6 | **Compaction 增强**：跨分区合并（按时间范围把多个分区目录合并成少分区目录——可选，低优先级） | 中 | 分区粒度变化后读端探测正常 |

> 约束：写入产物必须保持 v4 契约（footer 权威、无 sidecar、part 相对路径
> 排序 = part 序）；`part_rows` 切分不得破坏全对齐公式（非最后 part 等大、
> 组间 part 数一致）。每步完成更新 AGENTS.md 进度与经验，跑 test_writer.ps1 /
> test_compaction.ps1 / test_aligned.ps1 全量回归。

## 4. 与读取链路的依赖

- W-S1 的多文件行序规则 = 读端 part 排序规则（相对路径字符串序），实现时
  复用 `DeriveGroupFromPath` 的排序思路；
- W-S3 的校验可复用读端 `ValidateRowSpace` 与 footer 读取（已存在）；
- W-2 的 part_rows 切分要求组内非最后 part 等大且组间 part 数一致（全对齐），
  基准结论已确认大 part 更优——默认配置应保持 `part_rows=4194304` 级别。