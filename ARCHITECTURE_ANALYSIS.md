# Aligned Extension — Module Architecture Analysis

Analysis of `extension/aligned/src/` (catalog/, resolver/, rewriter/, mutator/, scan/, execution/, transaction/, compaction/). All file:line references are to the current source tree. No files were modified.

---

## 1. Module boundary problems (with file evidence)

### 1.1 `catalog/manifest.cpp` does far more than "manifest" — it is the plan/resolver core
The module is named "manifest" and its header `catalog/manifest.hpp:33` defines `GroupManifest`, but AGENTS.md §4 explicitly states **"无 Manifest（不做 Catalog DB）…没有 `_table.json`，没有 `_group.json`"**. The file actually:
- Discovers groups via glob and derives group names from paths (`manifest.cpp:37-59` `DeriveGroupFromPath`, `:218` glob).
- Parses part file names into index + row count (`manifest.cpp:126-145` `ParsePartFileName`).
- Computes `start_row` by cumulative file-name rows (`manifest.cpp:151-181` `AppendPartitionParts`).
- Enforces the partition-aligned contract — subset keys, equal totals, equal shared-index row counts (`manifest.cpp:361-468`).
- Reads the index group footer and enforces the v8 primary-key contract — col0=symbol, col1=DATE/TIMESTAMP (`manifest.cpp:328-355`).
- Derives partition templates from paths by calling `DerivePartitioningFromPaths` (`manifest.cpp:354`, `:465`).

So `manifest.{cpp,hpp}` is really the **table-plan builder + contract enforcer**, not a "manifest". It owns `TablePlan`/`GroupPlan`/`PartInfo`/`GroupPartition`/`PartitionTemplate` and `BuildTablePlan`. The name "manifest" is a leftover from an earlier design and misrepresents the module (see §4).

### 1.2 `resolver/row_space.cpp` is dead code
`ValidateRowSpace` is declared in `row_space.hpp:11` and defined in `row_space.cpp:7`, but a grep for `ValidateRowSpace(` across the whole tree returns **only the definition and the declaration — no caller**. The tiling check it performs (parts cover `[0, row_count)` with no gaps/overlaps) is instead done inline inside `BuildTablePlan` via the index-consecutiveness check (`manifest.cpp:302-310`) and the cross-group row-count agreement checks (`manifest.cpp:412-436`). The `resolver/row_space` module is effectively unused and its stated responsibility (contract §7 invariant validation) is silently absorbed by `catalog/manifest.cpp`.

### 1.3 `mutator/aligned_mutator.cpp` is a 1768-line god-module
`aligned_mutator.cpp` concentrates responsibilities that are arguably distinct layers:
- **Bind** for two different table functions (upsert + delete): `MutateBind` `:82-402`, plus two parallel "bind from in-memory collection" variants `BuildUpsertBindFromCollection` `:422-595` and `BuildDeleteBindFromCollection` `:625-676` — near-duplicates of `MutateBind`.
- **Source reading**: `ReadSourceColumns` `:738-781` (parquet → ColumnDataCollection) and `ReadSourceFromCollection` `:786-828` (collection → collection) — generic parquet/column IO that belongs in an IO helper, not the mutator.
- **Plan lookup helpers**: `FindPartition` `:831`, `IndexPartOffset` `:843`, `FindIndexPart` `:864`, `FindPartByPosition` `:882` — these are general `TablePlan`/`GroupPlan` queries and belong in the plan/resolver layer (next to `manifest.cpp` or a `plan_query` helper), not the mutator.
- **Cross-group append-to-last validation** `:1351-1446` — a partition-wide consistency pre-check that re-implements the resolver's `append_to_last`/`append_new_part` decision logic (see §2.5).
- **Staging + atomic commit** `ExecuteAndCommit` `:1030-1216` — file-system transaction protocol (staged writes under `_tmp/transaction-<id>/`, move into place, remove superseded parts, cleanup). This protocol is duplicated almost verbatim in `compaction/aligned_compactor.cpp:147-343` (see §2.4).

### 1.4 `execution/aligned_dml.cpp` duplicates mutator bind logic and scan plumbing
`PhysicalAlignedInsert::Finalize` calls `AlignedUpsertFromCollection` (`aligned_dml.cpp:104`), which is the right layering (delegating to the mutator). But `PhysicalAlignedDelete` and `PhysicalAlignedUpdate` each re-implement a **rowid → (symbol, date) key scan** by hand:
- `ResolveKeysForRowids` (delete version) `aligned_dml.cpp:188-273`.
- `ResolveKeysForRowids` (update version) `aligned_dml.cpp:391-467`.

These two functions are ~80 lines each and near-identical: both do `AlignedBindForCatalog` → locate `date`/`symbol` positions by name → `AlignedInitGlobal`/`AlignedInitLocal` → loop `AlignedScanFunction` → match sorted rowids against `abs_row` → append matched `(symbol, date)` to a collection. The only differences are the output column layout (`(symbol,date)` vs `(rowid,symbol,date)`) and the error string ("aligned DELETE" vs "aligned UPDATE"). This is a single helper, copy-pasted. Additionally, the delete version hardcodes `scan_names[i] == "date"/"symbol"` name matching (`aligned_dml.cpp:201-206`) instead of using the plan's `partition_source`/`symbol_column` — so it would silently break if a table's key columns were named differently (the v8 contract says col0=symbol, col1=date, but the names are not fixed to those literals).

### 1.5 `compaction/aligned_compactor.cpp` reimplements mutator's commit protocol
The compactor duplicates the staging + atomic-move + lock + txid pattern from the mutator (see §2.4) rather than calling a shared commit primitive. It also re-implements `NextPartIndex` by parsing part file names inline (`aligned_compactor.cpp:47-76`) — a third copy of the part-name parsing logic (see §2.3).

### 1.6 `scan/aligned_scan.cpp` depends on the plan but also on partition_resolver for pruning
`aligned_scan.cpp` correctly depends on `catalog/manifest.hpp` (for `TablePlan`/`GroupPlan`) and `resolver/partition_resolver.hpp` (for `PrunePartsByFilter`, `:15` include, `:364` call). This is a reasonable layering. However, the scan also re-derives the "index group is groups[0]" assumption in two places (`aligned_scan.cpp:192` `ResolveColumnTypes`, and implicitly via `group.full_coverage` at `:531`) — this invariant is established by `BuildTablePlan` (`manifest.cpp:469` inserts index at position 0) but is not encoded in the `TablePlan` struct, so every consumer re-discovers it by name match. A `TablePlan::index_group_index` field would remove this implicit coupling.

### 1.7 The "groups[0] == index" invariant is scattered and implicit
At least 6 sites assume `plan.groups[0]` is the index group without a shared accessor: `aligned_scan.cpp:192`, `key_resolver.cpp:22`, `aligned_mutator.cpp:280,1242,1609`, `aligned_create.cpp:332,484`, `aligned_catalog.cpp:393`. Each re-validates with `StringUtil::CIEquals(..., "index")`. A single `TablePlan::IndexGroup()` helper would centralize this.

---

## 2. Duplicated logic (concrete file:line pairs)

### 2.1 ParquetWriter construction — 3 copies of an identical 14-argument call
The exact same `ParquetWriter` constructor with the exact same constant arguments appears in:
- `catalog/aligned_create.cpp:29-34` (inside `CreateParquetWriter`).
- `rewriter/part_rewriter.cpp:292-296`.
- `compaction/aligned_compactor.cpp:223-227`.

All three pass `CompressionCodec::ZSTD`, `ChildFieldIDs()`, `ShreddingType()`, `vector<pair<string,string>>()`, `nullptr`, `optional_idx()`, `1073741824ULL`, `1`, `0.01`, `ZStdFileSystem::DefaultCompressionLevel()`, `ParquetVersion::V1`, `GeoParquetVersion::V1`. `aligned_create.cpp` already factored this into `CreateParquetWriter` (`:25-35`) and notes "Shared by WriteEmptyParquet and WriteNullParquet" — but the rewriter and compactor did not adopt it. A single shared `CreateAlignedParquetWriter(context, fs, path, names, types)` would eliminate the duplication.

### 2.2 Parquet read-all-columns loop — 4 copies
The "open a ParquetReader, push all column ids, GetPartitionStats, InitializeScan over all RGs, Scan loop draining into a buffer" pattern is repeated:
- `rewriter/part_rewriter.cpp:269-281` (open old part in `RewritePart`).
- `compaction/aligned_compactor.cpp:236-282` (read each old part in compaction).
- `resolver/key_resolver.cpp:50-129` (read symbol/date columns of each index part).
- `mutator/aligned_mutator.cpp:738-781` `ReadSourceColumns` (read named columns from a source parquet).

The key_resolver and mutator versions differ only in which columns they project; the rewriter and compactor versions are nearly identical (read all columns, append to a `ColumnDataCollection`, flush to writer at `rgs` rows). A shared `OpenAndScanPart(context, path, column_ids?) -> ColumnDataCollection` helper would cover all four.

### 2.3 Part-file-name parsing/formatting — 4 copies of parse, 3 of format
**Parsing `"{idx:04d}-{rows:10d}.parquet"`:**
- `catalog/manifest.cpp:126-145` `ParsePartFileName` (the canonical one — validates 15 chars, digit check, `std::stoull`).
- `compaction/aligned_compactor.cpp:54-73` inline in `NextPartIndex` (re-checks `base.size()==15`, `base[4]=='-'`, digit loop, `std::stoull(base.substr(0,4))`).
- (The mutator does not parse names — it only formats them.)

**Formatting `"{idx:04d}-{rows:10d}.parquet"` / variants:**
- `mutator/aligned_mutator.cpp:1067` `StringUtil::Format("%04llu-0000000000.parquet", t.part_index)` (staging name, rows placeholder).
- `mutator/aligned_mutator.cpp:1163` `StringUtil::Format("%04llu-%010llu.parquet", t.part_index, t.new_row_count)` (final name).
- `catalog/aligned_create.cpp:361` `StringUtil::Format("0000-%010llu.parquet", part.row_count)` (extend-mode placeholder).
- `catalog/aligned_create.cpp:434` literal `"0000-0000000000.parquet"` (new-table placeholder).
- `compaction/aligned_compactor.cpp:218` `StringUtil::Format("0000-%010llu", row_count)` (merged part, note: no `.parquet` suffix appended here, added at `:222`).

There is no single `FormatPartName(idx, rows)` / `ParsePartName(name, idx, rows)` pair shared across the codebase. The compactor's `NextPartIndex` is a particularly clear re-implementation of `ParsePartFileName` with a different return shape.

### 2.4 Staging + atomic-move + lock + txid protocol — 2 copies
`mutator/aligned_mutator.cpp:1030-1216` `ExecuteAndCommit` and `compaction/aligned_compactor.cpp:136-343` `AlignedCompactFunction` share:
- `TableWriteLock write_lock(fs, bind.plan.table_path)` — mutator `:1035`, compactor `:149`.
- `idx_t txid = NextTransactionId()` — mutator `:31` + `:1036`, compactor `:42` + `:152` (**two separate static counters** that do not share a txid space).
- `string tmp_root = ... + "/_tmp/transaction-" + to_string(txid)` — mutator `:1038`, compactor `:153`.
- `staged_dir = tmp_root + "/" + group.manifest.group + ...` — mutator `:1065`, compactor `:220`.
- Success cleanup: `RemoveDirectory(tmp_root)` then best-effort `RemoveDirectory(tmp_parent = table_path + "/_tmp")` — mutator `:1206-1215`, compactor `:312-321`.
- Failure cleanup: identical `try { RemoveDirectory(tmp_root); RemoveDirectory(tmp_parent); } catch(...) {}` — mutator `:1199-1204`, compactor `:330-342`.

This is a file-system transaction protocol that should be one RAII helper (e.g. `StagedTransaction(fs, table_path)` that acquires the lock, mints the txid, creates the tmp dir, and on destruction moves/cleans up). The `NextTransactionId` counter being duplicated means mutator and compactor txids can collide (both start at 1 and increment independently — `_tmp/transaction-1` could be created by either).

### 2.5 "Compute partition-wide max part index + 1 for append_new_part" — 3 copies
The loop "scan all groups, for each partition with key K, find the max `partition_index`, then `new_part_index = max + 1`" appears:
- `resolver/key_resolver.cpp:244-259` (fast-path append past symbol range).
- `resolver/key_resolver.cpp:330-345` (slow-path append at partition end).
- `mutator/aligned_mutator.cpp:1422-1437` (fallback when append_to_last is rejected).

All three are structurally identical: `idx_t max_index = 0; for (group) for (partition matching key) for (part) if (pk.partition_index > max_index) max_index = ...; new = max_index + 1;`. The mutator copy exists because it re-applies the resolver's decision after cross-group validation; a single `NextPartIndexForPartition(plan, key)` in the plan-query layer would serve all three.

### 2.6 NULL-fill / placeholder parquet writing — split across 3 sites
- `catalog/aligned_create.cpp:39-46` `WriteEmptyParquet` (0-row placeholder, footer carries schema) — used by CREATE TABLE `:453` and CREATE PARTITION `:536`.
- `catalog/aligned_create.cpp:52-79` `WriteNullParquet` (N-row all-NULL placeholder, for adding a new column group to an existing table) — used by extend mode `:364`.
- `mutator/aligned_mutator.cpp:1554-1583` inline "fill synthesized parts": builds an N-row all-NULL `DataChunk` with `SetAllInvalid(n)` per column, then sets keyed-row values — used for UPDATE on keys that exist in index but not in a group's partition.

The mutator's synth fill (`:1554-1583`) is a third variant of "write N all-NULL rows with some keyed values overlaid" — the same shape as `WriteNullParquet` but into a `ColumnDataCollection` with a position column instead of directly to parquet. The "all-NULL chunk with overlaid values" pattern (`SetAllInvalid` then `SetValue` for keyed rows) is not shared.

### 2.7 `BindForCatalog` + key-column-by-name lookup — duplicated in DML
`aligned_dml.cpp` calls `AlignedBindForCatalog` then locates `date`/`symbol` by literal name in two places (`:196-209` and `:398-412`). The mutator does the same lookup but uses `bind.plan.groups[0].partition_source` / `.symbol_column` (`aligned_mutator.cpp:281-282`). The DML code uses hardcoded `"date"`/`"symbol"` string literals instead of the plan's key-column fields, so it is both duplicated and subtly wrong for tables whose key columns are named differently.

### 2.8 "aligned_data_root" resolution — 3 copies
The "root = named param 'root', else `TryGetCurrentSetting("aligned_data_root")`, else BinderException" block appears in:
- `mutator/aligned_mutator.cpp:69-80` `ResolveRoot`.
- `scan/aligned_scan.cpp:294-306` (inline in `AlignedBind`).
- `compaction/aligned_compactor.cpp:92-101` (inline in `AlignedCompactBind`).

`aligned_mutator.cpp` already factored this into `ResolveRoot`; the scan and compactor did not adopt it.

### 2.9 Source-key vectorized read loop — 2 copies within the mutator
`AlignedUpsertFunction` `:1278-1332` and `AlignedDeleteFunction` `:1628-1681` contain the same vectorized chunk-scan that extracts `(partition_key, symbol, date, src_row)` into `SortedRow` rows, with the same `ToUnifiedFormat`/TIMESTAMP-vs-DATE branch and the same NULL-check-and-throw. The two differ only in which source columns they read; the loop body is otherwise identical.

### 2.10 `groups_map`→`GroupPlan` materialization for new groups — 2 copies
The block that creates a `GroupPlan` for a mapping entry not yet in the plan, setting `manifest.group`, `group_path`, and `lv1`/`lv2` by parsing the slash, appears at:
- `mutator/aligned_mutator.cpp:153-167` (empty-table first write).
- `mutator/aligned_mutator.cpp:184-196` (non-empty table, schema evolution).
- `mutator/aligned_mutator.cpp:469-479` (in `BuildUpsertBindFromCollection`).

Three copies of the same ~10-line `GroupPlan` constructor from a mapping key, within one file.

---

## 3. Suggested shared-module extractions (concrete)

### 3.1 Extract `io/parquet_io.{cpp,hpp}`
- `CreateAlignedParquetWriter(context, fs, path, names, types)` — replaces `aligned_create.cpp:25-35`, `part_rewriter.cpp:292-296`, `aligned_compactor.cpp:223-227`.
- `WriteEmptyParquet(context, fs, path, names, types)` — move from `aligned_create.cpp:39-46`.
- `WriteNullParquet(context, fs, path, names, types, row_count)` — move from `aligned_create.cpp:52-79`.
- `ReadPartToCollection(context, path, col_names?) -> ColumnDataCollection` — consolidates `aligned_mutator.cpp:738-781` `ReadSourceColumns`, the read loop in `aligned_compactor.cpp:236-282`, and the read-all-columns path in `part_rewriter.cpp:269-281`. The key_resolver's two-column read (`key_resolver.cpp:50-129`) is a projected-columns specialization.
- `OpenAndInitializeScan(context, reader, out scan_state)` — consolidates the `GetPartitionStats` + build `all_rgs` + `InitializeScan` triplet repeated at `part_rewriter.cpp:273-279`, `aligned_compactor.cpp:257-264`, `key_resolver.cpp:77-84`, `aligned_mutator.cpp:757-764`.

### 3.2 Extract `catalog/part_name.{cpp,hpp}` (or fold into `manifest.hpp`)
- `bool ParsePartName(const string &file_name, idx_t &index, idx_t &rows)` — canonical, replaces `manifest.cpp:126-145` and `aligned_compactor.cpp:54-73`.
- `string FormatPartName(idx_t index, idx_t rows)` — replaces the 5 format-string sites in §2.3.
- `idx_t NextPartIndexInDir(fs, dir)` — replaces `aligned_compactor.cpp:47-76`.

### 3.3 Extract `mutator/staged_transaction.{cpp,hpp}` (or `transaction/staged_commit.hpp`)
A single RAII type owning the lock + txid + tmp dir:
```
class StagedTransaction {
  TableWriteLock lock;
  idx_t txid;            // single shared counter
  string tmp_root;
  FileSystem &fs;
  StagedTransaction(fs, table_path);   // lock + txid + mkdir _tmp/transaction-<id>
  string StageDirFor(group, partition_key); // mkdir + return path
  void CommitMove(staged_path -> final_path, remove_old);
  ~StagedTransaction();  // success: remove tmp_root + _tmp; failure: same
};
```
Replaces `ExecuteAndCommit`'s protocol (`aligned_mutator.cpp:1030-1216`) and `AlignedCompactFunction`'s protocol (`aligned_compactor.cpp:147-343`), and unifies the two `NextTransactionId` counters (`:31` and `:42`) into one.

### 3.4 Extract `catalog/plan_query.{cpp,hpp}` (plan navigation helpers)
- `const GroupPlan& IndexGroup(const TablePlan&)` — replaces the 6 "groups[0] is index" assumptions (§1.7).
- `const GroupPartition* FindPartition(const GroupPlan&, const string &key)` — move from `aligned_mutator.cpp:831`.
- `const PartInfo* FindIndexPart(const GroupPlan&, key, part_index)` — move from `aligned_mutator.cpp:864`.
- `const PartInfo* FindPartByPosition(const GroupPlan&, key, pos, &local)` — move from `aligned_mutator.cpp:882`.
- `idx_t NextPartIndexForPartition(const TablePlan&, key)` — replaces the 3 copies in §2.5.
- `idx_t IndexPartOffset(const GroupPlan&, key, part_index)` — move from `aligned_mutator.cpp:843`.

These are pure queries over `TablePlan`/`GroupPlan` and belong with the plan, not in the mutator.

### 3.5 Extract `resolver/key_scan.{cpp,hpp}` (rowid → keys)
- `ColumnDataCollection ResolveKeysForRowids(context, root, table, rowids, out_layout)` — consolidates the two copies in `aligned_dml.cpp:188-273` and `:391-467`. Use `plan.groups[0].partition_source`/`symbol_column` instead of hardcoded `"date"`/`"symbol"` literals to fix the latent bug in §2.7.

### 3.6 Extract `resolver/partition_template.{cpp,hpp}` (template parse/eval/validate)
Currently the three template kinds (`year=%Y`, `month=%Y-%m`, `date=%Y-%m-%d`) are handled in:
- `resolver/partition_resolver.cpp:10-57` `EvaluatePartitionTemplate` (eval).
- `resolver/partition_resolver.cpp:77-89` `SegmentValueToTemplate` (value → template).
- `catalog/aligned_create.cpp:184-195` `DefaultPartitionKey` (template → default key).
- `catalog/aligned_create.cpp:199-238` `ValidatePartitionKey` (key ↔ template).
- `catalog/aligned_create.cpp:384-390` template-format validation.
- `resolver/key_resolver.cpp:29-33` and `mutator/aligned_mutator.cpp:1221-1227` `IndexTemplate` (default `"month=%Y-%m"`).

A single module exposing `IsKnownTemplate`, `DefaultKeyForTemplate`, `ValidateKeyAgainstTemplate`, `EvaluateTemplate`, `DeriveTemplateFromValue` would let `aligned_create`, `key_resolver`, `mutator`, and `partition_resolver` all share one source of truth for the three supported kinds and the default.

### 3.7 Consolidate `mutator/aligned_mutator.cpp` bind paths
`MutateBind` (`:82-402`), `BuildUpsertBindFromCollection` (`:422-595`), and `BuildDeleteBindFromCollection` (`:625-676`) share the "BuildTablePlan → parse mapping → materialize new groups → resolve group_mapping → validate key columns → compute needed_names" spine. Factor the common spine into a `BuildMutateBind(context, table, root, mapping, is_delete, source_schema_fn)` where `source_schema_fn` is either "open parquet" or "read from collection", so the three entry points become thin wrappers.

---

## 4. Naming / organization mismatches

### 4.1 `catalog/manifest.{cpp,hpp}` is misnamed
AGENTS.md §4 says "无 Manifest". The file defines `GroupManifest` (`manifest.hpp:33`) but `GroupManifest` is just `{string group; vector<PartitionTemplate> partitioning;}` — a name+partitioning pair, not a manifest file or catalog DB. The module's real content is `TablePlan`/`GroupPlan`/`PartInfo`/`GroupPartition` and `BuildTablePlan` — a **plan builder + contract enforcer**. Better name: `catalog/plan.{cpp,hpp}` (or `catalog/table_plan.{cpp,hpp}`), with `GroupManifest` renamed to `GroupDescriptor` or folded into `GroupPlan`. The struct name `GroupManifest` and the file name `manifest.cpp` both contradict AGENTS.md's "no manifest" stance and mislead readers into looking for a persisted manifest file.

### 4.2 AGENTS.md §10 lists files that do not exist
AGENTS.md §10 prescribes:
```
catalog/  logical_table.cpp  schema.cpp  manifest.cpp  aligned_catalog.cpp  aligned_create.cpp  aligned_dml.cpp
resolver/ group_resolver.cpp  partition_resolver.cpp  row_space.cpp  key_resolver.cpp
scan/     aligned_scan.cpp  aligned_scan_state.cpp  group_scan.cpp  scheduler.cpp
mutator/  aligned_mutator.cpp
rewriter/ part_rewriter.cpp
compaction/ compactor.cpp
optimizer/ projection.cpp  filter.cpp
```
Actual tree:
- `catalog/`: has `manifest.cpp`, `aligned_catalog.cpp`, `aligned_create.cpp` — **no** `logical_table.cpp`, `schema.cpp`, or `aligned_dml.cpp` (the last is under `execution/`).
- `resolver/`: has `row_space.cpp`, `partition_resolver.cpp`, `key_resolver.cpp` — **no** `group_resolver.cpp`.
- `scan/`: has only `aligned_scan.cpp` — **no** `aligned_scan_state.cpp`, `group_scan.cpp`, `scheduler.cpp` (all folded into `aligned_scan.cpp`).
- `compaction/`: `aligned_compactor.cpp` (AGENTS.md says `compactor.cpp`).
- `optimizer/`: **does not exist at all** — projection/filter pushdown is inline in `aligned_scan.cpp` (`ResolveColumnTypes` is projection; `ApplyPartitionPruning`/`ComputeRowGroupWindow`/`ApplyRowFilters` are filter pushdown).
- `execution/`: exists (`aligned_dml.cpp`) but is **not listed** in AGENTS.md §10.
- `transaction/`: exists (`aligned_transaction.cpp`) but is **not listed** in AGENTS.md §10.

The prescribed structure is aspirational; the actual code consolidated scan-state/scheduler/group_scan into one 1280-line `aligned_scan.cpp`, dropped `group_resolver`/`optimizer` as separate files, and added `execution/`+`transaction/` not in the doc.

### 4.3 `resolver/row_space` should be removed or wired in
It is dead code (§1.2). Either delete it, or call `ValidateRowSpace` from `BuildTablePlan` (after parts are built per group) to actually enforce contract §7. Currently the tiling invariant is only partially checked (index consecutiveness + cross-group row-count equality), not the full `[0, row_count)` tiling for every group.

### 4.4 `aligned_create.cpp` mixes two distinct DDL operations
`AlignedCreateTable` (`:244`) handles **both** new-table creation (write 0-row placeholders per group) and extend-table (add a new column group, write N-row all-NULL placeholders per existing partition) in one function, branching on `table_exists` (`:296`). These are two different contract operations (§7 of AGENTS.md distinguishes "列组扩展" from "建表"). Splitting into `CreateNewTable` and `ExtendTableWithGroup` would clarify the two code paths (`:370-455` vs `:296-368`) and make the `WriteEmptyParquet` vs `WriteNullParquet` distinction explicit per operation.

### 4.5 `part_rewriter.hpp` is correctly scoped but underused
`RewritePart` (`part_rewriter.cpp:252`) is a clean, well-bounded "read-modify-write one part" primitive — the mutator is its only caller (`aligned_mutator.cpp:1092,1121`). The compactor does **not** use it; instead it re-implements read-all-parts + write-one-part inline (`aligned_compactor.cpp:236-285`). The compactor's merge is simpler (no inserts/updates/deletes, just concatenation), but it could still call `RewritePart` with empty inserts/updates/deletes, or a thinner `ConcatParts` helper could be extracted to share the writer + buffer + flush logic.

### 4.6 `aligned_dml.hpp`/`aligned_dml.cpp` vs AGENTS.md `catalog/aligned_dml.cpp`
AGENTS.md §10 places `aligned_dml.cpp` under `catalog/`, but the file lives in `execution/` (correctly, since it defines `PhysicalOperator` subclasses). The `PhysicalAligned*` operators are the standard-DML bridge (Phase 8b) and are correctly a separate layer from the catalog; the AGENTS.md listing is stale.

---

## Summary of highest-value extractions (priority order)

1. **`StagedTransaction` RAII** (§3.3) — unifies lock + txid + `_tmp/transaction-<id>/` + cleanup across mutator and compactor, fixes the duplicate `NextTransactionId` counters.
2. **`parquet_io` module** (§3.1) — removes 3 ParquetWriter copies and 4 parquet-read-loop copies.
3. **`part_name` module** (§3.2) — one parse + one format for the `"{idx:04d}-{rows:10d}.parquet"` contract.
4. **`plan_query` module** (§3.4) — moves `FindPartition`/`FindIndexPart`/`FindPartByPosition`/`NextPartIndexForPartition`/`IndexGroup` out of the mutator and deduplicates the `max_index+1` logic (3 copies).
5. **`key_scan` helper** (§3.5) — merges the two `ResolveKeysForRowids` in `aligned_dml.cpp` and fixes the hardcoded `"date"`/`"symbol"` bug.
6. **Rename `manifest` → `plan`** (§4.1) — aligns the module name with AGENTS.md's "no manifest" and the file's actual content.
7. **Delete or wire `row_space`** (§4.3) — removes dead code or actually enforces contract §7.
