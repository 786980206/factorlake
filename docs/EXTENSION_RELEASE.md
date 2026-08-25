# DuckDB 扩展发布 / 安装机制（研究结论 + 实施记录）

> 2026-08 · 基于 DuckDB **v1.5.5 源码实证**（`src/main/extension/extension_install.cpp`、
> `extension_load.cpp`、`extension/extension_build_tools.cmake`）与本地全链路验证。
> 目标场景：仓库迁移 GitHub 后，打 tag → GitHub Actions 构建 → GitHub Release 发布，
> 用户通过 `duckdb -unsigned -c "INSTALL '<release-url>'"` 安装使用。

## 1. 结论（TL;DR）

- `INSTALL '<直接 URL>'` 语法 **可行**（含 `http://` 和 `https://`，不需要搭建仓库结构）。
- 扩展为 **unsigned**（无官方签名）→ 用户必须 `duckdb -unsigned` 才能 INSTALL 和 LOAD。
- 扩展内嵌 **DuckDB engine version**（v1.5.5），加载时强校验与 CLI 版本一致（版本不匹配会报错）。
- 本仓库扩展**自包含**（静态链接 DuckDB 核心 + parquet 扩展源码）→ 用户**无需**先装 parquet。
- 本地已全链路验证：官方 CLI v1.5.5 + `-unsigned` + `INSTALL 'D:/.../aligned.duckdb_extension'`
  + `LOAD aligned` + `SELECT * FROM aligned_scan('writetest')` → 6000 行、mis=0 ✓；
  不带 `-unsigned` → `IO Error: ... doesn't have a valid signature`（按预期拒绝）。

## 2. INSTALL 的三种输入（v1.5.5 源码路径）

判定函数 `ExtensionHelper::IsFullPath`：**含 `.` 或 `/` 或 `\` 即视为完整路径**
（`extension_load.cpp:588`）。

| 输入 | 分支 | 说明 |
|------|------|------|
| `INSTALL 'http://...'` | `InstallFromHttpUrl` | 专用路径，**不需要 httpfs** 扩展 |
| `INSTALL 'https://...'` | `DirectInstallExtension` | 自动 autoload httpfs |
| `INSTALL '本地路径'` | `DirectInstallExtension` | 文件复制安装 |
| `INSTALL name FROM 'repo-url'` | `InstallFromRepository` | URL 模板 = `${repo}/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension[.gz]` |
| `INSTALL name`（无 repo） | 默认 core 仓库 | `extensions.duckdb.org` |

- `${REVISION}` = `GetVersionDirectoryName()` = `v1.5.5`；`${PLATFORM}` = `DuckDB::Platform()`
  （本机 `windows_amd64`）。
- **GitHub Release 资产平铺、无目录结构** → 放不了 `v1.5.5/windows_amd64/` 布局 →
  **repository 方式不适合 GitHub Release，直接 URL 安装是正解**。
- `FORCE INSTALL` 可覆盖已安装版本；本地缓存目录：
  `~/.duckdb/extensions/<version>/<platform>/<name>.duckdb_extension`（含 `.info` metadata）。
- `LOAD '<url>'` 同样支持完整路径（`extension_load.cpp:376`）。

## 3. 三个硬约束

1. **签名**：`WriteExtensionFileToDisk` / LOAD 均校验 `AllowUnsignedExtensionsSetting`
   （`extension_install.cpp:226`）；未开启时无签名/签名无效 →
   `IO Error: Attempting to install an extension file that doesn't have a valid signature`。
   → CLI 必须 `duckdb -unsigned`（或 `SET allow_unsigned_extensions=true`）。
2. **版本强校验**：扩展二进制 footer 内嵌 `DUCKDB_NORMALIZED_VERSION`（`append_metadata.cmake`），
   加载时 `engine_version != duckdb_version` 直接拒绝
   （`src/main/extension.cpp`）。→ **必须用与目标 CLI 相同的 DuckDB 版本构建**。
3. **依赖自包含**：官方发布用 `EXTENSION_STATIC_BUILD=1`（DuckDB 核心静态链入扩展，
   `extension_build_tools.cmake:135`）。本项目额外把 `parquet_extension` 静态库 +
   `duckdb_mbedtls` + `duckdb_zstd` 链入（ParquetReader 复用）→ 单文件
   `aligned.duckdb_extension`（Windows 实测 24.3 MB）**无需预装 parquet**。

## 4. 已落地改造（本次实施）

### 4.1 `extension/aligned/CMakeLists.txt`

```cmake
build_static_extension(aligned ${ALIGNED_EXTENSION_FILES})
build_loadable_extension(aligned "" ${ALIGNED_EXTENSION_FILES})
target_link_libraries(aligned_loadable_extension parquet_extension duckdb_mbedtls duckdb_zstd)
```

> 坑：`build_loadable_extension` 的第二个参数是 `PARAMETERS`（parquet 传 `-warnings`）。
> 直接 `build_loadable_extension(aligned ${FILES})` 会把第一个源文件（extension.cpp）当作
> PARAMETERS 吃掉 → 产物缺入口函数，LOAD 报
> `did not contain the expected entrypoint function`。必须显式传空串。

### 4.2 `extension/aligned/src/extension.cpp` 末尾加入口

```cpp
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(aligned, loader) {
	duckdb::AlignedExtension ext;
	ext.Load(loader);
}
}
```

> 与官方 extension-template（v1.5）写法一致；静态链接构建走 generated loader，不调用它。
> 入口签名 = `void aligned_duckdb_cpp_init(duckdb::ExtensionLoader &loader)`。

### 4.3 构建命令（Windows，产出可加载二进制）

```powershell
cmd /c "call ""<vcvars64.bat>"" && cmake -S . -B build-rel -G Ninja -DCMAKE_BUILD_TYPE=Release ^
  -DEXTENSION_STATIC_BUILD=1 -DDUCKDB_EXTENSION_CONFIGS=D:/proj/factorlake/scripts/aligned_extension_config.cmake"
cmd /c "call ""<vcvars64.bat>"" && ninja -C build-rel aligned_loadable_extension"
# 产物：build-rel/extension/aligned/aligned.duckdb_extension
```

### 4.4 `.github/workflows/release.yml`

- 触发：`push tags: v*`
- `build-windows`（windows-latest + ilammy/msvc-dev-cmd + choco ninja）、
  `build-linux`（ubuntu-latest + apt ninja）：clone duckdb v1.5.5（--recurse-submodules）+
  本仓库，`EXTENSION_STATIC_BUILD=1` 构建 `aligned_loadable_extension`
- `release`（needs 前两者）：`softprops/action-gh-release` 上传
  `aligned-windows_amd64.duckdb_extension` / `aligned-linux_amd64.duckdb_extension`

## 5. 用户安装命令（发布后）

```sql
-- 一次性安装（下载到 ~/.duckdb/extensions/v1.5.5/<platform>/）
INSTALL 'https://github.com/<org>/<repo>/releases/download/v0.1.0/aligned-windows_amd64.duckdb_extension';
-- 使用
LOAD aligned-windows_amd64;   -- 扩展名 = URL 文件 base name
SET aligned_data_root='D:/data';
SELECT * FROM aligned_scan('cnstk_ixday');
```

> 说明：
> - 资产名带平台后缀时，安装后的扩展名 = base name（`aligned-windows_amd64`）。
>   想保持 `LOAD aligned`，发布两个 job 均命名 `aligned.duckdb_extension` 会资产名冲突；
>   或仅发布单平台资产 `aligned.duckdb_extension`。
> - CLI 版本必须 v1.5.5（与构建版本一致）；不带 `-unsigned` 会报签名错误。
> - 更新版本用 `FORCE INSTALL '<url>'`。

## 6. 验证矩阵（本地已完成）

| 验证项 | 结果 |
|--------|------|
| `dumpbin /exports` 含 `aligned_duckdb_cpp_init` | ✓ |
| `-unsigned` + `INSTALL` 本地路径 → `.info` metadata 生成 | ✓ |
| `LOAD aligned` + 6000 行查询（sum(rowid)=17997000, mis=0） | ✓ |
| 不带 `-unsigned` INSTALL → 签名拒绝（exit=1） | ✓ |
| 版本/ABI 校验（v1.5.5 == v1.5.5 CLI） | ✓（未触发错误） |

## 7. 待办 / 已知边界

- [ ] 实际 GitHub Release 端到端验证（需仓库已迁移 GitHub）
- [ ] Linux 构建产物验证（本机无 Linux 链路时依赖 Actions 日志）
- [ ] 未来可选项：`allow_unsigned_extensions` 的 SQL 设置路径（替代 CLI flag）、
      扩展签名（对接 DuckDB 官方签名流程）后免 `-unsigned`
- [ ] 平台名：Windows `windows_amd64`；Linux 构建产物平台名以
      `DuckDB::Platform()`（`PRAGMA platform`）实测为准（影响安装目录名，不影响 direct URL 安装）
