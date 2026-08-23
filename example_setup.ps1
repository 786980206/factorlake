# example_setup.ps1 — 创建 FactorLake 示例数据库
# 生成分钟级行情数据: NSYM 标的 × NDAYS 交易日 × 240 根 K 线/天
# 包含 5 个 column group: index(sym,dt) + OHLC + MA + EMA + KDJ, 按日分区
# 用法: powershell -ExecutionPolicy Bypass -File example_setup.ps1
$ErrorActionPreference = 'Stop'
# 脚本所在目录 (兼容 pwsh -File 和 dot-source)
if ($PSCommandPath) { $root = Split-Path -Parent $PSCommandPath }
elseif ($PSScriptRoot) { $root = $PSScriptRoot }
else { $root = $PWD.Path }
$duckdb = Join-Path $root 'duckdb\build3\duckdb_al3.exe'          # DuckDB 二进制
$dir    = Join-Path $root 'example'                               # 输出目录

# ── 参数 (可按需调整) ─────────────────────────────────────────
$NSYM   = 500     # 标的数量 (8000 → 全量约 42M 行; 2000 → ~10M 行; 500 → ~2.6M 行)
$NDAYS  = 22      # 交易日数
$DBASE  = '2025-06-02'  # 起始日期

if (-not (Test-Path $duckdb)) { throw "找不到 DuckDB: $duckdb (请先运行 scripts\build.ps1)" }

# ── 1. 创建 example 目录 ──────────────────────────────────────
if (Test-Path $dir) { Remove-Item -Recurse -Force $dir }
New-Item -ItemType Directory -Path $dir | Out-Null

# ── 2. 构建 SQL: 建表 + 批量生成数据 ─────────────────────────
# 表结构: index(sym,dt) + 4 个 column group, 按日分区
# 数据: 每标的每天 240 根 1 分钟 K 线 (09:31-11:30, 13:01-15:00)
$sql = @"
SET aligned_data_root='$dir';
ATTACH '$dir' AS al (TYPE ALIGNED);

CREATE TABLE al.minute_bars (
    sym VARCHAR, dt TIMESTAMP,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT, amount DOUBLE,
    ma5 DOUBLE, ma10 DOUBLE, ma20 DOUBLE, ma60 DOUBLE,
    ema12 DOUBLE, ema26 DOUBLE, macd DOUBLE,
    k_val DOUBLE, d_val DOUBLE, j_val DOUBLE
) WITH (
    groups='index:sym,dt;quote/ohlc:open,high,low,close,volume,amount;indicator/ma:ma5,ma10,ma20,ma60;indicator/ema:ema12,ema26,macd;indicator/kdj:k_val,d_val,j_val',
    partition_template='date=%Y-%m-%d'
);

INSERT INTO al.minute_bars
WITH params AS (SELECT $NSYM AS nsym, DATE '$DBASE' AS dbase),
sym AS (
    SELECT printf('%06d', s) AS sym, s AS sid FROM range((SELECT nsym FROM params)) t(s)
), days AS (
    SELECT day FROM (
        SELECT DATE '$DBASE' + d * INTERVAL '1 day' AS day
        FROM range(60) t(d)
    ) WHERE extract(dow FROM day) BETWEEN 1 AND 5
    ORDER BY day LIMIT $NDAYS
), bar AS (
    SELECT CASE WHEN m < 120
         THEN INTERVAL '9 hours 31 minutes' + m * INTERVAL '1 minute'
         ELSE INTERVAL '13 hours 1 minute' + (m - 120) * INTERVAL '1 minute'
    END AS t FROM range(240) t(m)
), raw AS (
    SELECT s.sym, s.sid, d.day::TIMESTAMP + b.t AS dt,
           5.0 + (s.sid % 95) AS bp
    FROM sym s CROSS JOIN days d CROSS JOIN bar b
)
SELECT sym, dt,
    bp + random()*0.5            AS open,
    bp + random()*1.0            AS high,
    bp - random()*1.0            AS low,
    bp + (random()-0.5)*0.8      AS close,
    (500+random()*99500)::BIGINT  AS volume,
    bp*(500+random()*99500)      AS amount,
    bp+(random()-0.5)*0.3       AS ma5,
    bp+(random()-0.5)*0.5       AS ma10,
    bp+(random()-0.5)*0.8       AS ma20,
    bp+(random()-0.5)*1.2       AS ma60,
    bp+(random()-0.5)*0.4       AS ema12,
    bp+(random()-0.5)*0.6       AS ema26,
    (random()-0.5)*4.0          AS macd,
    random()*100                 AS k_val,
    random()*100                 AS d_val,
    random()*100                 AS j_val
FROM raw;
"@

# ── 3. 执行 SQL (写入 parquet 列组) ──────────────────────────
$totalRows = $NSYM * $NDAYS * 240
Write-Host "生成数据: $NSYM 标的 x $NDAYS 交易日 x 240 K 线/天 = $totalRows 行 ..."
$tmp = [System.IO.Path]::GetTempFileName()
Set-Content -Path $tmp -Encoding UTF8 -Value $sql
$sw = [System.Diagnostics.Stopwatch]::StartNew()
cmd /c "`"$duckdb`" -csv -noheader < `"$tmp`"" 2>&1 | Out-Null
$sw.Stop()
Remove-Item $tmp -Force

# ── 4. 验证结果 ────────────────────────────────────────────────
$probeSym = '{0:D6}' -f [int]([math]::Floor($NSYM / 2))  # 取中间标的做跨组查询
$chk = @"
SET aligned_data_root='$dir';
ATTACH '$dir' AS al (TYPE ALIGNED);
SELECT group_name, columns, partition_count FROM aligned_groups('minute_bars', root='$dir');
SELECT count(*) AS rows, count(DISTINCT sym) AS syms, count(DISTINCT dt::DATE) AS days FROM al.minute_bars;
SELECT min(dt)::VARCHAR||' ~ '||max(dt)::VARCHAR AS date_range FROM al.minute_bars;
-- 跨组查询: 从 index + quote/ohlc + indicator/ma + indicator/kdj 取数据
SELECT sym, dt::VARCHAR AS dt, open, high, low, close, ma5, ma20, k_val, d_val
FROM al.minute_bars WHERE sym='$probeSym' AND dt::DATE=DATE '$DBASE' ORDER BY dt LIMIT 3;
"@
$tmp2 = [System.IO.Path]::GetTempFileName()
Set-Content -Path $tmp2 -Encoding UTF8 -Value $chk
Write-Host "`n--- 验证结果 ---"
cmd /c "`"$duckdb`" -csv -noheader < `"$tmp2`"" 2>&1
Remove-Item $tmp2 -Force

# ── 5. 目录统计 ────────────────────────────────────────────────
$pqCount = (Get-ChildItem $dir -Recurse -Filter "*.parquet").Count
Write-Host "`nParquet 文件数: $pqCount"
Write-Host "数据目录: $dir"
Write-Host "总行数: $totalRows"
Write-Host "耗时: $([math]::Round($sw.ElapsedMilliseconds/1000, 1))s"
