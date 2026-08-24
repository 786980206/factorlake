-- bench_copy_perf.sql
-- Compare COPY TO (FORMAT aligned) vs DuckDB native COPY TO PARQUET (hive partitioned)

LOAD 'aligned';
SET aligned_data_root = 'bench_data';

-- =============================================================
-- Setup: create mock data (same for both paths)
-- 5,263,600 rows = 400 symbols × 13,159 days (1990-01-01 ~ 2026-01-10)
-- =============================================================

CREATE TEMP TABLE mock AS SELECT
    s.symbol,
    d.date,
    ROUND(100 + RANDOM() * 50, 2) AS o,
    ROUND(105 + RANDOM() * 50, 2) AS h,
    ROUND(95  + RANDOM() * 50, 2) AS l,
    ROUND(100 + RANDOM() * 50, 2) AS c,
    ROUND(100 + RANDOM() * 50, 2) AS v
FROM
    UNNEST(generate_series('1990-01-01'::DATE, '2026-01-10'::DATE, INTERVAL 1 DAY)) AS d(date)
CROSS JOIN
    UNNEST(ARRAY(SELECT LPAD(i::VARCHAR, 6, '0') FROM generate_series(1, 400) AS t(i))) AS s(symbol);

SELECT count(*) AS total_rows, count(DISTINCT symbol) AS symbols,
       min(date) AS min_date, max(date) AS max_date FROM mock;

-- =============================================================
-- Test 1: COPY TO (FORMAT aligned) — index group (2 cols: symbol+date)
-- =============================================================

SELECT * FROM aligned_create('bench_copy', 'index', 'symbol VARCHAR, date DATE',
                             partition_template => 'year=%Y');

.timer ON
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'bench_copy'
    (FORMAT aligned, GROUP 'index');
.timer OFF

-- =============================================================
-- Test 1b: Aligned overwrite (same data, second time — measures overwrite cost)
-- =============================================================

.timer ON
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'bench_copy'
    (FORMAT aligned, GROUP 'index');
.timer OFF

-- =============================================================
-- Test 2: COPY TO (FORMAT aligned) — panel/ma group (7 cols, all data cols)
-- =============================================================

SELECT * FROM aligned_create('bench_copy', 'panel/ma');

.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock ORDER BY symbol, date) TO 'bench_copy'
    (FORMAT aligned, GROUP 'panel/ma');
.timer OFF

-- =============================================================
-- Test 3: DuckDB native COPY TO PARQUET (hive partitioned by year)
-- Native PARTITION_BY creates "year=YYYY" directories, writes parquet
-- files inside. WRITE_PARTITION_COLUMNS defaults false (partition col
-- not written inside parquet — same as aligned non-index groups).
-- We include year_part in the SELECT for PARTITION_BY to find it.
-- =============================================================

.timer ON
COPY (SELECT symbol, date, o, h, l, c, v, strftime(date, 'year=%Y') AS year_part
      FROM mock ORDER BY symbol, date)
    TO 'bench_native_year'
    (FORMAT PARQUET, PARTITION_BY (year_part), OVERWRITE_OR_IGNORE true,
     COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF

-- =============================================================
-- Test 4: DuckDB native COPY TO PARQUET (flat, no partitioning)
-- =============================================================

.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock ORDER BY symbol, date)
    TO 'bench_native_flat'
    (FORMAT PARQUET, OVERWRITE_OR_IGNORE true,
     COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF

-- =============================================================
-- Verification
-- =============================================================

SELECT 'aligned index' AS source, count(*) AS rows FROM aligned_scan('bench_copy');
SELECT 'native year-part' AS source, count(*) AS rows
    FROM read_parquet('bench_native_year/**/*.parquet', hive_partitioning = true);
SELECT 'native flat' AS source, count(*) AS rows
    FROM read_parquet('bench_native_flat.parquet');
