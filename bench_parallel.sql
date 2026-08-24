-- bench_parallel.sql
LOAD 'aligned';
SET aligned_data_root = 'bench_data3';
SET preserve_insertion_order = false;
SET threads = 8;

CREATE TEMP TABLE mock AS SELECT
    s.symbol, d.date,
    ROUND(100 + RANDOM() * 50, 2) AS o,
    ROUND(105 + RANDOM() * 50, 2) AS h,
    ROUND(95  + RANDOM() * 50, 2) AS l,
    ROUND(100 + RANDOM() * 50, 2) AS c,
    ROUND(100 + RANDOM() * 50, 2) AS v
FROM
    UNNEST(generate_series('1990-01-01'::DATE, '2026-01-10'::DATE, INTERVAL 1 DAY)) AS d(date)
CROSS JOIN
    UNNEST(ARRAY(SELECT LPAD(i::VARCHAR, 6, '0') FROM generate_series(1, 400) AS t(i))) AS s(symbol);

SELECT * FROM aligned_create('bench_copy', 'index', 'symbol VARCHAR, date DATE', partition_template => 'year=%Y');
SELECT * FROM aligned_create('bench_copy', 'panel/ma');

-- Aligned index (2 cols) — parallel
.timer ON
COPY (SELECT * FROM mock) TO 'bench_copy' (FORMAT aligned, GROUP 'index');
.timer OFF

-- Aligned panel/ma (7 cols) — parallel
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock) TO 'bench_copy' (FORMAT aligned, GROUP 'panel/ma');
.timer OFF

-- Native year-partitioned (7 cols) — parallel
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v, strftime(date, 'year=%Y') AS year_part FROM mock)
    TO 'bench_native_year' (FORMAT PARQUET, PARTITION_BY (year_part), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF

-- Native flat (7 cols) — parallel
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v FROM mock)
    TO 'bench_native_flat.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF

-- Verify
SELECT 'aligned' AS src, count(*) AS rows FROM aligned_scan('bench_copy');
SELECT 'native_year' AS src, count(*) AS rows FROM read_parquet('bench_native_year/**/*.parquet', hive_partitioning = true);
SELECT 'native_flat' AS src, count(*) AS rows FROM read_parquet('bench_native_flat.parquet');
