-- bench_no_order.sql
LOAD 'aligned';
SET aligned_data_root = 'bench_data2';

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

SELECT * FROM aligned_create('bench_copy', 'index', 'symbol VARCHAR, date DATE', partition_template => 'year=%Y');

-- Aligned WITHOUT ORDER BY — still REGULAR_COPY_TO_FILE (single thread)
.timer ON
COPY (SELECT * FROM mock) TO 'bench_copy' (FORMAT aligned, GROUP 'index');
.timer OFF

-- Aligned WITH ORDER BY
.timer ON
COPY (SELECT * FROM mock ORDER BY symbol, date) TO 'bench_copy' (FORMAT aligned, GROUP 'index');
.timer OFF

-- Native partitioned WITHOUT ORDER BY (should be parallel)
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v, strftime(date, 'year=%Y') AS year_part FROM mock)
    TO 'bench_native_year2' (FORMAT PARQUET, PARTITION_BY (year_part), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF

-- Native partitioned WITH ORDER BY (PARTITION_BY forces preserve_order=false, so still parallel)
.timer ON
COPY (SELECT symbol, date, o, h, l, c, v, strftime(date, 'year=%Y') AS year_part FROM mock ORDER BY symbol, date)
    TO 'bench_native_year3' (FORMAT PARQUET, PARTITION_BY (year_part), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', PARQUET_VERSION 'v1', ROW_GROUP_SIZE 131072);
.timer OFF
