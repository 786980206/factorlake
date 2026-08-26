-- gen_feature_lake.sql
-- Complete end-to-end Feature Lake generation script.
--
-- This script demonstrates the FULL lifecycle of a FactorLake Feature Lake:
--   1. Create the data root and logical table with multiple column groups
--   2. Bulk-load data via COPY TO (FORMAT aligned) — the primary write path
--   3. Query across column groups with aligned_scan (zero-JOIN)
--   4. Update data via standard DML (INSERT/UPDATE/DELETE)
--   5. Compact parts for optimal read performance
--   6. Verify data integrity and alignment
--
-- Data model: A stock factor lake with 100 symbols × 3 years of daily data.
--   - index group:     symbol, date (key columns, stored once)
--   - panel/price group: open, high, low, close, volume (price panel)
--   - factor/alpha101 group: alpha001..alpha010 (alpha factors, sparse)
--   - fieldset/ma group: ma5, ma10, ma20, ma60 (moving averages)
--
-- Usage:
--   duckdb_al3.exe -unsigned < gen_feature_lake.sql
-- Or:
--   powershell -Command "& 'duckdb\build\duckdb_al3.exe' -unsigned < test\gen_feature_lake.sql"
--
-- Prerequisites: duckdb_al3.exe built with the aligned extension (static or loaded).

LOAD 'aligned';

-- ============================================================
-- 0. Configuration
-- ============================================================

SET aligned_data_root = 'feature_lake';
SET threads = 8;
SET preserve_insertion_order = false;

-- ============================================================
-- 1. Create the logical table and column groups
-- ============================================================

-- 1a. Create the index group (key columns: symbol, date)
--     partition_template = year=%Y → one directory per year
SELECT * FROM aligned_create(
    'cnstk_ixday', 'index',
    'symbol VARCHAR, date DATE',
    partition_template => 'year=%Y'
);

-- 1b. Create empty groups for non-key columns (2-arg form)
--     Schema will be inferred on first COPY TO from query columns.
SELECT * FROM aligned_create('cnstk_ixday', 'panel/price');
SELECT * FROM aligned_create('cnstk_ixday', 'factor/alpha101');
SELECT * FROM aligned_create('cnstk_ixday', 'fieldset/ma');

-- Verify group structure
SELECT * FROM aligned_groups('cnstk_ixday');

-- ============================================================
-- 2. Generate source data and bulk-load via COPY TO
-- ============================================================

-- 2a. Generate mock data: 100 symbols × ~750 trading days (2023-2025)
CREATE TEMP TABLE mock AS
SELECT
    s.symbol,
    d.date,
    -- Price columns (panel/price group)
    ROUND(100 + RANDOM() * 50, 2) AS open,
    ROUND(105 + RANDOM() * 50, 2) AS high,
    ROUND(95  + RANDOM() * 50, 2) AS low,
    ROUND(100 + RANDOM() * 50, 2) AS close,
    CAST(FLOOR(1000 + RANDOM() * 10000) AS BIGINT) AS volume,
    -- Alpha factors (factor/alpha101 group) — 50% sparse
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha001,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha002,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha003,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha004,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha005,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha006,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha007,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha008,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha009,
    CASE WHEN RANDOM() > 0.5 THEN ROUND(RANDOM() * 10 - 5, 4) ELSE NULL END AS alpha010,
    -- Moving averages (fieldset/ma group)
    ROUND(100 + RANDOM() * 10, 2) AS ma5,
    ROUND(100 + RANDOM() * 15, 2) AS ma10,
    ROUND(100 + RANDOM() * 20, 2) AS ma20,
    ROUND(100 + RANDOM() * 40, 2) AS ma60
FROM
    UNNEST(generate_series('2023-01-01'::DATE, '2025-12-31'::DATE, INTERVAL 1 DAY)) AS d(date)
CROSS JOIN
    UNNEST(ARRAY(SELECT LPAD(i::VARCHAR, 6, '0') FROM generate_series(1, 100) AS t(i))) AS s(symbol);

-- Total rows
SELECT 'Total source rows' AS info, count(*) AS value FROM mock;

-- 2b. Bulk-load the index group (key columns only)
COPY (SELECT symbol, date FROM mock ORDER BY symbol, date)
    TO 'cnstk_ixday' (FORMAT aligned, GROUP 'index');

-- 2c. Bulk-load the price panel
COPY (SELECT symbol, date, open, high, low, close, volume FROM mock ORDER BY symbol, date)
    TO 'cnstk_ixday' (FORMAT aligned, GROUP 'panel/price');

-- 2d. Bulk-load the alpha factors
COPY (SELECT symbol, date, alpha001, alpha002, alpha003, alpha004, alpha005,
             alpha006, alpha007, alpha008, alpha009, alpha010
      FROM mock ORDER BY symbol, date)
    TO 'cnstk_ixday' (FORMAT aligned, GROUP 'factor/alpha101');

-- 2e. Bulk-load the moving averages
COPY (SELECT symbol, date, ma5, ma10, ma20, ma60 FROM mock ORDER BY symbol, date)
    TO 'cnstk_ixday' (FORMAT aligned, GROUP 'fieldset/ma');

-- ============================================================
-- 3. Query across column groups (zero-JOIN aligned scan)
-- ============================================================

-- 3a. Full scan: all columns from all groups
SELECT 'Full scan row count' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday');

-- 3b. Cross-group query: price + alpha + ma in one scan
SELECT date, symbol, close, alpha001, ma20
FROM aligned_scan('cnstk_ixday')
WHERE date = DATE '2024-06-15'
ORDER BY symbol
LIMIT 10;

-- 3c. Projection pushdown: only read alpha columns (only alpha101 group opened)
SELECT 'Alpha-only query' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday')
    WHERE alpha001 IS NOT NULL;

-- 3d. Partition pruning + filter: only 2024 data, only specific symbols
SELECT '2024 filtered count' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday')
    WHERE date >= DATE '2024-01-01' AND date < DATE '2025-01-01'
      AND symbol IN ('000001', '000050', '000100');

-- 3e. Aggregation across groups
SELECT
    'Cross-group stats' AS info,
    count(*) AS rows,
    count(alpha001) AS non_null_alpha,
    round(avg(close), 2) AS avg_close,
    round(avg(ma20), 2) AS avg_ma20
FROM aligned_scan('cnstk_ixday')
WHERE date >= DATE '2024-01-01' AND date < DATE '2025-01-01';

-- ============================================================
-- 4. DML: Update data via standard ATTACH
-- ============================================================

ATTACH 'feature_lake' AS al (TYPE ALIGNED);

-- 4a. INSERT a new row
INSERT INTO al.cnstk_ixday (symbol, date, open, high, low, close, volume,
    alpha001, alpha002, alpha003, alpha004, alpha005,
    alpha006, alpha007, alpha008, alpha009, alpha010,
    ma5, ma10, ma20, ma60)
VALUES ('000999', DATE '2025-12-31', 99.5, 100.0, 99.0, 99.8, 5000,
    1.5, NULL, -0.3, 2.1, NULL, 0.8, -1.2, NULL, 0.5, 0.3,
    99.5, 99.3, 99.0, 98.5);

-- 4b. UPDATE: modify a specific row
UPDATE al.cnstk_ixday SET close = 200.0 WHERE symbol = '000001' AND date = DATE '2024-06-15';

-- 4c. Verify the update
SELECT 'After UPDATE' AS info, close AS value
    FROM al.cnstk_ixday
    WHERE symbol = '000001' AND date = DATE '2024-06-15';

-- 4d. DELETE: remove a row
DELETE FROM al.cnstk_ixday WHERE symbol = '000999' AND date = DATE '2025-12-31';

-- 4e. Verify deletion
SELECT 'After DELETE' AS info, count(*) AS value
    FROM al.cnstk_ixday
    WHERE symbol = '000999';

DETACH al;

-- ============================================================
-- 5. Compact: merge parts for optimal read performance
-- ============================================================

SELECT * FROM aligned_compact('cnstk_ixday', 'all');

-- ============================================================
-- 6. Verify data integrity
-- ============================================================

-- 6a. Total row count after all operations
SELECT 'Final row count' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday');

-- 6b. Verify cross-group alignment: every row has matching key columns
SELECT 'Alignment check' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday')
    WHERE symbol IS NOT NULL AND date IS NOT NULL;

-- 6c. Verify per-year partition counts
SELECT 'Partition check' AS info, strftime(date, '%Y') AS year, count(*) AS rows
    FROM aligned_scan('cnstk_ixday')
    GROUP BY 2
    ORDER BY 2;

-- 6d. Verify no NULL keys
SELECT 'Null key check' AS info, count(*) AS value
    FROM aligned_scan('cnstk_ixday')
    WHERE symbol IS NULL OR date IS NULL;

-- 6e. Verify alpha sparsity (should be ~50%)
SELECT 'Alpha sparsity' AS info,
    count(alpha001) AS non_null,
    count(*) AS total,
    round(count(alpha001)::DOUBLE / count(*) * 100, 1) || '%' AS sparsity
    FROM aligned_scan('cnstk_ixday');

-- ============================================================
-- 7. Show final group structure
-- ============================================================

SELECT * FROM aligned_groups('cnstk_ixday');

-- Done! Feature Lake is ready for analysis.
