# bench_polars.py —Phase 6 baseline: read each Column Group's parquet
# separately, then horizontal concat (hstack) on the row index —the
# "traditional" wide-table assembly cost the aligned engine eliminates.
#
# Usage: python test/bench_polars.py <workload> <threads>
#   workload: p5 | p25 | p100 | s25 | s100
# Prints:
#   COUNTS <csv of non-null counts>
#   TIMES <cold_s> <warm_s>          (two runs in one process)
import os
import sys
import time

threads = int(sys.argv[2])
os.environ["POLARS_MAX_THREADS"] = str(threads)

import polars as pl  # noqa: E402

ROOT = "D:/proj/factorlake/testdata/bench_baseline"
IDX = f"{ROOT}/join_index.parquet"
ALPHA = f"{ROOT}/join_alpha.parquet"
MA = f"{ROOT}/join_ma.parquet"

A5 = [f"alpha{i:03d}" for i in range(5)]
A25 = [f"alpha{i:03d}" for i in range(25)]
A100 = [f"alpha{i:03d}" for i in range(100)]
M20 = [f"ma{i:03d}" for i in range(20)]


def run(workload):
    # Position-aligned HORIZONTAL concat of the per-group parquet frames —this
    # is exactly the wide-table assembly path the aligned engine eliminates
    # (the groups share the same physical row order).
    if workload == "p5":
        cols = A5
        a = pl.read_parquet(ALPHA, columns=cols)
        idx = pl.read_parquet(IDX, columns=["date"])
        df = pl.concat([idx, a], how="horizontal")
        return df.select([pl.col(c).count() for c in cols])
    if workload == "p25":
        cols = A25
        a = pl.read_parquet(ALPHA, columns=cols)
        idx = pl.read_parquet(IDX, columns=["date"])
        df = pl.concat([idx, a], how="horizontal")
        return df.select([pl.col(c).count() for c in cols])
    if workload == "p100":
        cols = A100 + M20
        a = pl.read_parquet(ALPHA, columns=A100)
        m = pl.read_parquet(MA, columns=M20)
        idx = pl.read_parquet(IDX, columns=["date"])
        df = pl.concat([idx, a, m], how="horizontal")
        return df.select([pl.col(c).count() for c in cols])
    if workload == "s25":
        cols = A25
        a = pl.read_parquet(ALPHA, columns=cols)
        idx = pl.read_parquet(IDX, columns=["date"])
        df = pl.concat([idx, a], how="horizontal").filter(pl.col("date") == pl.date(2026, 9, 1))
        return df.select([pl.col(c).count() for c in cols])
    if workload == "s100":
        cols = A25
        a = pl.read_parquet(ALPHA, columns=cols)
        idx = pl.read_parquet(IDX, columns=["date"])
        df = pl.concat([idx, a], how="horizontal")
        return df.select([pl.col(c).count() for c in cols])
    raise ValueError(workload)


workload = sys.argv[1]
start = time.perf_counter()
result = run(workload)
cold = time.perf_counter() - start
start = time.perf_counter()
result = run(workload)
warm = time.perf_counter() - start

counts = ",".join(str(v) for v in result.row(0))
print(f"COUNTS {counts}")
print(f"TIMES {cold:.4f} {warm:.4f}")
