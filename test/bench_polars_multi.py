#!/usr/bin/env python3
# bench_polars_multi.py — polars engines for the multi-scenario benchmark.
#
# Engines:
#   P-CONCAT : read each Column Group separately, then HORIZONTAL hstack
#              (position-aligned concat) — the classic wide-table assembly path
#              the aligned engine eliminates.
#   P-JOIN   : read the groups and HASH-JOIN on the key (rowid) — the join cost.
#
# Both must observe the SAME logical table as the DuckDB/aligned engines
# (same key order, same NULL distribution) so counts are directly comparable.
#
# Usage:
#   bench_polars_multi.py <engine> <query> <filter> <sel> \
#       <alpha_count> <fs_count> <base_dir> <rows> <threads>
# Prints: COUNTS <csv>   TIMES <cold> <warm>
#
# Requires polars (install via uv: `uv pip install --python .venv-bench/bin/python polars`).
import os
import sys
import time


def main():
    engine, query, filt, sel = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    alpha_n = int(sys.argv[5])
    fs_n = int(sys.argv[6])
    base = sys.argv[7]
    rows = int(sys.argv[8])
    threads = int(sys.argv[9])
    os.environ["POLARS_MAX_THREADS"] = str(threads)

    import polars as pl

    IDX = f"{base}/join_index.parquet"
    ALPHA = f"{base}/join_alpha.parquet"
    FS = f"{base}/join_fs.parquet"

    # ---- projection columns for the query group --------------------------------
    def proj_cols():
        ix = ["date", "symbol", "close", "volume", "rowid"] + [f"ix{i:03d}" for i in range(1, 16)]
        al = [f"alpha{i:03d}" for i in range(alpha_n)]
        fs = [f"fs{i:03d}" for i in range(fs_n)]
        if query == "Q1":
            return ["date", "symbol", "close"], ["date"]
        if query == "Q2":
            cols = ["date", "symbol", "close", "volume", "rowid"] + al[:20] + fs[:10]
            return cols, cols
        if query == "Q3":
            cols = ["date", "symbol", "close", "volume", "rowid"] + ix[5:] + al[:400] + fs[:80]
            return cols, cols
        if query == "Q4":
            cols = ["date", "symbol", "close", "volume", "rowid"] + ix[5:] + al[:4400] + fs[:580]
            return cols, cols
        if query == "Q5":  # ALL
            return al + fs, ["date", "symbol", "close", "volume", "rowid"] + al + fs
        raise ValueError(query)

    proj, full = proj_cols()

    # ---- filter predicate (returns a polars Expr) ------------------------------
    def predicate():
        if filt == "F2":
            expr = pl.col("date") == pl.date(2026, 9, 2)
        elif filt == "F3":
            expr = (pl.col("date") >= pl.date(2026, 9, 1)) & (pl.col("date") <= pl.date(2026, 9, 3))
        elif filt == "F4":
            expr = pl.col("close") > 0
        elif filt == "F5":
            expr = pl.col("symbol") == "000001"
        else:  # F1
            expr = pl.lit(True)
        frac = {"S0": 1.0, "S1": 0.10, "S2": 0.01, "S3": 0.001}[sel]
        if frac < 1.0:
            limit = int(rows * frac)
            expr = expr & (pl.col("rowid") < limit)
        return expr

    # Load with minimal projection: index needs the filter cols + rowid; alpha/fs
    # need their projected columns. For P-JOIN we also need the key columns.
    idx_cols = ["date", "symbol", "close", "volume", "rowid"]
    al_read = list(dict.fromkeys([c for c in full if c.startswith("alpha")] + ["rowid_alpha"]))
    fs_read = list(dict.fromkeys([c for c in full if c.startswith("fs")] + ["rowid_fs"]))

    def assemble():
        idx = pl.read_parquet(IDX, columns=idx_cols)
        a = pl.read_parquet(ALPHA, columns=al_read)
        f = pl.read_parquet(FS, columns=fs_read)
        if engine == "P-CONCAT":
            # P-CONCAT = the classic wide-table assembly path the aligned engine
            # eliminates: read each Column Group separately, then HSTACK them by
            # physical row position (no key). DataFrame.hstack does exactly this
            # pure position-aligned horizontal concat. Expressing it as hstack
            # (rather than fila horizontal concat) makes the no-key-matching
            # intent explicit and matches the polars wide-assembly baseline.
            df = idx
            df = df.hstack(a.drop("rowid_alpha"))
            df = df.hstack(f.drop("rowid_fs"))
        else:  # P-JOIN
            df = idx.join(a, left_on="rowid", right_on="rowid_alpha", how="inner")
            df = df.join(f, left_on="rowid", right_on="rowid_fs", how="inner")
        return df.filter(predicate())

    def run():
        return assemble().select([pl.col(c).count() for c in proj])

    t0 = time.perf_counter()
    r = run()
    cold = time.perf_counter() - t0
    counts = ",".join(str(v) for v in r.row(0))
    nrows = assemble().height  # row count after filter (before projection)
    t0 = time.perf_counter()
    r = run()
    warm = time.perf_counter() - t0
    print(f"COUNTS {counts}")
    print(f"ROWS {nrows}")
    print(f"TIMES {cold:.4f} {warm:.4f}")


if __name__ == "__main__":
    main()
