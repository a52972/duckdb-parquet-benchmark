#!/usr/bin/env python3
"""Summaries and comparisons of experiments E4-E10 and D1.
Usage: python analysis/experiments.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from common import BASE, DERIVED, INV, SEED, VM, add_engine_latency, boot_ratio_ci, holm, run_path


def read(run_id, name="benchmark_results.csv"):
    d = pd.read_csv(run_path(run_id) / name)
    return add_engine_latency(d) if name == "benchmark_results.csv" else d


def cold(d):
    return d[(d.RunType == "COLD") & (d.Status == "OK")]


def med(x):
    return x.median()


def compare(rows, rng, exp, c, a, b, metric="EngineLatency_ms", label=None):
    """a, b: dicts of column → value selecting the two samples in c."""
    sa = c.loc[np.logical_and.reduce([c[k] == v for k, v in a.items()]), metric].dropna().to_numpy(float)
    sb = c.loc[np.logical_and.reduce([c[k] == v for k, v in b.items()]), metric].dropna().to_numpy(float)
    if len(sa) < 3 or len(sb) < 3:
        return
    lo, hi = boot_ratio_ci(sa, sb, rng)
    rows.append(dict(
        Experiment=exp, Comparison=label or f"{b} vs {a}", Metric=metric,
        A=" ".join(f"{k}={v}" for k, v in a.items()),
        B=" ".join(f"{k}={v}" for k, v in b.items()),
        nA=len(sa), nB=len(sb), MedianA=np.median(sa), MedianB=np.median(sb),
        Ratio=np.median(sb) / np.median(sa), Ratio_CI95_lo=lo, Ratio_CI95_hi=hi,
        p=mannwhitneyu(sb, sa, alternative="two-sided", method="asymptotic").pvalue))


def finish(rows, path):
    r = pd.DataFrame(rows)
    r["p_holm"] = holm(r.p.to_numpy())
    r.to_csv(path, index=False, float_format="%.6g")
    return r


def e5(rng):
    c = cold(read("e5_keyorder"))
    c = c[c.Query != "Q0"]
    s = (c.groupby(["SF", "Model", "Query"])
           .agg(n=("EngineLatency_ms", "size"), WallClock_ms=("WallClock_ms", med),
                EngineLatency_ms=("EngineLatency_ms", med), PlannerBinding_ms=("PlannerBinding_ms", med),
                FilesRead=("FilesRead", med), ScanRowsOut=("ScanRowsOut", med)).reset_index())
    s.to_csv(DERIVED / "e5_summary.csv", index=False)
    rows = []
    for sf in (1, 10):
        for q in ("Q3_Year", "Q3_Month", "Q3_Both"):
            for m in ("EngineLatency_ms", "WallClock_ms"):
                compare(rows, rng, "E5", c, dict(SF=sf, Model="B3", Query=q),
                        dict(SF=sf, Model="C1", Query=q), m, f"SF{sf} {q}: C1 vs B3")
    return finish(rows, DERIVED / "e5_comparisons.csv")


def d1(rng):
    """High-cardinality top-level key (bare metal): /supp/year/ (E5b_SY, 200 top-level"""
    if not run_path("bm_d1_keycard").exists():
        return pd.DataFrame()
    c = cold(read("bm_d1_keycard"))
    c = c[c.Query != "Q0"]
    s = (c.groupby(["Model", "Query"])
           .agg(n=("EngineLatency_ms", "size"), EngineLatency_ms=("EngineLatency_ms", med),
                PlannerBinding_ms=("PlannerBinding_ms", med), FilesRead=("FilesRead", med),
                ScanRowsOut=("ScanRowsOut", med)).reset_index())
    if run_path("bm_d1_strace").exists():
        st = read("bm_d1_strace", "strace_results.csv")
        st = st[st.Syscall.isin(["getdents64", "openat", "pread64"])]
        s = s.merge(st.pivot_table(index=["Model", "Query"], columns="Syscall", values="Calls",
                                   aggfunc="median").reset_index(), on=["Model", "Query"], how="left")
    s.to_csv(DERIVED / "d1_summary.csv", index=False)
    rows = []
    for q in ("Q3_Year", "Q3_Supp", "Q3_SuppYear"):
        for m in ("EngineLatency_ms", "PlannerBinding_ms"):
            compare(rows, rng, "D1", c, dict(Model="E5b_YS", Query=q), dict(Model="E5b_SY", Query=q),
                    m, f"{q}: /supp/year/ vs /year/supp/")
    return finish(rows, DERIVED / "d1_comparisons.csv")


def e10(rng):
    """Skewed data and skewed file sizes (bare metal)."""
    if not run_path("bm_e10_skew").exists():
        return pd.DataFrame()
    c = cold(read("bm_e10_skew"))
    c = c[c.Query != "Q0"]
    s = (c.groupby(["Model", "Query"])
           .agg(n=("EngineLatency_ms", "size"), Files=("LayoutFiles", "first"),
                EngineLatency_ms=("EngineLatency_ms", med), WallClock_ms=("WallClock_ms", med),
                PlannerBinding_ms=("PlannerBinding_ms", med), FilesRead=("FilesRead", med),
                ScanRowsOut=("ScanRowsOut", med), PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1024),
                FSInput_MB=("FSInputBlocks", lambda x: x.median() * 512 / 1e6)).reset_index())
    s.to_csv(DERIVED / "e10_summary.csv", index=False)
    rows = []
    pairs = [("A4", "Z75", ("Q1", "Q4", "Q3_B1"), "file sizes Zipf vs equal, 75 files"),
             ("A6", "Z750", ("Q1", "Q4", "Q3_B1"), "file sizes Zipf vs equal, 750 files"),
             ("A4", "S_B1", ("Q1", "Q4"), "skewed vs uniform data, 75 flat files"),
             ("S_B1", "S_S75", ("Q1", "Q3_B1", "Q3_B1_cold"), "skewed data: sorted vs hash-distributed rows")]
    for a, b, qs, fam in pairs:
        for q in qs:
            compare(rows, rng, "E10", c, dict(Model=a, Query=q), dict(Model=b, Query=q),
                    "EngineLatency_ms", f"{fam}, {q}: {b} vs {a}")
    for flat, hive, data in (("A4", "B3", "uniform"), ("S_B1", "S_B3", "skewed")):
        for qf, qh, month in (("Q3_B1", "Q3_B3", "hot month 1996-01"), ("Q3_B1_cold", "Q3_B3_cold", "cold month 1992-01")):
            compare(rows, rng, "E10", c, dict(Model=flat, Query=qf), dict(Model=hive, Query=qh),
                    "EngineLatency_ms", f"pruning, {data} data, {month}: {hive} vs {flat}")
    for m, qh, qc in (("S_B1", "Q3_B1", "Q3_B1_cold"), ("S_B3", "Q3_B3", "Q3_B3_cold"), ("S_S75", "Q3_B1", "Q3_B1_cold")):
        compare(rows, rng, "E10", c, dict(Model=m, Query=qc), dict(Model=m, Query=qh),
                "EngineLatency_ms", f"hot vs cold month, {m}")
    for a, b, q in (("A4", "Z75", "Q1"), ("A6", "Z750", "Q1"), ("B3", "S_B3", "Q1_hive")):
        compare(rows, rng, "E10", c, dict(Model=a, Query=q), dict(Model=b, Query=q),
                "PeakRSS_KB", f"peak memory, {q}: {b} vs {a}")
    return finish(rows, DERIVED / "e10_comparisons.csv")


E4_FILTER = {"E4_D1": "Q3_D1", "B3": "Q3_B3", "E4_D3": "Q3_D3", "E4_D2x10": "Q3_B3",
             "E4_D2x30": "Q3_B3", "E4_F2526": "Q3_B1", "B4": "Q3_B4_month"}
E4_COUNT = {m: ("Q4" if m == "E4_F2526" else "Q4_hive") for m in E4_FILTER}


def e4(rng):
    c = cold(read("e4_depth_count"))
    inv = pd.read_csv(INV / "inventory_v2.csv").query("sf == 10").set_index("model")
    s = (c.groupby(["Model", "Query"])
           .agg(WallClock_ms=("WallClock_ms", med), EngineLatency_ms=("EngineLatency_ms", med),
                PlannerBinding_ms=("PlannerBinding_ms", med),
                PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1e3),
                FilesRead=("FilesRead", med), ScanRowsOut=("ScanRowsOut", med),
                FSInput_MB=("FSInputBlocks", lambda x: x.median() * 512 / 1e6)).reset_index())
    s["Files"] = s.Model.map(inv.files)
    s["LeafDirs"] = s.Model.map(inv.leaf_dirs)
    s["Kind"] = np.select([s.Query.isin(["Q4", "Q4_hive"]), s.Query == "Q0"],
                          ["full_count", "baseline"], "filter_jan1996")
    s.to_csv(DERIVED / "e4_summary.csv", index=False)
    rows = []
    for kind, Q in (("filter", E4_FILTER), ("count", E4_COUNT)):
        pairs = [("B3", "E4_D1", "depth@84"), ("B3", "E4_D3", "depth@84"),
                 ("B3", "E4_D2x10", "count@depth2"), ("B3", "E4_D2x30", "count@depth2"),
                 ("E4_F2526", "E4_D2x30", "~2520 files"), ("E4_F2526", "B4", "~2520 files"),
                 ("E4_D2x30", "B4", "~2520 files")]
        for ma, mb, fam in pairs:
            for m in ("EngineLatency_ms", "WallClock_ms"):
                compare(rows, rng, "E4", c, dict(Model=ma, Query=Q[ma]), dict(Model=mb, Query=Q[mb]),
                        m, f"{fam} {kind}: {mb} vs {ma}")
    return finish(rows, DERIVED / "e4_comparisons.csv")


def e7(rng):
    c = cold(read("e7_sorted"))
    c = c[c.Query != "Q0"]
    s = (c.groupby(["Model", "Query"])
           .agg(Files=("LayoutFiles", "first"), WallClock_ms=("WallClock_ms", med),
                EngineLatency_ms=("EngineLatency_ms", med), RowsScanned=("RowsScanned", med),
                ScanRowsOut=("ScanRowsOut", med), FilesRead=("FilesRead", med),
                FSInput_MB=("FSInputBlocks", lambda x: x.median() * 512 / 1e6),
                PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1e3)).reset_index())
    s.to_csv(DERIVED / "e7_summary.csv", index=False)
    rows = []
    for m in ("A1", "A4", "A6"):
        for q in ("Q1", "Q2a", "Q2b", "Q2c", "Q5"):
            for met in ("EngineLatency_ms", "WallClock_ms", "FSInputBlocks"):
                compare(rows, rng, "E7", c, dict(Model=m, Query=q), dict(Model=m + "s", Query=q),
                        met, f"{m} {q}: sorted vs unsorted")
    return finish(rows, DERIVED / "e7_comparisons.csv")


E6_ORDER = ["A1", "E6_512MB", "E6_256MB", "E6_128MB", "E6_64MB", "E6_32MB", "E6_16MB",
            "E6_8MB", "E6_4MB", "E6_1MB", "E6_duckdb_fsb128", "E6_polars_default"]


def e6(rng, run_id="e6_size", inventory="inventory_v2.csv", prefix="e6", exp="E6"):
    """Real file-size sweep. VM: run e6_size (one layout at a time, incl. two real"""
    if not run_path(run_id).exists():
        return pd.DataFrame()
    c = cold(read(run_id))
    c = c[c.Query != "Q0"]
    inv = (pd.read_csv(INV / inventory).query("sf == 10")
             .drop_duplicates("model", keep="last").set_index("model"))
    lat = c.groupby(["Model", "Query"]).EngineLatency_ms.median().unstack().add_prefix("lat_")
    rss = (c.groupby(["Model", "Query"]).PeakRSS_KB.median().unstack() / 1e3).add_prefix("rssMB_")
    s = pd.DataFrame({"files": inv.files, "mean_MB": inv.size_mean_bytes / 1e6,
                      "total_MB": inv.bytes / 1e6, "row_groups": inv.row_groups,
                      "rg_rows_mean": inv.rg_rows_mean})
    order = [m for m in E6_ORDER if m in set(c.Model)]
    s = s.reindex(order).join(lat).join(rss)
    s.index.name = "Model"
    s.to_csv(DERIVED / f"{prefix}_summary.csv")
    rows = []
    for m in order[1:]:
        for q in ("Q1", "Q2a", "Q4", "Q5", "Q6", "Q7"):
            base = "A1"
            if m != base:
                compare(rows, rng, exp, c, dict(Model=base, Query=q), dict(Model=m, Query=q),
                        "EngineLatency_ms", f"{q}: {m} vs {base}")
    return finish(rows, DERIVED / f"{prefix}_comparisons.csv")


def e8():
    d = cold(read("e8_mechanisms"))
    d = d[d.Query != "Q0"].assign(Q=lambda x: x.Query.str.replace("_hive", ""))
    s = (d.groupby(["Model", "LayoutFiles", "Q"])
           .agg(WallClock_ms=("WallClock_ms", med), EngineLatency_ms=("EngineLatency_ms", med),
                PlannerBinding_ms=("PlannerBinding_ms", med), CPUTime_ms=("CPUTime_ms", med),
                PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1e3),
                FSInput_MB=("FSInputBlocks", lambda x: x.median() * 512 / 1e6)).reset_index())
    st = read("e8_mechanisms", "strace_results.csv").assign(Q=lambda x: x.Query.str.replace("_hive", ""))
    p = st.pivot_table(index=["Model", "Q"], columns="Syscall", values="Calls", aggfunc="sum")
    keep = [k for k in ("openat", "pread64", "read", "getdents64", "mmap", "munmap",
                        "close", "futex", "madvise") if k in p.columns]
    s = s.merge(p[keep].reset_index(), on=["Model", "Q"], how="left")
    s.to_csv(DERIVED / "e8_duckdb_summary.csv", index=False)
    pl = pd.read_csv(VM / "e8_mechanisms" / "polars_results.csv")
    pl = pl[pl.Status == "OK"]
    ps = (pl.groupby(["Model", "LayoutFiles", "Query"])
            .agg(WallClock_ms=("WallClock_ms", med), Query_ms=("Query_ms", med),
                 PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1e3),
                 FSInput_MB=("FSInputBlocks", lambda x: x.median() * 512 / 1e6)).reset_index())
    ps.to_csv(DERIVED / "e8_polars_summary.csv", index=False)
    rows = []
    for q in ("Q1", "Q4"):
        for eng, df, ycol in (("DuckDB", s.rename(columns={"Q": "Query"}), "EngineLatency_ms"),
                              ("Polars", ps, "Query_ms")):
            f = df[(df.Query == q) & df.Model.str.match(r"A\d$")]
            ft = f[f.Model != "A1"] if q == "Q4" else f
            rows.append(dict(Engine=eng, Query=q,
                             ms_per_file=np.polyfit(ft.LayoutFiles, ft[ycol], 1)[0],
                             KB_per_file=np.polyfit(f.LayoutFiles, f.PeakRSS_MB * 1e3, 1)[0]))
    out = pd.DataFrame(rows)
    out.to_csv(DERIVED / "e8_engine_slopes.csv", index=False, float_format="%.6g")
    return out


def e9(rng):
    frames = [add_engine_latency(pd.read_csv(p)) for p in sorted(VM.glob("e9_t*_m*/benchmark_results.csv"))]
    c = cold(pd.concat(frames)).assign(Q=lambda x: x.Query.str.replace("_hive", ""))
    s = (c.groupby(["Model", "Q", "Threads", "MemoryLimit"])
           .agg(EngineLatency_ms=("EngineLatency_ms", med),
                PeakRSS_MB=("PeakRSS_KB", lambda x: x.median() / 1e3),
                PeakTempDirSize=("PeakTempDirSize", "max")).reset_index())
    s.to_csv(DERIVED / "e9_summary.csv", index=False)
    rows = []
    for m in ("A1", "A4", "A6", "A7", "B3", "B4"):
        for q in ("Q1", "Q4"):
            base = dict(Model=m, Q=q, Threads=4, MemoryLimit="10GB")
            for t, mem in ((1, "10GB"), (2, "10GB"), (4, "8GB"), (4, "4GB")):
                compare(rows, rng, "E9", c, base, dict(Model=m, Q=q, Threads=t, MemoryLimit=mem),
                        "EngineLatency_ms", f"{m} {q}: {t} threads/{mem} vs 4 threads/10GB")
    return finish(rows, DERIVED / "e9_comparisons.csv")


def main():
    rng = np.random.default_rng(SEED)
    parts = [e5(rng), e4(rng), e7(rng), e6(rng), e9(rng)]
    parts.append(e6(np.random.default_rng(SEED), "bm_e6_size", "bm_inventory_v2.csv", "bm_e6", "E6-BM"))
    parts.append(d1(np.random.default_rng(SEED)))
    parts.append(e10(np.random.default_rng(SEED)))
    allc = pd.concat([x for x in parts if not x.empty], ignore_index=True)
    allc.to_csv(DERIVED / "stats_experiments.csv", index=False, float_format="%.6g")
    print(e8().round(4).to_string(index=False))
    print(f"{len(allc)} comparisons → {(DERIVED / 'stats_experiments.csv').relative_to(BASE)}")


if __name__ == "__main__":
    main()
