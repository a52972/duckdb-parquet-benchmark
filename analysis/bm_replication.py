#!/usr/bin/env python3
"""E4, E7, E8 and E9 on bare metal vs the VM.
Usage: python analysis/bm_replication.py
"""
import numpy as np
import pandas as pd

from common import DERIVED, SEED, add_engine_latency, run_path

LAT = "EngineLatency_ms"
E9_CFG = ["t1_m10", "t2_m10", "t4_m10", "t4_m8", "t4_m4"]


def runs(rid):
    p = run_path(rid) / "benchmark_results.csv"
    if not p.exists():
        return None
    d = add_engine_latency(pd.read_csv(p))
    return d[(d.RunType == "COLD") & (d.Status == "OK")]


def vals(d, metric, **sel):
    m = np.ones(len(d), bool)
    for k, v in sel.items():
        m &= (d[k] == v).to_numpy()
    return d.loc[m, metric].dropna().to_numpy(float)


def ratio_boot(a, b, rng, n=10_000):
    ia = rng.integers(0, len(a), (n, len(a)))
    ib = rng.integers(0, len(b), (n, len(b)))
    return np.median(b) / np.median(a), np.median(b[ib], axis=1) / np.median(a[ia], axis=1)


def slope_boot(d, metric, rng, n=2_000, xcol="LayoutFiles"):
    d = d.dropna(subset=[metric, xcol])
    x, y = d[xcol].to_numpy(float), d[metric].to_numpy(float)

    def ols(xx, yy):
        xm = xx - xx.mean()
        return float((xm * (yy - yy.mean())).sum() / (xm ** 2).sum())
    groups = [np.flatnonzero((d[xcol] == v).to_numpy()) for v in d[xcol].unique()]
    boot = np.empty(n)
    for i in range(n):
        ix = np.concatenate([rng.choice(g, len(g)) for g in groups])
        boot[i] = ols(x[ix], y[ix])
    return ols(x, y), boot


ROWS = []


def add(exp, item, metric, kind, vm, bm):
    """vm, bm: (point, boot) or None. kind: 'ratio' (null 1) or 'slope' (null 0)."""
    null = 1.0 if kind == "ratio" else 0.0
    row = dict(Experiment=exp, Item=item, Metric=metric, Kind=kind)
    for tag, est in (("VM", vm), ("BM", bm)):
        if est is None:
            row[tag] = np.nan
            continue
        p, b = est
        lo, hi = np.percentile(b, [2.5, 97.5])
        row.update({tag: p, f"{tag}_lo": lo, f"{tag}_hi": hi,
                    f"{tag}_sig": bool(lo > null or hi < null)})
    if vm is not None and bm is not None:
        n = min(len(vm[1]), len(bm[1]))
        rr = bm[1][:n] / vm[1][:n]
        lo, hi = np.percentile(rr, [2.5, 97.5])
        row.update(BM_over_VM=bm[0] / vm[0], BMVM_lo=lo, BMVM_hi=hi,
                   SameDirection=bool((vm[0] > null) == (bm[0] > null)),
                   Replicates=bool(row.get("VM_sig") and row.get("BM_sig")
                                   and (vm[0] > null) == (bm[0] > null)))
    ROWS.append(row)


def pair_ratio(exp, item, metric, dv, db, a, b, rng):
    est = []
    for d in (dv, db):
        if d is None:
            est.append(None); continue
        x, y = vals(d, metric, **a), vals(d, metric, **b)
        est.append(ratio_boot(x, y, rng) if len(x) >= 3 and len(y) >= 3 else None)
    add(exp, item, metric, "ratio", *est)


def e4(rng):
    dv, db = runs("e4_depth_count"), runs("bm_e4")
    q = {"E4_D1": "Q3_D1", "B3": "Q3_B3", "E4_D3": "Q3_D3", "E4_D2x10": "Q3_B3",
         "E4_D2x30": "Q3_B3", "E4_F2526": "Q3_B1", "B4": "Q3_B4_month"}
    cnt = {m: ("Q4" if m == "E4_F2526" else "Q4_hive") for m in q}
    for lab, a, b in [("depth 1 vs 2 (84 leaves)", "B3", "E4_D1"), ("depth 3 vs 2", "B3", "E4_D3"),
                      ("840 vs 84 files (depth 2)", "B3", "E4_D2x10"),
                      ("2,520 vs 84 files (depth 2)", "B3", "E4_D2x30"),
                      ("B4 vs flat, 2,526 files", "E4_F2526", "B4"),
                      ("B4 vs depth 2 x 30 (2,526 vs 84 folders)", "E4_D2x30", "B4")]:
        for metric in (LAT, "PlannerBinding_ms"):
            pair_ratio("E4", f"one-month filter: {lab}", metric, dv, db,
                       dict(Model=a, Query=q[a]), dict(Model=b, Query=q[b]), rng)
        pair_ratio("E4", f"COUNT(*): {lab}", LAT, dv, db,
                   dict(Model=a, Query=cnt[a]), dict(Model=b, Query=cnt[b]), rng)
    est = []
    for d in (dv, db):
        if d is None:
            est.append(None); continue
        a = vals(d, "PlannerBinding_ms", Model="E4_D2x30", Query="Q3_B3")
        b = vals(d, "PlannerBinding_ms", Model="B4", Query="Q3_B4_month")
        ia = rng.integers(0, len(a), (10_000, len(a))); ib = rng.integers(0, len(b), (10_000, len(b)))
        est.append(((np.median(b) - np.median(a)) / 2442,
                    (np.median(b[ib], 1) - np.median(a[ia], 1)) / 2442))
    add("E4", "planner_binding per folder (ms), B4 vs depth 2 x 30", "PlannerBinding_ms", "slope", *est)


def e7(rng):
    dv, db = runs("e7_sorted"), runs("bm_e7")
    for m in ("A1", "A4", "A6"):
        for q in ("Q1", "Q2a", "Q2b", "Q2c", "Q5"):
            pair_ratio("E7", f"{m}: sorted vs unsorted, {q}", LAT, dv, db,
                       dict(Model=m, Query=q), dict(Model=f"{m}s", Query=q), rng)


def e8(rng):
    dv, db = runs("e8_mechanisms"), runs("bm_e8")
    for q in ("Q1", "Q4"):
        for metric, unit in ((LAT, "ms/file"), ("PeakRSS_KB", "KB/file")):
            est = []
            for d in (dv, db):
                if d is None:
                    est.append(None); continue
                g = d[(d.Query == q) & d.Model.isin(["A1", "A4", "A6", "A7"])]
                if q == "Q4" and metric == LAT:
                    g = g[g.Model != "A1"]
                est.append(slope_boot(g, metric, rng))
            add("E8", f"DuckDB slope {q} ({unit}), flat A1–A7", metric, "slope", *est)
    per_file = {}
    for rid, tag in (("e8_mechanisms", "VM"), ("bm_e8", "BM")):
        p = run_path(rid) / "strace_results.csv"
        if not p.exists():
            continue
        s = pd.read_csv(p)
        s = s[s.Syscall.isin(["openat", "pread64", "getdents64"]) & (s.Query != "Q0")]
        for (m, q, sc), g in s.groupby(["Model", "Query", "Syscall"]):
            per_file.setdefault((m, q, sc), {})[tag] = g.Calls.median() / g.LayoutFiles.iloc[0]
    for (m, q, sc), v in sorted(per_file.items()):
        ROWS.append(dict(Experiment="E8", Item=f"strace {m} {q}: {sc} per file", Metric="Calls",
                         Kind="count", **v,
                         BM_over_VM=v["BM"] / v["VM"] if {"VM", "BM"} <= set(v) else np.nan))
    pol = {}
    for rid, tag in (("e8_mechanisms", "VM"), ("bm_e8", "BM")):
        p = run_path(rid) / "polars_results.csv"
        if p.exists():
            x = pd.read_csv(p)
            pol[tag] = x[(x.RunType == "COLD") & (x.Status == "OK")]
    for q in ("Q1", "Q4"):
        for metric, unit in (("Query_ms", "ms/file"), ("PeakRSS_KB", "KB/file")):
            est = []
            for tag in ("VM", "BM"):
                d = pol.get(tag)
                g = None if d is None else d[(d.Query == q) & d.Model.isin(["A1", "A4", "A6", "A7"])]
                est.append(None if g is None or g.empty else slope_boot(g, metric, rng))
            add("E8", f"Polars slope {q} ({unit}), flat A1–A7", metric, "slope", *est)


def e9(rng):
    data = {(plat, c): runs(("bm_" if plat == "BM" else "") + f"e9_{c}")
            for plat in ("VM", "BM") for c in E9_CFG}
    for m, q in [("A1", "Q1"), ("A4", "Q1"), ("A6", "Q1"), ("A7", "Q1"), ("B3", "Q1_hive"), ("B4", "Q1_hive"),
                 ("A4", "Q4"), ("A6", "Q4"), ("A7", "Q4"), ("B3", "Q4_hive"), ("B4", "Q4_hive")]:
        for lab, ca, cb in (("speed-up 1→4 threads", "t4_m10", "t1_m10"),
                            ("speed-up 2→4 threads", "t4_m10", "t2_m10"),
                            ("memory_limit 4GB vs 10GB (4 threads)", "t4_m10", "t4_m4")):
            est = []
            for plat in ("VM", "BM"):
                a, b = data[(plat, ca)], data[(plat, cb)]
                if a is None or b is None:
                    est.append(None); continue
                x, y = vals(a, LAT, Model=m, Query=q), vals(b, LAT, Model=m, Query=q)
                est.append(ratio_boot(x, y, rng) if len(x) >= 3 and len(y) >= 3 else None)
            add("E9", f"{m} {q}: {lab}", LAT, "ratio", *est)


def main():
    rng = np.random.default_rng(SEED)
    for f in (e4, e7, e8, e9):
        f(rng)
    out = pd.DataFrame(ROWS)
    out.to_csv(DERIVED / "bm_replication.csv", index=False, float_format="%.6g")
    cols = [c for c in ["Experiment", "Item", "Metric", "VM", "VM_lo", "VM_hi", "BM", "BM_lo", "BM_hi",
                        "BM_over_VM", "BMVM_lo", "BMVM_hi", "Replicates"] if c in out]
    with pd.option_context("display.width", 260, "display.max_colwidth", 60, "display.max_rows", 400):
        print(out[cols].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
