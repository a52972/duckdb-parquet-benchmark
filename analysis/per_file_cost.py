#!/usr/bin/env python3
"""Per-file latency and memory slopes (OLS on the real number of files).
Usage: python analysis/per_file_cost.py
"""
import numpy as np
import pandas as pd

from common import (DERIVED, INV, V10, SEED, glob_runs, add_engine_latency, hier_slope, ratio_of_ratios,
                    safe_write, slope)

N_BOOT = 2_000
QUERIES = ["Q1", "Q2a", "Q2b", "Q2c", "Q4", "Q5", "Q6", "Q7"]
METRICS = [("EngineLatency_ms", "ms/file"), ("WallClock_ms", "ms/file"), ("PeakRSS_KB", "KB/file")]


def v1_published():
    d = pd.read_csv(V10)
    inv = pd.read_csv(INV / "inventory_original.csv")
    files = {(r.sf, r.model): r.files for r in inv.itertuples()}
    d["LayoutFiles"] = [files.get((s, "A4" if m == "B1" else m)) for s, m in zip(d.SF, d.Model)]
    d["EngineLatency_ms"] = d.DuckDB_Total_ms
    d["PeakRSS_KB"] = pd.to_numeric(d.PeakRAM_KB, errors="coerce")
    return d.assign(Condition="v1-default-writer", Source="v1.0 published", Campaign="v1.0")


def runs(pattern, condition, source, one_campaign=False):
    """one_campaign: the run ids are parts of a single campaign (E2b was split"""
    frames = [pd.read_csv(p).assign(Campaign=source if one_campaign else p.parent.name)
              for p in glob_runs(pattern)]
    if not frames:
        return pd.DataFrame()
    return add_engine_latency(pd.concat(frames)).assign(Condition=condition, Source=source)


def ols(x, y):
    b1 = slope(x, y)
    return b1, float(np.mean(y) - b1 * np.mean(x))


def fit_single(g, metric, rng):
    g = g.dropna(subset=[metric, "LayoutFiles"])
    if g.Model.nunique() < 2:
        return None
    x, y = g.LayoutFiles.to_numpy(float), g[metric].to_numpy(float)
    b1, b0 = ols(x, y)
    groups = [np.flatnonzero((g.Model == m).to_numpy()) for m in g.Model.unique()]
    slopes = [slope(x[i], y[i]) for i in
              (np.concatenate([rng.choice(ix, size=len(ix)) for ix in groups]) for _ in range(N_BOOT))]
    lo, hi = np.percentile(slopes, [2.5, 97.5])
    r2 = 1 - ((y - (b0 + b1 * x)) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    return dict(Slope=b1, Slope_CI95_lo=lo, Slope_CI95_hi=hi, R2=r2, N_campaigns=1,
                Slope_min=b1, Slope_max=b1, Consistent=bool(lo > 0 or hi < 0),
                CI_type="bootstrap (runs)", n=len(g), Models=" ".join(sorted(g.Model.unique())),
                MinFiles=int(x.min()), MaxFiles=int(x.max()))


def fit_multi(g, metric, rng):
    g = g.dropna(subset=[metric, "LayoutFiles"])
    if g.Model.nunique() < 2:
        return None
    cells = {c: {xv: gg[metric].to_numpy(float) for xv, gg in gc.groupby("LayoutFiles")}
             for c, gc in g.groupby("Campaign")}
    h = hier_slope(cells, rng, N_BOOT)
    x, y = g.LayoutFiles.to_numpy(float), g[metric].to_numpy(float)
    b1, b0 = ols(x, y)
    r2 = 1 - ((y - (b0 + b1 * x)) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    out = dict(Slope=b1, Slope_CI95_lo=h["hier_lo"], Slope_CI95_hi=h["hier_hi"], R2=r2,
               N_campaigns=len(cells), Slope_min=h["slope_min"], Slope_max=h["slope_max"],
               Consistent=h["consistent"], CI_type="hierarchical bootstrap (campaigns, runs)",
               n=len(g), Models=" ".join(sorted(g.Model.unique())),
               MinFiles=int(x.min()), MaxFiles=int(x.max()), _boot=h["boot"])
    for c, v in h["per_campaign"].items():
        out[f"Slope_{c}"] = v
    return out


def fit_all(d, rng, boots):
    d = d[(d.RunType == "COLD") & (d.Status == "OK") & d.Model.str.match(r"A\d$")]
    rows = []
    for (cond, src, sf, q), g in d[d.Query.isin(QUERIES)].groupby(["Condition", "Source", "SF", "Query"]):
        for metric, unit in METRICS:
            gg = g[~((g.Query == "Q4") & (g.Model == "A1"))] if metric != "PeakRSS_KB" else g
            fit = fit_multi if gg.Campaign.nunique() > 1 else fit_single
            r = fit(gg, metric, rng)
            if r:
                boot = r.pop("_boot", None)
                if boot is not None:
                    boots[(sf, q, metric)] = boot
                rows.append(dict(Condition=cond, Source=src, SF=sf, Query=q,
                                 Metric=metric, Unit=unit, **r))
    return pd.DataFrame(rows)


def show(out):
    out = out[out.Query.isin(["Q1", "Q4", "Q5", "Q7"]) & (out.Metric != "WallClock_ms")]
    with pd.option_context("display.width", 230, "display.max_columns", 20):
        print(out[["Condition", "Source", "SF", "Query", "Metric", "Slope", "Slope_CI95_lo",
                   "Slope_CI95_hi", "Slope_min", "Slope_max", "Consistent", "R2"]]
              .round(4).to_string(index=False))


def main():
    vm_boots, bm_boots = {}, {}
    vm = fit_all(pd.concat([
        v1_published(),
        runs("e2b*", "v1-default-writer", "E2b", one_campaign=True),
        runs("core_v2*", "v2", "E2"),
    ], ignore_index=True), np.random.default_rng(SEED), vm_boots)
    safe_write(vm, DERIVED / "per_file_cost.csv", index=False, float_format="%.6g")
    show(vm)
    bmr = runs("bm_core_*", "v2", "E2-bm")
    if bmr.empty:
        return
    bm = fit_all(bmr, np.random.default_rng(SEED), bm_boots)
    bm.insert(0, "Platform", "bm")
    bm.to_csv(DERIVED / "per_file_cost_bm.csv", index=False, float_format="%.6g")
    show(bm)
    a = vm[vm.Source == "E2"].set_index(["SF", "Query", "Metric"])
    b = bm.set_index(["SF", "Query", "Metric"])
    rows = []
    for k in a.index.intersection(b.index):
        if k not in vm_boots or k not in bm_boots:
            continue
        x, y = a.loc[k], b.loc[k]
        ror, lo, hi = ratio_of_ratios(vm_boots[k], bm_boots[k], x.Slope, y.Slope)
        rows.append(dict(SF=k[0], Query=k[1], Metric=k[2], Unit=x.Unit,
                         Slope_vm=x.Slope, CI_vm_lo=x.Slope_CI95_lo, CI_vm_hi=x.Slope_CI95_hi,
                         Consistent_vm=x.Consistent, N_vm=x.N_campaigns,
                         Slope_bm=y.Slope, CI_bm_lo=y.Slope_CI95_lo, CI_bm_hi=y.Slope_CI95_hi,
                         Consistent_bm=y.Consistent, N_bm=y.N_campaigns,
                         SameSign=bool(np.sign(x.Slope) == np.sign(y.Slope)),
                         SlopeRatio_bm_vm=ror, SR_CI95_lo=lo, SR_CI95_hi=hi,
                         MagnitudeDiffers=bool(lo > 1 or hi < 1),
                         ConsistentBoth=bool(x.Consistent and y.Consistent
                                             and np.sign(x.Slope) == np.sign(y.Slope))))
    pf = pd.DataFrame(rows)
    pf.to_csv(DERIVED / "per_file_cost_platforms.csv", index=False, float_format="%.6g")
    print(pf[pf.Query.isin(["Q1", "Q4", "Q5", "Q7"]) & (pf.Metric != "WallClock_ms")]
          [["SF", "Query", "Metric", "Slope_vm", "Slope_bm", "SlopeRatio_bm_vm",
            "SR_CI95_lo", "SR_CI95_hi", "ConsistentBoth"]].round(4).to_string(index=False))


if __name__ == "__main__":
    main()
