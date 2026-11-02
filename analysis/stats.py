#!/usr/bin/env python3
"""Comparisons of the core benchmark: Mann-Whitney U, bootstrap CIs, Holm; across campaigns and platforms.
Usage: python analysis/stats.py
"""
import sys

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from common import (BM, DERIVED, PRIMARY, VM, SEED, boot_ratio_ci, hier_ratio, holm, iqr,
                    load_runs, ratio_of_ratios, safe_write)

METRICS = [PRIMARY, "WallClock_ms", "PeakRSS_KB"]
SUFFIX = {"vm": "", "bm": "_bm"}
BANDS = [1.0, 1.05, 1.11, 1.28, 1.65, np.inf]


def comparisons(d):
    """Yield (family, label, filter_a, filter_b); each filter selects one sample."""
    for sf in sorted(d.SF.unique()):
        flat = sorted(m for m in d[d.SF == sf].Model.unique() if m.startswith("A"))
        for q in ["Q1", "Q2a", "Q2b", "Q2c", "Q4", "Q5", "Q6", "Q7"]:
            for m in flat[1:]:
                yield ("RQ1_vs_A1", f"SF{sf} {q}: {m} vs A1",
                       dict(SF=sf, Model="A1", Query=q), dict(SF=sf, Model=m, Query=q))
            for a, b in zip(flat, flat[1:]):
                yield ("RQ1_adjacent", f"SF{sf} {q}: {b} vs {a}",
                       dict(SF=sf, Model=a, Query=q), dict(SF=sf, Model=b, Query=q))
        for m in flat:
            for a, b in [("Q2a", "Q2b"), ("Q2b", "Q2c"), ("Q2a", "Q2c")]:
                yield ("Q2_selectivity", f"SF{sf} {m}: {b} vs {a}",
                       dict(SF=sf, Model=m, Query=a), dict(SF=sf, Model=m, Query=b))
        for m, q in [("B2", "Q3_B2"), ("B3", "Q3_B3"), ("B4", "Q3_B4_month")]:
            yield ("RQ2_depth", f"SF{sf}: {m} {q} vs B1 Q3_B1",
                   dict(SF=sf, Model="B1", Query="Q3_B1"), dict(SF=sf, Model=m, Query=q))
        yield ("RQ2_depth", f"SF{sf}: B4 Q3_B4_day vs B4 Q3_B4_month",
               dict(SF=sf, Model="B4", Query="Q3_B4_month"), dict(SF=sf, Model="B4", Query="Q3_B4_day"))
        yield ("RQ3_keyorder", f"SF{sf}: C1 Q3_Both vs B3 Q3_B3",
               dict(SF=sf, Model="B3", Query="Q3_B3"), dict(SF=sf, Model="C1", Query="Q3_Both"))
        yield ("RQ3_keyorder", f"SF{sf}: C1 Q3_Month vs C1 Q3_Year",
               dict(SF=sf, Model="C1", Query="Q3_Year"), dict(SF=sf, Model="C1", Query="Q3_Month"))
    both = set(d[d.SF == 1].Model) & set(d[d.SF == 10].Model)
    for m in sorted(both):
        for q in sorted(set(d[(d.SF == 1) & (d.Model == m)].Query)):
            if q != "Q0":
                yield ("RQ4_scale", f"{m} {q}: SF10 vs SF1",
                       dict(SF=1, Model=m, Query=q), dict(SF=10, Model=m, Query=q))


def select(d, f):
    m = np.ones(len(d), dtype=bool)
    for k, v in f.items():
        m &= (d[k] == v).to_numpy()
    return d[m]


def describe(f):
    return " ".join(f"{k}={v}" for k, v in f.items())


def within(d, rng):
    """1. Tests inside each campaign of one platform."""
    rows = []
    for camp, dc in d.groupby("Campaign"):
        for family, label, fa, fb in comparisons(dc):
            sa, sb = select(dc, fa), select(dc, fb)
            if sa.empty or sb.empty:
                continue
            for metric in METRICS:
                a = sa[metric].dropna().to_numpy(float)
                b = sb[metric].dropna().to_numpy(float)
                if len(a) < 3 or len(b) < 3:
                    continue
                ties = len(np.unique(np.concatenate([a, b]))) < len(a) + len(b)
                method = "asymptotic" if ties else "exact"
                u = mannwhitneyu(b, a, alternative="two-sided", method=method)
                lo, hi = boot_ratio_ci(a, b, rng)
                src = ",".join(sorted(set(sa.LatencySource) | set(sb.LatencySource)))
                rows.append(dict(
                    Campaign=camp, Family=family, Comparison=label, Metric=metric,
                    LatencySource=src if metric == PRIMARY else "",
                    A=describe(fa), B=describe(fb),
                    FilesA=sa.LayoutFiles.iloc[0], FilesB=sb.LayoutFiles.iloc[0],
                    nA=len(a), nB=len(b), MedianA=np.median(a), IQR_A=iqr(a),
                    MedianB=np.median(b), IQR_B=iqr(b),
                    Ratio=np.median(b) / np.median(a), Ratio_CI95_lo=lo, Ratio_CI95_hi=hi,
                    U=u.statistic, p=u.pvalue, p_method=method))
    s = pd.DataFrame(rows)
    s["p_holm"] = np.nan
    for _, g in s.groupby(["Campaign", "Metric"]):
        s.loc[g.index, "p_holm"] = holm(g.p.to_numpy())
    return s


def across(d, rng):
    """2. Consistency across the campaigns of one platform. Also returns the"""
    camps = sorted(d.Campaign.unique())
    crow, boots = [], {}
    for family, label, fa, fb in comparisons(d):
        for metric in METRICS:
            A = [select(d[d.Campaign == c], fa)[metric].dropna().to_numpy(float) for c in camps]
            B = [select(d[d.Campaign == c], fb)[metric].dropna().to_numpy(float) for c in camps]
            r = hier_ratio(A, B, rng)
            if r is None:
                continue
            row = dict(Family=family, Comparison=label, Metric=metric,
                       A=describe(fa), B=describe(fb),
                       N_campaigns=r["n_campaigns"], Ratio_pooled=r["ratio_pooled"],
                       Ratio_min=r["ratio_min"], Ratio_max=r["ratio_max"],
                       Hier_CI95_lo=r["hier_lo"], Hier_CI95_hi=r["hier_hi"],
                       Consistent=r["consistent"])
            for c, v in zip([c for c in camps], r["per_campaign"]):
                row[f"Ratio_{c}"] = v
            crow.append(row)
            boots[(label, metric)] = r["boot"]
    return pd.DataFrame(crow), boots


def platforms(cs, boots):
    """3. Agreement between platforms for every comparison present on both."""
    key = ["Family", "Comparison", "Metric"]
    vm, bm = cs["vm"].set_index(key), cs["bm"].set_index(key)
    rows = []
    for k in vm.index.intersection(bm.index):
        a, b = vm.loc[k], bm.loc[k]
        ror, lo, hi = ratio_of_ratios(boots["vm"][k[1:]], boots["bm"][k[1:]],
                                      a.Ratio_pooled, b.Ratio_pooled)
        same = (a.Ratio_pooled > 1) == (b.Ratio_pooled > 1)
        if a.Consistent and b.Consistent:
            agree = "consistent on both, same direction" if same else "consistent on both, OPPOSITE"
        elif a.Consistent or b.Consistent:
            agree = "consistent on vm only" if a.Consistent else "consistent on bm only"
        else:
            agree = "consistent on neither"
        rows.append(dict(zip(key, k), A=a.A, B=a.B,
                         Ratio_vm=a.Ratio_pooled, CI_vm_lo=a.Hier_CI95_lo, CI_vm_hi=a.Hier_CI95_hi,
                         Consistent_vm=a.Consistent, N_vm=a.N_campaigns,
                         Ratio_bm=b.Ratio_pooled, CI_bm_lo=b.Hier_CI95_lo, CI_bm_hi=b.Hier_CI95_hi,
                         Consistent_bm=b.Consistent, N_bm=b.N_campaigns,
                         SameDirection=bool(same), RatioOfRatios_bm_vm=ror,
                         RoR_CI95_lo=lo, RoR_CI95_hi=hi,
                         MagnitudeDiffers=bool(lo > 1 or hi < 1),
                         ConsistentBoth=bool(a.Consistent and b.Consistent and same),
                         Agreement=agree))
    return pd.DataFrame(rows)


def min_effect(cs):
    """4. Smallest consistent effect per platform × metric, and consistency by band."""
    rows = []
    for plat, c in cs.items():
        for metric in METRICS:
            x = c[(c.Metric == metric) & (c.Ratio_pooled > 0)]
            if x.empty:
                continue
            eff = np.maximum(x.Ratio_pooled, 1 / x.Ratio_pooled)
            ok = x.Consistent.to_numpy(bool)
            row = dict(Platform=plat, Metric=metric, N_campaigns=int(x.N_campaigns.max()),
                       n=len(x), n_consistent=int(ok.sum()),
                       MinConsistentEffect=eff[ok].min() if ok.any() else np.nan,
                       MaxNonConsistentEffect=eff[~ok].max() if (~ok).any() else np.nan)
            for lo, hi in zip(BANDS, BANDS[1:]):
                band = (eff >= lo) & (eff < hi)
                tag = f"{lo:.2f}-{hi:.2f}" if np.isfinite(hi) else f">{lo:.2f}"
                row[f"band_{tag}_n"] = int(band.sum())
                row[f"band_{tag}_consistent"] = int((band & ok).sum())
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    run_ids = sys.argv[1:] or sorted([p.name for p in VM.glob("core_v2*") if p.is_dir()]
                                     + [p.name for p in BM.glob("bm_core_*") if p.is_dir()])
    d_all = load_runs(run_ids)
    cs, boots = {}, {}
    for plat, d in d_all.groupby("Plat"):
        rng = np.random.default_rng(SEED)
        camps = sorted(d.Campaign.unique())
        s = within(d, rng)
        write = safe_write
        write(s, DERIVED / f"stats_all{SUFFIX[plat]}.csv", index=False, float_format="%.6g")
        cs[plat], boots[plat] = across(d, rng)
        write(cs[plat], DERIVED / f"stats_consistency{SUFFIX[plat]}.csv",
                   index=False, float_format="%.6g")
        print(f"[{plat}] {len(s)} within-campaign tests → stats_all{SUFFIX[plat]}.csv; "
              f"{len(cs[plat])} cross-campaign comparisons ({len(camps)} campaigns: "
              f"{', '.join(camps)}) → stats_consistency{SUFFIX[plat]}.csv")
        for metric in METRICS:
            x = cs[plat][cs[plat].Metric == metric]
            print(f"  {metric}: consistent {x.Consistent.sum()}/{len(x)}")
    me = min_effect(cs)
    me.to_csv(DERIVED / "stats_min_effect.csv", index=False, float_format="%.6g")
    print(me[["Platform", "Metric", "N_campaigns", "n", "n_consistent",
              "MinConsistentEffect", "MaxNonConsistentEffect"]].round(3).to_string(index=False))
    if {"vm", "bm"} <= set(cs):
        pf = platforms(cs, boots)
        pf.to_csv(DERIVED / "stats_platforms.csv", index=False, float_format="%.6g")
        print(f"{len(pf)} comparisons on both platforms → stats_platforms.csv")
        print(pf.groupby(["Metric", "Agreement"]).size().to_string())


if __name__ == "__main__":
    main()
