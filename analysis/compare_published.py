#!/usr/bin/env python3
"""Lists each bare-metal result of a re-run next to the published one (run with RESULTS_ROOT set).
Usage: RESULTS_ROOT=results/local python analysis/compare_published.py
"""
import re

import numpy as np
import pandas as pd

from common import BASE, DERIVED

PUB = BASE / "results" / "v1.1" / "derived"
CI = re.compile(r"([\d.]+) \(([\d.]+)–([\d.]+)\)")


def claims(d):
    out = []
    for r in d.itertuples():
        m = CI.match(str(r.BM))
        if m:
            out.append((f"{r.Claim}: {r.Comparison}", r.Metric, *map(float, m.groups())))
    return out


def slopes(d):
    d = d[(d.SF == 10) & d.Query.isin(["Q1", "Q4", "Q5", "Q7"]) & d.Metric.isin(["EngineLatency_ms", "PeakRSS_KB"])]
    return [(f"per-file slope SF10 {r.Query}", f"{r.Metric} ({r.Unit})", r.Slope, r.Slope_CI95_lo, r.Slope_CI95_hi)
            for r in d.itertuples()]


def replication(d):
    return [(f"{r.Experiment}: {r.Item}", r.Metric, r.BM, r.BM_lo, r.BM_hi) for r in d.itertuples()]


def ratios(d):
    item = d.Comparison + (" [" + d.Query + "]" if "Query" in d else "")
    metric = d.Metric if "Metric" in d else pd.Series("EngineLatency_ms", index=d.index)
    return list(zip(item, metric, d.Ratio, d.Ratio_CI95_lo, d.Ratio_CI95_hi))


SOURCES = [("main comparisons", "claims_platforms.csv", claims),
           ("per-file cost", "per_file_cost_bm.csv", slopes),
           ("E4, E7, E8, E9", "bm_replication.csv", replication),
           ("E6 file size", "bm_e6_comparisons.csv", ratios),
           ("D1 key cardinality", "d1_comparisons.csv", ratios),
           ("D2 writers", "bm_d2_comparisons.csv", ratios),
           ("E10 skew", "e10_comparisons.csv", ratios)]


def main():
    if DERIVED.resolve() == PUB.resolve():
        raise SystemExit("set RESULTS_ROOT to the folder of your runs, e.g. RESULTS_ROOT=results/local")
    rows = []
    for name, f, parse in SOURCES:
        if not (PUB / f).exists() or not (DERIVED / f).exists():
            print(f"{name:<20} not in this re-run")
            continue
        try:
            pub = {(i, m): (v, lo, hi) for i, m, v, lo, hi in parse(pd.read_csv(PUB / f))}
            mine = parse(pd.read_csv(DERIVED / f))
        except (AttributeError, KeyError):
            print(f"{name:<20} not in this re-run")
            continue
        for i, m, v, lo, hi in mine:
            if (i, m) not in pub or not np.isfinite(v):
                continue
            pv, plo, phi = pub[(i, m)]
            if parse is slopes or "slope" in i or "per folder" in i:
                same = np.sign(v) == np.sign(pv)
            else:
                small = abs(np.log(v)) < np.log(1.05) and abs(np.log(pv)) < np.log(1.05)
                same = small or np.sign(np.log(v)) == np.sign(np.log(pv))
            rows.append(dict(Source=name, Item=i, Metric=m, Published=pv, Published_lo=plo, Published_hi=phi,
                             Yours=v, Yours_lo=lo, Yours_hi=hi, Yours_over_published=v / pv if pv else np.nan,
                             SameDirection=bool(same), InsidePublishedCI=bool(plo <= v <= phi) if np.isfinite([plo, phi]).all()
                             else bool(np.isclose(v, pv, rtol=0.05))))
    out = pd.DataFrame(rows)
    out.to_csv(DERIVED / "compare_published.csv", index=False, float_format="%.6g")
    if out.empty:
        print("nothing to compare")
        return
    print(f"\n{len(out)} results compared → {DERIVED / 'compare_published.csv'}")
    print(out.groupby("Source").agg(n=("Item", "size"), same_direction=("SameDirection", "sum"),
                                     inside_published_ci=("InsidePublishedCI", "sum"),
                                     median_yours_over_published=("Yours_over_published", "median"))
          .round(3).to_string())
    print("\nInsidePublishedCI: inside the published 95 % CI (within 5 % for counts without a CI).")
    print("Absolute times differ between machines; compare directions and orders of magnitude.")


if __name__ == "__main__":
    main()
