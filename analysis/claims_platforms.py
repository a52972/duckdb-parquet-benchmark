#!/usr/bin/env python3
"""Main comparisons of the core benchmark on both platforms.
Usage: python analysis/claims_platforms.py
"""
import pandas as pd

from common import DERIVED

CLAIMS = {
    "RQ1 Q1 A3/A1": "SF10 Q1: A3 vs A1",
    "RQ1 Q1 A4/A1": "SF10 Q1: A4 vs A1",
    "RQ1 Q1 A5/A1": "SF10 Q1: A5 vs A1",
    "RQ1 Q1 A6/A1": "SF10 Q1: A6 vs A1",
    "RQ1 Q1 A7/A1": "SF10 Q1: A7 vs A1",
    "No cliff A5/A4 Q1": "SF10 Q1: A5 vs A4",
    "A5/A4 Q4": "SF10 Q4: A5 vs A4",
    "Q4 A7/A1": "SF10 Q4: A7 vs A1",
    "Q4 A7/A6": "SF10 Q4: A7 vs A6",
    "RQ2 B3 vs B1": "SF10: B3 Q3_B3 vs B1 Q3_B1",
    "RQ2 B2 vs B1": "SF10: B2 Q3_B2 vs B1 Q3_B1",
    "RQ2 B4 vs B1": "SF10: B4 Q3_B4_month vs B1 Q3_B1",
    "RQ2 B4 day vs month": "SF10: B4 Q3_B4_day vs B4 Q3_B4_month",
    "RQ3 C1 Both vs B3": "SF10: C1 Q3_Both vs B3 Q3_B3",
    "RQ3 C1 Month vs Year": "SF10: C1 Q3_Month vs C1 Q3_Year",
    "RQ4 A1 Q1": "A1 Q1: SF10 vs SF1",
    "RQ4 A4 Q1": "A4 Q1: SF10 vs SF1",
    "RQ4 A6 Q1": "A6 Q1: SF10 vs SF1",
    "RQ4 A7 Q1": "A7 Q1: SF10 vs SF1",
    "Q2a vs Q2c A1 (unsorted ~equal)": "SF10 A1: Q2c vs Q2a",
    "Q7 A7/A1": "SF10 Q7: A7 vs A1",
    "Q7 A3/A1": "SF10 Q7: A3 vs A1",
}
METRICS = ["EngineLatency_ms", "WallClock_ms", "PeakRSS_KB"]


def verdict(r):
    if r.Agreement == "consistent on both, same direction":
        return "consistent on both"
    return {"consistent on vm only": "VM only", "consistent on bm only": "BM only",
            "consistent on both, OPPOSITE": "OPPOSITE"}.get(r.Agreement, "neither")


def fmt(x, lo, hi, nd=3):
    return f"{x:.{nd}f} ({lo:.{nd}f}–{hi:.{nd}f})"


def main():
    sp = pd.read_csv(DERIVED / "stats_platforms.csv")
    rows = []
    for claim, comp in CLAIMS.items():
        for _, r in sp[(sp.Comparison == comp) & sp.Metric.isin(METRICS)].iterrows():
            rows.append(dict(Claim=claim, Comparison=comp, Metric=r.Metric,
                             VM=fmt(r.Ratio_vm, r.CI_vm_lo, r.CI_vm_hi), VM_consistent=r.Consistent_vm,
                             BM=fmt(r.Ratio_bm, r.CI_bm_lo, r.CI_bm_hi), BM_consistent=r.Consistent_bm,
                             BM_over_VM=fmt(r.RatioOfRatios_bm_vm, r.RoR_CI95_lo, r.RoR_CI95_hi),
                             Verdict=verdict(r),
                             Magnitude="differs" if r.MagnitudeDiffers else "same"))
    pf = pd.read_csv(DERIVED / "per_file_cost_platforms.csv")
    for _, r in pf[pf.Metric.isin(["EngineLatency_ms", "PeakRSS_KB"])].iterrows():
        both = r.Consistent_vm and r.Consistent_bm and r.SameSign
        rows.append(dict(Claim=f"Per-file slope SF{r.SF} {r.Query} ({r.Unit})",
                         Comparison="OLS slope, flat A-series", Metric=r.Metric,
                         VM=fmt(r.Slope_vm, r.CI_vm_lo, r.CI_vm_hi), VM_consistent=r.Consistent_vm,
                         BM=fmt(r.Slope_bm, r.CI_bm_lo, r.CI_bm_hi), BM_consistent=r.Consistent_bm,
                         BM_over_VM=fmt(r.SlopeRatio_bm_vm, r.SR_CI95_lo, r.SR_CI95_hi),
                         Verdict="consistent on both" if both else
                         ("OPPOSITE" if r.Consistent_vm and r.Consistent_bm else
                          "VM only" if r.Consistent_vm else "BM only" if r.Consistent_bm else "neither"),
                         Magnitude="differs" if r.MagnitudeDiffers else "same"))
    out = pd.DataFrame(rows)
    out.to_csv(DERIVED / "claims_platforms.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_colwidth", 40):
        print(out.drop(columns="Comparison").to_string(index=False))


if __name__ == "__main__":
    main()
