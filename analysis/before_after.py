#!/usr/bin/env python3
"""Each quantitative statement of the v1.0 manuscript next to the v1.1 measurement.
Usage: python analysis/before_after.py
"""
import numpy as np
import pandas as pd

from common import BASE, ROOT, VM, BM, INV, DERIVED, V10, SEED, hier_ratio, load_runs, ratio_of_ratios

MIN_EFFECT = 1.05

rng = np.random.default_rng(SEED)
rng_bm = np.random.default_rng(SEED)

v1 = pd.read_csv(V10)
v1 = v1[(v1.RunType == "COLD") & (v1.Status == "OK")].assign(
    EngineLatency_ms=lambda x: x.DuckDB_Total_ms,
    PeakRSS_KB=lambda x: pd.to_numeric(x.PeakRAM_KB, errors="coerce"), Campaign="v1.0")
core_ids = sorted(p.name for p in VM.glob("core_v2*") if p.is_dir())
v2 = load_runs(core_ids)
bm_ids = sorted(p.name for p in BM.glob("bm_core_*") if p.is_dir())
bm = load_runs(bm_ids)
e5 = load_runs(["e5_keyorder"])
e4 = load_runs(["e4_depth_count"])
bm_e4 = load_runs(["bm_e4"])
inv1 = pd.read_csv(INV / "inventory_original.csv").set_index(["sf", "model"])
inv2 = pd.read_csv(INV / "inventory_v2.csv").set_index(["sf", "model"])


def by_campaign(d, sf, model, query, col):
    m = (d.Model == model) & (d.Query == query) & (d.SF == sf)
    return [d.loc[m & (d.Campaign == c), col].dropna().to_numpy(float)
            for c in sorted(d.Campaign.unique())]


def fmt_hier(d, a, b, col, raw=False, r_=None):
    r = hier_ratio(by_campaign(d, *a, col), by_campaign(d, *b, col), r_ or rng)
    if r is None:
        return ("n/a", "", "", None) if raw else ("n/a", "", "")
    rng_txt = (f"{r['ratio_min']:.3g}–{r['ratio_max']:.3g}" if r["n_campaigns"] > 1 else "")
    out = (f"{r['ratio_pooled']:.3g}× ({r['hier_lo']:.3g}–{r['hier_hi']:.3g})", rng_txt,
           ("yes" if r["consistent"] else "NO") + f" ({r['n_campaigns']} camp.)")
    return out + (r,) if raw else out


def platforms(rv, rb):
    """bm/vm ratio of ratios with CI, and the both-platforms verdict."""
    if rv is None or rb is None:
        return "", ""
    ror, lo, hi = ratio_of_ratios(rv["boot"], rb["boot"], rv["ratio_pooled"], rb["ratio_pooled"])
    eff = lambda x: max(x, 1 / x)
    both = (rv["consistent"] and rb["consistent"]
            and (rv["ratio_pooled"] > 1) == (rb["ratio_pooled"] > 1)
            and min(eff(rv["ratio_pooled"]), eff(rb["ratio_pooled"])) >= MIN_EFFECT)
    return f"{ror:.3g} ({lo:.3g}–{hi:.3g})", "yes" if both else "NO"


def v1_ratio(a, b, col):
    sa = by_campaign(v1, *a, col)[0]
    sb = by_campaign(v1, *b, col)[0]
    if len(sa) == 0 or len(sb) == 0:
        return "n/a (no profile)"
    return f"{np.median(sb) / np.median(sa):.3g}×"


def files(inv, sf, model):
    return int(inv.loc[(sf, "A4" if model == "B1" else model), "files"])


rows = []


def add(claim, where, published, a, b, d2=v2, source="E2 (core campaigns)", note="",
        rss=False):
    primary = "PeakRSS_KB" if rss else "EngineLatency_ms"
    val, rng_txt, cons, rv = fmt_hier(d2, a, b, primary, raw=True)
    wall = "" if rss else fmt_hier(d2, a, b, "WallClock_ms")[0]
    bval, brng, bcons, rb = fmt_hier(bm, a, b, primary, raw=True, r_=rng_bm)
    bwall = "" if rss else fmt_hier(bm, a, b, "WallClock_ms", r_=rng_bm)[0]
    ror, both = platforms(rv, rb)
    rows.append(dict(bm=bval, bm_campaign_range=brng, bm_consistent=bcons, bm_wall=bwall,
                     bm_over_vm=ror, both_consistent=both, **dict(
        Claim=claim, Where=where, Published=published,
        Metric="peak RSS" if rss else "engine latency",
        v1_recomputed=v1_ratio(a, b, primary),
        v1_wall="" if rss else v1_ratio(a, b, "WallClock_ms"),
        v1_files=f"{files(inv1, b[0], b[1])} vs {files(inv1, a[0], a[1])}",
        v11=val, v11_campaign_range=rng_txt, v11_consistent=cons, v11_wall=wall,
        v11_files=f"{files(inv2, b[0], b[1])} vs {files(inv2, a[0], a[1])}",
        Source=source, Note=note)))


add("2-level Hive (B3) faster than flat (B1), selective query: B1/B3", "Highlight 1, abstract",
    "3.4× (wall)", (10, "B3", "Q3_B3"), (10, "B1", "Q3_B1"), note="holds")
add("3-level Hive (B4) slower than flat (B1)", "Highlight 1, abstract", ">24× (wall)",
    (10, "B1", "Q3_B1"), (10, "B4", "Q3_B4_month"),
    note="v1 B4 had 278,680 files; E4: with equal file counts B4 is faster than flat (filter)")
add("Full-scan latency at 150 files (A5 vs A4)", "Abstract", "3.5× (wall)",
    (10, "A4", "Q1"), (10, "A5", "Q1"), note="v1 A5 had 5,923 files")
add("Metadata operation, worst fragmentation (A7 vs A1, Q4)", "Abstract", ">2,400× (wall)",
    (10, "A1", "Q4"), (10, "A7", "Q4"),
    note="A1 Q4 has no profile: engine latency = wall-clock minus the Q0 baseline (a few ms), "
         "so this ratio is unstable; the wall-clock ratio is shown too")
add("Full scan, worst fragmentation (A7 vs A1, Q1)", "Section 4.1", "428× (wall, v1 CSV)",
    (10, "A1", "Q1"), (10, "A7", "Q1"))
add("Scale dependence (A6 Q1, SF-10 vs SF-1)", "Highlight 2, abstract", "244× (wall)",
    (1, "A6", "Q1"), (10, "A6", "Q1"),
    note="v1 compared 75 files (SF-1) with 75,000 (SF-10); v1.1 compares 75 with 750")
add("Peak memory, A7 vs A1 (Q1)", "Sections 4.1.5, 4.5.1", "172× / +160× (RSS)",
    (10, "A1", "Q1"), (10, "A7", "Q1"), rss=True,
    note="v1.1: 12.3 KB per file (Q4), linear")

r1 = v1_ratio((10, "C1", "Q3_Month"), (10, "C1", "Q3_Year"), "EngineLatency_ms")
r1w = v1_ratio((10, "C1", "Q3_Month"), (10, "C1", "Q3_Year"), "WallClock_ms")
val, _, cons = fmt_hier(e5, (10, "B3", "Q3_Year"), (10, "C1", "Q3_Year"), "EngineLatency_ms")
wall = fmt_hier(e5, (10, "B3", "Q3_Year"), (10, "C1", "Q3_Year"), "WallClock_ms")[0]
bval, brng, bcons = fmt_hier(bm, (10, "B3", "Q3_B3"), (10, "C1", "Q3_Both"), "EngineLatency_ms",
                             r_=rng_bm)
bwall = fmt_hier(bm, (10, "B3", "Q3_B3"), (10, "C1", "Q3_Both"), "WallClock_ms", r_=rng_bm)[0]
d1 = pd.read_csv(DERIVED / "d1_comparisons.csv").set_index(["Comparison", "Metric"])
d1y = d1.loc[("Q3_Year: /supp/year/ vs /year/supp/", "EngineLatency_ms")]
d1txt = f"{d1y.Ratio:.3g}× ({d1y.Ratio_CI95_lo:.3g}–{d1y.Ratio_CI95_hi:.3g})"
rows.append(dict(bm=f"{bval} [both keys, core]; D1 /supp(200)/year/ vs /year/supp/, year filter: {d1txt}",
                 bm_campaign_range=brng, bm_consistent=bcons, bm_wall=bwall, bm_over_vm="",
                 both_consistent="no relevant effect on either (< 1.05×)",
                 Claim="Nested partition key slower than top-level key", Where="Highlight 4, abstract",
                 Published="8% (wall)", Metric="engine latency", v1_recomputed=r1, v1_wall=r1w,
                 v1_files="12 vs 7 folders read", v11=val, v11_campaign_range="",
                 v11_consistent=cons, v11_wall=wall, v11_files="12 vs 12 folders read",
                 Source="E5 (same predicate, C1 vs B3)",
                 Note="v1 compared different selectivities; same predicate → no effect "
                      "(all E5 CIs include 1)"))

e6 = pd.read_csv(DERIVED / "e6_comparisons.csv")
q1 = e6[e6.Comparison.str.startswith("Q1:") & (e6.Metric == "EngineLatency_ms")]


d2 = pd.read_csv(DERIVED / "bm_d2_comparisons.csv").query("Query == 'Q1'")


def d2r(prefix):
    r = d2[d2.Comparison.str.startswith(prefix)].iloc[0]
    return f"{r.Ratio:.3g}× ({r.Ratio_CI95_lo:.3g}–{r.Ratio_CI95_hi:.3g})"


bm_e6 = pd.read_csv(DERIVED / "bm_e6_comparisons.csv")
q1_bm = bm_e6[bm_e6.Comparison.str.startswith("Q1:") & (bm_e6.Metric == "EngineLatency_ms")]


def e6r(m, src=None):
    s = q1 if src is None else src
    r = s[s.Comparison == f"Q1: {m} vs A1"].iloc[0]
    return f"{r.Ratio:.3g}× ({r.Ratio_CI95_lo:.3g}–{r.Ratio_CI95_hi:.3g})"


rows.append(dict(Claim="Optimal file size", Where="Highlight 3, abstract", Published="100–200 MB",
                 Metric="engine latency", v1_recomputed="labels: real A2–A4 = 200/43/22 MB",
                 v1_wall="", v1_files="",
                 v11=(f"Q1 vs 1 file: 128 MB {e6r('E6_128MB')}, 32 MB {e6r('E6_32MB')}, "
                      f"16 MB {e6r('E6_16MB')}, 8 MB {e6r('E6_8MB')}, 1 MB {e6r('E6_1MB')}"),
                 v11_campaign_range="", v11_consistent="1 campaign", v11_wall="",
                 v11_files="12/48/96/191/1,531", Source="E6 (both) / D2 (bare metal)",
                 bm=(f"Q1 vs 1 file: 128 MB {e6r('E6_128MB', q1_bm)}, 32 MB {e6r('E6_32MB', q1_bm)}, "
                     f"16 MB {e6r('E6_16MB', q1_bm)}, 8 MB {e6r('E6_8MB', q1_bm)}, "
                     f"4 MB {e6r('E6_4MB', q1_bm)}, 1 MB {e6r('E6_1MB', q1_bm)}; D2: "
                     f"DuckDB defaults (Snappy, 123k RG) {d2r('DuckDB defaults')}; "
                     f"codec alone {d2r('codec:')}; row groups alone {d2r('row groups:')}"),
                 bm_campaign_range="", bm_consistent="1 campaign", bm_wall="", bm_over_vm="",
                 both_consistent="no optimum on either; the Q1 floor is platform-dependent",
                 Note="no size beats one file; Q1 degrades below ~16–32 MB per file (VM) and "
                      "~4–8 MB (bare metal); writer codec/row groups matter more than file size"))

p1 = (v1.DuckDB_Planning_ms.fillna(0) == 0).mean()
b4 = e4[(e4.Model == "B4") & (e4.Query == "Q3_B4_month")].PlannerBinding_ms
bb4 = bm_e4[(bm_e4.Model == "B4") & (bm_e4.Query == "Q3_B4_month")]
cb4 = {k: d[(d.Model == "B4") & (d.Query == "Q3_B4_month") & (d.SF == 10)] for k, d in (("vm", v2), ("bm", bm))}
rows.append(dict(bm=f"B4 Q3_B4_month = {cb4['bm'].PlannerBinding_ms.median():.0f} ms of "
                    f"{cb4['bm'].EngineLatency_ms.median():.0f} ms (core); E4 run: "
                    f"{bb4.PlannerBinding_ms.median():.0f} of {bb4.EngineLatency_ms.median():.0f} ms",
                 bm_campaign_range="", bm_consistent="", bm_wall="", bm_over_vm="",
                 both_consistent="yes (dominant on both)",
                 Claim="Planning time", Where="Section 4.5.2", Published="0.0 ms everywhere",
                 Metric="planner_binding", v1_recomputed=f"{p1:.0%} of runs = 0 (parser bug)",
                 v1_wall="", v1_files="", v11=(f"B4 Q3_B4_month = {cb4['vm'].PlannerBinding_ms.median():.0f} ms of "
                                         f"{cb4['vm'].EngineLatency_ms.median():.0f} ms (core); E4 run: {b4.median():.0f} ms"),
                 v11_campaign_range="", v11_consistent="", v11_wall="", v11_files="2,526",
                 Source="core / E4", Note="measured with profiling_mode='detailed'"))

startup = {k: d[(d.Query == "Q0") & (d.RunType == "COLD")].WallClock_ms.median()
           for k, d in (("vm", v2), ("bm", bm))}
out = pd.DataFrame(rows)
out = out.rename(columns={"v11": "vm", "v11_campaign_range": "vm_campaign_range",
                          "v11_consistent": "vm_consistent", "v11_wall": "vm_wall"})
out.to_csv(DERIVED / "before_after.csv", index=False)
cols = ["Claim", "Where", "Published", "Metric", "v1_recomputed", "v1_wall", "v1_files",
        "vm", "vm_campaign_range", "vm_consistent", "vm_wall", "bm", "bm_campaign_range",
        "bm_consistent", "bm_wall", "bm_over_vm", "both_consistent", "v11_files", "Source", "Note"]
md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
md += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for _, r in out.iterrows()]
(DERIVED / "before_after.md").write_text(
    "# Before (v1.0) vs after (v1.1)\n\n"
    "Ratios of medians of COLD runs. `Metric` names the primary metric of the row "
    "(engine latency = DuckDB profile `latency`, without process start-up); the wall-clock "
    f"ratio (incl. process start-up: {startup['vm']:.0f} ms VM, {startup['bm']:.0f} ms bare metal, "
    "median wall-clock of `SELECT 1`) is given in the `*_wall` "
    "columns. v1.1 is given per platform: `vm_*` pools the VM core campaigns "
    f"({', '.join(core_ids)}), `bm_*` the bare-metal ones ({', '.join(bm_ids)}), each with a "
    "hierarchical bootstrap 95 % CI (campaigns, then runs); `*_campaign_range` = ratio in each "
    "campaign; `*_consistent` = same direction in every campaign and CI excluding 1; "
    "`bm_over_vm` = ratio of the two ratios with a CI from both bootstraps; `both_consistent` = "
    "consistent on both platforms, same direction and ≥ 1.05× on both. Files: B vs A "
    "(same layouts on both platforms).\n\n" + "\n".join(md) + "\n")
with pd.option_context("display.width", 250, "display.max_colwidth", 50):
    print(out[["Claim", "Published", "v1_recomputed", "vm", "vm_consistent", "bm", "bm_consistent",
               "bm_over_vm", "both_consistent"]].to_string(index=False))
