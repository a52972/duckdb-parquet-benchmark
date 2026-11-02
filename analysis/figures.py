#!/usr/bin/env python3
"""Figures of the article.
Usage: python analysis/figures.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common import DERIVED, FIG, add_engine_latency, glob_runs

OUT = FIG

C = ["#2a78d6", "#eb6834", "#1baf7a"]
MK = ["o", "s", "^"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "font.size": 9, "axes.labelsize": 9, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "legend.fontsize": 8, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False,
    "axes.spines.right": False, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "lines.linewidth": 2,
    "lines.markersize": 6, "figure.dpi": 150, "savefig.bbox": "tight",
    "pdf.fonttype": 42,
})


def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(OUT / f"{name}.{ext}", dpi=300 if ext == "png" else None)
    plt.close(fig)
    print("→", name)


def cold(d):
    return d[(d.RunType == "COLD") & (d.Status == "OK")]


def flat_medians(d, q, col, sf=10):
    x = d[(d.SF == sf) & (d.Query == q) & d.Model.str.match(r"A\d$")]
    return x.groupby("LayoutFiles")[col].median().sort_index()


order = [("E4_D1", "depth 1, 84 files"), ("B3", "depth 2, 84 files"), ("E4_D3", "depth 3, 84 files"),
         ("E4_D2x10", "depth 2, 840 files"), ("E4_D2x30", "depth 2, 2,520 files"),
         ("B4", "depth 3, 2,526 files\n(2,526 folders)"), ("E4_F2526", "flat, 2,526 files")]
def runs(pattern):
    return cold(add_engine_latency(pd.concat([pd.read_csv(p) for p in glob_runs(pattern)])))


PLAT = [("VM (VMware, 2 campaigns)", "vm"), ("bare metal (SATA SSD, 5 campaigns)", "bm")]
SHORT = {"VM (VMware, 2 campaigns)": "VM", "bare metal (SATA SSD, 5 campaigns)": "bare metal"}
EXP_ID = {"e4": ("e4_depth_count", "bm_e4"), "e7": ("e7_sorted", "bm_e7")}


_core = {}


def core(p):
    """Cold runs of the core campaigns of one platform (cached)."""
    if p not in _core:
        _core[p] = runs("core_v2*" if p == "vm" else "bm_core_*")
    return _core[p]


def exp(name, plat):
    return runs(EXP_ID[name][0 if plat == "vm" else 1])


def file_size_sweep():
    sizes = ["A1"] + [f"E6_{m}MB" for m in (512, 256, 128, 64, 32, 16, 8, 4, 1)]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0), sharey=True)
    for ax, (title, f) in zip(axes, (("VM (VMware)", "e6_summary.csv"),
                                     ("bare metal (SATA SSD)", "bm_e6_summary.csv"))):
        s = pd.read_csv(DERIVED / f).set_index("Model").reindex(sizes).dropna(subset=["mean_MB"])
        for i, q in enumerate(("Q1", "Q7")):
            ax.plot(s.mean_MB, s[f"lat_{q}"] / s.loc["A1", f"lat_{q}"], marker=MK[i], color=C[i], label=q)
        ax.set_xscale("log"); ax.invert_xaxis()
        ax.axhline(1, color=INK2, lw=1, ls="--")
        ax.set_title(title, fontsize=9)
        ax.set_xlabel("Mean file size (MB, real)")
    axes[0].set_ylabel("Engine latency relative to one file")
    axes[0].legend(frameon=False, loc="upper left")
    save(fig, "fig_file_size_sweep_platforms")


def latency_vs_files():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0), sharey=True)
    for ax, (title, p) in zip(axes, PLAT):
        for i, q in enumerate(("Q1", "Q4")):
            s = flat_medians(core(p), q, "EngineLatency_ms").dropna()
            if q == "Q4":
                s = s[s.index > 1]
            ax.plot(s.index, s.values, marker=MK[i], color=C[i],
                    label="Q1 full scan" if q == "Q1" else "Q4 COUNT(*) (from 8 files)")
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xlabel(f"Number of files (real), SF-10\n{title}")
    axes[0].set_ylabel("Engine latency (ms)")
    axes[0].legend(frameon=False, loc="upper left")
    save(fig, "fig_latency_vs_files_platforms")


def memory_vs_files():
    pfc = {"vm": pd.read_csv(DERIVED / "per_file_cost.csv"), "bm": pd.read_csv(DERIVED / "per_file_cost_bm.csv")}
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0), sharey=True)
    for ax, (title, p) in zip(axes, PLAT):
        s = flat_medians(core(p), "Q4", "PeakRSS_KB") / 1024
        ax.plot(s.index, s.values, marker=MK[0], color=C[0], linestyle="none", label="median peak RSS")
        k = pfc[p].query("Condition == 'v2' and SF == 10 and Query == 'Q4' and Metric == 'PeakRSS_KB'").Slope.iloc[0]
        xx = np.logspace(0, np.log10(7500), 50)
        ax.plot(xx, s.iloc[0] + (xx - 1) * k / 1024, color=INK2, lw=1, ls="--",
                label=f"OLS slope {k:.2f} KiB per file")
        ax.set_xscale("log")
        ax.set_xlabel(f"Number of files (real), SF-10, Q4\n{title}")
        ax.legend(frameon=False, loc="upper left")
    axes[0].set_ylabel("Peak RSS (MiB)")
    save(fig, "fig_memory_vs_files_platforms")


def depth_vs_count():
    qmap = {"E4_D1": "Q3_D1", "B3": "Q3_B3", "E4_D3": "Q3_D3", "E4_D2x10": "Q3_B3",
            "E4_D2x30": "Q3_B3", "B4": "Q3_B4_month", "E4_F2526": "Q3_B1"}
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.2), sharey=True, sharex=True)
    for ax, (title, p) in zip(axes, PLAT):
        d = exp("e4", p)
        y = np.arange(len(order))[::-1]
        sel = [d[(d.Model == m) & (d.Query == qmap[m])] for m, _ in order]
        lat = np.array([x.EngineLatency_ms.median() for x in sel])
        bind = np.array([x.PlannerBinding_ms.median() for x in sel])
        ax.barh(y, bind, color=C[1], height=0.6, label="planner_binding (listing + bind)")
        ax.barh(y, lat - bind, left=bind, color=C[0], height=0.6, label="rest of execution",
                edgecolor="white", linewidth=1)
        for yi, v in zip(y, lat):
            ax.annotate(f"{v:.0f}", (v, yi), xytext=(3, 0), textcoords="offset points",
                        va="center", fontsize=7, color=INK)
        ax.set_xlabel(f"Latency, one-month filter (ms)\n{SHORT[title]} (E4, 1 campaign)")
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(np.arange(len(order))[::-1], [lbl for _, lbl in order])
    fig.legend(*axes[1].get_legend_handles_labels(), frameon=False, loc="lower center",
               ncol=2, bbox_to_anchor=(0.6, 0.9))
    save(fig, "fig_depth_vs_count_platforms")


def sorted_vs_unsorted():
    qs = ["Q1", "Q2a", "Q2b", "Q2c", "Q5"]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9), sharey=True)
    for ax, (title, p) in zip(axes, PLAT):
        d = exp("e7", p)
        med = d.groupby(["Model", "Query"]).EngineLatency_ms.median()
        for i, (m, nf) in enumerate((("A1", 1), ("A4", 75), ("A6", 750))):
            r = [med[(m + "s", q)] / med[(m, q)] for q in qs]
            ax.plot(qs, r, marker=MK[i], color=C[i], label=f"{nf:,} file{'s' if nf > 1 else ''}")
        ax.axhline(1, color=INK2, lw=1, ls="--")
        ax.set_yscale("log")
        ax.set_xlabel(f"Query (Q1 = control)\n{SHORT[title]} (E7, 1 campaign)")
    axes[0].set_ylabel("Sorted / unsorted engine latency")
    axes[0].legend(frameon=False, loc="lower right", fontsize=7)
    save(fig, "fig_sorted_vs_unsorted_platforms")


def scale():
    sd = pd.read_csv(DERIVED / "scale_decomposition.csv")
    models = ["A1", "A3", "A4", "A5", "A6", "A7"]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0), sharey=True)
    for ax, (title, p) in zip(axes, PLAT):
        s = sd[(sd.Platform == p) & (sd.Query == "Q1")].set_index("Model").loc[models]
        ax.plot(models, s.lat_ratio, marker=MK[0], color=C[0], label="engine latency")
        ax.plot(models, s.wall_ratio, marker=MK[1], color=C[1], label="wall-clock (incl. start-up)")
        ax.axhline(10, color=INK2, lw=1, ls="--")
        ax.annotate("10× data", (2.5, 10), xytext=(0, 3), textcoords="offset points", fontsize=7,
                    color=INK2, ha="center")
        ax.set_ylim(0, 11.5)
        ax.set_xlabel(f"Model (same label at both scale factors)\n{SHORT[title]}")
    axes[0].set_ylabel("Q1, SF-10 / SF-1")
    axes[0].legend(frameon=False, loc="lower right")
    save(fig, "fig_scale_platforms")


def main():
    for f in (file_size_sweep, latency_vs_files, memory_vs_files, depth_vs_count, sorted_vs_unsorted, scale):
        try:
            f()
        except (FileNotFoundError, KeyError, IndexError, ValueError) as e:
            plt.close("all")
            print(f"skipped {f.__name__}: input missing ({type(e).__name__}: {str(e)[:80]})")


if __name__ == "__main__":
    main()
