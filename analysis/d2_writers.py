#!/usr/bin/env python3
"""Experiment D2: codec, row-group size and file size of real writers; row-group share of the per-file cost.
Usage: python analysis/d2_writers.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from common import BM, DERIVED, FIG, INV, SEED, add_engine_latency, boot_ratio_ci, holm

OUT = BM / "bm_d2_writers"
LAYOUTS = {
    "A1": ("generator", "1 file, ZSTD, ROW_GROUP_SIZE 1048576"),
    "D2_gen128": ("generator", "12 files (~128 MB target), ZSTD, ROW_GROUP_SIZE 1048576"),
    "D2_duck_default": ("DuckDB COPY", "FILE_SIZE_BYTES '128MB', defaults"),
    "D2_duck_zstd": ("DuckDB COPY", "FILE_SIZE_BYTES '128MB', COMPRESSION ZSTD"),
    "D2_duck_zstd_rg1m": ("DuckDB COPY", "FILE_SIZE_BYTES '128MB', COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576"),
    "D2_polars": ("Polars 2.1.0", "sink_parquet defaults"),
}
QUERIES = ["Q1", "Q4", "Q7"]
FACTORS = [
    ("codec: Snappy vs ZSTD (DuckDB, 123k-row groups)", "codec", "D2_duck_zstd", "D2_duck_default"),
    ("row groups: 123k vs 1 M rows (DuckDB, ZSTD)", "row groups", "D2_duck_zstd_rg1m", "D2_duck_zstd"),
    ("files: 12 vs 1 (generator, ZSTD, 1 M-row groups)", "file count", "A1", "D2_gen128"),
    ("writer: DuckDB COPY vs generator (ZSTD, 1 M-row groups; 28 vs 12 files)", "writer", "D2_gen128", "D2_duck_zstd_rg1m"),
    ("DuckDB defaults vs single file", "all", "A1", "D2_duck_default"),
    ("Polars defaults vs single file", "all", "A1", "D2_polars"),
]


def cold():
    d = add_engine_latency(pd.read_csv(OUT / "benchmark_results.csv"))
    return d[(d.RunType == "COLD") & (d.Status == "OK") & (d.Query != "Q0")]


def sample(c, m, q):
    return c[(c.Model == m) & (c.Query == q)].EngineLatency_ms.dropna().to_numpy(float)


def ref(q):
    return "D2_gen128" if q == "Q4" else "A1"


def summary(c):
    meta = pd.read_csv(OUT / "writers_metadata.csv").set_index("model")
    inv = pd.read_csv(INV / "bm_inventory_v2.csv").query("sf == 10").drop_duplicates("model", keep="last").set_index("model")
    rows = []
    for m, (writer, opts) in LAYOUTS.items():
        r = dict(Model=m, Writer=writer, Options=opts, Codec=meta.loc[m, "codecs"],
                 Files=int(meta.loc[m, "files"]), Total_MB=inv.loc[m, "bytes"] / 1e6,
                 Mean_file_MB=inv.loc[m, "bytes"] / 1e6 / meta.loc[m, "files"],
                 Row_groups=int(meta.loc[m, "row_groups"]), RG_rows_mean=meta.loc[m, "rg_rows_mean"])
        for q in QUERIES:
            x = c[(c.Model == m) & (c.Query == q)]
            r[f"lat_{q}"] = x.EngineLatency_ms.median()
            r[f"rel_{q}"] = np.median(sample(c, m, q)) / np.median(sample(c, ref(q), q))
            r[f"FSread_MB_{q}"] = x.FSInputBlocks.median() * 512 / 1e6
            r[f"src_{q}"] = x.LatencySource.iloc[0]
        rows.append(r)
    s = pd.DataFrame(rows)
    s.to_csv(DERIVED / "bm_e6_writers.csv", index=False, float_format="%.6g")
    return s


def comparisons(c, rng):
    rows = []
    for label, factor, a, b in FACTORS:
        for q in QUERIES:
            if "A1" in (a, b) and q == "Q4":
                continue
            sa, sb = sample(c, a, q), sample(c, b, q)
            lo, hi = boot_ratio_ci(sa, sb, rng)
            rows.append(dict(Comparison=label, Factor=factor, Query=q, A=a, B=b,
                             MedianA=np.median(sa), MedianB=np.median(sb),
                             Ratio=np.median(sb) / np.median(sa), Ratio_CI95_lo=lo, Ratio_CI95_hi=hi,
                             p=mannwhitneyu(sb, sa, alternative="two-sided").pvalue))
    r = pd.DataFrame(rows)
    r["p_holm"] = holm(r.p.to_numpy())
    r.to_csv(DERIVED / "bm_d2_comparisons.csv", index=False, float_format="%.6g")
    return r


def rowgroup_share(c, s, rng, n_boot=10_000):
    """Cost per extra row group, same writer/codec, 489 vs 58 row groups. The"""
    meta = s.set_index("Model")
    drg = meta.loc["D2_duck_zstd", "Row_groups"] - meta.loc["D2_duck_zstd_rg1m", "Row_groups"]
    slopes = {}
    for plat, f in (("bm", "per_file_cost_bm.csv"), ("vm", "per_file_cost.csv")):
        p = pd.read_csv(DERIVED / f)
        p = p[(p.Condition == "v2") & (p.SF == 10) & (p.Metric == "EngineLatency_ms")]
        slopes[plat] = p.set_index("Query").Slope
    rows = []
    for q in QUERIES:
        a, b = sample(c, "D2_duck_zstd_rg1m", q), sample(c, "D2_duck_zstd", q)
        ia = rng.integers(0, len(a), (n_boot, len(a)))
        ib = rng.integers(0, len(b), (n_boot, len(b)))
        boot = (np.median(b[ib], axis=1) - np.median(a[ia], axis=1)) / drg
        per_rg = (np.median(b) - np.median(a)) / drg
        lo, hi = np.percentile(boot, [2.5, 97.5])
        row = dict(Query=q, Extra_row_groups=int(drg), ms_per_row_group=per_rg,
                   CI95_lo=lo, CI95_hi=hi)
        for plat in ("bm", "vm"):
            sl = slopes[plat].get(q, np.nan)
            row[f"per_file_slope_{plat}"] = sl
            row[f"share_of_{plat}_slope"] = per_rg / sl
            row[f"share_{plat}_CI95_lo"] = lo / sl
            row[f"share_{plat}_CI95_hi"] = hi / sl
        rows.append(row)
    r = pd.DataFrame(rows)
    r.to_csv(DERIVED / "rowgroup_share.csv", index=False, float_format="%.6g")
    return r


def figure(s):
    C, INK, INK2, GRID = ["#2a78d6", "#eb6834"], "#0b0b0b", "#52514e", "#e4e3df"
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
                         "axes.axisbelow": True, "axes.edgecolor": INK2, "xtick.color": INK2,
                         "ytick.color": INK2, "pdf.fonttype": 42, "savefig.bbox": "tight"})
    cats = [("D2_gen128", "E6_128MB", "generator\n12 files\nZSTD\n1 M RG"),
            ("D2_duck_zstd_rg1m", None, "DuckDB\n28 files\nZSTD\n1 M RG"),
            ("D2_duck_zstd", None, "DuckDB\n13 files\nZSTD\n123k RG"),
            ("D2_duck_default", "E6_duckdb_fsb128", "DuckDB\ndefaults\nSnappy\n123k RG"),
            ("D2_polars", "E6_polars_default", "Polars\ndefaults\nZSTD\n116k RG")]
    vm = pd.read_csv(DERIVED / "e6_summary.csv").set_index("Model")
    bm = s.set_index("Model")
    fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.6), sharey=True)
    x = np.arange(len(cats)); w = 0.38
    for ax, plat in zip(axes, ("vm", "bm")):
        for j, q in enumerate(("Q1", "Q7")):
            ys = []
            for bm_m, vm_m, _ in cats:
                if plat == "bm":
                    ys.append(bm.loc[bm_m, f"rel_{q}"])
                else:
                    ys.append(vm.loc[vm_m, f"lat_{q}"] / vm.loc["A1", f"lat_{q}"] if vm_m else np.nan)
            xs = x + (j - 0.5) * w
            ax.bar(xs, ys, width=w, color=C[j], edgecolor="white", linewidth=1.5,
                   label=f"{q}" + (" (full scan)" if q == "Q1" else " (join)"))
            for xi, yi in zip(xs, ys):
                if not np.isnan(yi):
                    ax.text(xi, yi + 0.02, f"{yi:.2f}", ha="center", va="bottom", fontsize=6, color=INK)
        for xi, (_, vm_m, _) in zip(x, cats):
            if plat == "vm" and vm_m is None:
                ax.text(xi, 0.05, "not\nmeasured", ha="center", va="bottom", fontsize=6.5, color=INK2)
        ax.axhline(1, color=INK2, lw=1, ls="--")
        ax.set_xticks(x, [c[2] for c in cats], fontsize=7)
        ax.set_xlabel("VM (E6, 1 campaign)" if plat == "vm" else "bare metal (D2, 1 campaign)", color=INK)
    axes[0].set_ylabel("Engine latency relative to one file (A1)", color=INK)
    axes[0].legend(frameon=False, loc="upper left", fontsize=7.5)
    for ext in ("pdf", "png"):
        fig.savefig(FIG / f"fig_writers_decomposition.{ext}", dpi=300 if ext == "png" else None)
    plt.close(fig)
    print("→ fig_writers_decomposition")


def main():
    rng = np.random.default_rng(SEED)
    c = cold()
    s = summary(c)
    r = comparisons(c, rng)
    p = rowgroup_share(c, s, rng)
    with pd.option_context("display.width", 220, "display.max_colwidth", 60):
        print(s[["Model", "Codec", "Files", "Mean_file_MB", "Row_groups", "RG_rows_mean",
                 "lat_Q1", "rel_Q1", "lat_Q4", "rel_Q4", "lat_Q7", "rel_Q7", "FSread_MB_Q1"]].round(3).to_string(index=False))
        print(r[["Comparison", "Query", "Ratio", "Ratio_CI95_lo", "Ratio_CI95_hi", "p_holm"]].round(4).to_string(index=False))
        print(p.round(4).to_string(index=False))
    figure(s)


if __name__ == "__main__":
    main()
