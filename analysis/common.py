"""Shared loading, metric and bootstrap helpers for the analysis scripts.
"""
import os
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
RESULTS = BASE / "results"
V10 = RESULTS / "v1.0" / "benchmark_results.csv"
_env_root = os.environ.get("RESULTS_ROOT")
ROOT = (BASE / _env_root if _env_root else RESULTS / "v1.1")
VM, BM = ROOT / "vm", ROOT / "baremetal"
INV = ROOT / "inventories"
DERIVED = ROOT / "derived"
FIG = ROOT / "figures" if _env_root else BASE / "figures" / "v1.1"
for _d in (DERIVED, FIG):
    _d.mkdir(parents=True, exist_ok=True)
SEED = 20261026
N_BOOT = 10_000

PRIMARY = "EngineLatency_ms"
METRIC_LABEL = {
    "EngineLatency_ms": "engine latency (ms)",
    "Latency_ms": "engine latency (ms)",
    "WallClock_ms": "wall-clock incl. process start-up (ms)",
    "PeakRSS_KB": "peak RSS (KB)",
}


PLATFORM_LABEL = {"vm": "VM (VMware, NVMe host)", "bm": "bare metal (SATA SSD)"}


def platform_key(platform: str) -> str:
    """Short platform key: "bm" for any baremetal-* label, else "vm"."""
    return "bm" if str(platform).startswith("baremetal") else "vm"


def run_path(run_id: str) -> Path:
    """Folder of a run id (results/v1.1/vm/<id> or results/v1.1/baremetal/<id>)."""
    for d in (VM, BM):
        if (d / run_id).is_dir():
            return d / run_id
    return VM / run_id


def glob_runs(pattern: str, name: str = "benchmark_results.csv"):
    """Files <name> of every run folder matching <pattern>, on both platforms,"""
    return sorted((p for d in (VM, BM) for p in d.glob(f"{pattern}/{name}")),
                  key=lambda p: p.parent.name)


def safe_write(df: pd.DataFrame, path: Path, **kw):
    """Write a CSV (kept as a function so every table is written the same way)."""
    path.write_text(df.to_csv(**kw))


def add_engine_latency(d: pd.DataFrame) -> pd.DataFrame:
    """Add EngineLatency_ms and LatencySource (see module docstring)."""
    d = d.copy()
    d["EngineLatency_ms"] = d["Latency_ms"]
    d["LatencySource"] = np.where(d["Latency_ms"].notna(), "profile", "")
    if "Query" in d and (d.Query == "Q0").any():
        key = ["RunID", "SF", "Model", "RunType"]
        q0 = (d[(d.Query == "Q0") & (d.Status == "OK")]
              .groupby(key).WallClock_ms.median().rename("_q0"))
        d = d.join(q0, on=key)
        fill = d.EngineLatency_ms.isna() & (d.Query != "Q0") & (d.Status == "OK") & d._q0.notna()
        d.loc[fill, "EngineLatency_ms"] = d.loc[fill, "WallClock_ms"] - d.loc[fill, "_q0"]
        d.loc[fill, "LatencySource"] = "wall-minus-Q0"
        d = d.drop(columns="_q0")
    return d


def load_runs(run_ids, cold_only=True) -> pd.DataFrame:
    frames = []
    for rid in run_ids:
        p = run_path(rid) / "benchmark_results.csv"
        if p.exists():
            frames.append(pd.read_csv(p).assign(Campaign=rid))
    d = add_engine_latency(pd.concat(frames, ignore_index=True))
    d["Platform"] = d["Platform"].fillna("vm-vmware") if "Platform" in d else "vm-vmware"
    d["Plat"] = d["Platform"].map(platform_key)
    d = d[d.Status == "OK"]
    return d[d.RunType == "COLD"] if cold_only else d


def iqr(x):
    q1, q3 = np.percentile(x, [25, 75])
    return q3 - q1


def holm(p):
    p = np.asarray(p, dtype=float)
    out = np.full_like(p, np.nan)
    ok = ~np.isnan(p)
    m = ok.sum()
    if m == 0:
        return out
    idx = np.argsort(p[ok])
    adj = np.maximum.accumulate((m - np.arange(m)) * p[ok][idx])
    tmp = np.empty(m)
    tmp[idx] = np.minimum(adj, 1.0)
    out[ok] = tmp
    return out


def boot_ratio_ci(a, b, rng, n_boot=N_BOOT):
    """Percentile CI of median(b)/median(a), one campaign (runs resampled)."""
    ia = rng.integers(0, len(a), size=(n_boot, len(a)))
    ib = rng.integers(0, len(b), size=(n_boot, len(b)))
    r = np.median(b[ib], axis=1) / np.median(a[ia], axis=1)
    return np.percentile(r, [2.5, 97.5])


def _stack(groups, rng):
    """Equal-length (K, n) matrix of runs per campaign (n = median size;"""
    n = int(np.median([len(g) for g in groups]))
    return np.stack([g if len(g) == n else rng.choice(g, size=n) for g in groups])


def hier_ratio(a_by_camp, b_by_camp, rng, n_boot=N_BOOT):
    """Ratio of pooled medians b/a with a hierarchical bootstrap CI."""
    keep = [i for i in range(len(a_by_camp))
            if len(a_by_camp[i]) >= 3 and len(b_by_camp[i]) >= 3]
    if not keep:
        return None
    A = [np.asarray(a_by_camp[i], float) for i in keep]
    B = [np.asarray(b_by_camp[i], float) for i in keep]
    K = len(A)
    per = [np.median(b) / np.median(a) for a, b in zip(A, B)]
    pooled = np.median(np.concatenate(B)) / np.median(np.concatenate(A))
    MA, MB = _stack(A, rng), _stack(B, rng)
    ic = rng.integers(0, K, size=(n_boot, K))
    ra = rng.integers(0, MA.shape[1], size=(n_boot, K, MA.shape[1]))
    rb = rng.integers(0, MB.shape[1], size=(n_boot, K, MB.shape[1]))
    sa = MA[ic[:, :, None], ra].reshape(n_boot, -1)
    sb = MB[ic[:, :, None], rb].reshape(n_boot, -1)
    r = np.median(sb, axis=1) / np.median(sa, axis=1)
    lo, hi = np.percentile(r, [2.5, 97.5])
    same_dir = all(x > 1 for x in per) or all(x < 1 for x in per)
    return dict(n_campaigns=K, ratio_pooled=pooled, ratio_min=min(per), ratio_max=max(per),
                per_campaign=per, hier_lo=lo, hier_hi=hi, boot=r,
                consistent=bool(same_dir and (lo > 1 or hi < 1)))


def slope(x, y):
    """OLS slope of y on x (closed form; same as np.polyfit(x, y, 1)[0])."""
    x = np.asarray(x, float); y = np.asarray(y, float)
    xm = x - x.mean()
    return float((xm * (y - y.mean())).sum() / (xm ** 2).sum())


def hier_slope(cells, rng, n_boot=2_000):
    """OLS slope y ~ x with a hierarchical bootstrap CI."""
    camps = list(cells)
    xs = sorted({x for c in camps for x in cells[c]})
    per = {}
    for c in camps:
        x = np.concatenate([[xv] * len(cells[c][xv]) for xv in xs if xv in cells[c]])
        y = np.concatenate([cells[c][xv] for xv in xs if xv in cells[c]])
        per[c] = slope(x, y)
    slopes = np.empty(n_boot)
    for b in range(n_boot):
        X, Y = [], []
        for c in rng.choice(camps, size=len(camps)):
            for xv in xs:
                if xv in cells[c]:
                    v = cells[c][xv]
                    X.append(np.full(len(v), xv, float))
                    Y.append(rng.choice(v, size=len(v)))
        X, Y = np.concatenate(X), np.concatenate(Y)
        slopes[b] = slope(X, Y)
    lo, hi = np.percentile(slopes, [2.5, 97.5])
    vals = list(per.values())
    same = all(v > 0 for v in vals) or all(v < 0 for v in vals)
    return dict(per_campaign=per, slope_min=min(vals), slope_max=max(vals),
                hier_lo=lo, hier_hi=hi, boot=slopes,
                consistent=bool(same and (lo > 0 or hi < 0)))


def ratio_of_ratios(boot_a, boot_b, point_a, point_b):
    """Ratio b/a of two independent estimates (e.g. the same ratio on two"""
    n = min(len(boot_a), len(boot_b))
    r = np.asarray(boot_b[:n], float) / np.asarray(boot_a[:n], float)
    lo, hi = np.percentile(r, [2.5, 97.5])
    return point_b / point_a, lo, hi
