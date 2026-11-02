#!/usr/bin/env python3
"""SF-10/SF-1 ratios split into start-up, planning and execution.
Usage: python analysis/scale_decomposition.py
"""
import pandas as pd

from common import BM, DERIVED, VM, load_runs

MODELS = ["A1", "A3", "A4", "A5", "A6", "A7"]
QUERIES = ["Q1", "Q6"]


def main():
    ids = sorted([p.name for p in VM.glob("core_v2*") if p.is_dir()]
                 + [p.name for p in BM.glob("bm_core_*") if p.is_dir()])
    d = load_runs(ids)
    d["Exec_ms"] = d.EngineLatency_ms - d.PlannerBinding_ms
    rows = []
    for plat, g in d.groupby("Plat"):
        for m in MODELS:
            for q in QUERIES:
                r = {"Platform": plat, "Model": m, "Query": q}
                for sf in (1, 10):
                    x = g[(g.SF == sf) & (g.Model == m) & (g.Query == q)]
                    q0 = g[(g.SF == sf) & (g.Model == m) & (g.Query == "Q0")]
                    if x.empty:
                        break
                    r.update({f"files_{sf}": int(x.LayoutFiles.iloc[0]),
                              f"wall_{sf}": x.WallClock_ms.median(),
                              f"start_{sf}": q0.WallClock_ms.median(),
                              f"plan_{sf}": x.PlannerBinding_ms.median(),
                              f"lat_{sf}": x.EngineLatency_ms.median(),
                              f"exec_{sf}": x.Exec_ms.median()})
                else:
                    for k in ("wall", "lat", "exec"):
                        r[f"{k}_ratio"] = r[f"{k}_10"] / r[f"{k}_1"]
                    r["fixed_share_SF1"] = (r["start_1"] + r["plan_1"]) / r["wall_1"]
                    rows.append(r)
    out = pd.DataFrame(rows)
    out.to_csv(DERIVED / "scale_decomposition.csv", index=False, float_format="%.6g")
    print(out[["Platform", "Model", "Query", "files_1", "files_10", "wall_ratio", "lat_ratio",
               "exec_ratio", "fixed_share_SF1"]].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
