#!/usr/bin/env python3
"""Run-to-run and campaign-to-campaign variation of the core benchmark.
Usage: python analysis/campaign_variation.py
"""
import pandas as pd

from common import BM, DERIVED, VM, load_runs

rows = []
for platform, ids in (("bm", sorted(p.name for p in BM.glob("bm_core_*") if p.is_dir())),
                      ("vm", sorted(p.name for p in VM.glob("core_v2*") if p.is_dir()))):
    d = load_runs(ids)
    d = d[(d.RunType == "COLD") & (d.Query != "Q0")]
    for metric in ("EngineLatency_ms", "WallClock_ms"):
        g = d.groupby(["Campaign", "SF", "Model", "Query"])[metric]
        cv = (g.std() / g.mean()).median()
        med = g.median().unstack(0)
        rows.append(dict(Platform=platform, Metric=metric, Campaigns=len(ids),
                         WithinCampaignCV=cv,
                         BetweenCampaignMaxMin=(med.max(axis=1) / med.min(axis=1)).median()))
out = pd.DataFrame(rows)
out.to_csv(DERIVED / "campaign_variation.csv", index=False, float_format="%.4f")
print(out.to_string(index=False))
