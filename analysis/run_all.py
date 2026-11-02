#!/usr/bin/env python3
"""Regenerates every table in results/v1.1/derived and every figure in figures/v1.1.
Usage: python analysis/run_all.py            (RESULTS_ROOT=<dir> analyses a re-run; steps without inputs are skipped)
"""
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
STEPS = [
    "e2b_summary.py",
    "stats.py",
    "experiments.py",
    "per_file_cost.py",
    "claims_platforms.py",
    "d2_writers.py",
    "bm_replication.py",
    "scale_decomposition.py",
    "campaign_variation.py",
    "before_after.py",
    "figures.py",
]


def main():
    rerun = bool(os.environ.get("RESULTS_ROOT"))
    steps = STEPS + (["compare_published.py"] if rerun else [])
    failed = []
    for s in steps:
        t0 = time.time()
        r = subprocess.run([sys.executable, str(HERE / s)], cwd=HERE.parent,
                           capture_output=True, text=True)
        if r.returncode == 0:
            print(f"{s:<26} ok  {time.time() - t0:6.1f} s")
            if s == "figures.py":
                for ln in r.stdout.splitlines():
                    if ln.startswith("skipped"):
                        print("    " + ln)
            if s == "compare_published.py":
                print(r.stdout)
            continue
        if not rerun:
            sys.stderr.write(r.stdout[-2000:] + r.stderr[-4000:])
            sys.exit(f"FAILED: analysis/{s}")
        last = (r.stderr.strip().splitlines() or ["?"])[-1]
        print(f"{s:<26} skipped (inputs of this step not in the re-run: {last[:90]})")
        failed.append(s)
    if failed:
        print(f"{len(failed)} step(s) skipped; run the missing experiments to complete them.")


if __name__ == "__main__":
    main()
