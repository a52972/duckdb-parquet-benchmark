#!/usr/bin/env python3
"""E2b: the original (v1.0) layouts re-measured with the current runner.
Usage: python analysis/e2b_summary.py
"""
from pathlib import Path

import pandas as pd

from common import DERIVED, VM
SYSCALLS = ["openat", "pread64", "read", "statx", "newfstatat", "getdents64",
            "mmap", "munmap", "close", "futex"]


def load(name):
    frames = [pd.read_csv(p) for p in sorted(VM.glob(f"e2b*/{name}"))]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    b = load("benchmark_results.csv")
    b = b[(b.RunType == "COLD") & (b.Status == "OK")]
    cols = ["WallClock_ms", "Latency_ms", "PlannerBinding_ms", "AllOptimizers_ms",
            "CPUTime_ms", "PeakRSS_KB", "FSInputBlocks", "FilesRead", "ScanRowsOut",
            "PeakBufferMemory", "PeakTempDirSize"]
    t = (b.groupby(["SF", "Model", "LayoutFiles", "Query"])[cols]
           .median().assign(n=b.groupby(["SF", "Model", "LayoutFiles", "Query"]).size())
           .reset_index())
    t["FSInput_MB"] = t.FSInputBlocks * 512 / 1e6
    t.to_csv(DERIVED / "e2b_summary_timing.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        print(t.round(1).to_string(index=False))

    s = load("strace_results.csv")
    if len(s):
        p = (s.pivot_table(index=["SF", "Model", "LayoutFiles", "Query"],
                           columns="Syscall", values="Calls", aggfunc="sum")
               .reindex(columns=SYSCALLS).reset_index())
        p.to_csv(DERIVED / "e2b_summary_strace.csv", index=False)
        p["openat_per_file"] = p.openat / p.LayoutFiles
        p["pread64_per_file"] = p.pread64 / p.LayoutFiles
        with pd.option_context("display.width", 250, "display.max_columns", 30):
            print(p.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
