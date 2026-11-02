#!/usr/bin/env python3
"""Runs the same layouts with Polars (second engine, E8).
Usage: python3 scripts/tools/polars_runner.py --help
"""
import argparse
import csv
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]
OUT = BASE / "results" / "local" / "polars_results.csv"
PY = str(BASE / ".venv" / "bin" / "python")
TIME_FIELDS = ["PeakRSS_KB", "UserCPU_s", "SysCPU_s", "FSInputBlocks"]

CODE = {
    "P0": "import polars as pl",
    "Q1": "import polars as pl, time; t=time.perf_counter(); "
          "r=pl.scan_parquet({src!r}, hive_partitioning={hive}).select("
          "pl.col('l_quantity').mean(), pl.col('l_extendedprice').sum()).collect(); "
          "print((time.perf_counter()-t)*1000)",
    "Q4": "import polars as pl, time; t=time.perf_counter(); "
          "r=pl.scan_parquet({src!r}, hive_partitioning={hive}).select(pl.len()).collect(); "
          "print((time.perf_counter()-t)*1000)",
}
HEADER = ["Timestamp", "Platform", "Engine", "EngineVersion", "SF", "Model", "LayoutFiles", "Query",
          "RunType", "RunNumber", "Threads", "WallClock_ms", "Query_ms"] + TIME_FIELDS + ["Status"]


def drop_caches():
    subprocess.run(["sudo", "-n", "/usr/local/sbin/drop-caches"], check=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sf", type=int, default=10)
    p.add_argument("--models", required=True)
    p.add_argument("--queries", default="P0,Q1,Q4")
    p.add_argument("--cold", type=int, default=10)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--out", type=Path, default=OUT, help="output CSV (appends; resumable)")
    p.add_argument("--platform", default=os.environ.get("BENCH_PLATFORM", "local"))
    a = p.parse_args()
    out = a.out

    version = subprocess.run([PY, "-c", "import polars; print(polars.__version__)"],
                             capture_output=True, text=True, check=True).stdout.strip()
    env = dict(os.environ, POLARS_MAX_THREADS=str(a.threads))
    out.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if out.exists():
        with open(out) as f:
            done = {(r["SF"], r["Model"], r["Query"], r["RunNumber"]) for r in csv.DictReader(f)}
    tmp = out.parent / "_time_tmp.txt"

    for model in a.models.split(","):
        d = BASE / "layouts" / f"sf{a.sf}" / model
        hive = any(x.is_dir() for x in d.iterdir())
        src = str(d / ("**/*.parquet" if hive else "*.parquet"))
        nfiles = sum(len([f for f in fs if f.endswith(".parquet")]) for _, _, fs in os.walk(d))
        for q in a.queries.split(","):
            code = CODE[q].format(src=src, hive=hive)
            for n in range(1, a.cold + 1):
                if (str(a.sf), model, q, str(n)) in done:
                    continue
                drop_caches()
                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
                t0 = time.perf_counter()
                pr = subprocess.run(["/usr/bin/time", "-f", "%M\n%U\n%S\n%I", "-o", str(tmp),
                                     PY, "-c", code], capture_output=True, text=True, env=env)
                wall = round((time.perf_counter() - t0) * 1000, 2)
                tv = tmp.read_text().strip().splitlines()[-4:]
                row = dict(zip(TIME_FIELDS, tv))
                row.update(Timestamp=ts, Platform=a.platform, Engine="polars", EngineVersion=version, SF=a.sf,
                           Model=model, LayoutFiles=nfiles, Query=q, RunType="COLD",
                           RunNumber=n, Threads=a.threads, WallClock_ms=wall,
                           Query_ms=pr.stdout.strip().splitlines()[-1] if pr.stdout.strip() else "",
                           Status="OK" if pr.returncode == 0 else f"ERR:{pr.returncode}")
                if pr.returncode != 0:
                    print(pr.stderr[-300:], file=sys.stderr)
                new = not out.exists()
                with open(out, "a", newline="") as f:
                    hdr = HEADER if new else next(csv.reader(open(out)))
                    w = csv.DictWriter(f, fieldnames=hdr, extrasaction="ignore")
                    if new:
                        w.writeheader()
                    w.writerow(row)
                print(f"{model} {q} {n:02d} wall={wall} ms query={row['Query_ms']} ms "
                      f"RSS={row['PeakRSS_KB']} KB [{row['Status']}]", flush=True)
    if tmp.exists():
        tmp.unlink()


if __name__ == "__main__":
    main()
