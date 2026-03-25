#!/usr/bin/env python3
"""
DuckDB Parquet Layout Benchmark — Main Runner
Usage:
    python3 02_run_benchmark.py --sf 1
    python3 02_run_benchmark.py --sf 10
    python3 02_run_benchmark.py --sf 1 --sf 10   (run both sequentially)
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent.resolve()
DUCKDB_BIN   = BASE_DIR / "duckdb"
QUERIES_DIR  = BASE_DIR / "queries"
RESULTS_DIR  = BASE_DIR / "results"
PROFILES_DIR = BASE_DIR / "profiles"
RESULTS_CSV  = RESULTS_DIR / "benchmark_results.csv"

COLD_RUNS = 20
WARM_RUNS = 3

CSV_HEADER = [
    "SF", "Model", "Query", "RunType", "RunNumber",
    "WallClock_ms", "DuckDB_Total_ms", "DuckDB_Planning_ms",
    "PeakRAM_KB", "Status"
]

# ─────────────────────────────────────────────────────────────────────────────
# Experiment definitions
# ─────────────────────────────────────────────────────────────────────────────
FLAT_QUERIES = ["Q1", "Q2a", "Q2b", "Q2c", "Q4", "Q5", "Q6", "Q7"]

HIER_QUERIES = {
    "B1": ["Q3_B1"],
    "B2": ["Q3_B2"],
    "B3": ["Q3_B3"],
    "B4": ["Q3_B4_month", "Q3_B4_day"],
    "C1": ["Q3_Year", "Q3_Month", "Q3_Both"],
}

# B1 reuses A4's path (flat control for the hierarchy benchmark)
MODELS_BY_SF = {
    1:  {"flat": ["A1", "A3", "A4", "A5", "A6", "A7"],
         "hier": ["B1", "B2", "B3", "B4", "C1"]},
    10: {"flat": ["A1", "A2", "A3", "A4", "A5", "A6", "A7"],
         "hier": ["B1", "B2", "B3", "B4", "C1"]},
}

def experiment_path(sf: int, model: str) -> tuple:
    """Return (dir_path, glob_pattern) for a model."""
    base = BASE_DIR / "experiments" / f"sf{sf}"
    actual = "A4" if model == "B1" else model
    path = base / actual
    if model in ("B2", "B3", "B4", "C1"):
        return str(path), "**/*.parquet"
    return str(path), "*.parquet"

def orders_path(sf: int) -> str:
    return str(BASE_DIR / "data" / f"sf{sf}" / "orders.parquet")

# ─────────────────────────────────────────────────────────────────────────────
# Cache management
# ─────────────────────────────────────────────────────────────────────────────
def drop_caches():
    try:
        subprocess.run(
            ["sudo", "bash", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
            check=True, capture_output=True
        )
    except subprocess.CalledProcessError as e:
        print(f"  [WARN] Cache drop failed: {e.stderr.decode().strip()}")

# ─────────────────────────────────────────────────────────────────────────────
# DuckDB profiling JSON parser — compatible with DuckDB 1.x and 0.10.x
# ─────────────────────────────────────────────────────────────────────────────
def _find_planning_time(node: dict) -> float:
    name = node.get("name", "").upper()
    if "PLANNER" in name or "PLAN" in name:
        t = node.get("timing", 0.0)
        if t and t > 0:
            return float(t)

    for child in node.get("children", []):
        result = _find_planning_time(child)
        if result > 0:
            return result
    return 0.0


def parse_profile(profile_path: Path) -> tuple:
    try:
        with open(profile_path) as f:
            data = json.load(f)

        # ── Total execution time ──────────────────────────────────────────────
        # Try keys in order of likelihood for each version.
        # 'timing' is the canonical key in DuckDB 1.x at root level.
        # 'latency' and 'execution_time' were used in 0.x variants.
        total_s = (
            data.get("timing")          # DuckDB 1.x  ← primary
            or data.get("latency")      # DuckDB 0.x variant
            or data.get("execution_time")  # DuckDB 0.x variant
            or 0.0
        )

        # ── Planning time ─────────────────────────────────────────────────────
        # DuckDB 0.x: top-level 'planning_time' key.
        # DuckDB 1.x: no top-level key; search the operator tree instead.
        planning_s = data.get("planning_time")  # 0.x path
        if planning_s is None or planning_s == 0.0:
            planning_s = _find_planning_time(data)  # 1.x path

        return round(float(total_s) * 1000, 3), round(float(planning_s) * 1000, 3)

    except Exception:
        return None, None

# ─────────────────────────────────────────────────────────────────────────────
# Single query execution
# ─────────────────────────────────────────────────────────────────────────────
def run_one(sf: int, model: str, query_id: str, run_type: str, run_num: int,
            profile_dir: Path, ram_tmp: Path) -> dict:

    sql_path = QUERIES_DIR / f"{query_id}.sql"
    if not sql_path.exists():
        return {"Status": f"MISSING_SQL:{query_id}"}

    raw_sql = sql_path.read_text().strip()
    dir_path, _ = experiment_path(sf, model)

    sql = (raw_sql
           .replace("TARGET_PATH", dir_path)
           .replace("ORDERS_PATH", orders_path(sf)))

    profile_path = profile_dir / f"{model}_{query_id}_{run_type}_{run_num:02d}.json"

    if model == "A7":
        mem_pragma = "SET memory_limit='10GB'; "
    else:
        mem_pragma = ""

    pragma = (
        f"PRAGMA enable_profiling='json'; "
        f"PRAGMA profile_output='{profile_path}'; "
        f"{mem_pragma}"
    )

    cmd = [
        "/usr/bin/time", "-f", "%M", "-o", str(ram_tmp),
        str(DUCKDB_BIN), "-c", pragma + sql
    ]

    if run_type == "COLD":
        drop_caches()

    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.perf_counter()
    wall_ms = round((t1 - t0) * 1000, 2)

    ram_kb = ram_tmp.read_text().strip() if ram_tmp.exists() else "0"
    duckdb_total, duckdb_plan = parse_profile(profile_path)

    status = "OK" if proc.returncode == 0 else f"ERR:{proc.returncode}"
    if proc.returncode != 0:
        print(f"    [STDERR] {proc.stderr[:300]}")

    return {
        "SF":                 sf,
        "Model":              model,
        "Query":              query_id,
        "RunType":            run_type,
        "RunNumber":          run_num,
        "WallClock_ms":       wall_ms,
        "DuckDB_Total_ms":    duckdb_total,
        "DuckDB_Planning_ms": duckdb_plan,
        "PeakRAM_KB":         ram_kb,
        "Status":             status,
    }

# ─────────────────────────────────────────────────────────────────────────────
# Resume support
# ─────────────────────────────────────────────────────────────────────────────
def load_completed() -> set:
    completed = set()
    if not RESULTS_CSV.exists():
        return completed
    with open(RESULTS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["SF"], row["Model"], row["Query"],
                   row["RunType"], row["RunNumber"])
            completed.add(key)
    return completed

def append_row(row: dict):
    write_header = not RESULTS_CSV.exists()
    with open(RESULTS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# ─────────────────────────────────────────────────────────────────────────────
# Main benchmark loop
# ─────────────────────────────────────────────────────────────────────────────
def run_benchmark(sf: int):
    print(f"\n{'='*60}")
    print(f"  BENCHMARK  SF-{sf}   ({COLD_RUNS} cold + {WARM_RUNS} warm runs each)")
    print(f"{'='*60}")

    RESULTS_DIR.mkdir(exist_ok=True)
    profile_dir = PROFILES_DIR / f"sf{sf}"
    profile_dir.mkdir(parents=True, exist_ok=True)
    ram_tmp = BASE_DIR / f"_ram_tmp_{sf}.txt"

    completed = load_completed()
    cfg = MODELS_BY_SF[sf]

    experiments = []
    for model in cfg["flat"]:
        for q in FLAT_QUERIES:
            experiments.append((model, q))
    for model in cfg["hier"]:
        for q in HIER_QUERIES.get(model, []):
            experiments.append((model, q))

    total_combos = len(experiments)
    total_runs   = total_combos * (COLD_RUNS + WARM_RUNS)
    print(f"  {total_combos} model×query combinations  →  {total_runs} total DuckDB invocations")

    combo_n = 0
    for model, query_id in experiments:
        combo_n += 1

        dir_path, _ = experiment_path(sf, model)
        if not Path(dir_path).exists():
            print(f"\n  [SKIP] {model}/{query_id} — path not found: {dir_path}")
            continue

        print(f"\n  [{combo_n}/{total_combos}] SF-{sf} | {model} | {query_id}")

        # ── Cold runs ─────────────────────────────────────────────────────────
        for i in range(1, COLD_RUNS + 1):
            key = (str(sf), model, query_id, "COLD", str(i))
            if key in completed:
                print(f"    [skip] COLD {i:02d} (already recorded)")
                continue
            print(f"    COLD {i:02d}/{COLD_RUNS} ...", end=" ", flush=True)
            row = run_one(sf, model, query_id, "COLD", i, profile_dir, ram_tmp)
            append_row(row)
            completed.add(key)
            print(f"{row['WallClock_ms']} ms  RAM={row['PeakRAM_KB']} KB  [{row['Status']}]")

        # ── Warm runs ─────────────────────────────────────────────────────────
        # Verify if all runs has been already completed, skips if they are
        all_warm_completed = all(
            (str(sf), model, query_id, "WARM", str(i)) in completed
            for i in range(1, WARM_RUNS + 1)
        )

        if not all_warm_completed:
            # One discarded run to prime the OS page cache and DuckDB metadata cache
            print(f"    Warming up (1 discarded run)…", end=" ", flush=True)
            run_one(sf, model, query_id, "WARM", 0, profile_dir, ram_tmp)
            print("done")
        else:
            print(f"    [skip] Warming up (all WARM runs already recorded)")

        for i in range(1, WARM_RUNS + 1):
            key = (str(sf), model, query_id, "WARM", str(i))
            if key in completed:
                print(f"    [skip] WARM {i} (already recorded)")
                continue
            print(f"    WARM {i}/{WARM_RUNS} ...", end=" ", flush=True)
            row = run_one(sf, model, query_id, "WARM", i, profile_dir, ram_tmp)
            append_row(row)
            completed.add(key)
            print(f"{row['WallClock_ms']} ms  [{row['Status']}]")

    if ram_tmp.exists():
        ram_tmp.unlink()

    print(f"\n{'='*60}")
    print(f"  SF-{sf} complete.  Results → {RESULTS_CSV}")
    print(f"{'='*60}\n")

# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    os.chdir(BASE_DIR)

    parser = argparse.ArgumentParser(description="DuckDB Parquet Layout Benchmark")
    parser.add_argument("--sf", type=int, choices=[1, 10], action="append",
                        help="Scale factor(s) to benchmark (1 and/or 10)")
    args = parser.parse_args()

    sfs = args.sf if args.sf else [10]

    if not DUCKDB_BIN.exists():
        sys.exit(f"ERROR: DuckDB binary not found at {DUCKDB_BIN}")

    for sf in sfs:
        run_benchmark(sf)

    print("All benchmarks finished.")
    print(f"Results file: {RESULTS_CSV}")

if __name__ == "__main__":
    main()
