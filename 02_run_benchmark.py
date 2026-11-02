#!/usr/bin/env python3
"""Runs the benchmark: one DuckDB process per run, cold (page cache dropped) and warm runs.
Usage: python3 02_run_benchmark.py --help
"""
import argparse
import csv
import gzip
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR     = Path(__file__).parent.resolve()
DUCKDB_BIN   = BASE_DIR / "duckdb"
QUERIES_DIR  = BASE_DIR / "queries"
RESULTS_ROOT = Path(os.environ.get("RESULTS_ROOT", BASE_DIR / "results" / "local"))
PROFILES_ROOT = BASE_DIR / "profiles"
DEFAULT_EXPERIMENTS = BASE_DIR / "layouts"
DROP_CACHES_BIN = "/usr/local/sbin/drop-caches"

SEED = 20261026

CSV_HEADER = [
    "RunID", "Timestamp", "DuckDBVersion", "Platform", "Layout",
    "SF", "Model", "LayoutFiles", "Query", "RunType", "RunNumber",
    "Threads", "MemoryLimit", "Order", "HostFlush", "HostFlush_s",
    "WallClock_ms", "PeakRSS_KB", "UserCPU_s", "SysCPU_s",
    "FSInputBlocks", "MajorFaults", "VolCtxSw", "InvolCtxSw",
    "Latency_ms", "CPUTime_ms", "Planner_ms", "PlannerBinding_ms",
    "AllOptimizers_ms", "PhysicalPlanner_ms",
    "RowsScanned", "ScanRowsOut", "FilesRead", "BytesRead",
    "PeakBufferMemory", "PeakTempDirSize",
    "Status",
]

STRACE_HEADER = [
    "RunID", "Timestamp", "Layout", "SF", "Model", "LayoutFiles", "Query",
    "RunNumber", "WallClock_ms", "Status",
    "Syscall", "Calls", "Errors", "Seconds",
]

TIME_FORMAT = "%M\n%U\n%S\n%I\n%F\n%w\n%c"
TIME_FIELDS = ["PeakRSS_KB", "UserCPU_s", "SysCPU_s", "FSInputBlocks",
               "MajorFaults", "VolCtxSw", "InvolCtxSw"]

BASELINE_QUERY = "Q0"
FLAT_QUERIES = ["Q1", "Q2a", "Q2b", "Q2c", "Q4", "Q5", "Q6", "Q7"]

HIER_QUERIES = {
    "B1": ["Q3_B1"],
    "B2": ["Q3_B2"],
    "B3": ["Q3_B3"],
    "B4": ["Q3_B4_month", "Q3_B4_day"],
    "C1": ["Q3_Year", "Q3_Month", "Q3_Both"],
}

MODELS_BY_SF = {
    1:  ["A1", "A3", "A4", "A5", "A6", "A7", "B1", "B2", "B3", "B4", "C1"],
    10: ["A1", "A2", "A3", "A4", "A5", "A6", "A7", "B1", "B2", "B3", "B4", "C1"],
}


def default_queries(model: str) -> list:
    return HIER_QUERIES.get(model, FLAT_QUERIES)


def layout_dir(exp_dir: Path, sf: int, model: str) -> Path:
    return exp_dir / f"sf{sf}" / ("A4" if model == "B1" else model)


def platform_dir(label: str) -> str:
    """Output sub-folder for a platform label."""
    if label.startswith("baremetal"):
        return "baremetal"
    return "vm" if label.startswith("vm") else "local"


def orders_path(sf: int) -> str:
    return str(BASE_DIR / "data" / f"sf{sf}" / "orders.parquet")


def count_files(path: Path) -> int:
    n = 0
    for _, _, files in os.walk(path):
        n += sum(1 for f in files if f.endswith(".parquet"))
    return n

def drop_caches():
    """Drop the OS page cache. Raises if it fails: a COLD run without a cache"""
    if os.geteuid() == 0:
        os.sync()
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    else:
        subprocess.run(["sudo", "-n", DROP_CACHES_BIN], check=True, capture_output=True)

def _scan_nodes(node: dict):
    if node.get("operator_name") in ("READ_PARQUET", "PARQUET_SCAN", "TABLE_SCAN"):
        yield node
    for child in node.get("children", []):
        yield from _scan_nodes(child)


def _ms(x):
    return None if x is None else round(float(x) * 1000, 4)


def parse_profile(data: dict) -> dict:
    files_read = 0
    scan_rows = 0
    for n in _scan_nodes(data):
        scan_rows += int(n.get("operator_cardinality", 0) or 0)
        info = n.get("extra_info") or {}
        try:
            files_read += int(info.get("Total Files Read", 0))
        except (TypeError, ValueError):
            pass
    return {
        "Latency_ms":         _ms(data.get("latency")),
        "CPUTime_ms":         _ms(data.get("cpu_time")),
        "Planner_ms":         _ms(data.get("planner")),
        "PlannerBinding_ms":  _ms(data.get("planner_binding")),
        "AllOptimizers_ms":   _ms(data.get("all_optimizers")),
        "PhysicalPlanner_ms": _ms(data.get("physical_planner")),
        "RowsScanned":        data.get("cumulative_rows_scanned"),
        "ScanRowsOut":        scan_rows,
        "FilesRead":          files_read,
        "BytesRead":          data.get("total_bytes_read"),
        "PeakBufferMemory":   data.get("system_peak_buffer_memory"),
        "PeakTempDirSize":    data.get("system_peak_temp_dir_size"),
    }


def read_time_file(path: Path) -> dict:
    """The last len(TIME_FIELDS) lines are ours; earlier lines can be messages"""
    out = dict.fromkeys(TIME_FIELDS)
    if not path.exists():
        return out
    lines = path.read_text().strip().splitlines()[-len(TIME_FIELDS):]
    for k, v in zip(TIME_FIELDS, lines):
        out[k] = v.strip()
    return out


_STRACE_LINE = re.compile(
    r"^\s*([\d.,]+)\s+([\d.,]+)\s+(\d+)?\s+(\d+)\s+(\d+)?\s*(\w+)\s*$")


def parse_strace(path: Path) -> list:
    """Parse the summary table of `strace -c`: % time, seconds, usecs/call,"""
    rows = []
    for line in path.read_text().splitlines():
        m = _STRACE_LINE.match(line)
        if m and m.group(6) != "total":
            rows.append({"Syscall": m.group(6), "Calls": int(m.group(4)),
                         "Errors": int(m.group(5) or 0),
                         "Seconds": float(m.group(2).replace(",", "."))})
    return rows

class Runner:
    def __init__(self, args, run_dir: Path, profile_root: Path, version: str):
        self.args = args
        self.run_dir = run_dir
        self.profile_root = profile_root
        self.version = version
        self.csv_path = run_dir / "benchmark_results.csv"
        self.strace_path = run_dir / "strace_results.csv"
        self.tmp = run_dir / "_time_tmp.txt"
        self.strace_tmp = run_dir / "_strace_tmp.txt"

    def build_sql(self, sf: int, model: str, query_id: str, profile_path: Path) -> str:
        raw_sql = (QUERIES_DIR / f"{query_id}.sql").read_text().strip()
        sql = (raw_sql
               .replace("TARGET_PATH", str(layout_dir(self.args.exp_dir, sf, model)))
               .replace("ORDERS_PATH", orders_path(sf)))
        settings = (
            f"SET threads={self.args.threads}; "
            f"SET memory_limit='{self.args.memory_limit}'; "
            f"PRAGMA enable_profiling='json'; "
            f"SET profiling_mode='detailed'; "
            f"PRAGMA profile_output='{profile_path}'; "
        )
        return settings + sql

    def execute(self, sf, model, query_id, run_type, run_num, strace=False) -> dict:
        profile_dir = self.profile_root / f"sf{sf}"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / f"{model}_{query_id}_{run_type}_{run_num:02d}.json"
        if profile_path.exists():
            profile_path.unlink()

        cmd = [str(DUCKDB_BIN), "-c", self.build_sql(sf, model, query_id, profile_path)]
        if strace:
            cmd = ["strace", "-c", "-f", "-o", str(self.strace_tmp), "--"] + cmd
        cmd = ["/usr/bin/time", "-f", TIME_FORMAT, "-o", str(self.tmp)] + cmd

        host_flush, flush_s = "none", None
        if run_type in ("COLD", "STRACE"):
            drop_caches()
            if self.args.pre_cold_cmd and run_type == "COLD":
                t_f = time.perf_counter()
                pf = subprocess.run(self.args.pre_cold_cmd, shell=True,
                                    capture_output=True, text=True)
                flush_s = round(time.perf_counter() - t_f, 3)
                if pf.returncode != 0:
                    sys.exit(f"ERROR: --pre-cold-cmd failed (rc={pf.returncode}): "
                             f"{(pf.stderr or pf.stdout)[-300:]}")
                host_flush = "ok"

        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=self.args.timeout)
            rc, err = proc.returncode, proc.stderr
        except subprocess.TimeoutExpired:
            rc, err = "TIMEOUT", ""
        wall_ms = round((time.perf_counter() - t0) * 1000, 2)

        prof = {}
        if profile_path.exists():
            try:
                data = json.loads(profile_path.read_text())
                prof = parse_profile(data)
            except (OSError, ValueError):
                pass
            with open(profile_path, "rb") as fi, gzip.open(f"{profile_path}.gz", "wb") as fo:
                shutil.copyfileobj(fi, fo)
            profile_path.unlink()

        status = "OK" if rc == 0 else f"ERR:{rc}"
        if rc != 0:
            print(f"    [STDERR] {err[:300]}")

        row = {
            "RunID": self.args.run_id, "Timestamp": ts, "DuckDBVersion": self.version,
            "Platform": self.args.platform,
            "Layout": self.args.layout_label, "SF": sf, "Model": model,
            "LayoutFiles": self.layout_files[(sf, model)], "Query": query_id,
            "RunType": run_type, "RunNumber": run_num,
            "Threads": self.args.threads, "MemoryLimit": self.args.memory_limit,
            "Order": self.args.order, "HostFlush": host_flush, "HostFlush_s": flush_s,
            "WallClock_ms": wall_ms, "Status": status,
        }
        row.update(read_time_file(self.tmp))
        row.update(prof)
        if strace:
            row["_strace"] = parse_strace(self.strace_tmp) if self.strace_tmp.exists() else []
        return row

    def load_completed(self) -> set:
        done = set()
        for path in (self.csv_path, self.strace_path):
            if not path.exists():
                continue
            with open(path, newline="") as f:
                for r in csv.DictReader(f):
                    done.add((r["SF"], r["Model"], r["Query"],
                              r.get("RunType", "STRACE"), r["RunNumber"]))
        return done

    @staticmethod
    def append(path: Path, header: list, rows: list):
        new = not path.exists()
        if not new:
            with open(path, newline="") as f:
                header = next(csv.reader(f))
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
            if new:
                w.writeheader()
            w.writerows(rows)

    def _cold(self, combo, n, completed):
        sf, model, query_id = combo
        if (str(sf), model, query_id, "COLD", str(n)) in completed:
            return
        row = self.execute(sf, model, query_id, "COLD", n)
        self.append(self.csv_path, CSV_HEADER, [row])
        print(f"    COLD {n:02d}/{self.args.cold}  SF-{sf} {model} {query_id}  "
              f"{row['WallClock_ms']} ms  lat={row.get('Latency_ms')} ms  "
              f"RSS={row['PeakRSS_KB']} KB  [{row['Status']}]", flush=True)

    def _warm(self, combo, n, completed):
        sf, model, query_id = combo
        if n and (str(sf), model, query_id, "WARM", str(n)) in completed:
            return
        row = self.execute(sf, model, query_id, "WARM", n)
        if n == 0:
            return
        self.append(self.csv_path, CSV_HEADER, [row])
        print(f"    WARM {n}/{self.args.warm}  SF-{sf} {model} {query_id}  "
              f"{row['WallClock_ms']} ms  [{row['Status']}]", flush=True)

    def _strace(self, combo, n, completed):
        sf, model, query_id = combo
        if (str(sf), model, query_id, "STRACE", str(n)) in completed:
            return
        row = self.execute(sf, model, query_id, "STRACE", n, strace=True)
        base = {k: row[k] for k in STRACE_HEADER if k in row}
        srows = [dict(base, **s) for s in row["_strace"]] or [dict(base, Syscall="", Calls=0)]
        self.append(self.strace_path, STRACE_HEADER, srows)
        calls = {s["Syscall"]: s["Calls"] for s in row["_strace"]}
        print(f"    STRACE {n}  SF-{sf} {model} {query_id}  {row['WallClock_ms']} ms  "
              f"openat={calls.get('openat')}  pread64={calls.get('pread64')}  "
              f"[{row['Status']}]", flush=True)

    def _warm_done(self, combo, completed):
        sf, model, query_id = combo
        return all((str(sf), model, query_id, "WARM", str(n)) in completed
                   for n in range(1, self.args.warm + 1))

    def run(self, combos: list):
        a = self.args
        completed = self.load_completed()
        n_total = len(combos)
        print(f"  {n_total} SF×model×query combinations, {a.cold} cold + {a.warm} warm"
              f" + {a.strace_runs} strace each; order={a.order}; threads={a.threads},"
              f" memory_limit={a.memory_limit}"
              f"{'; pre-cold-cmd set' if a.pre_cold_cmd else ''}")

        if a.order == "blocked":
            for i, combo in enumerate(combos, 1):
                sf, model, query_id = combo
                print(f"\n  [{i}/{n_total}] SF-{sf} | {model} "
                      f"({self.layout_files[(sf, model)]} files) | {query_id}")
                for n in range(1, a.cold + 1):
                    self._cold(combo, n, completed)
                if a.warm and not self._warm_done(combo, completed):
                    for n in range(0, a.warm + 1):
                        self._warm(combo, n, completed)
                for n in range(1, a.strace_runs + 1):
                    self._strace(combo, n, completed)
        else:
            for n in range(1, a.cold + 1):
                order = combos[:]
                random.Random(a.seed + n).shuffle(order)
                print(f"\n  COLD repetition {n}/{a.cold}", flush=True)
                for combo in order:
                    self._cold(combo, n, completed)
            todo = [c for c in combos if a.warm and not self._warm_done(c, completed)]
            for n in range(0, a.warm + 1):
                order = todo[:]
                random.Random(a.seed + 1000 + n).shuffle(order)
                if order:
                    print(f"\n  WARM pass {n}/{a.warm}", flush=True)
                for combo in order:
                    self._warm(combo, n, completed)
            for n in range(1, a.strace_runs + 1):
                for combo in combos:
                    self._strace(combo, n, completed)

        for p in (self.tmp, self.strace_tmp):
            if p.exists():
                p.unlink()

def csv_list(s):
    return [x.strip() for x in s.split(",") if x.strip()]


def main():
    p = argparse.ArgumentParser(description="DuckDB Parquet Layout Benchmark")
    p.add_argument("--sf", type=int, action="append",
                   help="Scale factor(s); repeat for several (default: 1 and 10)")
    p.add_argument("--models", type=csv_list,
                   help="Comma-separated models (default: all for the SF). Any "
                        "directory under <experiments-dir>/sf<SF>/ can be named.")
    p.add_argument("--queries", type=csv_list,
                   help="Comma-separated queries; overrides the per-model defaults. "
                        "MODEL:QUERY restricts a query to one model")
    p.add_argument("--no-baseline", action="store_true",
                   help=f"Do not add the {BASELINE_QUERY} (SELECT 1) baseline per model")
    p.add_argument("--cold", type=int, default=20)
    p.add_argument("--warm", type=int, default=3)
    p.add_argument("--strace-runs", type=int, default=0,
                   help="Extra cold runs under strace -c -f, saved apart from the timings")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--memory-limit", default="10GB")
    p.add_argument("--timeout", type=float, default=None, help="Per-run timeout (s)")
    p.add_argument("--experiments-dir", type=Path, default=DEFAULT_EXPERIMENTS, dest="exp_dir")
    p.add_argument("--layout-label", default="v2",
                   help="Stored in the Layout column (e.g. v1-default-writer, v2)")
    p.add_argument("--run-id", default=None,
                   help="Output folder name; reuse it to resume (default: timestamp)")
    p.add_argument("--seed", type=int, default=SEED)
    p.add_argument("--platform", default=os.environ.get("BENCH_PLATFORM", "local"),
                   help="Label of the machine, stored in every row and used for the output "
                        "folder: baremetal* -> baremetal/, vm* -> vm/, else local/ "
                        "(default: $BENCH_PLATFORM or local)")
    p.add_argument("--results-root", type=Path, default=RESULTS_ROOT,
                   help="Root of the result folders (default: $RESULTS_ROOT or results/local)")
    p.add_argument("--order", choices=["blocked", "interleaved"], default="blocked",
                   help="blocked: runs of a combination together; interleaved: one run of "
                        "every combination per repetition (see module docstring)")
    p.add_argument("--pre-cold-cmd", default=None,
                   help="Shell command run (and waited for) after drop_caches and before "
                        "every COLD run, e.g. a host-cache flush; failure aborts the run")
    args = p.parse_args()

    os.chdir(BASE_DIR)
    args.exp_dir = args.exp_dir.expanduser().resolve()
    args.run_id = args.run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S")
    sfs = args.sf or [1, 10]

    if not DUCKDB_BIN.exists():
        sys.exit(f"ERROR: DuckDB binary not found at {DUCKDB_BIN}")
    if args.strace_runs and not shutil.which("strace"):
        sys.exit("ERROR: --strace-runs needs strace (sudo apt install strace)")
    if args.cold or args.strace_runs:
        try:
            drop_caches()
        except Exception as e:
            sys.exit(f"ERROR: cannot drop the page cache ({e}); run with sudo or after scripts/setup/setup_sudo.sh")

    version = subprocess.run([str(DUCKDB_BIN), "-csv", "-noheader", "-c",
                              "SELECT version()"], capture_output=True,
                             text=True, check=True).stdout.strip()

    combos, layout_files = [], {}
    for sf in sfs:
        for model in (args.models or MODELS_BY_SF[sf]):
            path = layout_dir(args.exp_dir, sf, model)
            if not path.exists():
                print(f"  [SKIP] SF-{sf} {model}: {path} not found")
                continue
            layout_files[(sf, model)] = count_files(path)
            if layout_files[(sf, model)] == 0:
                sys.exit(f"ERROR: SF-{sf} {model}: no .parquet files in {path} "
                         "(interrupted generation?); regenerate it with --force")
            if args.queries:
                queries = [q.split(":", 1)[-1] for q in args.queries
                           if ":" not in q or q.split(":", 1)[0] == model]
            else:
                queries = default_queries(model)
            if not args.no_baseline and BASELINE_QUERY not in queries:
                queries.insert(0, BASELINE_QUERY)
            for q in queries:
                if not (QUERIES_DIR / f"{q}.sql").exists():
                    sys.exit(f"ERROR: missing query file queries/{q}.sql")
                combos.append((sf, model, q))
    random.Random(args.seed).shuffle(combos)

    run_dir = args.results_root / platform_dir(args.platform) / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = run_dir / "run_config.json"
    invocations = json.loads(cfg_path.read_text()) if cfg_path.exists() else []
    cfg = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    cfg.update(argv=sys.argv, duckdb_version=version,
               started=datetime.now(timezone.utc).isoformat(),
               combinations=[list(c) for c in combos],
               layout_files={f"sf{sf}/{m}": n for (sf, m), n in layout_files.items()})
    cfg_path.write_text(json.dumps(invocations + [cfg], indent=2))

    print(f"Run {args.run_id}  DuckDB {version}  layouts: {args.exp_dir}")
    runner = Runner(args, run_dir, PROFILES_ROOT / args.run_id, version)
    runner.layout_files = layout_files
    runner.run(combos)

    uid, gid = os.environ.get("SUDO_UID"), os.environ.get("SUDO_GID")
    if os.geteuid() == 0 and uid:
        for root in (run_dir, PROFILES_ROOT):
            for dp, dns, fns in os.walk(root):
                for n in [dp] + [os.path.join(dp, x) for x in fns]:
                    os.chown(n, int(uid), int(gid))

    print(f"\nDone. Results → {runner.csv_path}")


if __name__ == "__main__":
    main()
