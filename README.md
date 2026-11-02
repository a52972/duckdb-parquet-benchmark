# Analyzing DuckDB's In-Situ Query Performance on Hierarchical and Flat Parquet Layouts

[![DuckDB - 1.5.0](https://img.shields.io/badge/DuckDB-1.5.0-yellow)](https://duckdb.org/)
[![Python - 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![License - MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

Code, raw measurements and analysis of the article *Analyzing DuckDB's In-Situ Query
Performance on Hierarchical and Flat Parquet Layouts* (Eleuterio, Oliveira, Matos, 2026).

## The study

DuckDB can query Parquet files where they lie. How the files are laid out (how many,
how large, in which Hive folder tree, in which row order) then decides much of the
cost of a query. The study measures that cost for DuckDB 1.5.0 on TPC-H `lineitem` at
scale factors 1 and 10: eleven layouts (1 to 7,500 flat files; Hive trees by year,
year/month, year/month/day and month/year), eight queries, 20 cold and 3 warm runs
per combination, plus experiments that isolate hierarchy depth, key order, file
size, writer settings, row order, threads and skewed data. Everything was measured on
two machines with the same binary, data and byte-identical layouts: a bare-metal
Linux PC (5 campaigns) and a VMware VM (2 campaigns). A comparison is claimed only if it
holds in every campaign on both; results measured on one machine are marked below.

Main results (SF-10, DuckDB engine latency, bare metal / VM):

| | Bare metal | VM |
|---|---|---|
| Peak memory per extra file, `COUNT(*)` / full scan | 12.6 / 22.2 KiB | 12.3 / 21.4 KiB |
| Latency per extra file, `COUNT(*)` / full scan | 0.11 / 0.18 ms | 0.14 / 0.23 ms |
| One-month query, `/year/month/` Hive vs flat files | 8.6× faster | 8.7× faster |
| Planning cost per Hive leaf folder (the whole tree is listed before pruning) | 0.18 ms | 0.12 ms |
| Order of the partition keys, same predicate (point estimates) | ≤ 2 % | ≤ 5 % |
| 200-value top-level partition key vs 7-value one | no significant difference | not measured |
| Rows sorted by the filter column, one-month query, 1 / 75 files | 13× / 15× faster | 13× / 13× faster |
| Best file size between 1 and 512 MB | none (full scans degrade below ~8 MB) | none (below ~32 MB) |
| Unequal (Zipf) file sizes vs equal sizes, latency / peak memory | ≤ 7 % / up to 1.9× | not measured |
| `/year/month/` gain for a month holding 20 % of skewed data | 4.8× (rare month 10.9×) | not measured |

The cost grows linearly with the number of files: each file costs two `openat`
and two `pread64` calls and a fixed amount of memory. Above 8–32 MB per file, the writer's codec and row-group
size, and the row order, matter more than file size.

## Reproducing

### Tables and figures from the published measurements (any OS, Python ≥ 3.11, ~5 min)

```bash
bash reproduce.sh tables
```

or, without bash: `python -m venv .venv`, `.venv/bin/pip install -r requirements.txt`,
`.venv/bin/python analysis/run_all.py`. This rewrites `results/v1.1/derived/` and
`figures/v1.1/` from the raw runs; the tables come out identical to the published ones.

### The whole benchmark (Linux x86_64)

Needs 16 GB RAM, about 40 GB of free disk, `sudo` once (cold runs drop the page cache
through a small helper, `scripts/setup/setup_sudo.sh`) and internet access for the
DuckDB binary and its `tpch` extension.

```bash
git clone https://github.com/a52972/duckdb-parquet-benchmark.git
cd duckdb-parquet-benchmark
bash reproduce.sh setup   # packages, DuckDB 1.5.0 (SHA-256 checked), .venv, cache-drop helper
bash reproduce.sh smoke   # ~10 min: SF-1 only, a few runs per combination (needs sudo and internet, like the full run)
setsid nohup bash reproduce.sh all > reproduce.log 2>&1 < /dev/null &   # ~7 h; survives a closed SSH session
```

`all` runs these steps, which can also be run one at a time:

| Step | What it does | Time on our bare-metal machine |
|---|---|---|
| `setup` | system packages, DuckDB CLI, Python environment, cache-drop helper | a few minutes |
| `data` | TPC-H `lineitem` and `orders` at SF-1 and SF-10 (`scripts/setup/make_data.sh`) | a few minutes |
| `layouts` | the eleven layouts at both scale factors (`01_generate_layouts.sh`), ~17 GB | ~16 min |
| `core` | five core campaigns, 20 cold + 3 warm runs, interleaved order | ~3.6 h |
| `experiments` | E4, E7, E8, E9, E6, D1, D2, E10 (each writes and later deletes its layouts) | ~2.5 h |
| `analysis` | every table and figure for your runs, and the comparison with ours | ~5 min |

New runs go to `results/local/` (not tracked by git), so the published data are never
modified. `analysis` treats your machine as the bare-metal platform and uses the
published VM runs as the second platform; its outputs are in `results/local/derived/`
and `results/local/figures/`. `results/local/derived/compare_published.csv` lists every
bare-metal result of your run next to ours, with the published confidence interval.
Absolute times depend on the machine; the per-file and per-folder costs, the
directions of the effects and their orders of magnitude are the quantities to compare.

`make_data.sh` checks the generated data against `data/FINGERPRINTS.csv` (row counts
and an order-independent hash of all rows of the published input files) and then
against `data/SHA256SUMS`. On our bare-metal machine the regenerated files were identical
to the published ones byte for byte; on another machine the bytes may differ while the
rows are the same.

The VM campaigns used the scripts in `scripts/vm/`. `scripts/vm/host_cache/` empties
the file cache of a Windows host from inside a VMware guest; it is only needed to
repeat the cache experiments on a VM.

## Contents

```
reproduce.sh             one entry point for all steps above
00_check_env.sh          environment check
01_generate_layouts.sh   writes the layouts (one COPY per file; file and row counts are checked)
02_run_benchmark.py      runs the benchmark (one DuckDB process per run)
queries/                 the SQL of every query
scripts/setup/           input data and fingerprints, cache-drop helper, environment capture
scripts/baremetal/       campaign and experiment drivers (bare metal)
scripts/vm/              campaign and experiment drivers (VM)
scripts/tools/           layout inventory, cold-cache test, Polars runner
analysis/                statistics, tables and figures (run_all.py runs everything)
results/v1.1/            raw runs per platform, layout inventories, derived tables
results/v1.0/            measurements of release 1.0.0
figures/v1.1/            the figures of the article
data/                    fingerprints and checksums of the input data (the data are generated)
```

### Run folders

| Folder (`results/v1.1/…`) | Experiment |
|---|---|
| `baremetal/bm_core_a` … `bm_core_e`, `vm/core_v2`, `vm/core_v2_b` | core benchmark, SF-1 and SF-10 |
| `baremetal/bm_e4`, `vm/e4_depth_count` | E4: hierarchy depth vs number of files vs number of folders |
| `vm/e5_keyorder` | E5: key order with the same predicate |
| `baremetal/bm_d1_keycard`, `baremetal/bm_d1_strace` | D1: high-cardinality top-level key, and its system calls |
| `baremetal/bm_e6_size`, `vm/e6_size` | E6: file size from 512 MB to 1 MB |
| `baremetal/bm_d2_writers` | D2: codec, row-group size and file size of real writers |
| `baremetal/bm_e7`, `vm/e7_sorted` | E7: rows sorted by `l_shipdate` |
| `baremetal/bm_e8`, `vm/e8_mechanisms` | E8: system calls per file; Polars as a second reader |
| `baremetal/bm_e9_*`, `vm/e9_t*` | E9: threads (1, 2, 4) and memory limit (4, 8, 10 GB) |
| `baremetal/bm_e10_skew` | E10: skewed file sizes and skewed data |
| `baremetal/bm_cold_cache`, `vm/e9_cold_cache`, `vm/e9_true_cold_validation` | cold-read checks before the campaigns |
| `vm/e2b`, `vm/e2b_a7sf10` | the layouts of release 1.0.0 re-measured with the current runner |
| `inventories/` | every file of every layout: size, rows, row groups |
| `derived/` | every table computed by `analysis/` |

Each run folder holds `benchmark_results.csv` (one row per run), `run_config.json`
(the exact invocation and the shuffled order) and `env.txt` (hardware, OS and software).
Machine names and home directories were replaced by neutral labels.

Columns of `benchmark_results.csv`: `RunType` is `COLD` (page cache dropped before the
run) or `WARM`; `WallClock_ms` is the time of the whole DuckDB process; `Latency_ms` and
`PlannerBinding_ms` come from DuckDB's detailed profile (`Latency_ms` is the engine
latency used in the article; `COUNT(*)` on one file, which DuckDB answers without a
profile, has none: the article leaves that case out, and the derived tables that fill it
with the wall-clock time minus the `Q0` start-up baseline mark it `LatencySource = wall-minus-Q0`);
`PeakRSS_KB`, `UserCPU_s`, `SysCPU_s` and `FSInputBlocks` (512-byte blocks read from disk;
the MB read quoted in the article are `FSInputBlocks` × 512) come from GNU `time`;
`RowsScanned`, `ScanRowsOut`, `FilesRead` and `BytesRead` from the profile (`BytesRead`
is DuckDB's own counter, not the bytes read from disk); `LayoutFiles` is the real number of files of the layout.

## Versions

Version 1.0.0 of this repository (DOI [10.5281/zenodo.19232333](https://doi.org/10.5281/zenodo.19232333))
accompanied an earlier version of the manuscript. Its layouts had been written with DuckDB's
`COPY ... PARTITION_BY`, which by default keeps at most 100 files open and silently split
partitions into many more files than intended (e.g. 850,600 instead of 7,500). Version
1.1.0 writes every file with its own `COPY` and checks file and row counts; all
results above come from 1.1.0. `results/v1.0/` keeps the 1.0.0 measurements, and
`vm/e2b` re-measures those layouts with the current runner.

## Citation

```bib
@software{eleuterio2026duckdb_v11,
  author  = {Eleuterio, Davi Silva and Oliveira, Pedro Filipe and Matos, Paulo},
  title   = {Analyzing DuckDB's In-Situ Query Performance on Hierarchical and Flat Parquet Layouts},
  version = {1.1.0},
  doi     = {10.5281/zenodo.XXXXXXXX},
  url     = {https://github.com/a52972/duckdb-parquet-benchmark},
  year    = {2026}
}
```

## License

MIT, see `LICENSE`.
