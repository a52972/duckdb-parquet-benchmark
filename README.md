# DuckDB Parquet In-Situ Benchmark

Benchmark scripts, raw results, and analysis notebooks for the paper:

> **"Analyzing DuckDB's In-Situ Query Performance on Hierarchical and Flat Parquet Layouts"**  
> D.S. Eleuterio, P. Matos, P. Oliveira — *Journal of Systems and Software*, 2026

---

## Repository Structure
To satisfy the script execution paths, the benchmark scripts and the duckdb binary must reside in the root directory.

```
duckdb-parquet-benchmark/
├── README.md                   ← This file
├── requirements.txt            ← Python dependencies for notebooks
├── duckdb                      ← DuckDB binary (to be downloaded)
├── 00_check_env.sh             ← Validates environment and dependencies
├── 01_generate_layouts.sh      ← TPC-H generation & builds all 11 file layouts
├── 02_run_benchmark.py         ← Executes queries, writes CSV output
├── queries/                    ← SQL definitions for all 8 query types
│   ├── Q1.sql
│   ├── Q2a.sql
│   └── ...
├── notebooks/                  ← Python Notebooks for data analysis
│   ├── 00_data_quality.ipynb
│   ├── 01_rq1_file_granularity.ipynb
│   ├── 02_rq2_rq3_hierarchy.ipynb
│   └── 03_summary_statistics.ipynb
├── data/                       ← (Auto-generated) Staging TPC-H Parquet files
├── experiments/                ← (Auto-generated) 11 Parquet layouts (A1-A7, B1-B4, C1)
├── profiles/                   ← (Auto-generated) JSON profile outputs from DuckDB
├── results/                    ← (Auto-generated) Raw benchmark outputs (.csv)
└── figures/                    ← (Auto-generated) Plots produced by notebooks

```

---

# Reproducing the Benchmark: Step-by-Step

### Step 1: Clone and Install Dependencies
Ensure you have Python 3.x installed. Clone this repository and install the required Python libraries for the analysis notebooks:

```bash
git clone [https://github.com/a52972/duckdb-parquet-benchmark.git](https://github.com/a52972/duckdb-parquet-benchmark.git)
cd duckdb-parquet-benchmark
pip install -r requirements.txt
```

### Step 2: Download the DuckDB Binary
The benchmark scripts expect a standalone DuckDB executable in the root directory. Download DuckDB v1.5.0 (the version used in the paper) for Linux:

```bash
wget [https://github.com/duckdb/duckdb/releases/download/v1.1.2/duckdb_cli-linux-amd64.zip](https://github.com/duckdb/duckdb/releases/download/v1.1.2/duckdb_cli-linux-amd64.zip)
unzip duckdb_cli-linux-amd64.zip
chmod +x duckdb
```

(Note: adjust the URL to strictly match DuckDB v1.5.0 once released, or the specific version utilized in your environment).

---

### Step 3: Verify the Environment
Run the environment checker to ensure your system meets all prerequisites (DuckDB execution, TPC-H extension, Python, <code>/usr/bin/time</code>, <code>sudo</code> for cache dropping, and disk space):

```bash
./00_check_env.sh
```

---

### Step 4: Generate Data and Layouts (Series A, B, and C)
This script generates the TPC-H data via DuckDB's internal dbgen extension and organizes it into the 11 specific layout models (Flat, Hierarchical, and Inverted Hierarchies).

Run the script specifying the Scale Factor (1 for ~1GB or 10 for ~10GB):

```bash
./01_generate_layouts.sh 1 # for SF-1
./01_generate_layouts.sh 10 # for SF-10
```

Note: Generating the SF-10 layouts requires at least 50GB of free disk space.

### Step 5: Run the Benchmark Workload

Execute the benchmark runner. This will perform 20 cold-cache runs and 3 warm-cache runs for every model-query combination. <code>sudo</code> privileges are required to clear the OS page cache (echo 3 > /proc/sys/vm/drop_caches) between cold runs. We recommend to run this with root user.

```bash
sudo python3 02_run_benchmark.py --sf 10
```

Once finished, the raw results will be compiled into <code>results/benchmark_results.csv</code>.

### Step 6: Analyze Results and Generate Figures
Start Jupyter Notebook and run the notebooks in sequence to reproduce all tables and figures presented in the paper:

```bash
jupyter notebook
```

- 00_data_quality.ipynb: Verifies benchmark completeness and schema.
- 01_rq1_file_granularity.ipynb: Analyzes the small-file problem (RQ1, RQ4, RQ5) and outputs Table 2 and Table 5.
- 02_rq2_rq3_hierarchy.ipynb: Analyzes partition depth and key ordering (RQ2, RQ3) and outputs Table 3 and Table 4.
- 03_summary_statistics.ipynb: Exports complete LaTeX tables and generates high-resolution figures.

## Experimental Notes
- Intra-file constraints: All Parquet files are written with ZSTD compression and a fixed row group size of 1,048,576 rows to isolate the performance impact of physical file layouts.
- Join evaluations: The orders table (~200 MB at SF-10) is stored as a single contiguous file and is read exclusively by Q7.
- Model A2 limitations: Model A2 (8 × 950 MB files) exists only at SF-10. At SF-1, it would yield a single file (identical to Model A1) and is thus omitted.
- Cache Isolation: Every cold-cache query executes in a completely fresh, isolated DuckDB sub-process.

## Citation
If you use this benchmark methodology or dataset in your research, please cite.