# Benchmarking Endee with VectorDBBench: Setup and Execution Guide

This guide provides step-by-step instructions to set up VectorDBBench and run performance benchmarks against the **Endee** vector database.

---

## Table of Contents
- [Prerequisites](#prerequisites)
- [Exclusive Endee Branch Utilities](#exclusive-endee-branch-utilities)
- [Step-by-Step Installation](#step-by-step-installation)
- [Running Benchmarks via CLI](#running-benchmarks-via-cli)
  - [Dense Performance Tests](#dense-performance-tests)
  - [Filter Performance Tests (Int and Label Filtering)](#filter-performance-tests-int-and-label-filtering)
- [Parameter Breakdown Dictionary](#parameter-breakdown-dictionary)
- [Available Datasets](#available-datasets)
- [Tracking Progress and Viewing Results](#tracking-progress-and-viewing-results)
- [Modifying the Endee Client Integration](#modifying-the-endee-client-integration)
- [Infrastructure and Hardware Optimization](#infrastructure-and-hardware-optimization)
  - [1. Storage: IOPS vs. Throughput Bottlenecks](#1-storage-iops-vs-throughput-bottlenecks)
  - [2. Networking: Public vs. Private IPs](#2-networking-public-vs-private-ips)
  - [3. Availability Zones (AZs)](#3-availability-zones-azs)
  - [4. CPU Architecture and SIMD Instructions](#4-cpu-architecture-and-simd-instructions)
  - [5. Server Thread Allocation](#5-server-thread-allocation)
- [System Monitoring During Benchmarks](#system-monitoring-during-benchmarks)
  - [Prerequisites: Installing the Tools](#prerequisites-installing-the-tools)
  - [`MONITOR.sh` (One-Command Monitoring Launch)](#monitorsh-one-command-monitoring-launch)
  - [1. `htop` (CPU and Memory Monitoring)](#1-htop-cpu-and-memory-monitoring)
  - [2. `iostat` (Disk I/O Bottlenecks)](#2-iostat-disk-io-bottlenecks)
  - [3. `nmon` (The All-in-One Dashboard)](#3-nmon-the-all-in-one-dashboard)
  - [4. `curl` (Network Latency and API Response Time)](#4-curl-network-latency-and-api-response-time)
  - [5. `iperf3` (Maximum Network Bandwidth Testing)](#5-iperf3-maximum-network-bandwidth-testing)

---

## Prerequisites

- You must use **Python 3.11** or **Python 3.12**. Older or newer versions of Python may cause dependency conflicts with the benchmarking tool.
- **Git** must be installed to clone the repository and manage version control.

---

## Exclusive Endee Branch Utilities

This README is specifically designed for the **Endee branch** of the VectorDBBench repository. This branch contains significant improvements, custom scripts, and modifications that simplify the benchmarking process and add advanced testing capabilities. 

Key additions in this branch include:

1. **`setup_bench.py` (Automated Installation)** Running this script completely automates the manual setup process. It clones the repository, switches to the Endee branch, detects your Python version (building the correct version if it is missing), and installs all required dependencies.
2. **`DOWNLOAD_DATASET.py` (Fast Parallel Downloading)**
   While the standard benchmark automatically downloads datasets on the fly, it can be extremely slow. This standalone utility allows you to pre-download datasets to a specific path using multiple parallel workers, drastically reducing wait times.
3. **`DEMO_CLI_COMMANDS.txt` (Command Templates)**
   A quick-reference file containing pre-formatted template commands for various Endee benchmarks. You can easily copy, paste, and tweak parameters without having to memorize the CLI structure.
4. **`stress_tests/` (Directory)**
   A collection of scripts designed to spawn multiple concurrent VectorDBBench instances simultaneously. This allows you to push the Endee database to its absolute limits to test connection handling and peak concurrency capacity.
5. **`stability_test_scripts/` (Directory)**
   Scripts to test the long-term stability of the database. These automate aggressive insertion and deletion loops (by specifying vector counts or percentages) immediately followed by benchmarking to measure performance degradation over time.
6. **Modified `serial_runner.py` (Resumable Benchmarks)**
   The official VectorDBBench tool does not support resuming a crashed or interrupted benchmark. In the Endee branch, the core `serial_runner.py` has been modified to track the exact number of vectors inserted. It creates an `insert_checkpoint.json` file (tied uniquely to the index name). If your load process drops, you can simply re-run your original CLI command, and the benchmark will resume loading from the exact point it stopped!

---

> **Note:** To edit this README, modify the file located at `.github/README.md` — not the one in the root directory.

---

## Step-by-Step Installation

### 1. Clone the Repository

First, clone the official VectorDBBench repository and navigate into the folder:

```bash
git clone https://github.com/zilliztech/VectorDBBench.git
cd VectorDBBench
```

**Note**: If using the Endee Repository (forked from VectorDBBench), which consists other scripts for stability/stress tests.

```bash
git clone https://github.com/endee-io/VectorDBBench.git
cd VectorDBBench
git checkout Endee
```

### 2. Create and Activate a Virtual Environment

It is highly recommended to isolate your dependencies using a virtual environment.

**For Mac/Linux:**

```bash
python3 -m venv venv
source venv/bin/activate
```

**For Windows:**

```powershell
python -m venv venv
.\venv\Scripts\activate
```

### 3. Install VectorDBBench and Endee Dependencies

Install the benchmarking tool along with the specific client dependencies for Endee.

If you are running directly from the cloned repository source (recommended for the latest changes):

```bash
pip install -e '.[endee]'
```

> **NOTE:** Alternatively, if installing via PyPI, you would use `pip install 'vectordb-bench[endee]'`

### 4. Quick Verification (UI)

To verify that the installation was successful, you can launch the UI by running:

```bash
init_bench
```

> **NOTE:** The UI is a fantastic way to explore available parameters, test connections, and run smaller-scale benchmarks. However, for heavy workloads or massive datasets, the CLI is highly recommended for maximum stability. When you are ready to execute your large-scale benchmarks, simply press `Ctrl+C` to stop the UI server and proceed to the CLI instructions.

---

## Running Benchmarks via CLI

The Command Line Interface (CLI) is the most robust way to run benchmarks.

### View All Available Options First

Before running your first benchmark, it is highly recommended to view the complete list of available flags and parameters supported by VectorDBBench and Endee client. You can do this by running the built-in help command:

```bash
vectordbbench endee --help
```

---

### Dense Performance Tests
#### The Standard CLI Command (Load + Query)

Create a bash script (e.g., `run_bench.sh`) or paste the following directly into your terminal. This command creates the index, loads the data, and runs both concurrent and serial searches.

```bash
#!/bin/bash

NUM_PER_BATCH=1000 \
DATASET_LOCAL_DIR="$HOME/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --region "YOUR_REGION" \
  --base-url "http://localhost:8080/api/v1" \
  --index-name "Index_name" \
  --task-label "Task_description" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float16 \
  --version 1 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --drop-old \
  --load \
  --search-concurrent \
  --search-serial
```

> **NOTE:** The command above is a baseline example. You can (and should) adjust parameters such as `--m`, `--ef-con`, `--ef-search`, `--precision`, `--case-type`, `--k`, and `--num-concurrency` to match your specific hardware capabilities and testing requirements. Refer to the **Parameter Breakdown Dictionary** below for a full list of allowed values and explanations.

### Query-Only Command (Skip Loading)

If the data is already loaded in the database and you **only want to run the search queries**, replace `--drop-old` and `--load` with their `--skip` counterparts:

```bash
  # ... (other parameters remain the same) ...
  --skip-drop-old \      # <--- CHANGED
  --skip-load \          # <--- CHANGED
  --search-concurrent \
  --search-serial
```

For example, if you only need to query an existing index without reupserting the data:

```bash
#!/bin/bash

NUM_PER_BATCH=1000 \
DATASET_LOCAL_DIR="$HOME/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --region "YOUR_REGION" \
  --base-url "http://localhost:8080/api/v1" \
  --index-name "Index_name" \
  --task-label "Task_description" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float16 \
  --version 1 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --skip-drop-old \
  --skip-load \
  --search-concurrent \
  --search-serial
```

### Load-Only Command (Skip Search)

If you only want to **load data into the index** without running any benchmarks — skipping both the QPS (throughput) test and the serial recall/latency test — replace `--search-concurrent` and `--search-serial` with their `--skip` counterparts:

```bash
  # ... (other parameters remain the same) ...
  --drop-old \
  --load \
  --skip-search-concurrent \    # <--- CHANGED: skips the concurrent QPS/throughput test
  --skip-search-serial           # <--- CHANGED: skips the serial recall and latency test
```

For example, if you want to (re)load data into the index and defer all benchmarking to a later run:

```bash
#!/bin/bash

NUM_PER_BATCH=1000 \
DATASET_LOCAL_DIR="$HOME/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --region "YOUR_REGION" \
  --base-url "http://localhost:8080/api/v1" \
  --index-name "Index_name" \
  --task-label "Task_description" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float16 \
  --version 1 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --drop-old \
  --load \
  --skip-search-concurrent \
  --skip-search-serial
```

> **NOTE:** After loading, you can run the Query-Only command above (with `--skip-drop-old` and `--skip-load`) to benchmark the already-loaded index at any time without reinserting the data.

---

### Filter Performance Tests (Int and Label Filtering)

VectorDBBench provides specific test cases to benchmark how well the database handles this filtered search. There are two main types of filter tests:

1. **Int Filter (`NewIntFilterPerformanceCase`)**: Tests performance when filtering by numeric/integer values (e.g., ID thresholds, timestamps).
2. **Label Filter (`LabelFilterPerformanceCase`)**: Tests performance when filtering by categorical string labels (e.g., tags, categories).

> **Important Note:** When running filter cases, you must change `--case-type` to the filter case name and explicitly define the underlying dataset using `--dataset-with-size-type`. Additionally, you **must** specify the filter's selectivity by adding `--filter-rate` (for Int filters) or `--label-percentage` (for Label filters).

#### Generic Command Example for Int Filter Test

```bash
#!/bin/bash

NUM_PER_BATCH=1000 \
DATASET_LOCAL_DIR="$HOME/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --region "YOUR_REGION" \
  --base-url "http://localhost:8080/api/v1" \
  --index-name "Index_name_int_filter" \
  --task-label "Task_description" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float16 \
  --version 1 \
  --prefilter-cardinality-threshold 10000 \
  --filter-boost-percentage 50 \
  --case-type NewIntFilterPerformanceCase \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --filter-rate 0.99 \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --drop-old \
  --load \
  --search-concurrent \
  --search-serial
```

#### Generic Command Example for Label Filter Test

```bash
#!/bin/bash

NUM_PER_BATCH=1000 \
DATASET_LOCAL_DIR="$HOME/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --region "YOUR_REGION" \
  --base-url "http://localhost:8080/api/v1" \
  --index-name "Index_name_label_filter" \
  --task-label "Task_description" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float16 \
  --version 1 \
  --prefilter-cardinality-threshold 10000 \
  --filter-boost-percentage 100 \
  --case-type LabelFilterPerformanceCase \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --label-percentage 0.001 \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --drop-old \
  --load \
  --search-concurrent \
  --search-serial
```

---

### Parameter Breakdown Dictionary

Understanding these parameters allows you to fine-tune the benchmark for different hardware sizes and scenarios.

#### Environment Variables (Set before the command)

| Variable | Description |
| --- | --- |
| `NUM_PER_BATCH` | The number of vectors the benchmark client will send to Endee in a single API payload. |
| `DATASET_LOCAL_DIR` | The absolute path where the downloaded `.parquet` dataset files are stored. |

#### Connection Parameters

| Parameter | Description |
| --- | --- |
| `--token` | Your Endee authentication token (can be omitted or set to `""` if auth is disabled). |
| `--base-url` | The HTTP endpoint of your Endee database instance. |
| `--region` | The region string (if applicable). |

#### Identification Parameters

| Parameter | Description |
| --- | --- |
| `--index-name` | The actual name of the collection/index that will be created inside the Endee database. |
| `--task-label` | The name given to the final results file. Using the exact same name as the index makes it easy to match results to specific configurations. |

#### Indexing Parameters (HNSW and Schema)

| Parameter | Description |
| --- | --- |
| `--m` | HNSW parameter defining the maximum number of bi-directional links created for every new element. Standard: 16–64. Higher = more accurate but slower to build/search and uses more RAM. |
| `--ef-con` | HNSW parameter defining the size of the dynamic list for the nearest neighbors during index construction. Standard: 100–200. Higher = better index quality but slower build time. |
| `--ef-search` | HNSW parameter for the size of the dynamic list during search. Must be ≥ `k`. Higher = better recall but higher latency. |
| `--space-type` | The distance metric used. Allowed values: `cosine`, `l2`, `ip`. |
| `--precision` | The quantization level. Allowed values: `binary`, `int8`, `int16`, `float16`, `float32`. |
| `--version` | Your Endee API/schema version. |

#### Benchmark Workload Settings

| Parameter | Description |
| --- | --- |
| `--case-type` | Defines the dataset and size to use (e.g., `Performance768D1M`). See the **Available Datasets** table below for all options. |
| `--k` | The Top-K neighbors to retrieve for Recall calculation. |
| `--num-concurrency` | The number of parallel client threads sending search requests. Can be a single number (`"8"`) or a comma-separated list to test multiple loads sequentially (`"1,10,20,50"`). |
| `--concurrency-duration` | How many seconds to sustain the concurrent search load before recording the final QPS. |
| `--concurrency-timeout` | Failsafe timeout in seconds if a concurrency test hangs. |

#### Execution Flags (Toggle Behavior)

| Flag | Skip Counterpart | Effect |
| --- | --- | --- |
| `--drop-old` | `--skip-drop-old` | Whether to delete an existing index with the same `--index-name` before starting. |
| `--load` | `--skip-load` | Whether to read the dataset and insert it into the database. |
| `--search-concurrent` | `--skip-search-concurrent` | Whether to run the high-throughput concurrent QPS test. |
| `--search-serial` | `--skip-search-serial` | Whether to run the single-thread serial recall and latency test. |

#### Filter-Specific Workload Settings

| Parameter | Description |
| --- | --- |
| `--dataset-with-size-type` | Required *only* for filter cases. Tells the benchmark which base dataset to apply the filters to (e.g., `"Medium Cohere (768dim, 1M)"`, `"Large OpenAI (1536dim, 5M)"`). |
| `--filter-rate` | Used in `NewIntFilterPerformanceCase`. Selectivity of the integer filter. `0.99` matches the last 1% of the dataset (highly restrictive); `0.01` is a light filter. **Allowed values:** `0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999` |
| `--label-percentage` | Used in `LabelFilterPerformanceCase`. Selectivity of the label filter. `0.001` means the target label exists on only 0.1% of the dataset. **Allowed values:** `0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5` |

#### Endee-Specific Filter Tuning Parameters

| Parameter | Description |
| --- | --- |
| `--prefilter-cardinality-threshold` | If the number of elements matching the filter is below this threshold (e.g., `10000`), Endee fetches filtered elements first and calculates exact distances (pre-filtering), bypassing the ANN graph for better accuracy on highly restrictive queries. |
| `--filter-boost-percentage` | Instructs the database to temporarily boost search exploration parameters dynamically when a filter is applied to maintain high recall. |

---

## Available Datasets

When setting the `--case-type` parameter, the benchmark tool automatically downloads the corresponding dataset. Below is a reference table for the available performance test cases and their approximate download sizes.

| `--case-type` | Dataset Source | Dimensions | Vector Count | Approx. Download Size |
| --- | --- | --- | --- | --- |
| **`Performance1536D50K`** | OpenAI (Small) | 1536 | 50,000 | ~450 MB |
| **`Performance768D1M`** | Cohere (Medium) | 768 | 1,000,000 | ~4.5 GB |
| **`Performance1536D500K`** | OpenAI (Medium) | 1536 | 500,000 | ~4.5 GB |
| **`Performance768D10M`** | Cohere (Large) | 768 | 10,000,000 | ~45 GB |
| **`Performance1536D5M`** | OpenAI (Large) | 1536 | 5,000,000 | ~45 GB |
| **`Performance768D100M`** | LAION (Large) | 768 | 100,000,000 | ~250 GB |

> **NOTE:** Make sure your `DATASET_LOCAL_DIR` has enough storage space to accommodate the dataset size before running the benchmark.

---

## Tracking Progress and Viewing Results

### 1. Live Logs

To watch the progress of the benchmark (data loading, query execution), navigate to the `VectorDBBench` folder, and `open` or tail the log file:

```bash
cd VectorDBBench
tail -f logs/vectordb_bench.log
```

### 2. Final Results

Once the benchmark finishes successfully, the comprehensive results (including QPS, Latency, and Recall) are saved as a JSON file. You can find it in the results directory:

```text
VectorDBBench/vectordb_bench/results/Endee/result_YYYYMMDD_run_id_endee.json
```

> **NOTE:** VectorDBBench replaces `YYYYMMDD_run_id` with the actual timestamp of your run.

---

## Modifying the Endee Client Integration

If you need to customize how VectorDBBench interacts with Endee (e.g., adding new search parameters, modifying the data insertion logic, or adding new CLI flags), you can edit the internal integration code directly.

Navigate to the Endee client directory within your cloned repository:

```bash
cd VectorDBBench/vectordb_bench/backend/clients/endee/
```

Inside this directory, you will find the three core files that power the integration. Here is where you should make your changes based on what you want to achieve:

* **`cli.py` (Command Line Interface):** This file defines the terminal commands. If you want to add a new flag to the `vectordbbench endee` command (like `--new-tuning-param`), you must define it here using the `click` decorators and add it to the `EndeeTypedDict`.
* **`config.py` (Configuration and Schema):** This file handles parameter validation and data structures using Pydantic. If you added a new CLI parameter, you need to add it to the `EndeeConfig` class here so it is securely passed down to the database client.
* **`endee.py` (Core Database Wrapper):** This is the main engine. It contains the `Endee` class which implements the required `VectorDB` benchmark methods. Edit this file to change the actual logic for how indexes are created (`_create_index`), how data is batched and uploaded (`insert_embeddings`), or how the search API is called (`search_embedding`).

> **TIP:** Because you installed the benchmark tool in "editable" mode (`pip install -e '.[endee]'`), any changes you save to these three Python files will take effect immediately the next time you run a `vectordbbench` command. You do not need to reinstall the package!

---

## Infrastructure and Hardware Optimization

> **Before running large benchmarks**, review this section to ensure your environment is configured correctly. Unoptimized infrastructure — such as under-provisioned disk IOPS, cross-region network routing, or mismatched CPU thread counts — will produce artificially low QPS and inflated latency results that do not reflect Endee's actual capabilities.

Benchmarking a vector database is as much a test of your cloud infrastructure as it is of the database software. To get the maximum Queries Per Second (QPS) and the lowest latency, your hardware environment must be configured correctly. Failing to optimize the following infrastructure components will result in artificially capped benchmark scores.

### 1. Storage: IOPS vs. Throughput Bottlenecks

Vector databases rely heavily on disk performance, especially when the dataset exceeds available RAM and the database must read the HNSW graph directly from storage. You must provision your cloud block storage (e.g., AWS EBS, GCP Persistent Disk) balancing two metrics:

* **Throughput (MB/s):** This dictates how fast data can be sequentially written to or read from the disk. 
  * *When it matters:* During the `--load` phase. High throughput ensures that massive datasets (like the 45GB 10M vectors) are ingested and saved to disk quickly without stalling the CPU.
* **IOPS (Input/Output Operations Per Second):** This measures how many distinct, random read/write requests the disk can handle per second.
  * *When it matters:* During the `--search-concurrent` phase. 
  * **The IOPS Bottleneck:** Traversing an HNSW index involves "hopping" across the graph to find the nearest neighbors. If the graph does not fit in memory, every hop requires a small, random read from the SSD. If you run 16 concurrent search threads (`--num-concurrency 16`), and each search requires 50 graph hops, your disk is suddenly bombarded with thousands of random read requests per second. If your cloud disk is provisioned for only 3,000 IOPS, the disk queue will fill up, the CPU will be forced to wait (`iowait`), and your search latency will spike massively. **Always use high-IOPS NVMe SSDs for production vector workloads.**

### 2. Networking: Public vs. Private IPs

Where you place the VectorDBBench client relative to the Endee server drastically alters your results.

* **Private IP (Highly Recommended):** If the benchmark machine and the Endee server are in the same cloud provider (e.g., both in AWS), *always* use the server's Private/Internal IP in the `--base-url`. Private IPs route traffic exclusively over the cloud provider's high-speed internal fiber backbone. This yields massive bandwidth, zero internet jitter, and $<1\text{ms}$ network latency.
* **Public IP (Not Recommended for Benchmarks):** Using a Public IP forces the traffic to leave the cloud's internal network, hit an Internet Gateway, and route back in. This introduces variable latency spikes, drastically lowers your maximum throughput (often capped by cloud NAT gateways), and can incur heavy data transfer out (DTO) costs.

### 3. Availability Zones (AZs)

Even within the same cloud region (e.g., `us-east-1`), the physical distance between datacenters matters for microsecond-level benchmarking.

* **Same-AZ Benchmarking:** For the purest test of Endee's raw performance, ensure both the VectorDBBench VM and the Endee Server VM are deployed in the **exact same Availability Zone** (e.g., `us-east-1a`). This minimizes network overhead to practically zero.
* **Cross-AZ Benchmarking:** If the client is in `us-east-1a` and the server is in `us-east-1b`, the traffic must cross regional fiber links. This generally adds `1ms` to `2ms` of unavoidable network latency to every single query. While $1\text{ms}$ sounds small, if Endee is processing a query in $2\text{ms}$, a $1\text{ms}$ Cross-AZ penalty artificially inflates your benchmarked latency by 50%.

### 4. CPU Architecture and SIMD Instructions

Vector similarity search (calculating Cosine, L2, or Inner Product distances) requires massive amounts of parallel mathematics. Endee leverages **SIMD (Single Instruction, Multiple Data)** CPU extensions to calculate distances for hundreds of vector dimensions in a single CPU clock cycle.

To get optimal performance, ensure your server hardware supports, and your Endee binary is compiled for, the highest available SIMD instruction set:

* **x86_64 Architecture (Intel/AMD):**
  * **AVX-512:** The absolute gold standard for vector math. Typically found on newer Intel Xeon or AMD EPYC processors. Provides the highest throughput.
  * **AVX2:** The baseline standard. Available on almost all modern x86 chips. Fast, but processes half the data per clock cycle compared to AVX-512.
* **ARM Architecture (AWS Graviton, GCP Axion, Apple Silicon):**
  * **SVE / SVE2 (Scalable Vector Extension):** Found on modern ARM datacenter chips (like AWS Graviton3/4). Highly optimized for vector machine learning workloads.
  * **NEON:** The standard ARM SIMD extension. Very efficient but lacks the massive width of SVE.

> **TIP:** You can check what SIMD instructions your Linux server supports by running `lscpu | grep -i 'avx\|sve\|neon'`. Ensure your Endee configuration is explicitly utilizing these instruction sets.

### 5. Server Thread Allocation

Finally, check the configuration file of the Endee server itself before running the benchmark. The number of background threads Endee allocates for indexing and searching should mathematically align with your hardware.

* **Search Threads:** Ideally, the number of active search threads should equal the number of physical CPU cores. Over-provisioning threads (e.g., 64 threads on a 16-core machine) causes heavy "context switching," where the CPU wastes time juggling tasks rather than calculating vector distances.
* **Client Concurrency vs. Server Threads:** Your VectorDBBench `--num-concurrency` setting dictates how many parallel requests hit the server. To find your system's "sweet spot" (maximum QPS before latency degrades), slowly step up the client concurrency (e.g., `--num-concurrency "4,8,16,32"`) and monitor when the server's CPU utilization hits 100%.

---

## System Monitoring During Benchmarks

Benchmarking isn't just about looking at the final QPS (Queries Per Second) or Latency numbers; it is about understanding *why* you got those numbers. Is the database limited by the CPU? Is it waiting on slow disk reads? Did it run out of RAM and start swapping?

To get a complete picture of performance, you should actively monitor the server's hardware during the test. Below are the three essential Linux monitoring tools (instructions tailored for **Debian/Ubuntu** systems).

---

### Prerequisites: Installing the Tools

Run this command on the server hosting the Endee database to install all required monitoring tools at once:

```bash
sudo apt update
sudo apt install -y htop sysstat nmon iperf3
```

> **NOTE:** The `sysstat` package provides the `iostat` command.

---

### `MONITOR.sh` (One-Command Monitoring Launch)

Rather than opening each tool manually across multiple terminal sessions, use the `MONITOR.sh` script included in this branch. It launches `htop`, `iostat`, and `nmon` simultaneously — each in its own pane — so you have a full system overview from a single command.

```bash
bash MONITOR.sh
```

> **NOTE:** Requires `tmux` to be installed (`sudo apt install -y tmux`). The script will open a new tmux session with each monitoring tool running in a dedicated pane.

---

### 1. `htop` (CPU and Memory Monitoring)

`htop` is an interactive, real-time process viewer. It is the best tool to see if your vector database is fully utilizing the available CPU cores and how much RAM the HNSW graph is consuming.

**How to run:**

```bash
htop
```

**What to watch for during the benchmark:**

* **CPU Bars (Top Left):** During the `--load` phase (index building) and `--search-concurrent` phase, you want to see these bars maxed out (mostly green and red). If CPU usage is low (e.g., 20%) but the benchmark is running, your database is bottlenecked by something else (likely Disk I/O or network).
* **Memory / MEM Bar:** Watch this closely during the `--load` phase. Vector databases store the HNSW graph in memory, so you should expect to see this bar climb steadily. If it maxes out, the OS will start swapping.
* **Load Average (Top Right):** These three numbers represent the system's average load over the last 1, 5, and 15 minutes. If this number significantly exceeds your total number of CPU cores, the system is suffering from severe thread contention or I/O queuing, which will drag down your benchmark scores.
* **Process State (`S` Column):** This single letter shows what the Endee process is currently doing. `R` means it is actively running. However, if you see `D` (Uninterruptible Sleep) frequently appearing, the database is frozen waiting for Disk I/O (often happening during heavy insertions or if RAM is exhausted).
* **The `RES` Column:** In the process list, look at the Endee process. The `RES` (Resident Set Size) column tells you exactly how much physical RAM the database is currently holding.

---

### 2. `iostat` (Disk I/O Bottlenecks)

When dealing with massive datasets (like the 10M or 100M vector sets) or when bypassing the RAM cache, the speed of your NVMe/SSD drive dictates your performance. `iostat` helps you identify disk bottlenecks.

**How to run:**
To monitor extended statistics (`-x`), in Megabytes (`-m`), with timestamps (`-t`), skipping the useless first historical report (`-y`), updating every `1` second, for a specific disk (replace `nvme0n1` with your actual disk name from `lsblk`):

```bash
iostat -xmty 1 /dev/nvme0n1
```

> **TIP:** If you are unsure of your disk name, just run `iostat -xmty 1` to see all disks.

**What to watch for during the benchmark:**

* **`%util` (Utilization):** How busy the disk is overall. If this hits **100%**, your disk is completely saturated, and your CPU is likely sitting idle waiting for the disk to fetch data. If this happens during a search test, your disk is your primary bottleneck.
* **`r/s` and `w/s` (IOPS):** Reads and Writes per second. This is a measure of *how many individual operations* the disk is handling. Vector databases heavily rely on high read IOPS (`r/s`) during searches because they jump around the disk reading small bits of the graph index.
* **`rMB/s` and `wMB/s` (Throughput):** Megabytes read/written per second. This measures the *total volume of data* moving.
* During the `--load` phase, `wMB/s` (Writes) should be very high as vectors are saved.
* During queries, `rMB/s` (Reads) will spike if your vectors exceed your available RAM.
* **`await` (Latency):** The average time (in milliseconds) it takes for a read or write request to be completed. If this number spikes (e.g., consistently > 5-10ms on an NVMe), your disk is struggling to keep up with the database's demands, causing your search queries to slow down.

---

### 3. `nmon` (The All-in-One Dashboard)

`nmon` (Nigel's Monitor) is an excellent tool for a bird's-eye view of the entire system. It allows you to toggle different hardware statistics on a single screen.

**How to run:**

```bash
nmon
```

Once the blank screen opens, press the following keys to toggle panels on/off:

* Press **`c`** to show CPU utilization.
* Press **`m`** to show Memory allocation.
* Press **`d`** to show Disk I/O.
* Press **`n`** to show Network traffic.

**What to watch for during the benchmark:**

* **CPU (`c`):** Watch this during the index building (`--load`) and querying phases. If your CPU cores are maxed out at 100%, Endee is calculating vector distances and traversing the HNSW graph as fast as your processor allows. This is generally a *good* sign that the database is fully utilizing your hardware.
* **Memory (`m`):** Vector databases are extremely RAM-hungry. Watch the "Active" and "Free" memory as the dataset loads. If your system completely runs out of free memory, the OS will start "swapping" (using the hard drive as temporary RAM), which will instantly crush your search performance (latency will spike, QPS will drop).
* **Disk (`d`):** Watch the `%Busy` column. You will see high write activity when Endee flushes and saves the index to the SSD. During the search phase, if the Disk is constantly at 100% busy, it means the dataset is too large to fit in your RAM, and the database is struggling to fetch vector data directly from the disk.
* **Network (`n`):** Watch the network panel when VectorDBBench is downloading the `.parquet` files from the remote server, or when the client is sending the massive `NUM_PER_BATCH` payloads to the Endee server. If your network caps out (e.g., at 1 Gbps / ~125 MB/s), your insertion speed is limited by the network interface, not the database.
* **Overall System Balance:** `nmon` is great for spotting the exact moment a bottleneck shifts. For example, during a benchmark, you might watch the CPU drop while the Disk `%Busy` spikes to 100%, instantly telling you the database just ran out of RAM cache and started reading directly from the SSD.

---

### 4. `curl` (Network Latency and API Response Time)

While the previous tools monitor the *server's internal hardware*, `curl` helps you monitor the *network overhead*. When your benchmark shows high latency, it isn't always the database's fault. It could be geographic distance, poor routing, or payload transfer times. Using a specially formatted `curl` command allows you to break down the exact lifecycle of an API request.

**How to run:**
Run this command from the machine hosting VectorDBBench, pointing it to your Endee server's IP and Port (replace `<SERVER_IP>:<PORT>` with your actual details):

```bash
curl -f -o /dev/null -s -w \
'DNS: %{time_namelookup}s\nConnect: %{time_connect}s\nTTFB: %{time_starttransfer}s\nTotal: %{time_total}s\n' \
http://<SERVER_IP>:<PORT>/api/v1/health

```

> **TIP:** You can replace `/api/v1/health` with a search endpoint if you want to test the latency of an actual query.

**How to Interpret the Results:**

* **DNS (`time_namelookup`):** The time it takes to translate a domain name into an IP address. If you are using a direct IP address (like `12.34.56.789`), this should be nearly `0.000s`. If it is high, your DNS server is slowing down your initial connection.
* **Connect (`time_connect`):** The time it takes to complete the TCP handshake. This represents the pure physical distance and routing quality between your VectorDBBench client and the Endee server. If this number is high (e.g., > 0.050s or 50ms), the servers are geographically too far apart, which will permanently skew your benchmark latency numbers.
* **TTFB / Time to First Byte (`time_starttransfer`):** This is the most critical metric for database performance. It measures the time from when the connection is established until the client receives the very first byte of the response.
  * **Why it matters:** TTFB represents the actual time Endee spent *thinking* (processing the query, traversing the HNSW graph, fetching from the SSD).
  * **How to spot issues:** If `Connect` is fast but `TTFB` is extremely slow, the network is fine, but the database itself is bottlenecked (likely maxed out CPU or waiting on slow Disk I/O).
* **Total (`time_total`):** The total time taken for the entire operation, including downloading the response payload.
  * **How to spot issues:** If `TTFB` is fast but `Total` is much higher, it means the database found the answer quickly, but your network bandwidth is struggling to download the massive JSON/MsgPack payload of vectors being returned.

---

### 5. `iperf3` (Maximum Network Bandwidth Testing)

While `curl` tests the latency of a single API request, `iperf3` tests the **maximum sheer throughput (bandwidth)** between the machine running VectorDBBench and the server hosting Endee. Because vector databases transfer massive payloads (especially when inserting `NUM_PER_BATCH=10000` dense vectors or downloading large result sets), a restricted network pipe will bottleneck your benchmark regardless of how fast your CPU or SSD is.

> **NOTE:** Ensure `iperf3` is installed on both the server and client machines (`sudo apt install iperf3`). Also, ensure **TCP port 5201** is open on the Endee server's firewall/security group.

**How to run:**
`iperf3` requires you to run a command on *both* servers.

1. **On the Endee Server (Receiver):** Start the server listener.
    ```bash
    iperf3 -s
    ```

2. **On the VectorDBBench Machine (Sender):** Run the client test. Replace `<SERVER_IP>` with your Endee server's actual IP address. Opening 1 parallel stream (`-P 1`) for 30 seconds (`-t 30`) usually saturates the link.
* **Test Upload (Client to Server):**
  ```bash
  iperf3 -c <SERVER_IP> -P 1 -t 30
  ```

* **Test Download (Server to Client):**
  ```bash
  iperf3 -c <SERVER_IP> -P 1 -t 30 --reverse
  
  # or equivalently
  iperf3 -c <SERVER_IP> -P 1 -t 30 -R
  ```

* **Test Full Duplex (Simultaneous Upload/Download):**
  ```bash
  iperf3 -c <SERVER_IP> -P 4 -t 30 --bidir
  ```

  > **Tip:** If you are testing a high-speed network (10Gbps+), a single TCP stream might hit a CPU bottleneck before maxing out the network card. Increase the parallel streams (e.g., `-P 4` or `-P 8`) to fully saturate the link.

**How to Interpret the Results:**

* **Bitrate (Bandwidth):** This is how fast data can move over your network. Look at the `[SUM]` row in iperf3 to see the total speed. If it’s much lower than your network’s advertised speed (for example, 10 Gbps but you see only 1 Gbps), your data transfers will be limited by the network.
* **Retr (TCP Retries):** This column shows how many packets had to be re-transmitted. Ideally, this number should be `0` or very low. If you see hundreds or thousands of retries, your network is experiencing heavy packet loss or congestion, which will cause random latency spikes during your search benchmarks.
* **Symmetric vs. Asymmetric Speeds:** Compare your Upload and Download test results. Some cloud VM sizes have heavily throttled upload speeds but fast download speeds. If your upload speed is poor, your index building phase will take exceptionally long, even if your search speeds are fast.
* **Duplex Degradation:** When running the `--bidir` test, watch to see if the total throughput collapses compared to the single-direction tests. If it does, the server's network interface struggles with full-duplex traffic routing under heavy load.

---

## Next Steps

Once you have a successful benchmark run, here are some natural next steps to deepen your evaluation:

- **Tune HNSW parameters** — Try varying `--m`, `--ef-con`, and `--ef-search` to find the recall/latency trade-off that best fits your use case.
- **Scale the dataset** — Move from a smaller case (e.g., `Performance768D1M`) to a larger one (e.g., `Performance768D10M`) to observe how Endee behaves under production-scale data volumes.
- **Sweep concurrency levels** — Use `--num-concurrency "1,8,16,32,64"` to plot your QPS curve and identify the saturation point for your hardware.
- **Compare precision modes** — Re-run the same benchmark with different `--precision` values (e.g., `float32` vs. `float16` vs. `int16`) to measure the accuracy-vs-performance trade-off.
- **Test filtered search** — Run `NewIntFilterPerformanceCase` or `LabelFilterPerformanceCase` to evaluate how well Endee handles real-world filtered queries at scale.

If you encounter unexpected results or want to dig deeper into what the system is doing during a run, revisit the **System Monitoring During Benchmarks** section above for live observability commands.
