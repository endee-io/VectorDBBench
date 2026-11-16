#!/usr/bin/env python3
"""
Runs a VectorDBBench custom-dataset benchmark against Endee and records Endee's RAM.

  [0] RAM before load (baseline)
  [1] load only   (--drop-old --load, no search)    -> insert + optimize, load_duration
  [2] wait until Endee's RAM stops changing, then record it  <- "everything inserted, before search"
  [3] search only (--skip-drop-old --skip-load)     -> recall, qps, serial p99
  [4] RAM after search
One row per run is appended to OUTPUT_CSV.

Where RAM is read: on the machine that runs Endee (Endee's API does not report memory).
  * SERVER_SSH = "user@host" -> this script SSHes to the Endee server for each reading.
    Needs key-based login without a password prompt:  ssh-copy-id user@host
  * SERVER_SSH = ""          -> Endee runs on this same machine.
Endee's RAM is the RSS of the Endee process (/proc/<pid>/status), or `docker stats` if it runs in Docker.
Peak RSS (VmHWM) is the highest RSS since the Endee process started; restart Endee before a run if you
want peak and baseline numbers that only reflect this run.

Check the RAM setup before a long run:   python run_benchmark_with_ram.py --check-ram
Run the benchmark:                        export ENDEE_TOKEN="..."; python run_benchmark_with_ram.py
"""
import csv
import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
# --- Endee / benchmark (same values as the vectordbbench command) ---
TOKEN = os.environ.get("ENDEE_TOKEN", "<ENDEE_API_TOKEN>")  # export ENDEE_TOKEN="..." (keeps the token out of this file)
REGION = "bellandur"
BASE_URL = "http://172.16.23.51:8080/api/v2"
DATASET_LOCAL_DIR = "/home/launchx/vectordataset"
COLLECTION_NAME = "3M_index"
TASK_LABEL = "3M_run1"
NUM_PER_BATCH = 1000
M = 16
EF_CON = 128
EF_SEARCH = 128
SPACE_TYPE = "cosine"
PRECISION = "int16"
K = 30
CONCURRENCY = 16
CONCURRENCY_DURATION = 30

# --- Custom dataset (folder: DATASET_LOCAL_DIR/DATASET_NAME/DATASET_NAME) ---
CUSTOM_CASE_NAME = "Custom 768D 3M"
DATASET_NAME = "custom_768d_3m"
DATASET_SIZE = 3_000_000
DATASET_DIM = 768
LOAD_TIMEOUT = 360000        # seconds
OPTIMIZE_TIMEOUT = 360000    # seconds

# --- RAM measurement on the Endee server ---
SERVER_SSH = "radxa@172.16.23.51"   # "" if Endee runs on this machine
ENDEE_DOCKER_CONTAINER = "endee"          # container name if Endee runs in Docker, else ""
ENDEE_PROCESS_PATTERN = "endee"      # native process: `pgrep -o -f` pattern (oldest match is used)
SETTLE_INTERVAL = 10                 # seconds between RAM samples after load
SETTLE_SAMPLES = 3                   # settled = this many samples in a row within SETTLE_TOLERANCE
SETTLE_TOLERANCE = 0.01              # 1%
SETTLE_MAX_WAIT = 900                # record anyway after this many seconds

RESULTS_DIR = ""                     # "" = VectorDBBench's results dir (vectordb_bench/results)
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ram_benchmark_results.csv")

GIB = 1024 ** 3


# ============================================================
# RAM MEASUREMENT
# ============================================================
def _ram_shell_script() -> str:
    """Shell snippet run on the Endee machine; prints one 'KEY value' per line."""
    system_mem = "free -b | awk '/^Mem:/ {print \"SYS_USED\", $3; print \"SYS_TOTAL\", $2}'\n"
    if ENDEE_DOCKER_CONTAINER:
        return (
            f"echo 'SOURCE docker container {ENDEE_DOCKER_CONTAINER}'\n"
            f"docker stats --no-stream --format 'DOCKER_MEM {{{{.MemUsage}}}}' {ENDEE_DOCKER_CONTAINER} || exit 3\n"
            + system_mem
        )
    # Fed through stdin to `bash -s`, so the pattern never appears in this shell's own command line.
    return (
        f"PID=$(pgrep -o -f '{ENDEE_PROCESS_PATTERN}')\n"
        f"if [ -z \"$PID\" ]; then echo \"ERROR no process matches '{ENDEE_PROCESS_PATTERN}'\"; exit 3; fi\n"
        "echo \"SOURCE pid $PID: $(ps -o args= -p $PID | cut -c1-120)\"\n"
        "grep -E '^(VmRSS|VmHWM):' /proc/$PID/status\n"
        + system_mem
    )


_SIZE_UNITS = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4,
               "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def _parse_size(text: str) -> float:
    """'1.5GiB' / '512MiB' / '800MB' -> bytes."""
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not m or m.group(2).lower() not in _SIZE_UNITS:
        raise ValueError(f"Cannot parse memory size: {text!r}")
    return float(m.group(1)) * _SIZE_UNITS[m.group(2).lower()]


def parse_ram_output(stdout: str) -> dict:
    result = {"source": None, "endee_rss": None, "endee_peak": None, "sys_used": None, "sys_total": None}
    for line in stdout.splitlines():
        if line.startswith("SOURCE "):
            result["source"] = line[len("SOURCE "):].strip()
        elif line.startswith("DOCKER_MEM "):
            result["endee_rss"] = _parse_size(line[len("DOCKER_MEM "):].split("/")[0])
        elif line.startswith("SYS_USED "):
            result["sys_used"] = float(line.split()[1])
        elif line.startswith("SYS_TOTAL "):
            result["sys_total"] = float(line.split()[1])
        else:
            m = re.match(r"^(VmRSS|VmHWM):\s+(\d+)\s*kB", line)
            if m:
                result["endee_rss" if m.group(1) == "VmRSS" else "endee_peak"] = int(m.group(2)) * 1024
    if result["endee_rss"] is None:
        raise RuntimeError(f"Could not read Endee RAM from:\n{stdout}")
    return result


def measure_ram() -> dict:
    if SERVER_SSH:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SERVER_SSH, "bash -s"]
    else:
        cmd = ["bash", "-s"]
    proc = subprocess.run(cmd, input=_ram_shell_script(), text=True, capture_output=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(
            f"RAM measurement failed (exit {proc.returncode}) using {' '.join(cmd)}\n"
            f"{proc.stdout.strip()}\n{proc.stderr.strip()}"
        )
    return parse_ram_output(proc.stdout)


def gib(n):
    return None if n is None else round(n / GIB, 3)


def describe_ram(r: dict) -> str:
    peak = f", peak {gib(r['endee_peak'])} GiB" if r["endee_peak"] is not None else ""
    return (f"Endee RSS {gib(r['endee_rss'])} GiB{peak} | machine used {gib(r['sys_used'])} / "
            f"{gib(r['sys_total'])} GiB | {r['source']}")


def wait_for_ram_to_settle():
    """Samples Endee's RAM until SETTLE_SAMPLES readings in a row are within SETTLE_TOLERANCE."""
    start = time.time()
    history = []
    while True:
        r = measure_ram()
        history.append(r["endee_rss"])
        elapsed = time.time() - start
        print(f"   t={elapsed:4.0f}s  Endee RSS {gib(r['endee_rss'])} GiB")
        recent = history[-SETTLE_SAMPLES:]
        if len(recent) == SETTLE_SAMPLES and max(recent) - min(recent) <= SETTLE_TOLERANCE * max(recent):
            return r, round(elapsed), True
        if elapsed >= SETTLE_MAX_WAIT:
            print(f"   [WARN] RAM still changing after {SETTLE_MAX_WAIT}s, recording the latest reading")
            return r, round(elapsed), False
        time.sleep(SETTLE_INTERVAL)


# ============================================================
# BENCHMARK RUNNER
# ============================================================
def results_dir() -> str:
    if RESULTS_DIR:
        return RESULTS_DIR
    from vectordb_bench import config  # same default VectorDBBench writes to
    return str(config.RESULTS_LOCAL_DIR)


def build_command(load: bool) -> list:
    cmd = [
        "vectordbbench", "endee",
        "--token", TOKEN,
        "--region", REGION,
        "--base-url", BASE_URL,
        "--collection-name", COLLECTION_NAME,
        "--task-label", TASK_LABEL,
        "--m", str(M),
        "--ef-con", str(EF_CON),
        "--ef-search", str(EF_SEARCH),
        "--space-type", SPACE_TYPE,
        "--precision", PRECISION,
        "--case-type", "PerformanceCustomDataset",
        "--custom-case-name", CUSTOM_CASE_NAME,
        "--custom-dataset-name", DATASET_NAME,
        "--custom-dataset-dir", DATASET_NAME,
        "--custom-dataset-size", str(DATASET_SIZE),
        "--custom-dataset-dim", str(DATASET_DIM),
        "--custom-dataset-metric-type", "COSINE",
        "--custom-dataset-file-count", "1",
        "--custom-dataset-with-gt",
        "--custom-case-load-timeout", str(LOAD_TIMEOUT),
        "--custom-case-optimize-timeout", str(OPTIMIZE_TIMEOUT),
        "--k", str(K),
        "--num-concurrency", str(CONCURRENCY),
        "--concurrency-duration", str(CONCURRENCY_DURATION),
        "--concurrency-timeout", "3600",
    ]
    if load:
        cmd += ["--drop-old", "--load", "--skip-search-serial", "--skip-search-concurrent"]
    else:
        cmd += ["--skip-drop-old", "--skip-load", "--search-serial", "--search-concurrent"]
    return cmd


def run_stage(name: str, load: bool) -> dict:
    rdir = results_dir()
    pattern = os.path.join(rdir, "**", "*.json")
    before = set(glob.glob(pattern, recursive=True))
    env = {**os.environ, "NUM_PER_BATCH": str(NUM_PER_BATCH), "DATASET_LOCAL_DIR": DATASET_LOCAL_DIR}

    proc = subprocess.run(build_command(load), env=env)
    if proc.returncode != 0:
        sys.exit(f"[ERROR] vectordbbench {name} run exited with code {proc.returncode}; stopping")

    time.sleep(2)
    new_files = set(glob.glob(pattern, recursive=True)) - before
    if not new_files:
        print(f"   [WARN] no new result file in {rdir} after the {name} run")
        return {}
    result_file = max(new_files, key=os.path.getmtime)
    print(f"   [FILE] {result_file}")
    with open(result_file) as f:
        return json.load(f)["results"][0]["metrics"]


def append_csv(row: dict):
    new_file = not os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def main():
    if "--check-ram" in sys.argv:
        print(describe_ram(measure_ram()))
        return
    if TOKEN.startswith("<") or SERVER_SSH.startswith("<"):
        sys.exit("Set the token (export ENDEE_TOKEN=...) and SERVER_SSH at the top of this script first")

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n[0] RAM before load")
    ram_before = measure_ram()  # also fails fast if SSH / process settings are wrong
    print("   " + describe_ram(ram_before))

    print(f"\n[1] Loading {DATASET_SIZE:,} vectors into {COLLECTION_NAME} (no search)")
    load_metrics = run_stage("load", load=True)
    print(f"   load_duration (insert + optimize) = {load_metrics.get('load_duration')} s")

    print("\n[2] Waiting for Endee's RAM to settle after load")
    ram_after_load, settle_seconds, settled = wait_for_ram_to_settle()
    print("   " + describe_ram(ram_after_load))

    print("\n[3] Search (serial + concurrent)")
    search_metrics = run_stage("search", load=False)
    print(f"   recall={search_metrics.get('recall')} qps={search_metrics.get('qps')} "
          f"serial_p99={search_metrics.get('serial_latency_p99')}")

    print("\n[4] RAM after search")
    ram_after_search = measure_ram()
    print("   " + describe_ram(ram_after_search))

    row = {
        "started": started,
        "task_label": TASK_LABEL,
        "collection": COLLECTION_NAME,
        "dataset": DATASET_NAME,
        "size": DATASET_SIZE,
        "dim": DATASET_DIM,
        "m": M,
        "ef_con": EF_CON,
        "ef_search": EF_SEARCH,
        "precision": PRECISION,
        "k": K,
        "concurrency": CONCURRENCY,
        "load_duration_s": load_metrics.get("load_duration"),
        "endee_rss_before_load_gib": gib(ram_before["endee_rss"]),
        "endee_rss_after_load_gib": gib(ram_after_load["endee_rss"]),
        "endee_peak_rss_after_load_gib": gib(ram_after_load["endee_peak"]),
        "endee_rss_after_search_gib": gib(ram_after_search["endee_rss"]),
        "machine_used_after_load_gib": gib(ram_after_load["sys_used"]),
        "machine_total_gib": gib(ram_after_load["sys_total"]),
        "ram_settled": settled,
        "settle_wait_s": settle_seconds,
        "recall": search_metrics.get("recall"),
        "qps": search_metrics.get("qps"),
        "serial_latency_p99": search_metrics.get("serial_latency_p99"),
        "endee_process": ram_after_load["source"],
    }
    append_csv(row)
    print(f"\nSaved to {OUTPUT_CSV}")
    print(f"Endee RAM after load (before search): {row['endee_rss_after_load_gib']} GiB")


if __name__ == "__main__":
    main()
