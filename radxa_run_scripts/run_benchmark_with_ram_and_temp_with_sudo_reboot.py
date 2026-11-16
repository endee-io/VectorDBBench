#!/usr/bin/env python3
"""
Runs a VectorDBBench custom-dataset benchmark against Endee and records Endee's RAM and the
Endee server's temperature / CPU speed.

Examples (run on the client, inside the VectorDBBench venv):
  python run_benchmark_with_ram_and_temp.py --size 4M                      load 4M, then 5 searches, top-k 30
  python run_benchmark_with_ram_and_temp.py --size 4M --k 100              same with top-k 100
  python run_benchmark_with_ram_and_temp.py --size 2M --search-only        2M_index already loaded: 5 searches
  python run_benchmark_with_ram_and_temp.py --size 3M --search-only --reboots 1
        5 searches, then on the server: docker compose down, sudo reboot, docker compose up, then 5 more
  python run_benchmark_with_ram_and_temp.py --size 4M --k 100 --reboots 1 --check
        checks everything that run would need (RAM/temperature reading, dataset, ground truth depth for
        k, collection, disk space, reboot permissions) without running anything
  python run_benchmark_with_ram_and_temp.py --help                          all options

--size N sets the collection (<N>_index), the dataset (custom_768d_<n>) and its size, e.g. 500K, 1M ... 7M.
The output also goes to a log file automatically (~/benchmark_logs/<label>.log), so no redirect is needed.

What a run does:
  [0] RAM + temperature at start
  [1] load only (--drop-old --load), unless --search-only; temperature logged during the load
  [2] wait until Endee's RAM stops changing after the load, then record it
  Then 1 + --reboots rounds; before every round after the first, the server is restarted:
      docker compose down (in ENDEE_COMPOSE_DIR), sudo reboot, wait until it is back,
      docker compose up -d, wait until Endee answers
  Each round: RAM + temperature + busiest processes at round start, then --searches times:
  [3] cool-down: wait while the hottest sensor is >= COOLDOWN_THRESHOLD_C, re-check every
      COOLDOWN_CHECK_INTERVAL s, start anyway after COOLDOWN_MAX_WAIT s
  [4] search (serial + concurrent), temperature logged every TEMP_SAMPLE_INTERVAL_SEARCH s
  [5] RAM + temperature after that search; one CSV row saved right away
At the end: QPS per search and the median per round (round 2+ = after a reboot).

Excel summary (SUMMARY_XLSX_DIR/<label>.xlsx, rewritten after every search so a stopped run still has one):
  top block:  Load Duration, NUM_PARALLEL_INSERTS, Data Dir Size (du -sh of the collection's folder on the
              server), Collection RAM, Process RES / private / shared, Board used / available, and when
              the RAM was measured (after the load, or after the first search if the load was skipped)
  table:      Run Count, Recall, nDCG, QPS, Latency (p99) in s, Latency (p95) in s (serial search latency),
              with a SUDO REBOOT row between rounds
  sheet 2:    per-search conditions (temperatures, throttling, governor, concurrent latency, RAM after)
  Needs openpyxl in the venv (pip install openpyxl); without it only the CSV is written.

Where RAM and temperature are read: on the Endee server, over SSH (SERVER_SSH, key login without a password).
Endee's RAM comes from /proc/<pid>/status of the ENDEE_PROCESS_NAME process (works inside Docker too):
  endee_res         = htop RES  (VmRSS): everything Endee has in RAM
  endee_private     = RES - SHR (RssAnon): Endee's own memory (graph, vector cache); cannot be given back
  endee_shared_mdbx = htop SHR (RssFile + RssShmem): mostly MDBX data pages mapped from disk; reclaimable,
                      but searches slow down when the kernel drops them
  endee_peak_res    = highest RES since the Endee process started (VmHWM)
  board_used / board_buff_cache / board_available = the same columns as `free` for the whole board
Temperature: /sys/class/thermal (hottest sensor decides the cool-down); CPU speed and governor:
/sys/devices/system/cpu/cpufreq. "min big-core MHz" well below the maximum under load = throttling.

--reboots needs passwordless sudo for reboot on the server, once (on the server):
  echo 'radxa ALL=(root) NOPASSWD: /usr/sbin/reboot, /sbin/reboot' | sudo tee /etc/sudoers.d/benchmark-reboot
  sudo chmod 440 /etc/sudoers.d/benchmark-reboot
"""
import argparse
import csv
import glob
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

# ============================================================
# CONFIGURATION (defaults; the most common ones can be set with flags, see --help)
# ============================================================
# --- Endee / benchmark ---
TOKEN = os.environ.get("ENDEE_TOKEN", "<ENDEE_API_TOKEN>")  # export ENDEE_TOKEN="..." (keeps the token out of this file)
REGION = "bellandur"
BASE_URL = "http://172.16.23.51:8080/api/v2"
DATASET_LOCAL_DIR = "/home/launchx/vectordataset"
DEFAULT_SIZE = "4M"                  # --size
DEFAULT_K = 30                       # --k
EF_SEARCH = 128                      # --ef-search
SEARCHES_PER_ROUND = 5               # --searches
NUM_PER_BATCH = 1000
M = 16
EF_CON = 128
SPACE_TYPE = "cosine"
PRECISION = "int16"
CONCURRENCY = 16
CONCURRENCY_DURATION = 30
COOLDOWN_BEFORE_EACH_SEARCH = True   # False = cool-down check only before the first search of a round
DATASET_DIM = 768
LOAD_TIMEOUT = 360000                # seconds
OPTIMIZE_TIMEOUT = 360000            # seconds

# Set from --size / --collection / --dataset / --k / --label by apply_settings()
COLLECTION_NAME = None               # e.g. 4M_index
DATASET_NAME = None                  # e.g. custom_768d_4m  (folder DATASET_LOCAL_DIR/<name>/<name>)
CUSTOM_CASE_NAME = None              # e.g. Custom 768D 4M
DATASET_SIZE = None                  # e.g. 4_000_000
K = DEFAULT_K
TASK_LABEL = None

# --- Endee server ---
SERVER_SSH = "radxa@172.16.23.51"    # "" if Endee runs on this machine (then --reboots is not possible)
ENDEE_PROCESS_NAME = "ndd-neon"      # Endee server process name as htop shows it
ENDEE_DOCKER_CONTAINER = "endee"     # `docker stats` is also recorded for this container; "" to skip
ENDEE_DATA_DIR = "/home/radxa/data"  # where Endee keeps collections on the server (free-space check)
ENDEE_COLLECTIONS_DIR = "/home/radxa/data/radxa_benchmark"  # one folder per collection (Data Dir Size)
DISK_GIB_PER_MILLION = 2.4           # disk per 1M vectors (768D int16, M=16), from the 1M/2M collections
NUM_PARALLEL_INSERTS = 8             # Endee server-side setting (constant), written into the Excel summary

# --- Restart between rounds (--reboots) ---
ENDEE_COMPOSE_DIR = "~/endee"        # folder on the server with Endee's docker compose file
ENDEE_COMPOSE_CMD = "docker compose"
REBOOT_TIMEOUT = 900                 # seconds to wait for the server to come back after reboot
POST_REBOOT_SETTLE = 60              # seconds to let boot-time services settle before docker compose up
ENDEE_READY_TIMEOUT = 300            # seconds to wait for Endee to answer after docker compose up
POST_REBOOT_COMMANDS = []            # shell commands run on the server after each reboot, before compose up,
                                     # e.g. ["sudo -n systemctl stop avahi-daemon"] (needs NOPASSWD for them)

# --- RAM settle after load ---
SETTLE_INTERVAL = 10                 # seconds between RAM samples after load
SETTLE_SAMPLES = 3                   # settled = this many samples in a row within SETTLE_TOLERANCE
SETTLE_TOLERANCE = 0.01              # 1%
SETTLE_MAX_WAIT = 900                # record anyway after this many seconds

# --- Temperature ---
COOLDOWN_THRESHOLD_C = 70.0          # wait before a search while the hottest sensor is at or above this
COOLDOWN_CHECK_INTERVAL = 120        # seconds between checks while waiting
COOLDOWN_MAX_WAIT = 300              # start the search after this many seconds, whatever the temperature
TEMP_SAMPLE_INTERVAL_LOAD = 300      # seconds between temperature log lines during load (0 = off)
TEMP_SAMPLE_INTERVAL_SEARCH = 10     # seconds between temperature log lines during search (0 = off)

# --- Output ---
RESULTS_DIR = ""                     # "" = VectorDBBench's results dir (vectordb_bench/results)
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ram_benchmark_results.csv")
LOG_DIR = "~/benchmark_logs"         # default log file: LOG_DIR/<label>.log  (--log / --no-log-file)
SUMMARY_XLSX_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_summaries")

GIB = 1024 ** 3


# ============================================================
# LOGGING (terminal + log file)
# ============================================================
_LOG_FH = None
_OUT_LOCK = threading.Lock()


def _emit(text: str):
    with _OUT_LOCK:
        sys.stdout.write(text)
        sys.stdout.flush()
        if _LOG_FH:
            _LOG_FH.write(text)
            _LOG_FH.flush()


def log(msg: str = ""):
    """Timestamped line to the terminal and the log file."""
    _emit(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")


def log_block(text: str, indent: str = "      "):
    for line in text.splitlines():
        if line.strip():
            log(indent + line.rstrip())


def fatal(msg: str):
    log(f"[ERROR] {msg}")
    sys.exit(1)


def problem(msg: str, fail: bool):
    """Stops the run (fail=True) or just reports the problem (--check)."""
    if fail:
        fatal(msg)
    log(f"   [PROBLEM] {msg}")


def try_measure(fn, what: str):
    """Runs a measurement; on failure logs a warning and returns None instead of stopping the run."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - a failed reading must not end a run after a long load
        log(f"   [WARN] could not read {what}: {e}")
        return None


# ============================================================
# RUNNING COMMANDS ON THE ENDEE SERVER
# ============================================================
def _ssh_cmd():
    return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SERVER_SSH]


def run_on_server(script: str, timeout: int = 60) -> str:
    """Runs a shell snippet on the Endee machine (over SSH, or locally) and returns its stdout."""
    cmd = _ssh_cmd() + ["bash -s"] if SERVER_SSH else ["bash", "-s"]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed (exit {proc.returncode}) using {' '.join(cmd)}\n"
            f"{proc.stdout.strip()}\n{proc.stderr.strip()}"
        )
    return proc.stdout


def server_boot_id():
    """The server's boot id (changes on every reboot), or None if it cannot be reached."""
    try:
        return run_on_server("cat /proc/sys/kernel/random/boot_id\n", timeout=20).strip() or None
    except Exception:  # noqa: BLE001
        return None


# ============================================================
# RAM MEASUREMENT
# ============================================================
def _ram_shell_script() -> str:
    """Shell snippet run on the Endee machine; prints one 'KEY value' per line.

    Endee's memory is read from /proc/<pid>/status of the Endee process. Processes inside a Docker
    container are visible on the host, so this works whether or not Endee runs in Docker.
    """
    script = (
        f"PID=$(pgrep -o -x '{ENDEE_PROCESS_NAME}')\n"
        f"if [ -z \"$PID\" ]; then echo \"ERROR no process named '{ENDEE_PROCESS_NAME}'\"; exit 3; fi\n"
        "echo \"SOURCE pid $PID: $(ps -o args= -p $PID | cut -c1-120)\"\n"
        "grep -E '^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem):' /proc/$PID/status\n"
        "free -b | awk '/^Mem:/ {print \"SYS_TOTAL\", $2; print \"SYS_USED\", $3; "
        "print \"SYS_BUFF_CACHE\", $6; print \"SYS_AVAILABLE\", $7}'\n"
    )
    if ENDEE_DOCKER_CONTAINER:
        script += (f"docker stats --no-stream --format 'DOCKER_MEM {{{{.MemUsage}}}}' "
                   f"{ENDEE_DOCKER_CONTAINER} 2>/dev/null || true\n")
    return script


_SIZE_UNITS = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4,
               "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def _parse_size(text: str) -> float:
    """'1.5GiB' / '512MiB' / '800MB' -> bytes."""
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not m or m.group(2).lower() not in _SIZE_UNITS:
        raise ValueError(f"Cannot parse memory size: {text!r}")
    return float(m.group(1)) * _SIZE_UNITS[m.group(2).lower()]


_STATUS_KEYS = {"VmRSS": "res", "VmHWM": "peak_res", "RssAnon": "private",
                "RssFile": "file", "RssShmem": "shmem"}
_SYS_KEYS = {"SYS_TOTAL": "sys_total", "SYS_USED": "sys_used",
             "SYS_BUFF_CACHE": "sys_buff_cache", "SYS_AVAILABLE": "sys_available"}


def parse_ram_output(stdout: str) -> dict:
    """Bytes for: res (htop RES), private (RES - SHR), shared (htop SHR: mostly mmap'd MDBX pages),
    peak_res, docker_mem, and the board's sys_used / sys_buff_cache / sys_available / sys_total."""
    r = {"source": None, "res": None, "peak_res": None, "private": None, "file": None, "shmem": None,
         "docker_mem": None, "sys_total": None, "sys_used": None, "sys_buff_cache": None, "sys_available": None}
    for line in stdout.splitlines():
        if line.startswith("SOURCE "):
            r["source"] = line[len("SOURCE "):].strip()
        elif line.startswith("DOCKER_MEM "):
            try:
                r["docker_mem"] = _parse_size(line[len("DOCKER_MEM "):].split("/")[0])
            except ValueError:
                pass
        elif line.split(" ")[0] in _SYS_KEYS:
            r[_SYS_KEYS[line.split()[0]]] = float(line.split()[1])
        else:
            m = re.match(r"^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem):\s+(\d+)\s*kB", line)
            if m:
                r[_STATUS_KEYS[m.group(1)]] = int(m.group(2)) * 1024
    if r["res"] is None:
        raise RuntimeError(f"Could not read Endee RAM from:\n{stdout}")
    if r["file"] is not None or r["shmem"] is not None:
        r["shared"] = (r["file"] or 0) + (r["shmem"] or 0)
    else:
        r["shared"] = None
    return r


def measure_ram() -> dict:
    return parse_ram_output(run_on_server(_ram_shell_script()))


def gib(n):
    return None if n is None else round(n / GIB, 3)


def ram_value(r, key):
    return None if r is None else gib(r.get(key))


def ram_columns(stage: str, r) -> dict:
    """CSV columns for one RAM reading, e.g. endee_res_after_load_gib."""
    return {
        f"endee_res_{stage}_gib": ram_value(r, "res"),
        f"endee_private_{stage}_gib": ram_value(r, "private"),
        f"endee_shared_mdbx_{stage}_gib": ram_value(r, "shared"),
        f"endee_peak_res_{stage}_gib": ram_value(r, "peak_res"),
        f"docker_mem_{stage}_gib": ram_value(r, "docker_mem"),
        f"board_used_{stage}_gib": ram_value(r, "sys_used"),
        f"board_buff_cache_{stage}_gib": ram_value(r, "sys_buff_cache"),
        f"board_available_{stage}_gib": ram_value(r, "sys_available"),
    }


def describe_ram(r) -> str:
    if r is None:
        return "RAM unavailable"
    return (f"Endee RES {gib(r['res'])} GiB = private {gib(r['private'])} + shared/MDBX {gib(r['shared'])}"
            f" (peak RES {gib(r['peak_res'])}, docker {gib(r['docker_mem'])}) | board used {gib(r['sys_used'])},"
            f" buff/cache {gib(r['sys_buff_cache'])}, available {gib(r['sys_available'])} of"
            f" {gib(r['sys_total'])} GiB | {r['source']}")


def wait_for_ram_to_settle():
    """Samples Endee's RAM until SETTLE_SAMPLES readings in a row are within SETTLE_TOLERANCE."""
    start = time.time()
    history = []
    last = None
    while True:
        r = try_measure(measure_ram, "Endee RAM")
        elapsed = time.time() - start
        if r is not None:
            last = r
            history.append(r["res"])
            log(f"   t={elapsed:4.0f}s  Endee RES {gib(r['res'])} GiB (private {gib(r['private'])}, "
                f"shared/MDBX {gib(r['shared'])})")
            recent = history[-SETTLE_SAMPLES:]
            if len(recent) == SETTLE_SAMPLES and max(recent) - min(recent) <= SETTLE_TOLERANCE * max(recent):
                return last, round(elapsed), True
        if elapsed >= SETTLE_MAX_WAIT:
            log(f"   [WARN] RAM still changing (or unreadable) after {SETTLE_MAX_WAIT}s, recording the latest reading")
            return last, round(elapsed), False
        time.sleep(SETTLE_INTERVAL)


# ============================================================
# TEMPERATURE / CPU SPEED / GOVERNOR
# ============================================================
_TEMP_SHELL_SCRIPT = (
    "for z in /sys/class/thermal/thermal_zone*; do "
    "echo \"TEMP $(cat $z/type) $(cat $z/temp)\"; done\n"
    "for p in /sys/devices/system/cpu/cpufreq/policy*; do "
    "echo \"FREQ $(basename $p) $(cat $p/scaling_cur_freq) $(cat $p/cpuinfo_max_freq)\"; "
    "echo \"GOV $(basename $p) $(cat $p/scaling_governor 2>/dev/null)\"; done\n"
)


def parse_temp_output(stdout: str) -> dict:
    zones = {}   # sensor name -> degrees C
    freqs = {}   # cpufreq policy -> (current MHz, max MHz)
    govs = {}    # cpufreq policy -> governor
    for line in stdout.splitlines():
        parts = line.split()
        try:
            if len(parts) == 3 and parts[0] == "TEMP":
                zones[parts[1].replace("-thermal", "")] = int(parts[2]) / 1000
            elif len(parts) == 4 and parts[0] == "FREQ":
                freqs[parts[1]] = (int(parts[2]) // 1000, int(parts[3]) // 1000)
            elif len(parts) == 3 and parts[0] == "GOV":
                govs[parts[1]] = parts[2]
        except ValueError:
            continue
    if not zones:
        raise RuntimeError(f"No temperature readings in:\n{stdout}")
    top_max = max((mx for _, mx in freqs.values()), default=None)
    big_now = [cur for cur, mx in freqs.values() if mx == top_max]
    return {
        "zones": zones,
        "max_c": max(zones.values()),
        "freqs": freqs,
        "min_big_mhz": min(big_now) if big_now else None,
        "governor": ",".join(sorted(set(govs.values()))) or None,
    }


def measure_temp() -> dict:
    return parse_temp_output(run_on_server(_TEMP_SHELL_SCRIPT))


def temp_value(t):
    return None if t is None else round(t["max_c"], 1)


def describe_temp(t) -> str:
    if t is None:
        return "temperature unavailable"
    zones = " ".join(f"{name} {c:.1f}" for name, c in t["zones"].items())
    freqs = " ".join(f"{p} {cur}/{mx}" for p, (cur, mx) in t["freqs"].items())
    return f"hottest {t['max_c']:.1f}C | {zones} | CPU MHz now/max: {freqs} | governor {t['governor']}"


def summarize_temps(samples):
    """(hottest reading, lowest big-core MHz) across the samples taken during a stage."""
    temps = [s["max_c"] for s in samples]
    bigs = [s["min_big_mhz"] for s in samples if s["min_big_mhz"] is not None]
    return (round(max(temps), 1) if temps else None, min(bigs) if bigs else None)


def cool_down():
    """Waits while the hottest sensor is >= COOLDOWN_THRESHOLD_C, up to COOLDOWN_MAX_WAIT seconds."""
    start = time.time()
    while True:
        t = try_measure(measure_temp, "temperature")
        elapsed = time.time() - start
        log(f"   t={elapsed:4.0f}s  {describe_temp(t)}")
        if t is not None and t["max_c"] < COOLDOWN_THRESHOLD_C:
            log(f"   below {COOLDOWN_THRESHOLD_C}C, starting search")
            return t, round(elapsed), True
        if elapsed >= COOLDOWN_MAX_WAIT:
            log(f"   [WARN] not below {COOLDOWN_THRESHOLD_C}C after {COOLDOWN_MAX_WAIT}s, starting search anyway")
            return t, round(elapsed), False
        wait = min(COOLDOWN_CHECK_INTERVAL, COOLDOWN_MAX_WAIT - elapsed)
        log(f"   at or above {COOLDOWN_THRESHOLD_C}C, checking again in {wait:.0f}s")
        time.sleep(wait)


def log_server_activity():
    """Load average and the processes that have used the most CPU (shows background services)."""
    try:
        out = run_on_server("uptime\n"
                            "echo 'busiest processes (average CPU % since each started):'\n"
                            "ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -8\n")
        log_block(out)
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read server activity: {e}")


# ============================================================
# CHECKS: dataset, ground truth, collection, disk, reboot setup
# ============================================================
def dataset_folder():
    for folder in (os.path.join(DATASET_LOCAL_DIR, DATASET_NAME, DATASET_NAME),
                   os.path.join(DATASET_LOCAL_DIR, DATASET_NAME)):
        if glob.glob(os.path.join(folder, "*.parquet")):
            return folder
    return None


def ground_truth_depth(folder):
    """Neighbors per query in neighbors.parquet (recall@k needs at least k), or None if unknown."""
    path = os.path.join(folder, "neighbors.parquet")
    if not os.path.exists(path):
        return None
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        for name in table.column_names:
            col = table.column(name)
            if len(col):
                value = col[0].as_py()
                if isinstance(value, (list, tuple)):
                    return len(value)
    except Exception:  # noqa: BLE001
        return None
    return None


def check_dataset(fail: bool):
    folder = dataset_folder()
    if not folder:
        problem(f"no .parquet files for dataset '{DATASET_NAME}' under {DATASET_LOCAL_DIR} "
                f"(expected {DATASET_LOCAL_DIR}/{DATASET_NAME}/{DATASET_NAME}); use --dataset if it is named "
                f"differently", fail)
        return
    log(f"   dataset files: {folder}")
    if not os.path.exists(os.path.join(folder, "neighbors.parquet")):
        problem(f"no neighbors.parquet (ground truth) in {folder}; recall cannot be computed", fail)
        return
    depth = ground_truth_depth(folder)
    if depth is None:
        log("   [WARN] could not read how many neighbors per query neighbors.parquet has; make sure it is "
            f">= k ({K})")
    elif depth < K:
        problem(f"--k {K}, but neighbors.parquet has only {depth} neighbors per query, so recall@{K} would be "
                f"wrong. Regenerate the ground truth with at least {K} neighbors, or use --k {depth} or lower.",
                fail)
    else:
        log(f"   ground truth: {depth} neighbors per query (enough for k={K})")


def collection_status():
    """(exists: True/False/None if unknown, total_elements, note) from Endee's API."""
    try:
        import requests
        from endee import endee
        nd = endee.Endee(token=TOKEN)  # resolves the token the same way the benchmark does
        nd.set_base_url(BASE_URL)
        resp = requests.get(f"{nd.base_url}/collection/{COLLECTION_NAME}",
                            headers={"Authorization": nd.token}, timeout=60)
    except Exception as e:  # noqa: BLE001
        return None, None, f"could not check ({e})"
    if resp.status_code == 404:
        return False, None, "not found"
    if not resp.ok:
        return None, None, f"HTTP {resp.status_code}"
    return True, resp.json().get("total_elements"), "ok"


def check_collection_for_search(fail: bool, force: bool):
    exists, count, note = collection_status()
    if exists is False:
        problem(f"collection {COLLECTION_NAME} not found on {BASE_URL} (is ENDEE_TOKEN the benchmark's "
                f"token?); run without --search-only to load it", fail)
    elif exists is None:
        log(f"   [WARN] could not verify collection {COLLECTION_NAME}: {note}")
    elif count != DATASET_SIZE:
        msg = (f"collection {COLLECTION_NAME} has {count:,} vectors, expected {DATASET_SIZE:,} "
               f"(partly loaded, or a different size)")
        if force:
            log(f"   [WARN] {msg}; continuing because of --force")
        else:
            problem(msg + "; reload it, or add --force to search it anyway", fail)
    else:
        log(f"   collection {COLLECTION_NAME}: {count:,} vectors, as expected")


def check_disk_space():
    need = DATASET_SIZE / 1e6 * DISK_GIB_PER_MILLION
    try:
        out = run_on_server(f"df -B1 --output=avail {ENDEE_DATA_DIR} | tail -1\n")
        avail = int(out.strip().split()[-1]) / GIB
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read free disk space in {ENDEE_DATA_DIR}: {e}")
        return
    log(f"   server disk: {avail:.1f} GiB free in {ENDEE_DATA_DIR}; {COLLECTION_NAME} needs about {need:.1f} GiB")
    if avail < need * 1.1:
        log(f"   [WARN] may not fit (an existing {COLLECTION_NAME} is dropped before loading, which frees its space)")


def check_restart_setup(fail: bool):
    if not SERVER_SSH:
        problem("--reboots needs SERVER_SSH (the server must be a different machine)", fail)
        return
    try:
        out = run_on_server(
            f"cd {ENDEE_COMPOSE_DIR} && {ENDEE_COMPOSE_CMD} config --services >/dev/null 2>&1 "
            "&& echo COMPOSE_OK || echo COMPOSE_FAIL\n"
            "sudo -n -l reboot >/dev/null 2>&1 && echo SUDO_OK || echo SUDO_FAIL\n")
    except Exception as e:  # noqa: BLE001
        problem(f"could not check the restart setup on the server: {e}", fail)
        return
    if "COMPOSE_OK" in out:
        log(f"   docker compose file found in {ENDEE_COMPOSE_DIR}")
    else:
        problem(f"`{ENDEE_COMPOSE_CMD}` has no valid compose file in {ENDEE_COMPOSE_DIR} on the server "
                f"(set ENDEE_COMPOSE_DIR / ENDEE_COMPOSE_CMD)", fail)
    if "SUDO_OK" in out:
        log("   passwordless `sudo reboot` allowed")
    else:
        problem("the server needs passwordless sudo for reboot. On the server run:\n"
                "      echo 'radxa ALL=(root) NOPASSWD: /usr/sbin/reboot, /sbin/reboot' | "
                "sudo tee /etc/sudoers.d/benchmark-reboot && sudo chmod 440 /etc/sudoers.d/benchmark-reboot", fail)


# ============================================================
# SERVER RESTART (between rounds)
# ============================================================
def endee_http_up() -> bool:
    try:
        urllib.request.urlopen(BASE_URL, timeout=5)
        return True
    except urllib.error.HTTPError:
        return True   # any HTTP answer means the server is listening
    except Exception:  # noqa: BLE001
        return False


def wait_for_endee():
    start = time.time()
    while True:
        process_ok = True
        try:
            measure_ram()
        except Exception:  # noqa: BLE001
            process_ok = False
        if process_ok and endee_http_up():
            log(f"   Endee is up ({time.time() - start:.0f}s after compose up)")
            return
        if time.time() - start > ENDEE_READY_TIMEOUT:
            fatal(f"Endee did not come up within {ENDEE_READY_TIMEOUT}s after `{ENDEE_COMPOSE_CMD} up -d`")
        time.sleep(5)


def restart_server() -> int:
    """docker compose down, sudo reboot, wait for the new boot, docker compose up -d, wait for Endee.
    Returns the seconds taken."""
    start = time.time()
    old_boot = server_boot_id()
    if old_boot is None:
        fatal("cannot reach the server before the restart")

    log(f"   {ENDEE_COMPOSE_CMD} down (in {ENDEE_COMPOSE_DIR})")
    try:
        log_block(run_on_server(f"cd {ENDEE_COMPOSE_DIR} && {ENDEE_COMPOSE_CMD} down 2>&1\n", timeout=300))
    except Exception as e:  # noqa: BLE001
        fatal(f"`{ENDEE_COMPOSE_CMD} down` failed: {e}")

    log("   sudo reboot")
    try:
        proc = subprocess.run(_ssh_cmd() + ["sudo -n reboot"], text=True, capture_output=True, timeout=60)
        err = (proc.stderr or "").lower()
        if "password" in err or "not allowed" in err or "may not run" in err:
            fatal(f"sudo refused the reboot: {proc.stderr.strip()} (see --check)")
    except subprocess.TimeoutExpired:
        pass  # the connection can hang while the server goes down

    log(f"   waiting for the server to come back (up to {REBOOT_TIMEOUT}s)")
    time.sleep(20)
    while True:
        boot = server_boot_id()
        if boot and boot != old_boot:
            break
        if time.time() - start > REBOOT_TIMEOUT:
            fatal(f"the server did not come back with a new boot within {REBOOT_TIMEOUT}s")
        time.sleep(10)
    log(f"   server is back {time.time() - start:.0f}s after the restart began; "
        f"letting boot settle for {POST_REBOOT_SETTLE}s")
    time.sleep(POST_REBOOT_SETTLE)

    for command in POST_REBOOT_COMMANDS:
        log(f"   post-reboot command: {command}")
        try:
            log_block(run_on_server(command + "\n", timeout=120))
        except Exception as e:  # noqa: BLE001
            log(f"   [WARN] post-reboot command failed: {e}")

    log(f"   {ENDEE_COMPOSE_CMD} up -d (in {ENDEE_COMPOSE_DIR})")
    try:
        log_block(run_on_server(f"cd {ENDEE_COMPOSE_DIR} && {ENDEE_COMPOSE_CMD} up -d 2>&1\n", timeout=600))
    except Exception as e:  # noqa: BLE001
        fatal(f"`{ENDEE_COMPOSE_CMD} up -d` failed: {e}")
    wait_for_endee()
    return round(time.time() - start)


# ============================================================
# COLLECTION RAM (Endee API) AND DATA DIR SIZE
# ============================================================
def collection_ram_gib():
    """Graph + vector cache of the collection from Endee's GET /collection/<name>/memory, in GiB."""
    try:
        import requests
        from endee import endee
        nd = endee.Endee(token=TOKEN)
        nd.set_base_url(BASE_URL)
        resp = requests.get(f"{nd.base_url}/collection/{COLLECTION_NAME}/memory",
                            headers={"Authorization": nd.token}, timeout=60)
        resp.raise_for_status()
        mem = resp.json()["mem"]
        return gib(mem["graph_bytes"] + mem["vector_cache_bytes"])
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read the collection's RAM from Endee: {e}")
        return None


def data_dir_size():
    """(du -sh text, GiB) of the collection's folder on the server, e.g. ('9.4G', 9.37)."""
    path = f"{ENDEE_COLLECTIONS_DIR}/{COLLECTION_NAME}"
    try:
        out = run_on_server(f"du -sb {path} | cut -f1\ndu -sh {path} | cut -f1\n", timeout=300).split()
        return out[1], gib(int(out[0]))
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read the size of {path}: {e}")
        return None, None


# ============================================================
# EXCEL SUMMARY
# ============================================================
_XLSX_WARNED = False


def summary_header_rows(load_duration, dir_h, dir_gib, collection_ram, ram_gib, ram_when):
    """Top block of the Summary sheet. ram_gib: dict with res, private, shared, sys_used, sys_available (GiB)."""
    ram_gib = ram_gib or {}
    return [
        ("Load Duration:", load_duration,
         "s" if load_duration is not None else "not loaded in this run (--search-only)"),
        ("NUM_PARALLEL_INSERTS:", NUM_PARALLEL_INSERTS, None),
        ("Data Dir Size:", dir_h,
         f"{dir_gib} GiB, du -sh {ENDEE_COLLECTIONS_DIR}/{COLLECTION_NAME}" if dir_h else None),
        ("Collection RAM (GiB):", collection_ram, "graph + vector cache (Endee API)"),
        ("Process RES (GiB):", ram_gib.get("res"), "htop RES of ndd-neon"),
        ("Process private (GiB):", ram_gib.get("private"), "RES - SHR"),
        ("Process shared / file-backed (GiB):", ram_gib.get("shared"), "htop SHR (MDBX pages)"),
        ("Board used (GiB):", ram_gib.get("sys_used"), None),
        ("Board available (GiB):", ram_gib.get("sys_available"), None),
        ("RAM measured:", ram_when, None),
    ]


def write_summary_xlsx(path, title, header_rows, results):
    """Summary sheet (header block + Run Count table with SUDO REBOOT rows) and a per-search details sheet."""
    global _XLSX_WARNED
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    except ImportError:
        if not _XLSX_WARNED:
            log("   [WARN] openpyxl is not installed, so no Excel summary (the CSV is still written). "
                "Install it in the venv: pip install openpyxl")
            _XLSX_WARNED = True
        return False

    font = Font(name="Arial", size=11)
    bold = Font(name="Arial", size=11, bold=True)
    head_font = Font(name="Arial", size=11, bold=True, color="FFFFFF")
    head_fill = PatternFill("solid", start_color="3D6B52")
    stripe = PatternFill("solid", start_color="F3F5F7")
    thin = Side(style="thin", color="3D6B52")
    right = Alignment(horizontal="right")

    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws.cell(1, 1, title).font = Font(name="Arial", size=13, bold=True)

    row = 3
    for label, value, note in header_rows:
        ws.cell(row, 1, label).font = bold
        c = ws.cell(row, 2, "" if value is None else value)
        c.font = font
        if label != "RAM measured:":
            c.alignment = right
        if note:
            ws.cell(row, 3, note).font = font
        row += 1

    row += 1
    columns = ["Run Count", "Recall", "nDCG", "QPS", "Latency (p99) in s", "Latency (p95) in s"]
    table_top = row
    for col, name in enumerate(columns, start=1):
        c = ws.cell(row, col, name)
        c.font, c.fill = head_font, head_fill
    row += 1
    previous_round = None
    for n, r in enumerate(results):
        if previous_round is not None and r["round"] != previous_round:
            ws.cell(row, 1, "SUDO REBOOT").font = Font(name="Arial", size=12, bold=True)
            row += 1
        values = [r["run_count"], r["recall"], r["ndcg"], r["qps"], r["serial_p99"], r["serial_p95"]]
        for col, value in enumerate(values, start=1):
            c = ws.cell(row, col, value)
            c.font = font
            if n % 2:
                c.fill = stripe
        previous_round = r["round"]
        row += 1
    for rr in range(table_top, row):
        ws.cell(rr, 1).border = Border(left=thin, top=thin if rr == table_top else None,
                                       bottom=thin if rr == row - 1 else None)
        ws.cell(rr, len(columns)).border = Border(right=thin, top=thin if rr == table_top else None,
                                                  bottom=thin if rr == row - 1 else None)
        for col in range(2, len(columns)):
            ws.cell(rr, col).border = Border(top=thin if rr == table_top else None,
                                             bottom=thin if rr == row - 1 else None)
    ws.column_dimensions["A"].width = 38
    for letter in "BCDEF":
        ws.column_dimensions[letter].width = 20
    ws.column_dimensions["C"].width = 26

    ds = wb.create_sheet("Per-search details")
    detail_columns = ["Run Count", "Round", "After reboot", "Temp at start (C)", "Max temp during search (C)",
                      "Lowest big-core MHz", "Governor", "Concurrent p99 (s)", "Concurrent p95 (s)",
                      "Endee RES after (GiB)", "Endee private after (GiB)", "Endee shared/MDBX after (GiB)",
                      "Board available after (GiB)"]
    for col, name in enumerate(detail_columns, start=1):
        c = ds.cell(1, col, name)
        c.font, c.fill = head_font, head_fill
        ds.column_dimensions[c.column_letter].width = max(14, len(name) + 2)
    for i, r in enumerate(results, start=2):
        values = [r["run_count"], r["round"], "yes" if r["round"] > 1 else "no", r["temp_start"], r["temp_max"],
                  r["min_big_mhz"], r["governor"], r["conc_p99"], r["conc_p95"], r["res_after"],
                  r["private_after"], r["shared_after"], r["available_after"]]
        for col, value in enumerate(values, start=1):
            ds.cell(i, col, value).font = font

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp.xlsx"
    wb.save(tmp)
    os.replace(tmp, path)
    return True


# ============================================================
# BENCHMARK RUNNER
# ============================================================
def results_dir() -> str:
    if RESULTS_DIR:
        return RESULTS_DIR
    from vectordb_bench import config  # same default VectorDBBench writes to
    return str(config.RESULTS_LOCAL_DIR)


def build_command(load: bool, label: str) -> list:
    cmd = [
        "vectordbbench", "endee",
        "--token", TOKEN,
        "--region", REGION,
        "--base-url", BASE_URL,
        "--collection-name", COLLECTION_NAME,
        "--task-label", label,
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


INSERT_WORKERS_SEEN = None             # "max_workers=N" from VectorDBBench's insert log line


def _pump(stream):
    """Copies VectorDBBench's output to the terminal and the log file."""
    global INSERT_WORKERS_SEEN
    for raw in iter(stream.readline, b""):
        text = raw.decode("utf-8", "replace")
        m = re.search(r"max_workers=(\d+)", text)
        if m:
            INSERT_WORKERS_SEEN = int(m.group(1))
        _emit(text)
    stream.close()


def run_stage(name: str, load: bool, temp_interval: int, label: str):
    """Runs vectordbbench; logs the server temperature every temp_interval seconds while it runs."""
    rdir = results_dir()
    pattern = os.path.join(rdir, "**", "*.json")
    before = set(glob.glob(pattern, recursive=True))
    env = {**os.environ, "NUM_PER_BATCH": str(NUM_PER_BATCH), "DATASET_LOCAL_DIR": DATASET_LOCAL_DIR,
           "PYTHONUNBUFFERED": "1"}

    samples = []
    proc = subprocess.Popen(build_command(load, label), env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    pump = threading.Thread(target=_pump, args=(proc.stdout,), daemon=True)
    pump.start()
    next_sample = time.time() + temp_interval if temp_interval else None
    while proc.poll() is None:
        time.sleep(1)
        if next_sample is not None and time.time() >= next_sample:
            t = try_measure(measure_temp, "temperature")
            if t is not None:
                samples.append(t)
                log(f"   [{name}] {describe_temp(t)}")
            next_sample = time.time() + temp_interval
    pump.join(timeout=30)
    if proc.returncode != 0:
        fatal(f"vectordbbench {name} run exited with code {proc.returncode}; stopping")

    time.sleep(2)
    new_files = set(glob.glob(pattern, recursive=True)) - before
    if not new_files:
        log(f"   [WARN] no new result file in {rdir} after the {name} run")
        return {}, samples
    result_file = max(new_files, key=os.path.getmtime)
    log(f"   [FILE] {result_file}")
    with open(result_file) as f:
        return json.load(f)["results"][0]["metrics"], samples


def first(value):
    """First element of a list metric (e.g. conc_latency_p99_list), else the value itself."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def append_csv(row: dict):
    """Adds a row; columns new to an existing file are added to its header, older rows get blanks."""
    rows, fields = [], []
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="") as f:
            reader = csv.DictReader(f)
            fields = list(reader.fieldnames or [])
            rows = list(reader)
    for key in row:
        if key not in fields:
            fields.append(key)
    rows.append(row)
    tmp = OUTPUT_CSV + ".tmp"
    with open(tmp, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, OUTPUT_CSV)


# ============================================================
# MAIN
# ============================================================
def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Endee benchmark with RAM + temperature recording (see the top of this file).",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--size", default=DEFAULT_SIZE,
                   help="collection size: 500K, 1M, 2M ... 7M -> <size>_index, custom_768d_<size> "
                        "(default %(default)s)")
    p.add_argument("--k", type=int, default=DEFAULT_K, help="top-k, e.g. 30 or 100 (default %(default)s)")
    p.add_argument("--ef-search", type=int, default=EF_SEARCH, help="ef_search (default %(default)s)")
    p.add_argument("--search-only", action="store_true",
                   help="the collection is already loaded: skip the load and only search")
    p.add_argument("--searches", type=int, default=SEARCHES_PER_ROUND,
                   help="searches per round (default %(default)s)")
    p.add_argument("--reboots", type=int, default=0,
                   help="after each round: docker compose down, reboot the server, docker compose up, then "
                        "another round of searches (default %(default)s)")
    p.add_argument("--collection", help="collection name (default <size>_index)")
    p.add_argument("--dataset", help="dataset name (default custom_768d_<size>)")
    p.add_argument("--label", help="task label (default <size>_k<k>[_search-only]_run<N>, N counting up "
                                   "from the previous runs with the same size, k and mode)")
    p.add_argument("--log", help="log file (default ~/benchmark_logs/<label>.log)")
    p.add_argument("--no-log-file", action="store_true", help="terminal output only")
    p.add_argument("--force", action="store_true",
                   help="with --search-only: search even if the collection's vector count differs from --size")
    p.add_argument("--check", "--check-ram", dest="check", action="store_true",
                   help="only check that everything this run needs works, then exit")
    return p.parse_args(argv)


def next_run_number(prefix: str) -> int:
    """1 + the highest <prefix>_run<N> found in the CSV, the Excel summaries and the log files."""
    pattern = re.compile(rf"^{re.escape(prefix)}_run(\d+)$")
    names = []
    if os.path.exists(OUTPUT_CSV):
        try:
            with open(OUTPUT_CSV, newline="") as f:
                names += [row.get("task_label") or "" for row in csv.DictReader(f)]
        except Exception:  # noqa: BLE001
            pass
    for folder, ext in ((SUMMARY_XLSX_DIR, ".xlsx"), (os.path.expanduser(LOG_DIR), ".log")):
        names += [os.path.basename(p)[: -len(ext)] for p in glob.glob(os.path.join(folder, f"*{ext}"))]
    numbers = [int(m.group(1)) for m in (pattern.match(n) for n in names) if m]
    return max(numbers, default=0) + 1


def apply_settings(args):
    global COLLECTION_NAME, DATASET_NAME, CUSTOM_CASE_NAME, DATASET_SIZE, K, EF_SEARCH, TASK_LABEL
    m = re.fullmatch(r"(\d+)\s*([KkMm])", args.size.strip())
    if not m:
        sys.exit(f"--size must look like 500K, 1M, 4M or 7M (got {args.size!r})")
    number, unit = int(m.group(1)), m.group(2).upper()
    tag = f"{number}{unit}"
    DATASET_SIZE = number * (1_000 if unit == "K" else 1_000_000)
    COLLECTION_NAME = args.collection or f"{tag}_index"
    DATASET_NAME = args.dataset or f"custom_768d_{tag.lower()}"
    CUSTOM_CASE_NAME = f"Custom {DATASET_DIM}D {tag}"
    if args.k < 1 or args.searches < 1 or args.reboots < 0:
        sys.exit("--k and --searches must be at least 1, --reboots at least 0")
    K = args.k
    EF_SEARCH = args.ef_search
    if args.label:
        TASK_LABEL = args.label
    else:
        prefix = f"{tag}_k{K}" + ("_search-only" if args.search_only else "")
        TASK_LABEL = f"{prefix}_run{next_run_number(prefix)}"


def run_checks(args):
    log(f"Checking setup for {COLLECTION_NAME} (dataset {DATASET_NAME}, {DATASET_SIZE:,} vectors, k={K}, "
        f"ef_search={EF_SEARCH})")
    log("RAM:")
    try:
        log("   " + describe_ram(measure_ram()))
    except Exception as e:  # noqa: BLE001
        log(f"   [PROBLEM] {e}")
    log("Temperature:")
    try:
        log("   " + describe_temp(measure_temp()))
    except Exception as e:  # noqa: BLE001
        log(f"   [PROBLEM] {e}")
    log("Dataset and ground truth:")
    check_dataset(fail=False)
    log("Collection:")
    exists, count, note = collection_status()
    if exists:
        log(f"   {COLLECTION_NAME}: {count:,} vectors (expected {DATASET_SIZE:,})"
            + ("" if count == DATASET_SIZE else "  <- differs; --search-only would stop unless --force"))
    else:
        log(f"   {COLLECTION_NAME}: {note}" + ("  (fine for a load run; needed for --search-only)"
                                                if exists is False else ""))
    log("Server disk:")
    check_disk_space()
    log("Restart setup (needed for --reboots):")
    check_restart_setup(fail=False)
    if EF_SEARCH < K:
        log(f"[PROBLEM] ef_search {EF_SEARCH} < k {K}: use --ef-search {K} or higher")
    log("Excel summary:")
    try:
        import openpyxl  # noqa: F401
        log(f"   openpyxl installed; summaries go to {SUMMARY_XLSX_DIR}")
    except ImportError:
        log("   [PROBLEM] openpyxl is not installed in this Python; run: pip install openpyxl")
    log("Check finished.")


def main():
    global _LOG_FH
    args = parse_args(sys.argv[1:])
    apply_settings(args)
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    if args.check:
        run_checks(args)
        return
    if TOKEN.startswith("<") or SERVER_SSH.startswith("<"):
        sys.exit("Set the token (export ENDEE_TOKEN=...) and SERVER_SSH at the top of this script first")

    log_path = None
    if not args.no_log_file:
        log_path = os.path.expanduser(args.log or os.path.join(LOG_DIR, f"{TASK_LABEL}.log"))
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        _LOG_FH = open(log_path, "a", encoding="utf-8")

    mode = "search-only" if args.search_only else "load+search"
    rounds = 1 + args.reboots
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"Run {TASK_LABEL}: {mode} | {COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, dataset "
        f"{DATASET_NAME}) | k={K} ef_search={EF_SEARCH} | {args.searches} searches x {rounds} round(s)"
        f"{f', server restart between rounds' if args.reboots else ''}")
    log(f"Server {BASE_URL} | CSV {OUTPUT_CSV} | log {log_path or '(terminal only)'}")
    if EF_SEARCH < K:
        log(f"[WARN] ef_search {EF_SEARCH} is lower than k {K}; a search cannot return more than ef_search "
            f"candidates well, so recall will suffer. Use --ef-search {K} or higher.")

    log("Pre-flight checks")
    check_dataset(fail=True)
    if args.reboots:
        check_restart_setup(fail=True)
    if args.search_only:
        check_collection_for_search(fail=True, force=args.force)
    else:
        log(f"   loading drops any existing {COLLECTION_NAME} first (--drop-old) and rebuilds it")
        check_disk_space()

    log("[0] RAM and temperature at start")
    ram_start = measure_ram()      # also fails fast if SSH / process settings are wrong
    temp_start = measure_temp()    # fails fast if the thermal sensors cannot be read
    log("   " + describe_ram(ram_start))
    log("   " + describe_temp(temp_start))

    base = {
        "started": started,
        "task_label": TASK_LABEL,
        "mode": mode,
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
        "searches_per_round": args.searches,
        "reboots": args.reboots,
        **ram_columns("before_load" if not args.search_only else "at_start", ram_start),
        "temp_at_start_c": temp_value(temp_start),
        "governor_at_start": temp_start["governor"],
        "cooldown_threshold_c": COOLDOWN_THRESHOLD_C,
        "num_parallel_inserts": NUM_PARALLEL_INSERTS,
        "log_file": log_path,
    }

    xlsx_path = os.path.join(SUMMARY_XLSX_DIR, f"{TASK_LABEL}.xlsx")
    summary = {"load_duration": None, "dir_h": None, "dir_gib": None,
               "collection_ram": None, "ram": None, "ram_when": None}
    results = []

    def save_summary():
        ram = summary["ram"]
        header_rows = summary_header_rows(
            summary["load_duration"], summary["dir_h"], summary["dir_gib"], summary["collection_ram"],
            {key: ram_value(ram, key) for key in ("res", "private", "shared", "sys_used", "sys_available")},
            summary["ram_when"])
        title = (f"{COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, {PRECISION}) | k={K}, ef_search={EF_SEARCH},"
                 f" M={M}, ef_con={EF_CON}, concurrency={CONCURRENCY} | {mode} | {TASK_LABEL} | {started}")
        try:
            summary["xlsx_written"] = write_summary_xlsx(xlsx_path, title, header_rows, results)
        except Exception as e:  # noqa: BLE001 - the summary must never stop the benchmark
            log(f"   [WARN] could not write the Excel summary: {e}")

    if args.search_only:
        summary["dir_h"], summary["dir_gib"] = data_dir_size()
        log(f"   data dir size of {COLLECTION_NAME}: {summary['dir_h']}")

    if not args.search_only:
        log(f"[1] Loading {DATASET_SIZE:,} vectors into {COLLECTION_NAME} (no search)")
        load_metrics, load_samples = run_stage("load", load=True, temp_interval=TEMP_SAMPLE_INTERVAL_LOAD,
                                               label=f"{TASK_LABEL}_load")
        log(f"   load_duration (insert + optimize) = {load_metrics.get('load_duration')} s")
        temp_load_end = try_measure(measure_temp, "temperature")
        log("   temperature at end of load: " + describe_temp(temp_load_end))

        log("[2] Waiting for Endee's RAM to settle after load")
        ram_after_load, settle_seconds, settled = wait_for_ram_to_settle()
        log("   " + describe_ram(ram_after_load))
        temp_after_settle = try_measure(measure_temp, "temperature")
        log("   temperature after RAM settled: " + describe_temp(temp_after_settle))

        summary["load_duration"] = load_metrics.get("load_duration")
        summary["dir_h"], summary["dir_gib"] = data_dir_size()
        log(f"   data dir size of {COLLECTION_NAME}: {summary['dir_h']} ({summary['dir_gib']} GiB)")
        collection_ram = collection_ram_gib()
        log(f"   collection RAM (graph + vector cache, Endee API): {collection_ram} GiB")
        if collection_ram:
            summary.update(collection_ram=collection_ram, ram=ram_after_load,
                           ram_when="after load, before search")
        save_summary()

        load_max_temp, load_min_big = summarize_temps(load_samples)
        base.update({
            "load_duration_s": load_metrics.get("load_duration"),
            **ram_columns("after_load", ram_after_load),
            "ram_settled": settled,
            "settle_wait_s": settle_seconds,
            "client_insert_workers": INSERT_WORKERS_SEEN,   # VectorDBBench's own workers (client side)
            "load_max_temp_c": load_max_temp,
            "load_min_bigcore_mhz": load_min_big,
            "temp_load_end_c": temp_value(temp_load_end),
            "temp_after_ram_settle_c": temp_value(temp_after_settle),
        })

    qps_by_round = {}
    search_overall = 0
    for rnd in range(1, rounds + 1):
        restart_seconds = None
        if rnd > 1:
            log("=" * 70)
            log(f"[R] Round {rnd}/{rounds}: restarting the Endee server "
                f"({ENDEE_COMPOSE_CMD} down, sudo reboot, {ENDEE_COMPOSE_CMD} up -d)")
            restart_seconds = restart_server()
            log(f"   restart took {restart_seconds}s")
            exists, count, note = collection_status()
            log(f"   {COLLECTION_NAME} after restart: "
                + (f"{count:,} vectors" if exists else note)
                + " (the first search of this round loads it from disk)")

        log(f"[round {rnd}/{rounds}] state at round start")
        ram_round = try_measure(measure_ram, "Endee RAM")
        temp_round = try_measure(measure_temp, "temperature")
        log("   " + describe_ram(ram_round))
        log("   " + describe_temp(temp_round))
        log_server_activity()
        round_columns = {
            "round": rnd,
            "after_reboot": rnd > 1,
            "restart_s": restart_seconds,
            **ram_columns("round_start", ram_round),
            "temp_round_start_c": temp_value(temp_round),
            "governor_round_start": temp_round["governor"] if temp_round else None,
        }

        for i in range(1, args.searches + 1):
            search_overall += 1
            tag = f"round {rnd}/{rounds}, search {i}/{args.searches}"
            label = f"{TASK_LABEL}_r{rnd}_s{i}"
            if i == 1 or COOLDOWN_BEFORE_EACH_SEARCH:
                log(f"[3] {tag}: cool-down check (threshold {COOLDOWN_THRESHOLD_C}C, check every "
                    f"{COOLDOWN_CHECK_INTERVAL}s, max wait {COOLDOWN_MAX_WAIT}s)")
                temp_search_start, cooldown_seconds, cooled = cool_down()
            else:
                temp_search_start = try_measure(measure_temp, "temperature")
                cooldown_seconds, cooled = 0, None
                log(f"[3] {tag}: no cool-down, {describe_temp(temp_search_start)}")

            log(f"[4] {tag}: search (serial + concurrent), k={K}, task label {label}")
            search_metrics, search_samples = run_stage(f"r{rnd}s{i}", load=False,
                                                       temp_interval=TEMP_SAMPLE_INTERVAL_SEARCH, label=label)
            log(f"   {tag}: recall={search_metrics.get('recall')} qps={search_metrics.get('qps')} "
                f"serial_p99={search_metrics.get('serial_latency_p99')}")

            log(f"[5] {tag}: RAM and temperature after search")
            ram_after_search = try_measure(measure_ram, "Endee RAM")
            temp_after_search = try_measure(measure_temp, "temperature")
            log("   " + describe_ram(ram_after_search))
            log("   " + describe_temp(temp_after_search))

            if summary["ram"] is None and rnd == 1 and i == 1:
                # load skipped (or collection RAM read 0 after the load): measure after the first search
                summary.update(collection_ram=collection_ram_gib(), ram=ram_after_search,
                               ram_when="after the first search (collection loaded into memory)")
                log(f"   collection RAM (graph + vector cache, Endee API): {summary['collection_ram']} GiB")

            search_max_temp, search_min_big = summarize_temps(search_samples)
            row = {
                **base,
                **round_columns,
                "search_run": i,
                "search_overall": search_overall,
                "search_task_label": label,
                "cooldown_wait_s": cooldown_seconds,
                "cooled_below_threshold": cooled,
                "temp_search_start_c": temp_value(temp_search_start),
                "governor_search_start": temp_search_start["governor"] if temp_search_start else None,
                "search_max_temp_c": search_max_temp,
                "search_min_bigcore_mhz": search_min_big,
                "temp_after_search_c": temp_value(temp_after_search),
                "recall": search_metrics.get("recall"),
                "ndcg": search_metrics.get("ndcg"),
                "qps": search_metrics.get("qps"),
                "serial_latency_p99": search_metrics.get("serial_latency_p99"),
                "serial_latency_p95": search_metrics.get("serial_latency_p95"),
                "conc_latency_p99": first(search_metrics.get("conc_latency_p99_list")),
                "conc_latency_p95": first(search_metrics.get("conc_latency_p95_list")),
                "conc_latency_avg": first(search_metrics.get("conc_latency_avg_list")),
                **ram_columns("after_search", ram_after_search),
                "data_dir_size": summary["dir_h"],
                "data_dir_gib": summary["dir_gib"],
                "collection_ram_gib": summary["collection_ram"],
                "summary_ram_measured": summary["ram_when"],
            }
            append_csv(row)
            results.append({
                "run_count": search_overall, "round": rnd,
                "recall": search_metrics.get("recall"), "ndcg": search_metrics.get("ndcg"),
                "qps": search_metrics.get("qps"),
                "serial_p99": search_metrics.get("serial_latency_p99"),
                "serial_p95": search_metrics.get("serial_latency_p95"),
                "conc_p99": first(search_metrics.get("conc_latency_p99_list")),
                "conc_p95": first(search_metrics.get("conc_latency_p95_list")),
                "temp_start": temp_value(temp_search_start), "temp_max": search_max_temp,
                "min_big_mhz": search_min_big,
                "governor": temp_search_start["governor"] if temp_search_start else None,
                "res_after": ram_value(ram_after_search, "res"),
                "private_after": ram_value(ram_after_search, "private"),
                "shared_after": ram_value(ram_after_search, "shared"),
                "available_after": ram_value(ram_after_search, "sys_available"),
            })
            save_summary()  # saved now, so finished searches are kept even if a later step fails
            log(f"   {tag}: saved to {OUTPUT_CSV}")
            if search_metrics.get("qps") is not None:
                qps_by_round.setdefault(rnd, []).append(search_metrics["qps"])

    log("=" * 70)
    log(f"Done: {TASK_LABEL} ({mode}, {COLLECTION_NAME}, k={K})")
    if not args.search_only:
        log(f"Endee RAM after load (before search): RES {base.get('endee_res_after_load_gib')} GiB = private "
            f"{base.get('endee_private_after_load_gib')} + shared/MDBX {base.get('endee_shared_mdbx_after_load_gib')}")
    for rnd, qps in qps_by_round.items():
        when = "after reboot" if rnd > 1 else ("after load" if not args.search_only else "first round")
        log(f"Round {rnd} ({when}): QPS " + ", ".join(f"{q:.1f}" for q in qps)
            + f" | median {statistics.median(qps):.1f}, min {min(qps):.1f}, max {max(qps):.1f}")
    log(f"CSV: {OUTPUT_CSV}")
    if summary.get("xlsx_written"):
        log(f"Excel summary: {xlsx_path}")
    else:
        log("Excel summary: NOT written (see the warning above; pip install openpyxl, then rebuild it from the "
            f"CSV: python {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'make_summary_xlsx.py')} "
            f"{TASK_LABEL})")
    if log_path:
        log(f"Log: {log_path}")


if __name__ == "__main__":
    main()