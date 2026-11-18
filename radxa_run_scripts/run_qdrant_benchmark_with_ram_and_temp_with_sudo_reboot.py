#!/usr/bin/env python3
"""
Runs a VectorDBBench benchmark against Qdrant (running in Docker on the Radxa) and records Qdrant's RAM and
the server's temperature / CPU speed. Same structure as the Endee and Vespa scripts: same flags, CSV, Excel
summary (with SUDO REBOOT rows) and log file.

Examples (on the client, inside the VectorDBBench venv):
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 1M --datatype turbo16
        load 1M (built-in Cohere 768D), then 5 searches, top-k 30, gRPC
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 2M --datatype float16 --k 100
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 1M --datatype turbo16 --search-only
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 1M --datatype turbo16 --search-only --reboots 1
        5 searches, then on the server: docker stop qdrant, sudo reboot, docker start qdrant, then 5 more
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 4M --datatype turbo16 --check
  python run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py --help

--size: 500K, 1M and 10M use VectorDBBench's built-in datasets (Performance1536D500K, Performance768D1M,
Performance768D10M = Cohere 10M); other sizes use the custom dataset custom_768d_<n> (PerformanceCustomDataset).
--datatype: passed to VectorDBBench as is, so any datatype your Qdrant build supports works. KNOWN_DATATYPES
(below) only drives the default names and the disk estimate; add new ones there (name -> bytes per value).
--quantization: passed as is (default none); KNOWN_QUANTIZATIONS works the same way.
--on-disk: store the vectors and the HNSW index on disk (memory-mapped) instead of in RAM (VectorDBBench's
--on-disk true). Off by default, as in VectorDBBench. Needed when the vectors do not fit in RAM (e.g. 10M on
the Radxa). On-disk collections get their own name and labels (...Disk, _ondisk).
Every datatype/quantization gets its own collection name by default (VectorDBBench768D1MTurbo16, ...), and
Qdrant keeps several collections side by side, so loading one does not remove another.

RAM (read on the server from /proc of the processes in the Qdrant container, via docker top):
  qdrant process  = the qdrant server: RES, private (RssAnon), shared/file-backed (RssFile + RssShmem; Qdrant
                    memory-maps its segment files, so on-disk/mmap data shows up here). The fair comparison with
                    Endee's ndd-neon.
  all processes   = sum over everything in the container (normally just qdrant).
  board           = free (used = total - available), the same as in the other scripts.
Qdrant loads every collection at startup and keeps them all, so process RAM covers all collections in it:
for per-collection numbers keep only the collection you measure (the script warns if there are more).
Qdrant has no per-collection memory API, so "Collection RAM" stays empty in the summary.

After a load, Qdrant keeps optimizing/indexing in the background (status yellow). The script waits until the
collection is green before searching, so searches never measure a half-built index.

--reboots needs passwordless sudo for reboot on the server (see the Endee script for the one-time setup).
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
# CONFIGURATION
# ============================================================
# --- Qdrant / benchmark ---
QDRANT_URL = "http://172.16.23.51:6333"      # --url for vectordbbench (REST port; gRPC is 6334)
PREFER_GRPC = "true"                          # --prefer-grpc (flag --prefer-grpc / --no-grpc)
ON_DISK = False                               # --on-disk: vectors + HNSW index on disk (VectorDBBench --on-disk true)
DATASET_LOCAL_DIR = "/home/launchx/vectordataset"
DEFAULT_SIZE = "1M"
DEFAULT_K = 30
DEFAULT_DATATYPE = "turbo16"
DEFAULT_QUANTIZATION = "none"
# name -> bytes per value (disk estimate) ; add new datatypes here. Unknown ones still work (4 bytes assumed).
KNOWN_DATATYPES = {"float32": 4, "float16": 2, "uint8": 1, "turbo16": 2}
KNOWN_QUANTIZATIONS = ("none", "scalar", "binary", "product")
HNSW_EF = 128                                 # --hnsw-ef (search ef)
SEARCHES_PER_ROUND = 5
NUM_PER_BATCH = 1000
M = 16
EF_CONSTRUCT = 128
CONCURRENCY = 16
CONCURRENCY_DURATION = 30
COOLDOWN_BEFORE_EACH_SEARCH = True
LOAD_TIMEOUT = 360000
OPTIMIZE_TIMEOUT = 360000

BUILTIN_CASES = {
    "500K": {"case_type": "Performance1536D500K", "dim": 1536, "dataset_dir": "openai/openai_medium_500k"},
    "1M": {"case_type": "Performance768D1M", "dim": 768, "dataset_dir": "cohere/cohere_medium_1m"},
    "10M": {"case_type": "Performance768D10M", "dim": 768, "dataset_dir": "cohere/cohere_large_10m"},
}
CUSTOM_DIM = 768

# Set by apply_settings()
CASE_TYPE = None
DATASET_DIM = CUSTOM_DIM
DATASET_DIR = None
DATASET_NAME = None
DATASET_SIZE = None
CUSTOM_CASE_NAME = None
COLLECTION_NAME = None
DATATYPE = DEFAULT_DATATYPE
QUANTIZATION = DEFAULT_QUANTIZATION
K = DEFAULT_K
EF_SEARCH = HNSW_EF
TASK_LABEL = None
DB_LABEL = None                               # --db-label (optional, passed to VectorDBBench)

# --- Server ---
SERVER_SSH = "radxa@172.16.23.51"
DB_CONTAINER = "qdrant"
MAIN_PROCESS = "qdrant"                       # process name of the Qdrant server
QDRANT_STORAGE_DIR = "/home/radxa/qdrant-data"   # host folder mounted at /qdrant/storage
DISK_FACTOR = 2.0                             # rough disk estimate = raw vector bytes x this

# --- Restart between rounds (--reboots) ---
REBOOT_TIMEOUT = 900
POST_REBOOT_SETTLE = 60
QDRANT_READY_TIMEOUT = 1800
GREEN_TIMEOUT = 86400                         # wait at most this long for the collection to become green
POST_REBOOT_COMMANDS = []

# --- RAM settle after load ---
SETTLE_INTERVAL = 10
SETTLE_SAMPLES = 3
SETTLE_TOLERANCE = 0.01
SETTLE_MAX_WAIT = 900

# --- Temperature ---
COOLDOWN_THRESHOLD_C = 70.0
COOLDOWN_CHECK_INTERVAL = 120
COOLDOWN_MAX_WAIT = 300
TEMP_SAMPLE_INTERVAL_LOAD = 300
TEMP_SAMPLE_INTERVAL_SEARCH = 10

# --- Output ---
RESULTS_DIR = ""
HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(HERE, "qdrant_benchmark_results.csv")
SUMMARY_XLSX_DIR = os.path.join(HERE, "qdrant_benchmark_summaries")
LOG_DIR = "~/benchmark_logs"

GIB = 1024 ** 3


# ============================================================
# LOGGING
# ============================================================
_LOG_FH = None
_OUT_LOCK = threading.Lock()


def _emit(text):
    with _OUT_LOCK:
        sys.stdout.write(text)
        sys.stdout.flush()
        if _LOG_FH:
            _LOG_FH.write(text)
            _LOG_FH.flush()


def log(msg=""):
    _emit(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")


def log_block(text, indent="      "):
    for line in text.splitlines():
        if line.strip():
            log(indent + line.rstrip())


def fatal(msg):
    log(f"[ERROR] {msg}")
    sys.exit(1)


def problem(msg, fail):
    if fail:
        fatal(msg)
    log(f"   [PROBLEM] {msg}")


def try_measure(fn, what):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read {what}: {e}")
        return None


def gib(n):
    return None if n is None else round(n / GIB, 3)


# ============================================================
# SERVER COMMANDS
# ============================================================
def _ssh_cmd():
    return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SERVER_SSH]


def run_on_server(script, timeout=60):
    cmd = _ssh_cmd() + ["bash -s"] if SERVER_SSH else ["bash", "-s"]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed (exit {proc.returncode}) using {' '.join(cmd)}\n"
                           f"{proc.stdout.strip()}\n{proc.stderr.strip()}")
    return proc.stdout


def server_boot_id():
    try:
        return run_on_server("cat /proc/sys/kernel/random/boot_id\n", timeout=20).strip() or None
    except Exception:  # noqa: BLE001
        return None


# ============================================================
# RAM (all processes of the Qdrant container)
# ============================================================
def _ram_shell_script():
    return (
        f"docker top {DB_CONTAINER} -eo pid,comm 2>/dev/null | awk 'NR>1 {{print $1, $2}}' | "
        "while read pid comm; do [ -r /proc/$pid/status ] || continue; "
        "awk -v p=$pid -v c=$comm '/^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem|VmSwap):/ {v[$1]=$2} "
        "END {print \"PROC\", p, c, v[\"VmRSS:\"]+0, v[\"VmHWM:\"]+0, v[\"RssAnon:\"]+0, v[\"RssFile:\"]+0, "
        "v[\"RssShmem:\"]+0, v[\"VmSwap:\"]+0}' /proc/$pid/status; done\n"
        "free -b | awk '/^Mem:/ {print \"SYS_TOTAL\", $2; print \"SYS_USED\", $3; "
        "print \"SYS_BUFF_CACHE\", $6; print \"SYS_AVAILABLE\", $7}'\n"
        "free -b | awk '/^Swap:/ {print \"SYS_SWAP_USED\", $3}'\n"
        f"docker stats --no-stream --format 'DOCKER_MEM {{{{.MemUsage}}}}' {DB_CONTAINER} 2>/dev/null || true\n"
    )


_SIZE_UNITS = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4,
               "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def _parse_size(text):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not m or m.group(2).lower() not in _SIZE_UNITS:
        raise ValueError(f"Cannot parse memory size: {text!r}")
    return float(m.group(1)) * _SIZE_UNITS[m.group(2).lower()]


def parse_ram_output(stdout):
    procs = []
    r = {"docker_mem": None, "sys_total": None, "sys_used": None, "sys_buff_cache": None,
         "sys_available": None, "sys_swap_used": None}
    keys = {"SYS_TOTAL": "sys_total", "SYS_USED": "sys_used", "SYS_BUFF_CACHE": "sys_buff_cache",
            "SYS_AVAILABLE": "sys_available", "SYS_SWAP_USED": "sys_swap_used"}
    for line in stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "PROC" and len(parts) in (8, 9):
            pid, comm = parts[1], parts[2]
            rss, hwm, anon, file_, shmem = map(int, parts[3:8])
            swap = int(parts[8]) * 1024 if len(parts) == 9 else None
            procs.append({"pid": pid, "comm": comm, "res": rss * 1024, "peak_res": hwm * 1024,
                          "private": anon * 1024, "shared": (file_ + shmem) * 1024, "swap": swap,
                          "private_plus_swap": anon * 1024 + (swap or 0)})
        elif parts[0] in keys and len(parts) == 2:
            r[keys[parts[0]]] = float(parts[1])
        elif line.startswith("DOCKER_MEM "):
            try:
                r["docker_mem"] = _parse_size(line[len("DOCKER_MEM "):].split("/")[0])
            except ValueError:
                pass
    if not procs:
        raise RuntimeError(f"no processes found in container '{DB_CONTAINER}' (is it running?)\n{stdout}")
    nodes = [p for p in procs if p["comm"].startswith(MAIN_PROCESS)]
    node = max(nodes, key=lambda p: p["res"]) if nodes else None
    r["node"] = node
    r["total"] = {key: sum(p[key] or 0 for p in procs)
                  for key in ("res", "private", "shared", "swap", "private_plus_swap")}
    r["procs"] = sorted(procs, key=lambda p: p["res"], reverse=True)
    return r


def measure_ram():
    return parse_ram_output(run_on_server(_ram_shell_script()))


def ram_value(r, part, key):
    """part: 'node' (the qdrant process), 'total' (all processes in the container) or None (board / docker keys)."""
    if r is None:
        return None
    src = r.get(part) if part else r
    return None if not src else gib(src.get(key))


def ram_columns(stage, r):
    return {
        f"qdrant_res_{stage}_gib": ram_value(r, "node", "res"),
        f"qdrant_private_{stage}_gib": ram_value(r, "node", "private"),
        f"qdrant_shared_{stage}_gib": ram_value(r, "node", "shared"),
        f"qdrant_peak_res_{stage}_gib": ram_value(r, "node", "peak_res"),
        f"qdrant_swap_{stage}_gib": ram_value(r, "node", "swap"),
        f"qdrant_private_plus_swap_{stage}_gib": ram_value(r, "node", "private_plus_swap"),
        f"container_total_res_{stage}_gib": ram_value(r, "total", "res"),
        f"container_total_private_{stage}_gib": ram_value(r, "total", "private"),
        f"container_total_shared_{stage}_gib": ram_value(r, "total", "shared"),
        f"container_total_swap_{stage}_gib": ram_value(r, "total", "swap"),
        f"docker_mem_{stage}_gib": ram_value(r, None, "docker_mem"),
        f"board_used_{stage}_gib": ram_value(r, None, "sys_used"),
        f"board_buff_cache_{stage}_gib": ram_value(r, None, "sys_buff_cache"),
        f"board_available_{stage}_gib": ram_value(r, None, "sys_available"),
        f"swap_used_{stage}_gib": ram_value(r, None, "sys_swap_used"),
    }


def describe_ram(r):
    if r is None:
        return "RAM unavailable"
    node = r["node"]
    node_text = (f"qdrant RES {gib(node['res'])} GiB = private {gib(node['private'])} + shared "
                 f"{gib(node['shared'])} (+ {gib(node['swap'])} in swap)" if node else "qdrant process NOT FOUND")
    top = ", ".join(f"{p['comm']} {gib(p['res'])}" for p in r["procs"][:4])
    return (f"{node_text} | all container processes RES {gib(r['total']['res'])} (private "
            f"{gib(r['total']['private'])}) [largest: {top}] | docker {gib(r['docker_mem'])} | board used "
            f"{gib(r['sys_used'])}, available {gib(r['sys_available'])} of {gib(r['sys_total'])} GiB, "
            f"swap used {gib(r['sys_swap_used'])}")


def wait_for_ram_to_settle():
    start, history, last = time.time(), [], None
    while True:
        r = try_measure(measure_ram, "Qdrant RAM")
        elapsed = time.time() - start
        if r is not None:
            last = r
            history.append(r["total"]["res"])
            log(f"   t={elapsed:4.0f}s  all container RES {gib(r['total']['res'])} GiB, qdrant "
                f"{ram_value(r, 'node', 'res')} GiB")
            recent = history[-SETTLE_SAMPLES:]
            if len(recent) == SETTLE_SAMPLES and max(recent) - min(recent) <= SETTLE_TOLERANCE * max(recent):
                return last, round(elapsed), True
        if elapsed >= SETTLE_MAX_WAIT:
            log(f"   [WARN] RAM still changing after {SETTLE_MAX_WAIT}s, recording the latest reading")
            return last, round(elapsed), False
        time.sleep(SETTLE_INTERVAL)


# ============================================================
# QDRANT: readiness, collections, data dir size
# ============================================================
def _get_json(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def qdrant_ready():
    """True when Qdrant answers /readyz (all shards loaded)."""
    try:
        with urllib.request.urlopen(f"{QDRANT_URL}/readyz", timeout=10) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001
        return False


def list_collections():
    """Names of all collections in Qdrant, or None if Qdrant cannot be reached."""
    try:
        data = _get_json(f"{QDRANT_URL}/collections")
        return [c["name"] for c in (data.get("result") or {}).get("collections", [])]
    except Exception:  # noqa: BLE001
        return None


def collection_info():
    """(info dict or None, note) from GET /collections/<name>."""
    try:
        data = _get_json(f"{QDRANT_URL}/collections/{COLLECTION_NAME}", timeout=60)
    except urllib.error.HTTPError as e:
        return None, "not found" if e.code == 404 else f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, f"could not query ({e})"
    return data.get("result") or {}, "ok"


def collection_points():
    info, note = collection_info()
    if info is None:
        return None, note
    return info.get("points_count"), f"status {info.get('status')}"


def wait_for_green(timeout=GREEN_TIMEOUT):
    """Waits until the collection's status is green (background optimization/indexing finished)."""
    start, last = time.time(), None
    while True:
        info, note = collection_info()
        status = (info or {}).get("status")
        if status == "green":
            if last is not None:
                log(f"   {COLLECTION_NAME} is green ({time.time() - start:.0f}s)")
            return round(time.time() - start)
        if status != last:
            log(f"   {COLLECTION_NAME} status {status or note}: waiting for green (Qdrant still optimizing)")
            last = status
        if time.time() - start > timeout:
            log(f"   [WARN] {COLLECTION_NAME} not green after {timeout}s, continuing anyway")
            return round(time.time() - start)
        time.sleep(30)


def data_dir_size():
    """(du -sh text, GiB, path) of the collection's folder in the Qdrant storage dir."""
    path = f"{QDRANT_STORAGE_DIR}/collections/{COLLECTION_NAME}"
    script = (f"D={path}; [ -d \"$D\" ] || D={QDRANT_STORAGE_DIR}\n"
              "echo \"PATH $D\"\ndu -sb \"$D\" 2>/dev/null | cut -f1\ndu -sh \"$D\" 2>/dev/null | cut -f1\n")
    try:
        words = run_on_server(script, timeout=300).split()   # ["PATH", path, bytes, human]
        return words[3], gib(int(words[2])), words[1]
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read the size of the Qdrant data folder: {e}")
        return None, None, None


# ============================================================
# TEMPERATURE / CPU SPEED / GOVERNOR (same as the Endee script)
# ============================================================
_TEMP_SHELL_SCRIPT = (
    "for z in /sys/class/thermal/thermal_zone*; do "
    "echo \"TEMP $(cat $z/type) $(cat $z/temp)\"; done\n"
    "for p in /sys/devices/system/cpu/cpufreq/policy*; do "
    "echo \"FREQ $(basename $p) $(cat $p/scaling_cur_freq) $(cat $p/cpuinfo_max_freq)\"; "
    "echo \"GOV $(basename $p) $(cat $p/scaling_governor 2>/dev/null)\"; done\n"
)


def parse_temp_output(stdout):
    zones, freqs, govs = {}, {}, {}
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
    return {"zones": zones, "max_c": max(zones.values()), "freqs": freqs,
            "min_big_mhz": min(big_now) if big_now else None,
            "governor": ",".join(sorted(set(govs.values()))) or None}


def measure_temp():
    return parse_temp_output(run_on_server(_TEMP_SHELL_SCRIPT))


def temp_value(t):
    return None if t is None else round(t["max_c"], 1)


def describe_temp(t):
    if t is None:
        return "temperature unavailable"
    zones = " ".join(f"{name} {c:.1f}" for name, c in t["zones"].items())
    freqs = " ".join(f"{p} {cur}/{mx}" for p, (cur, mx) in t["freqs"].items())
    return f"hottest {t['max_c']:.1f}C | {zones} | CPU MHz now/max: {freqs} | governor {t['governor']}"


def summarize_temps(samples):
    temps = [s["max_c"] for s in samples]
    bigs = [s["min_big_mhz"] for s in samples if s["min_big_mhz"] is not None]
    return (round(max(temps), 1) if temps else None, min(bigs) if bigs else None)


def cool_down():
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
    try:
        log_block(run_on_server("uptime\necho 'busiest processes (average CPU % since each started):'\n"
                                "ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -8\n"))
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read server activity: {e}")


# ============================================================
# CHECKS
# ============================================================
def dataset_folder():
    if CASE_TYPE:
        candidates = (os.path.join(DATASET_LOCAL_DIR, DATASET_DIR),)
    else:
        candidates = (os.path.join(DATASET_LOCAL_DIR, DATASET_NAME, DATASET_NAME),
                      os.path.join(DATASET_LOCAL_DIR, DATASET_NAME))
    for folder in candidates:
        if glob.glob(os.path.join(folder, "*.parquet")):
            return folder
    return None


def ground_truth_depth(folder):
    path = os.path.join(folder, "neighbors.parquet")
    if not os.path.exists(path):
        return None
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        for name in table.column_names:
            col = table.column(name)
            if len(col) and isinstance(col[0].as_py(), (list, tuple)):
                return len(col[0].as_py())
    except Exception:  # noqa: BLE001
        return None
    return None


def check_dataset(fail):
    folder = dataset_folder()
    if not folder:
        expected = (os.path.join(DATASET_LOCAL_DIR, DATASET_DIR) if CASE_TYPE
                    else f"{DATASET_LOCAL_DIR}/{DATASET_NAME}/{DATASET_NAME}")
        problem(f"no .parquet files for dataset '{DATASET_NAME}' (expected in {expected})", fail)
        return
    log(f"   dataset files: {folder}")
    if not os.path.exists(os.path.join(folder, "neighbors.parquet")):
        problem(f"no neighbors.parquet (ground truth) in {folder}", fail)
        return
    depth = ground_truth_depth(folder)
    if depth is None:
        log(f"   [WARN] could not read the ground truth depth; make sure it is >= k ({K})")
    elif depth < K:
        problem(f"--k {K}, but neighbors.parquet has only {depth} neighbors per query", fail)
    else:
        log(f"   ground truth: {depth} neighbors per query (enough for k={K})")


def check_collection_for_search(fail, force):
    count, note = collection_points()
    if count is None:
        problem(f"collection {COLLECTION_NAME}: {note}; run without --search-only to load it, or set --collection",
                fail)
    elif count != DATASET_SIZE:
        msg = f"{COLLECTION_NAME} has {count:,} points, expected {DATASET_SIZE:,}"
        if force:
            log(f"   [WARN] {msg}; continuing because of --force")
        else:
            problem(msg + "; reload it, or add --force", fail)
    else:
        log(f"   {COLLECTION_NAME}: {count:,} points, as expected ({note})")


def check_other_collections():
    names = list_collections()
    if names is None:
        log("   [WARN] could not list Qdrant's collections")
        return
    others = [n for n in names if n != COLLECTION_NAME]
    log(f"   collections in Qdrant: {', '.join(names) or 'none'}")
    if others:
        log(f"   [NOTE] Qdrant keeps all collections in memory, so its RAM figures include {', '.join(others)} too. "
            "For per-collection RAM, delete or move the others first (back them up with Qdrant snapshots if needed)")


def check_disk_space():
    bytes_per_value = KNOWN_DATATYPES.get(DATATYPE, 4)
    need = DATASET_SIZE * DATASET_DIM * bytes_per_value / GIB * DISK_FACTOR
    try:
        avail = int(run_on_server(f"df -B1 --output=avail {QDRANT_STORAGE_DIR} | tail -1\n").split()[-1]) / GIB
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read free disk space in {QDRANT_STORAGE_DIR}: {e}")
        return
    log(f"   server disk: {avail:.1f} GiB free in {QDRANT_STORAGE_DIR}; rough need {need:.1f} GiB "
        f"(raw vectors x {DISK_FACTOR}; refine DISK_FACTOR once a load has been measured)")
    if avail < need:
        log("   [WARN] may not fit")


def check_other_databases():
    try:
        names = run_on_server("docker ps --format '{{.Names}}'\n").split()
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not list running containers: {e}")
        return
    if DB_CONTAINER not in names:
        log(f"   [PROBLEM] container '{DB_CONTAINER}' is not running (docker start {DB_CONTAINER})")
    others = [n for n in names if n != DB_CONTAINER]
    if others:
        log(f"   [PROBLEM] other containers are running: {', '.join(others)}. They compete for RAM/CPU and make the "
            "results unfair; stop them first (Endee: cd ~/endee && docker compose down; Vespa: docker stop vespa)")


def check_restart_setup(fail):
    if not SERVER_SSH:
        problem("--reboots needs SERVER_SSH", fail)
        return
    try:
        out = run_on_server(
            f"docker inspect -f '{{{{.Name}}}}' {DB_CONTAINER} >/dev/null 2>&1 && echo CONTAINER_OK || "
            "echo CONTAINER_FAIL\n"
            "sudo -n -l reboot >/dev/null 2>&1 && echo SUDO_OK || echo SUDO_FAIL\n")
    except Exception as e:  # noqa: BLE001
        problem(f"could not check the restart setup: {e}", fail)
        return
    if "CONTAINER_OK" in out:
        log(f"   container '{DB_CONTAINER}' exists (docker stop/start will be used)")
    else:
        problem(f"no container named '{DB_CONTAINER}' on the server", fail)
    if "SUDO_OK" in out:
        log("   passwordless `sudo reboot` allowed")
    else:
        problem("the server needs passwordless sudo for reboot (see the Endee script for the setup)", fail)


# ============================================================
# RESTART (between rounds)
# ============================================================
def wait_for_qdrant(timeout=QDRANT_READY_TIMEOUT):
    start = time.time()
    while True:
        process_ok = False
        try:
            process_ok = measure_ram()["node"] is not None
        except Exception:  # noqa: BLE001
            pass
        if process_ok and qdrant_ready():
            log(f"   Qdrant is ready ({time.time() - start:.0f}s after docker start)")
            return
        if time.time() - start > timeout:
            fatal(f"Qdrant was not ready within {timeout}s after docker start")
        time.sleep(10)


def restart_server():
    start = time.time()
    old_boot = server_boot_id()
    if old_boot is None:
        fatal("cannot reach the server before the restart")
    log(f"   docker stop {DB_CONTAINER}")
    try:
        log_block(run_on_server(f"docker stop -t 120 {DB_CONTAINER} 2>&1\n", timeout=300))
    except Exception as e:  # noqa: BLE001
        fatal(f"docker stop failed: {e}")
    log("   sudo reboot")
    try:
        proc = subprocess.run(_ssh_cmd() + ["sudo -n reboot"], text=True, capture_output=True, timeout=60)
        err = (proc.stderr or "").lower()
        if "password" in err or "not allowed" in err or "may not run" in err:
            fatal(f"sudo refused the reboot: {proc.stderr.strip()}")
    except subprocess.TimeoutExpired:
        pass
    log(f"   waiting for the server to come back (up to {REBOOT_TIMEOUT}s)")
    time.sleep(20)
    while True:
        boot = server_boot_id()
        if boot and boot != old_boot:
            break
        if time.time() - start > REBOOT_TIMEOUT:
            fatal(f"the server did not come back within {REBOOT_TIMEOUT}s")
        time.sleep(10)
    log(f"   server is back {time.time() - start:.0f}s after the restart began; settling {POST_REBOOT_SETTLE}s")
    time.sleep(POST_REBOOT_SETTLE)
    for command in POST_REBOOT_COMMANDS:
        log(f"   post-reboot command: {command}")
        try:
            log_block(run_on_server(command + "\n", timeout=120))
        except Exception as e:  # noqa: BLE001
            log(f"   [WARN] post-reboot command failed: {e}")
    log(f"   docker start {DB_CONTAINER}")
    try:
        log_block(run_on_server(f"docker start {DB_CONTAINER} 2>&1\n", timeout=300))
    except Exception as e:  # noqa: BLE001
        fatal(f"docker start failed: {e}")
    wait_for_qdrant()
    wait_for_green()
    return round(time.time() - start)


# ============================================================
# EXCEL SUMMARY
# ============================================================
_XLSX_WARNED = False


def summary_header_rows(s):
    ram = s["ram"]
    return [
        ("Load Duration:", s["load_duration"], "s" if s["load_duration"] is not None else
         "not loaded in this run (--search-only)"),
        ("Datatype:", DATATYPE, None),
        ("Quantization:", QUANTIZATION, None),
        ("On disk:", "yes" if ON_DISK else "no", "vectors + HNSW index memory-mapped" if ON_DISK else "vectors in RAM"),
        ("Data Dir Size:", s["dir_h"], f"{s['dir_gib']} GiB, du -sh {s['dir_path']}" if s["dir_h"] else None),
        ("Collection RAM (GiB):", None, "n/a: Qdrant has no per-collection memory API"),
        ("Process RES (GiB):", ram_value(ram, "node", "res"), "qdrant process"),
        ("Process private (GiB):", ram_value(ram, "node", "private"), "RES - SHR"),
        ("Process shared / file-backed (GiB):", ram_value(ram, "node", "shared"), "SHR (memory-mapped segments)"),
        ("Process swap (GiB):", ram_value(ram, "node", "swap"), "VmSwap: moved to swap, not counted in RES"),
        ("Process private + swap (GiB):", ram_value(ram, "node", "private_plus_swap"),
         "memory that cannot be dropped"),
        ("Board used (GiB):", ram_value(ram, None, "sys_used"), "free's used (total - available)"),
        ("Board available (GiB):", ram_value(ram, None, "sys_available"), None),
        ("Board swap used (GiB):", ram_value(ram, None, "sys_swap_used"), "free: swap used by all processes"),
        ("RAM measured:", s["ram_when"], None),
    ]


def write_summary_xlsx(path, title, header_rows, results):
    global _XLSX_WARNED
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    except ImportError:
        if not _XLSX_WARNED:
            log("   [WARN] openpyxl is not installed, so no Excel summary (pip install openpyxl)")
            _XLSX_WARNED = True
        return False
    font = Font(name="Arial", size=11)
    bold = Font(name="Arial", size=11, bold=True)
    head_font = Font(name="Arial", size=11, bold=True, color="FFFFFF")
    head_fill = PatternFill("solid", start_color="3D6B52")
    stripe = PatternFill("solid", start_color="F3F5F7")
    thin = Side(style="thin", color="3D6B52")
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
            c.alignment = Alignment(horizontal="right")
        if note:
            ws.cell(row, 3, note).font = font
        row += 1
    row += 1
    columns = ["Run Count", "Recall", "nDCG", "QPS", "Latency (p99) in s", "Latency (p95) in s"]
    top = row
    for col, name in enumerate(columns, start=1):
        c = ws.cell(row, col, name)
        c.font, c.fill = head_font, head_fill
    row += 1
    previous_round = None
    for n, r in enumerate(results):
        if previous_round is not None and r["round"] != previous_round:
            ws.cell(row, 1, "SUDO REBOOT").font = Font(name="Arial", size=12, bold=True)
            row += 1
        for col, value in enumerate([r["run_count"], r["recall"], r["ndcg"], r["qps"], r["serial_p99"],
                                     r["serial_p95"]], start=1):
            c = ws.cell(row, col, value)
            c.font = font
            if n % 2:
                c.fill = stripe
        previous_round = r["round"]
        row += 1
    for rr in range(top, row):
        edge = {"top": thin if rr == top else None, "bottom": thin if rr == row - 1 else None}
        ws.cell(rr, 1).border = Border(left=thin, **edge)
        ws.cell(rr, len(columns)).border = Border(right=thin, **edge)
        for col in range(2, len(columns)):
            ws.cell(rr, col).border = Border(**edge)
    ws.column_dimensions["A"].width = 42
    for letter in "BCDEF":
        ws.column_dimensions[letter].width = 20
    ws.column_dimensions["C"].width = 30
    ds = wb.create_sheet("Per-search details")
    detail = ["Run Count", "Round", "After reboot", "Temp at start (C)", "Max temp during search (C)",
              "Lowest big-core MHz", "Governor", "Concurrent p99 (s)", "Concurrent p95 (s)",
              "Qdrant RES after (GiB)", "All container RES after (GiB)", "Board available after (GiB)",
              "Swap used after (GiB)", "Qdrant swap after (GiB)"]
    for col, name in enumerate(detail, start=1):
        c = ds.cell(1, col, name)
        c.font, c.fill = head_font, head_fill
        ds.column_dimensions[c.column_letter].width = max(14, len(name) + 2)
    for i, r in enumerate(results, start=2):
        for col, value in enumerate([r["run_count"], r["round"], "yes" if r["round"] > 1 else "no",
                                     r["temp_start"], r["temp_max"], r["min_big_mhz"], r["governor"],
                                     r["conc_p99"], r["conc_p95"], r["node_res_after"], r["total_res_after"],
                                     r["available_after"], r["swap_after"],
                                     r.get("node_swap_after")], start=1):
            ds.cell(i, col, value).font = font
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp.xlsx"
    wb.save(tmp)
    os.replace(tmp, path)
    return True


# ============================================================
# BENCHMARK RUNNER
# ============================================================
def results_dir():
    if RESULTS_DIR:
        return RESULTS_DIR
    from vectordb_bench import config
    return str(config.RESULTS_LOCAL_DIR)


def build_command(load, label):
    cmd = [
        "vectordbbench", "qdrantlocal",
        "--url", QDRANT_URL,
        "--prefer-grpc", PREFER_GRPC,
        "--collection-name", COLLECTION_NAME,
        "--m", str(M),
        "--ef-construct", str(EF_CONSTRUCT),
        "--hnsw-ef", str(EF_SEARCH),
        "--quantization", QUANTIZATION,
        "--datatype", DATATYPE,
    ]
    if ON_DISK:
        cmd += ["--on-disk", "true"]
    if CASE_TYPE:
        cmd += ["--case-type", CASE_TYPE]
    else:
        cmd += ["--case-type", "PerformanceCustomDataset",
                "--custom-case-name", CUSTOM_CASE_NAME,
                "--custom-dataset-name", DATASET_NAME,
                "--custom-dataset-dir", DATASET_NAME,
                "--custom-dataset-size", str(DATASET_SIZE),
                "--custom-dataset-dim", str(DATASET_DIM),
                "--custom-dataset-metric-type", "COSINE",
                "--custom-dataset-file-count", "1",
                "--custom-dataset-with-gt",
                "--custom-case-load-timeout", str(LOAD_TIMEOUT),
                "--custom-case-optimize-timeout", str(OPTIMIZE_TIMEOUT)]
    cmd += ["--k", str(K),
            "--num-concurrency", str(CONCURRENCY),
            "--concurrency-duration", str(CONCURRENCY_DURATION),
            "--concurrency-timeout", "3600",
            "--task-label", label]
    if DB_LABEL:
        cmd += ["--db-label", DB_LABEL]
    if load:
        cmd += ["--drop-old", "--load", "--skip-search-serial", "--skip-search-concurrent"]
    else:
        cmd += ["--skip-drop-old", "--skip-load", "--search-serial", "--search-concurrent"]
    return cmd


def _pump(stream):
    for raw in iter(stream.readline, b""):
        _emit(raw.decode("utf-8", "replace"))
    stream.close()


def run_stage(name, load, temp_interval, label):
    rdir = results_dir()
    pattern = os.path.join(rdir, "**", "*.json")
    before = set(glob.glob(pattern, recursive=True))
    env = {**os.environ, "NUM_PER_BATCH": str(NUM_PER_BATCH), "DATASET_LOCAL_DIR": DATASET_LOCAL_DIR,
           "PYTHONUNBUFFERED": "1"}
    samples = []
    proc = subprocess.Popen(build_command(load, label), env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
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
    if isinstance(value, list):
        return value[0] if value else None
    return value


def append_csv(row):
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


def next_run_number(prefix):
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


# ============================================================
# MAIN
# ============================================================
def title_case(name):
    return name[:1].upper() + name[1:]


def parse_args(argv):
    p = argparse.ArgumentParser(description="Qdrant benchmark with RAM + temperature recording (see the top of "
                                            "this file).", formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--size", default=DEFAULT_SIZE, help="500K, 1M, 2M ... (default %(default)s)")
    p.add_argument("--datatype", default=DEFAULT_DATATYPE,
                   help=f"vector datatype passed to VectorDBBench, e.g. {', '.join(KNOWN_DATATYPES)} "
                        "(default %(default)s; others are passed as is)")
    p.add_argument("--quantization", default=DEFAULT_QUANTIZATION,
                   help=f"e.g. {', '.join(KNOWN_QUANTIZATIONS)} (default %(default)s; passed as is)")
    p.add_argument("--no-grpc", action="store_true", help="use REST instead of gRPC (--prefer-grpc false)")
    p.add_argument("--on-disk", action="store_true",
                   help="store vectors and the HNSW index on disk instead of RAM (VectorDBBench --on-disk true)")
    p.add_argument("--k", type=int, default=DEFAULT_K, help="top-k (default %(default)s)")
    p.add_argument("--hnsw-ef", type=int, default=HNSW_EF, help="search ef (default %(default)s)")
    p.add_argument("--search-only", action="store_true", help="collection already loaded: only search")
    p.add_argument("--searches", type=int, default=SEARCHES_PER_ROUND, help="searches per round (default 5)")
    p.add_argument("--reboots", type=int, default=0,
                   help="after each round: docker stop qdrant, reboot the server, docker start qdrant, then another "
                        "round (default 0)")
    p.add_argument("--collection", help="collection name (default VectorDBBench<dim>D<size><Datatype>[<Quant>][Disk])")
    p.add_argument("--dataset", help="custom dataset name (500K, 1M and 10M use built-in datasets unless given)")
    p.add_argument("--db-label", help="VectorDBBench --db-label (optional)")
    p.add_argument("--label", help="task label (default qdrant_<size>_<datatype>[_<quant>][_ondisk]_k<k>"
                                   "[_search-only]_run<N>)")
    p.add_argument("--log", help="log file (default ~/benchmark_logs/<label>.log)")
    p.add_argument("--no-log-file", action="store_true")
    p.add_argument("--force", action="store_true", help="--search-only: search even if the count differs")
    p.add_argument("--check", action="store_true", help="only check that everything this run needs works")
    return p.parse_args(argv)


def apply_settings(args):
    global CASE_TYPE, DATASET_DIM, DATASET_DIR, DATASET_NAME, DATASET_SIZE, CUSTOM_CASE_NAME
    global COLLECTION_NAME, DATATYPE, QUANTIZATION, K, EF_SEARCH, TASK_LABEL, PREFER_GRPC, DB_LABEL, ON_DISK
    m = re.fullmatch(r"(\d+)\s*([KkMm])", args.size.strip())
    if not m:
        sys.exit(f"--size must look like 500K, 1M or 4M (got {args.size!r})")
    number, unit = int(m.group(1)), m.group(2).upper()
    tag = f"{number}{unit}"
    if args.k < 1 or args.searches < 1 or args.reboots < 0:
        sys.exit("--k and --searches must be at least 1, --reboots at least 0")
    if not re.fullmatch(r"[A-Za-z0-9_]+", args.datatype) or not re.fullmatch(r"[A-Za-z0-9_]+", args.quantization):
        sys.exit("--datatype and --quantization may only contain letters, digits and _")
    DATASET_SIZE = number * (1_000 if unit == "K" else 1_000_000)
    DATATYPE, QUANTIZATION = args.datatype, args.quantization
    K, EF_SEARCH = args.k, args.hnsw_ef
    PREFER_GRPC = "false" if args.no_grpc else "true"
    DB_LABEL = args.db_label
    ON_DISK = args.on_disk
    builtin = BUILTIN_CASES.get(tag) if not args.dataset else None
    if builtin:
        CASE_TYPE, DATASET_DIM, DATASET_DIR = builtin["case_type"], builtin["dim"], builtin["dataset_dir"]
        DATASET_NAME = os.path.basename(DATASET_DIR)
    else:
        CASE_TYPE, DATASET_DIM, DATASET_DIR = None, CUSTOM_DIM, None
        DATASET_NAME = args.dataset or f"custom_768d_{tag.lower()}"
    CUSTOM_CASE_NAME = f"Custom {DATASET_DIM}D {tag}"
    quant_part = "" if QUANTIZATION == "none" else title_case(QUANTIZATION)
    disk_part = "Disk" if ON_DISK else ""
    COLLECTION_NAME = (args.collection or
                       f"VectorDBBench{DATASET_DIM}D{tag}{title_case(DATATYPE)}{quant_part}{disk_part}")
    if args.label:
        TASK_LABEL = args.label
    else:
        quant_label = "" if QUANTIZATION == "none" else f"_{QUANTIZATION}"
        prefix = (f"qdrant_{tag}_{DATATYPE}{quant_label}" + ("_ondisk" if ON_DISK else "") + f"_k{K}"
                  + ("_rest" if args.no_grpc else "")
                  + ("_search-only" if args.search_only else ""))
        TASK_LABEL = f"{prefix}_run{next_run_number(prefix)}"


def warn_unknown_types():
    if DATATYPE not in KNOWN_DATATYPES:
        log(f"   [NOTE] datatype '{DATATYPE}' is not in KNOWN_DATATYPES; it is passed to VectorDBBench as is "
            "(add it to KNOWN_DATATYPES with its bytes per value for a better disk estimate)")
    if QUANTIZATION not in KNOWN_QUANTIZATIONS:
        log(f"   [NOTE] quantization '{QUANTIZATION}' is not in KNOWN_QUANTIZATIONS; passed as is")


def run_checks(args):
    log(f"Checking Qdrant setup for {COLLECTION_NAME} (dataset {DATASET_NAME}, {DATASET_SIZE:,} x {DATASET_DIM}D, "
        f"datatype {DATATYPE}, quantization {QUANTIZATION}, on disk {ON_DISK}, gRPC {PREFER_GRPC}, k={K}, "
        f"hnsw_ef={EF_SEARCH})")
    log("VectorDBBench search command: " + " ".join(build_command(load=False, label=TASK_LABEL)))
    warn_unknown_types()
    log("Containers:")
    check_other_databases()
    log(f"Qdrant ready ({QDRANT_URL}/readyz): {'yes' if qdrant_ready() else 'NO'}")
    log("Collections:")
    check_other_collections()
    count, note = collection_points()
    log(f"   {COLLECTION_NAME}: " + (f"{count:,} points (expected {DATASET_SIZE:,}), {note}" if count is not None
                                     else f"{note} (fine for a load run; needed for --search-only)"))
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
    log("Data dir:")
    h, g, path = data_dir_size()
    log(f"   {path}: {h} ({g} GiB)")
    log("Server disk:")
    check_disk_space()
    log("Restart setup (needed for --reboots):")
    check_restart_setup(fail=False)
    if EF_SEARCH < K:
        log(f"[PROBLEM] hnsw_ef {EF_SEARCH} < k {K}")
    try:
        import openpyxl  # noqa: F401
        log(f"Excel summary: openpyxl installed; summaries go to {SUMMARY_XLSX_DIR}")
    except ImportError:
        log("Excel summary: [PROBLEM] openpyxl is not installed (pip install openpyxl)")
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

    log_path = None
    if not args.no_log_file:
        log_path = os.path.expanduser(args.log or os.path.join(LOG_DIR, f"{TASK_LABEL}.log"))
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        _LOG_FH = open(log_path, "a", encoding="utf-8")

    mode = "search-only" if args.search_only else "load+search"
    rounds = 1 + args.reboots
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"Run {TASK_LABEL}: Qdrant {mode} | {COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, dataset "
        f"{DATASET_NAME}, case {CASE_TYPE or 'PerformanceCustomDataset'}) | datatype {DATATYPE}, quantization "
        f"{QUANTIZATION}, on disk {'yes' if ON_DISK else 'no'}, gRPC {PREFER_GRPC} | k={K} hnsw_ef={EF_SEARCH} | "
        f"{args.searches} searches x {rounds} "
        f"round(s){', server restart between rounds' if args.reboots else ''}")
    log(f"Server {QDRANT_URL} | CSV {OUTPUT_CSV} | log {log_path or '(terminal only)'}")

    log("Pre-flight checks")
    warn_unknown_types()
    check_other_databases()
    if not qdrant_ready():
        fatal(f"Qdrant is not ready at {QDRANT_URL}; start it first (docker start {DB_CONTAINER})")
    check_dataset(fail=True)
    check_other_collections()
    if args.reboots:
        check_restart_setup(fail=True)
    if args.search_only:
        check_collection_for_search(fail=True, force=args.force)
        wait_for_green()
    else:
        log(f"   loading drops any existing {COLLECTION_NAME} first (--drop-old)")
        check_disk_space()

    log("[0] RAM and temperature at start")
    ram_start = measure_ram()
    temp_start = measure_temp()
    log("   " + describe_ram(ram_start))
    log("   " + describe_temp(temp_start))

    base = {
        "started": started, "task_label": TASK_LABEL, "db": "qdrant", "mode": mode, "collection": COLLECTION_NAME,
        "dataset": DATASET_NAME, "case_type": CASE_TYPE or "PerformanceCustomDataset", "size": DATASET_SIZE,
        "dim": DATASET_DIM, "datatype": DATATYPE, "quantization": QUANTIZATION, "on_disk": ON_DISK, "grpc": PREFER_GRPC, "m": M,
        "ef_construct": EF_CONSTRUCT, "hnsw_ef": EF_SEARCH, "k": K, "concurrency": CONCURRENCY,
        "searches_per_round": args.searches, "reboots": args.reboots,
        **ram_columns("before_load" if not args.search_only else "at_start", ram_start),
        "temp_at_start_c": temp_value(temp_start), "governor_at_start": temp_start["governor"],
        "cooldown_threshold_c": COOLDOWN_THRESHOLD_C, "log_file": log_path,
    }

    xlsx_path = os.path.join(SUMMARY_XLSX_DIR, f"{TASK_LABEL}.xlsx")
    summary = {"load_duration": None, "dir_h": None, "dir_gib": None, "dir_path": None, "ram": None,
               "ram_when": None, "xlsx_written": False}
    results = []

    def save_summary():
        title = (f"Qdrant {COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, {DATATYPE}, quantization "
                 f"{QUANTIZATION}, {'on disk' if ON_DISK else 'in RAM'}) | k={K}, hnsw_ef={EF_SEARCH}, M={M}, ef_construct={EF_CONSTRUCT}, "
                 f"concurrency={CONCURRENCY}, gRPC {PREFER_GRPC} | {mode} | {TASK_LABEL} | {started}")
        try:
            summary["xlsx_written"] = write_summary_xlsx(xlsx_path, title, summary_header_rows(summary), results)
        except Exception as e:  # noqa: BLE001
            log(f"   [WARN] could not write the Excel summary: {e}")

    if args.search_only:
        # Qdrant loads its collections at startup, so measure before the first search
        summary["dir_h"], summary["dir_gib"], summary["dir_path"] = data_dir_size()
        summary.update(ram=ram_start, ram_when="before the first search (Qdrant keeps collections loaded)")
        save_summary()
    else:
        log(f"[1] Loading {DATASET_SIZE:,} vectors into {COLLECTION_NAME} (no search)")
        load_metrics, load_samples = run_stage("load", True, TEMP_SAMPLE_INTERVAL_LOAD, f"{TASK_LABEL}_load")
        log(f"   load_duration (insert + optimize) = {load_metrics.get('load_duration')} s")
        temp_load_end = try_measure(measure_temp, "temperature")
        log("   temperature at end of load: " + describe_temp(temp_load_end))
        log("   waiting until the collection is green (indexing finished)")
        green_wait = wait_for_green()
        count, note = collection_points()
        log(f"   {COLLECTION_NAME} after load: " + (f"{count:,} points, {note}" if count is not None else note))
        log("[2] Waiting for Qdrant's RAM to settle after load")
        ram_after_load, settle_seconds, settled = wait_for_ram_to_settle()
        log("   " + describe_ram(ram_after_load))
        temp_after_settle = try_measure(measure_temp, "temperature")
        summary["load_duration"] = load_metrics.get("load_duration")
        summary["dir_h"], summary["dir_gib"], summary["dir_path"] = data_dir_size()
        summary.update(ram=ram_after_load, ram_when="after load (collection green), before search")
        log(f"   data dir: {summary['dir_path']} {summary['dir_h']} ({summary['dir_gib']} GiB)")
        save_summary()
        load_max_temp, load_min_big = summarize_temps(load_samples)
        base.update({
            "load_duration_s": load_metrics.get("load_duration"), "green_wait_after_load_s": green_wait,
            "points_after_load": count, **ram_columns("after_load", ram_after_load), "ram_settled": settled,
            "settle_wait_s": settle_seconds, "load_max_temp_c": load_max_temp, "load_min_bigcore_mhz": load_min_big,
            "temp_load_end_c": temp_value(temp_load_end), "temp_after_ram_settle_c": temp_value(temp_after_settle),
        })

    qps_by_round, search_overall = {}, 0
    for rnd in range(1, rounds + 1):
        restart_seconds = None
        if rnd > 1:
            log("=" * 70)
            log(f"[R] Round {rnd}/{rounds}: restarting (docker stop {DB_CONTAINER}, sudo reboot, "
                f"docker start {DB_CONTAINER})")
            restart_seconds = restart_server()
            count, note = collection_points()
            log(f"   restart took {restart_seconds}s; {COLLECTION_NAME}: "
                + (f"{count:,} points, {note}" if count is not None else note))
        log(f"[round {rnd}/{rounds}] state at round start")
        ram_round = try_measure(measure_ram, "Qdrant RAM")
        temp_round = try_measure(measure_temp, "temperature")
        log("   " + describe_ram(ram_round))
        log("   " + describe_temp(temp_round))
        log_server_activity()
        round_columns = {"round": rnd, "after_reboot": rnd > 1, "restart_s": restart_seconds,
                         **ram_columns("round_start", ram_round), "temp_round_start_c": temp_value(temp_round),
                         "governor_round_start": temp_round["governor"] if temp_round else None}

        for i in range(1, args.searches + 1):
            search_overall += 1
            tag = f"round {rnd}/{rounds}, search {i}/{args.searches}"
            label = f"{TASK_LABEL}_r{rnd}_s{i}"
            if i == 1 or COOLDOWN_BEFORE_EACH_SEARCH:
                log(f"[3] {tag}: cool-down check (threshold {COOLDOWN_THRESHOLD_C}C)")
                temp_search_start, cooldown_seconds, cooled = cool_down()
            else:
                temp_search_start = try_measure(measure_temp, "temperature")
                cooldown_seconds, cooled = 0, None
            log(f"[4] {tag}: search (serial + concurrent), k={K}, task label {label}")
            metrics, samples = run_stage(f"r{rnd}s{i}", False, TEMP_SAMPLE_INTERVAL_SEARCH, label)
            log(f"   {tag}: recall={metrics.get('recall')} qps={metrics.get('qps')} "
                f"serial_p99={metrics.get('serial_latency_p99')}")
            log(f"[5] {tag}: RAM and temperature after search")
            ram_after = try_measure(measure_ram, "Qdrant RAM")
            temp_after = try_measure(measure_temp, "temperature")
            log("   " + describe_ram(ram_after))
            log("   " + describe_temp(temp_after))
            max_temp, min_big = summarize_temps(samples)
            row = {**base, **round_columns, "search_run": i, "search_overall": search_overall,
                   "search_task_label": label, "cooldown_wait_s": cooldown_seconds, "cooled_below_threshold": cooled,
                   "temp_search_start_c": temp_value(temp_search_start),
                   "governor_search_start": temp_search_start["governor"] if temp_search_start else None,
                   "search_max_temp_c": max_temp, "search_min_bigcore_mhz": min_big,
                   "temp_after_search_c": temp_value(temp_after),
                   "recall": metrics.get("recall"), "ndcg": metrics.get("ndcg"), "qps": metrics.get("qps"),
                   "serial_latency_p99": metrics.get("serial_latency_p99"),
                   "serial_latency_p95": metrics.get("serial_latency_p95"),
                   "conc_latency_p99": first(metrics.get("conc_latency_p99_list")),
                   "conc_latency_p95": first(metrics.get("conc_latency_p95_list")),
                   "conc_latency_avg": first(metrics.get("conc_latency_avg_list")),
                   **ram_columns("after_search", ram_after),
                   "data_dir_size": summary["dir_h"], "data_dir_gib": summary["dir_gib"],
                   "summary_ram_measured": summary["ram_when"]}
            append_csv(row)
            results.append({
                "run_count": search_overall, "round": rnd, "recall": metrics.get("recall"),
                "ndcg": metrics.get("ndcg"), "qps": metrics.get("qps"),
                "serial_p99": metrics.get("serial_latency_p99"), "serial_p95": metrics.get("serial_latency_p95"),
                "conc_p99": first(metrics.get("conc_latency_p99_list")),
                "conc_p95": first(metrics.get("conc_latency_p95_list")),
                "temp_start": temp_value(temp_search_start), "temp_max": max_temp, "min_big_mhz": min_big,
                "governor": temp_search_start["governor"] if temp_search_start else None,
                "node_res_after": ram_value(ram_after, "node", "res"),
                "total_res_after": ram_value(ram_after, "total", "res"),
                "available_after": ram_value(ram_after, None, "sys_available"),
                "swap_after": ram_value(ram_after, None, "sys_swap_used"),
                "node_swap_after": ram_value(ram_after, "node", "swap"),
            })
            save_summary()
            log(f"   {tag}: saved to {OUTPUT_CSV}")
            if metrics.get("qps") is not None:
                qps_by_round.setdefault(rnd, []).append(metrics["qps"])

    log("=" * 70)
    log(f"Done: {TASK_LABEL} (Qdrant {mode}, {COLLECTION_NAME}, datatype {DATATYPE}, k={K})")
    if summary["ram"]:
        log(f"RAM ({summary['ram_when']}): " + describe_ram(summary["ram"]))
    for rnd, qps in qps_by_round.items():
        when = "after reboot" if rnd > 1 else ("after load" if not args.search_only else "first round")
        log(f"Round {rnd} ({when}): QPS " + ", ".join(f"{q:.1f}" for q in qps)
            + f" | median {statistics.median(qps):.1f}, min {min(qps):.1f}, max {max(qps):.1f}")
    log(f"CSV: {OUTPUT_CSV}")
    log(f"Excel summary: {xlsx_path}" if summary["xlsx_written"] else "Excel summary: NOT written (pip install openpyxl)")
    if log_path:
        log(f"Log: {log_path}")


if __name__ == "__main__":
    main()