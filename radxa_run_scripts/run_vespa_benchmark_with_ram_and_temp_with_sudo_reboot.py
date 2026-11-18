#!/usr/bin/env python3
"""
Runs a VectorDBBench benchmark against Vespa (running in Docker on the Radxa) and records Vespa's RAM and
the server's temperature / CPU speed. Same structure as the Endee script
(run_benchmark_with_ram_and_temp_with_sudo_reboot.py): same flags, CSV, Excel summary and log file.

Examples (on the client, inside the VectorDBBench venv, with pyvespa installed: pip install pyvespa):
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 500K --quantization bfloat16
        load 500K (built-in OpenAI 1536D dataset), then 5 searches, top-k 30
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 2M --quantization none --k 100
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 2M --quantization bfloat16 --search-only
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 2M --search-only --reboots 1
        5 searches, then on the server: docker stop vespa, sudo reboot, docker start vespa, then 5 more
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --size 4M --check
        checks everything that run would need without running anything
  python run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py --help

--size: 500K, 1M and 10M use VectorDBBench's built-in datasets (Performance1536D500K, Performance768D1M,
Performance768D10M = Cohere 10M); other sizes use the custom dataset custom_768d_<n> (PerformanceCustomDataset).
--quantization: none (= float32), bfloat16 or int8 (default bfloat16). It takes effect when the data is
loaded. int8 may need extra VectorDBBench options: put them in INT8_EXTRA_ARGS below.

Before running: Vespa and Endee both use port 8080 on the Radxa, so only one can run at a time. Stop
Endee first (cd ~/endee && docker compose down), start Vespa, and do the reverse afterwards.

What a run does (same as the Endee script):
  [0] RAM + temperature at start
  [1] load (--drop-old --load), unless --search-only; temperature logged during the load
  [2] wait until Vespa's RAM stops changing after the load, then record it
  Then 1 + --reboots rounds; before every round after the first: docker stop vespa, sudo reboot, wait for
  the new boot, docker start vespa, wait until Vespa reports "up" (it reloads its index at startup)
  Each round: --searches times: cool-down check, search (serial + concurrent), RAM + temperature after.

RAM (read on the server from /proc/<pid>/status of the processes in the Vespa container, via docker top):
  content node   = vespa-proton-bin, the process that holds the HNSW index and the vectors: its RES,
                   private (RssAnon) and shared/file-backed (RssFile + RssShmem). The fairest comparison
                   with Endee's single process.
  all Vespa      = sum over every process in the container (content node, Java query/feed container,
                   config server, metrics proxy, ...). Java processes reserve their heap up front, so their
                   RES is mostly reservation. Summing RES counts shared libraries once per process.
  collection RAM = Vespa's own memory metric for the document type (VESPA_COLLECTION_RAM_METRICS, read from
                   the metrics proxy inside the container). Check with --check that it is found.
  board          = `free` (used = total - available), the same as in the Endee script.
Vespa loads its index when it starts, so for --search-only runs the RAM summary is measured before the
first search (the index is already in memory).

One schema at a time: Vespa runs one application package, and VectorDBBench deploys a package with the
schema of the collection it loads. So loading a different collection (another size or quantization) replaces
the deployed schema, and the previous data is no longer searchable. The script reads the deployed schema
from the config server and:
  - load:        stops if a different schema is deployed, unless --replace is given (finish all searches and
                 RAM measurements of the current one first)
  - search-only: stops if the requested collection is not the deployed schema
  - --check:     shows the deployed schema

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
import urllib.parse
import urllib.request
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
# --- Vespa / benchmark ---
VESPA_URI = "http://172.16.23.51"        # --uri for vectordbbench (the Radxa's LAN IP)
VESPA_PORT = 8080                        # query/feed port
VESPA_CONFIG_PORT = 19071                # config server port (health check)
DATASET_LOCAL_DIR = "/home/launchx/vectordataset"
DEFAULT_SIZE = "500K"                    # --size
DEFAULT_K = 30                           # --k
DEFAULT_QUANTIZATION = "bfloat16"        # --quantization
QUANTIZATIONS = ("none", "bfloat16", "int8")
QUANT_NAME = {"none": "Float32", "bfloat16": "Bf16", "int8": "Int8"}       # used in default collection names
BYTES_PER_VALUE = {"none": 4, "bfloat16": 2, "int8": 1}
INT8_EXTRA_ARGS = []                     # extra vectordbbench arguments needed for int8 (to be filled in)
EF_SEARCH = 128                          # --ef-search
SEARCHES_PER_ROUND = 5                   # --searches
NUM_PER_BATCH = 1000
M = 16
EF_CONSTRUCTION = 128
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
COLLECTION_NAME = None                   # vectordbbench --collection-name, e.g. VectorDBBench768D2MBf16
DOC_TYPE = None                          # Vespa document type of the collection (default: the collection name)
QUANTIZATION = DEFAULT_QUANTIZATION
K = DEFAULT_K
TASK_LABEL = None
DB_LABEL = None                          # --db-label (optional, passed to VectorDBBench)

# --- Server ---
SERVER_SSH = "radxa@172.16.23.51"
VESPA_CONTAINER = "vespa"
VESPA_DATA_DIR = "/home/radxa/vespa-data"            # host folder mounted at /opt/vespa/var
CONTENT_NODE_PROCESS = "vespa-proton"                # process name prefix of the content node
VESPA_METRICS_URL = "http://localhost:19092/metrics/v2/values"   # metrics proxy, read inside the container
VESPA_COLLECTION_RAM_METRICS = ["content.proton.documentdb.memory_usage.allocated_bytes"]
VESPA_SCHEMAS_PATH = ("/application/v2/tenant/default/application/default/environment/prod/region/default/"
                      "instance/default/content/schemas/")   # config server: files of the deployed schemas
DISK_FACTOR = 2.5                                    # rough disk estimate = raw vector bytes x this

# --- Restart between rounds (--reboots) ---
REBOOT_TIMEOUT = 900
POST_REBOOT_SETTLE = 60
VESPA_READY_TIMEOUT = 1800                           # Vespa reloads its index at startup: allow time
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
OUTPUT_CSV = os.path.join(HERE, "vespa_benchmark_results.csv")
SUMMARY_XLSX_DIR = os.path.join(HERE, "vespa_benchmark_summaries")
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
# RAM (all processes of the Vespa container)
# ============================================================
def _ram_shell_script():
    return (
        f"docker top {VESPA_CONTAINER} -eo pid,comm 2>/dev/null | awk 'NR>1 {{print $1, $2}}' | "
        "while read pid comm; do [ -r /proc/$pid/status ] || continue; "
        "awk -v p=$pid -v c=$comm '/^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem|VmSwap):/ {v[$1]=$2} "
        "END {print \"PROC\", p, c, v[\"VmRSS:\"]+0, v[\"VmHWM:\"]+0, v[\"RssAnon:\"]+0, v[\"RssFile:\"]+0, "
        "v[\"RssShmem:\"]+0, v[\"VmSwap:\"]+0}' /proc/$pid/status; done\n"
        "free -b | awk '/^Mem:/ {print \"SYS_TOTAL\", $2; print \"SYS_USED\", $3; "
        "print \"SYS_BUFF_CACHE\", $6; print \"SYS_AVAILABLE\", $7}'\n"
        "free -b | awk '/^Swap:/ {print \"SYS_SWAP_USED\", $3}'\n"
        f"docker stats --no-stream --format 'DOCKER_MEM {{{{.MemUsage}}}}' {VESPA_CONTAINER} 2>/dev/null || true\n"
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
        raise RuntimeError(f"no processes found in container '{VESPA_CONTAINER}' (is it running?)\n{stdout}")
    nodes = [p for p in procs if p["comm"].startswith(CONTENT_NODE_PROCESS)]
    node = max(nodes, key=lambda p: p["res"]) if nodes else None
    r["node"] = node
    r["total"] = {key: sum(p[key] or 0 for p in procs)
                  for key in ("res", "private", "shared", "swap", "private_plus_swap")}
    r["procs"] = sorted(procs, key=lambda p: p["res"], reverse=True)
    return r


def measure_ram():
    return parse_ram_output(run_on_server(_ram_shell_script()))


def ram_value(r, part, key):
    """part: 'node' (content node), 'total' (all Vespa processes) or None (board / docker keys)."""
    if r is None:
        return None
    src = r.get(part) if part else r
    return None if not src else gib(src.get(key))


def ram_columns(stage, r):
    return {
        f"node_res_{stage}_gib": ram_value(r, "node", "res"),
        f"node_private_{stage}_gib": ram_value(r, "node", "private"),
        f"node_shared_{stage}_gib": ram_value(r, "node", "shared"),
        f"node_peak_res_{stage}_gib": ram_value(r, "node", "peak_res"),
        f"node_swap_{stage}_gib": ram_value(r, "node", "swap"),
        f"node_private_plus_swap_{stage}_gib": ram_value(r, "node", "private_plus_swap"),
        f"vespa_total_res_{stage}_gib": ram_value(r, "total", "res"),
        f"vespa_total_private_{stage}_gib": ram_value(r, "total", "private"),
        f"vespa_total_shared_{stage}_gib": ram_value(r, "total", "shared"),
        f"vespa_total_swap_{stage}_gib": ram_value(r, "total", "swap"),
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
    node_text = (f"content node RES {gib(node['res'])} GiB = private {gib(node['private'])} + shared "
                 f"{gib(node['shared'])} (+ {gib(node['swap'])} in swap)" if node else "content node NOT FOUND")
    top = ", ".join(f"{p['comm']} {gib(p['res'])}" for p in r["procs"][:4])
    return (f"{node_text} | all Vespa processes RES {gib(r['total']['res'])} (private "
            f"{gib(r['total']['private'])}) [largest: {top}] | docker {gib(r['docker_mem'])} | board used "
            f"{gib(r['sys_used'])}, available {gib(r['sys_available'])} of {gib(r['sys_total'])} GiB, "
            f"swap used {gib(r['sys_swap_used'])}")


def wait_for_ram_to_settle():
    start, history, last = time.time(), [], None
    while True:
        r = try_measure(measure_ram, "Vespa RAM")
        elapsed = time.time() - start
        if r is not None:
            last = r
            history.append(r["total"]["res"])
            log(f"   t={elapsed:4.0f}s  all Vespa RES {gib(r['total']['res'])} GiB, content node "
                f"{ram_value(r, 'node', 'res')} GiB")
            recent = history[-SETTLE_SAMPLES:]
            if len(recent) == SETTLE_SAMPLES and max(recent) - min(recent) <= SETTLE_TOLERANCE * max(recent):
                return last, round(elapsed), True
        if elapsed >= SETTLE_MAX_WAIT:
            log(f"   [WARN] RAM still changing after {SETTLE_MAX_WAIT}s, recording the latest reading")
            return last, round(elapsed), False
        time.sleep(SETTLE_INTERVAL)


# ============================================================
# VESPA: readiness, document count, collection RAM metric, data dir size
# ============================================================
def _get_json(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def vespa_ready():
    """True when the query/feed container reports status 'up'."""
    try:
        data = _get_json(f"{VESPA_URI}:{VESPA_PORT}/state/v1/health", timeout=10)
        return (data.get("status") or {}).get("code") == "up"
    except Exception:  # noqa: BLE001
        return False


def config_server_ready():
    try:
        data = _get_json(f"{VESPA_URI}:{VESPA_CONFIG_PORT}/state/v1/health", timeout=10)
        return (data.get("status") or {}).get("code") == "up"
    except Exception:  # noqa: BLE001
        return False


def deployed_schemas():
    """Names of the schemas in the deployed application (from the config server), or None if unknown."""
    try:
        data = _get_json(f"{VESPA_URI}:{VESPA_CONFIG_PORT}{VESPA_SCHEMAS_PATH}", timeout=30)
    except urllib.error.HTTPError as e:
        return [] if e.code == 404 else None      # 404: nothing deployed yet
    except Exception:  # noqa: BLE001
        return None
    entries = data if isinstance(data, list) else data.get("files", []) if isinstance(data, dict) else []
    names = []
    for entry in entries:
        name = os.path.basename(str(entry).rstrip("/"))
        if name.endswith(".sd"):
            names.append(name[:-3])
    return names


def check_deployed_schema(load, replace, fail):
    """Load: a different deployed schema would be replaced. Search: the collection must be the deployed one."""
    schemas = deployed_schemas()
    if schemas is None:
        log(f"   [WARN] could not read the deployed schema from the config server ({VESPA_CONFIG_PORT})")
        return
    mine = [s for s in schemas if s.lower() == DOC_TYPE.lower()]
    others = [s for s in schemas if s.lower() != DOC_TYPE.lower()]
    log(f"   deployed schema(s): {', '.join(schemas) or 'none'}")
    if load and others:
        msg = (f"Vespa has schema {', '.join(others)} deployed. Loading {DOC_TYPE} replaces it, and its data will "
               f"no longer be searchable. Finish its searches and RAM measurements first, then rerun with --replace")
        if replace:
            log(f"   [NOTE] --replace given: {', '.join(others)} will be replaced by {DOC_TYPE}")
        else:
            problem(msg, fail)
    if not load and not mine:
        problem(f"{DOC_TYPE} is not the deployed schema ({', '.join(schemas) or 'none deployed'}); search the "
                f"deployed one with --collection / --doc-type, or load {DOC_TYPE} first", fail)


def vespa_document_count():
    """(count, note): number of documents of DOC_TYPE, from a hits=0 query."""
    yql = f"select * from {DOC_TYPE} where true"
    url = (f"{VESPA_URI}:{VESPA_PORT}/search/?yql={urllib.parse.quote(yql)}&hits=0&timeout=60s")
    try:
        data = _get_json(url, timeout=90)
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code} (document type '{DOC_TYPE}' may not exist)"
    except Exception as e:  # noqa: BLE001
        return None, f"could not query ({e})"
    root = data.get("root") or {}
    if root.get("errors"):
        return None, f"query error: {root['errors'][0].get('message', root['errors'][0])}"
    return (root.get("fields") or {}).get("totalCount"), "ok"


def _metric_entries(node):
    if isinstance(node, dict):
        if isinstance(node.get("values"), dict):
            yield node.get("dimensions") or {}, node["values"]
        for value in node.values():
            yield from _metric_entries(value)
    elif isinstance(node, list):
        for value in node:
            yield from _metric_entries(value)


def fetch_metrics():
    out = run_on_server(f"docker exec {VESPA_CONTAINER} curl -s {VESPA_METRICS_URL}\n", timeout=60)
    return json.loads(out)


def collection_ram_from_metrics(metrics):
    """Bytes of the first VESPA_COLLECTION_RAM_METRICS metric for DOC_TYPE, or None."""
    for base in VESPA_COLLECTION_RAM_METRICS:
        for dims, values in _metric_entries(metrics):
            if not any(str(v).lower() == DOC_TYPE.lower() for v in dims.values()):
                continue
            for suffix in (".last", ".max", ".average", ""):
                if base + suffix in values:
                    return float(values[base + suffix])
    return None


def collection_ram_gib():
    try:
        value = collection_ram_from_metrics(fetch_metrics())
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read Vespa's metrics: {e}")
        return None
    if value is None:
        log(f"   [WARN] no {VESPA_COLLECTION_RAM_METRICS} metric for document type {DOC_TYPE} (see --check)")
    return gib(value)


def data_dir_size():
    """(du -sh text, GiB, path): the document type's folder in the Vespa data dir, or the whole data dir."""
    script = (
        f"D=$(find {VESPA_DATA_DIR}/db/vespa/search -maxdepth 6 -type d -path '*/documents/*' "
        f"-iname '{DOC_TYPE}' 2>/dev/null | head -1)\n"
        f"[ -n \"$D\" ] || D={VESPA_DATA_DIR}\n"
        "echo \"PATH $D\"\n"
        "du -sb \"$D\" 2>/dev/null | cut -f1\n"
        "du -sh \"$D\" 2>/dev/null | cut -f1\n"
    )
    try:
        words = run_on_server(script, timeout=300).split()   # ["PATH", path, bytes, human]
        return words[3], gib(int(words[2])), words[1]
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read the size of the Vespa data folder: {e}")
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
    count, note = vespa_document_count()
    if count is None:
        problem(f"could not count documents of {DOC_TYPE}: {note}; run without --search-only to load it, or "
                f"set --collection / --doc-type", fail)
    elif count != DATASET_SIZE:
        msg = f"{DOC_TYPE} has {count:,} documents, expected {DATASET_SIZE:,}"
        if force:
            log(f"   [WARN] {msg}; continuing because of --force")
        else:
            problem(msg + "; reload it, or add --force", fail)
    else:
        log(f"   {DOC_TYPE}: {count:,} documents, as expected")


def check_disk_space():
    need = DATASET_SIZE * DATASET_DIM * BYTES_PER_VALUE[QUANTIZATION] / GIB * DISK_FACTOR
    try:
        avail = int(run_on_server(f"df -B1 --output=avail {VESPA_DATA_DIR} | tail -1\n").split()[-1]) / GIB
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not read free disk space in {VESPA_DATA_DIR}: {e}")
        return
    log(f"   server disk: {avail:.1f} GiB free in {VESPA_DATA_DIR}; rough need {need:.1f} GiB "
        f"(raw vectors x {DISK_FACTOR}; refine DISK_FACTOR once a load has been measured)")
    if avail < need:
        log("   [WARN] may not fit")


def check_endee_stopped():
    try:
        names = run_on_server("docker ps --format '{{.Names}}'\n").split()
    except Exception as e:  # noqa: BLE001
        log(f"   [WARN] could not list running containers: {e}")
        return
    if VESPA_CONTAINER not in names:
        log(f"   [PROBLEM] container '{VESPA_CONTAINER}' is not running (docker start {VESPA_CONTAINER})")
    others = [n for n in names if n != VESPA_CONTAINER]
    if any("endee" in n.lower() for n in others):
        log("   [PROBLEM] Endee is still running: it uses port 8080 too and competes for RAM/CPU. "
            "Stop it first: cd ~/endee && docker compose down")
    elif others:
        log(f"   other running containers: {', '.join(others)} (they compete for RAM/CPU)")


def check_restart_setup(fail):
    if not SERVER_SSH:
        problem("--reboots needs SERVER_SSH", fail)
        return
    try:
        out = run_on_server(
            f"docker inspect -f '{{{{.Name}}}}' {VESPA_CONTAINER} >/dev/null 2>&1 && echo CONTAINER_OK || "
            "echo CONTAINER_FAIL\n"
            "sudo -n -l reboot >/dev/null 2>&1 && echo SUDO_OK || echo SUDO_FAIL\n")
    except Exception as e:  # noqa: BLE001
        problem(f"could not check the restart setup: {e}", fail)
        return
    if "CONTAINER_OK" in out:
        log(f"   container '{VESPA_CONTAINER}' exists (docker stop/start will be used)")
    else:
        problem(f"no container named '{VESPA_CONTAINER}' on the server", fail)
    if "SUDO_OK" in out:
        log("   passwordless `sudo reboot` allowed")
    else:
        problem("the server needs passwordless sudo for reboot (see the Endee script for the setup)", fail)


# ============================================================
# RESTART (between rounds)
# ============================================================
def wait_for_vespa(timeout=VESPA_READY_TIMEOUT):
    start = time.time()
    while True:
        node_ok = False
        try:
            node_ok = measure_ram()["node"] is not None
        except Exception:  # noqa: BLE001
            pass
        if node_ok and config_server_ready() and vespa_ready():
            log(f"   Vespa is up ({time.time() - start:.0f}s after docker start)")
            return
        if time.time() - start > timeout:
            fatal(f"Vespa did not report 'up' within {timeout}s after docker start")
        time.sleep(10)


def restart_server():
    start = time.time()
    old_boot = server_boot_id()
    if old_boot is None:
        fatal("cannot reach the server before the restart")
    log(f"   docker stop {VESPA_CONTAINER}")
    try:
        log_block(run_on_server(f"docker stop -t 120 {VESPA_CONTAINER} 2>&1\n", timeout=300))
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
    log(f"   docker start {VESPA_CONTAINER}")
    try:
        log_block(run_on_server(f"docker start {VESPA_CONTAINER} 2>&1\n", timeout=300))
    except Exception as e:  # noqa: BLE001
        fatal(f"docker start failed: {e}")
    wait_for_vespa()
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
        ("Quantization:", QUANTIZATION, "none = float32"),
        ("Data Dir Size:", s["dir_h"], f"{s['dir_gib']} GiB, du -sh {s['dir_path']}" if s["dir_h"] else None),
        ("Collection RAM (GiB):", s["collection_ram"], f"Vespa metric {VESPA_COLLECTION_RAM_METRICS[0]}"),
        ("Content node RES (GiB):", ram_value(ram, "node", "res"), "vespa-proton-bin"),
        ("Content node private (GiB):", ram_value(ram, "node", "private"), "RES - SHR"),
        ("Content node shared / file-backed (GiB):", ram_value(ram, "node", "shared"), "SHR"),
        ("Content node swap (GiB):", ram_value(ram, "node", "swap"), "VmSwap: moved to swap, not counted in RES"),
        ("Content node private + swap (GiB):", ram_value(ram, "node", "private_plus_swap"),
         "memory that cannot be dropped"),
        ("All Vespa processes RES (GiB):", ram_value(ram, "total", "res"), "sum over the container"),
        ("All Vespa processes private (GiB):", ram_value(ram, "total", "private"), None),
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
              "Content node RES after (GiB)", "All Vespa RES after (GiB)", "Board available after (GiB)",
              "Swap used after (GiB)", "Content node swap after (GiB)"]
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
        "vectordbbench", "vespa",
        "--uri", VESPA_URI,
        "--port", str(VESPA_PORT),
        "--config-port", str(VESPA_CONFIG_PORT),
        "--collection-name", COLLECTION_NAME,
        "--m", str(M),
        "--ef-construction", str(EF_CONSTRUCTION),
        "--ef-search", str(EF_SEARCH),
        "--quantization", QUANTIZATION,
    ]
    if QUANTIZATION == "int8":
        cmd += INT8_EXTRA_ARGS
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


# ============================================================
# MAIN
# ============================================================
def parse_args(argv):
    p = argparse.ArgumentParser(description="Vespa benchmark with RAM + temperature recording (see the top "
                                            "of this file).", formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--size", default=DEFAULT_SIZE, help="500K, 1M, 2M ... (default %(default)s)")
    p.add_argument("--quantization", choices=QUANTIZATIONS, default=DEFAULT_QUANTIZATION,
                   help="none (= float32), bfloat16 or int8 (default %(default)s)")
    p.add_argument("--k", type=int, default=DEFAULT_K, help="top-k (default %(default)s)")
    p.add_argument("--ef-search", type=int, default=EF_SEARCH, help="ef_search (default %(default)s)")
    p.add_argument("--search-only", action="store_true", help="data already loaded: only search")
    p.add_argument("--searches", type=int, default=SEARCHES_PER_ROUND, help="searches per round (default 5)")
    p.add_argument("--reboots", type=int, default=0,
                   help="after each round: docker stop vespa, reboot the server, docker start vespa, then "
                        "another round (default 0)")
    p.add_argument("--collection", help="vectordbbench --collection-name (default "
                                        "VectorDBBench<dim>D<size><Float32|Bf16|Int8>)")
    p.add_argument("--doc-type", help="Vespa document type to count and measure (default: the collection name)")
    p.add_argument("--dataset", help="custom dataset name (500K, 1M and 10M use built-in datasets unless given)")
    p.add_argument("--db-label", help="VectorDBBench --db-label (optional)")
    p.add_argument("--label", help="task label (default vespa_<size>_<quantization>_k<k>[_search-only]_run<N>)")
    p.add_argument("--log", help="log file (default ~/benchmark_logs/<label>.log)")
    p.add_argument("--no-log-file", action="store_true")
    p.add_argument("--force", action="store_true", help="--search-only: search even if the count differs")
    p.add_argument("--replace", action="store_true",
                   help="load even though a different schema is deployed (it gets replaced)")
    p.add_argument("--check", action="store_true", help="only check that everything this run needs works")
    return p.parse_args(argv)


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


def apply_settings(args):
    global CASE_TYPE, DATASET_DIM, DATASET_DIR, DATASET_NAME, DATASET_SIZE, CUSTOM_CASE_NAME
    global COLLECTION_NAME, DOC_TYPE, QUANTIZATION, K, EF_SEARCH, TASK_LABEL, DB_LABEL
    m = re.fullmatch(r"(\d+)\s*([KkMm])", args.size.strip())
    if not m:
        sys.exit(f"--size must look like 500K, 1M or 4M (got {args.size!r})")
    number, unit = int(m.group(1)), m.group(2).upper()
    tag = f"{number}{unit}"
    if args.k < 1 or args.searches < 1 or args.reboots < 0:
        sys.exit("--k and --searches must be at least 1, --reboots at least 0")
    DATASET_SIZE = number * (1_000 if unit == "K" else 1_000_000)
    QUANTIZATION = args.quantization
    DB_LABEL = args.db_label
    K, EF_SEARCH = args.k, args.ef_search
    builtin = BUILTIN_CASES.get(tag) if not args.dataset else None
    if builtin:
        CASE_TYPE, DATASET_DIM, DATASET_DIR = builtin["case_type"], builtin["dim"], builtin["dataset_dir"]
        DATASET_NAME = os.path.basename(DATASET_DIR)
    else:
        CASE_TYPE, DATASET_DIM, DATASET_DIR = None, CUSTOM_DIM, None
        DATASET_NAME = args.dataset or f"custom_768d_{tag.lower()}"
    CUSTOM_CASE_NAME = f"Custom {DATASET_DIM}D {tag}"
    COLLECTION_NAME = args.collection or f"VectorDBBench{DATASET_DIM}D{tag}{QUANT_NAME[QUANTIZATION]}"
    DOC_TYPE = args.doc_type or COLLECTION_NAME
    if args.label:
        TASK_LABEL = args.label
    else:
        prefix = f"vespa_{tag}_{QUANTIZATION}_k{K}" + ("_search-only" if args.search_only else "")
        TASK_LABEL = f"{prefix}_run{next_run_number(prefix)}"


def run_checks(args):
    log(f"Checking Vespa setup for {COLLECTION_NAME} (document type {DOC_TYPE}, dataset {DATASET_NAME}, "
        f"{DATASET_SIZE:,} x {DATASET_DIM}D, quantization {QUANTIZATION}, k={K}, ef_search={EF_SEARCH})")
    log("VectorDBBench search command: " + " ".join(build_command(load=False, label=TASK_LABEL)))
    if QUANTIZATION == "int8" and not INT8_EXTRA_ARGS:
        log("   [NOTE] int8 selected and INT8_EXTRA_ARGS is empty; add the extra int8 options if they are needed")
    log("Containers:")
    check_endee_stopped()
    log("Vespa health:")
    log(f"   config server ({VESPA_CONFIG_PORT}): {'up' if config_server_ready() else 'NOT up'}")
    log(f"   query/feed ({VESPA_PORT}): {'up' if vespa_ready() else 'NOT up (normal before the first load: it only serves once a schema is deployed)'}")
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
    log("Deployed schema:")
    check_deployed_schema(load=not args.search_only, replace=args.replace, fail=False)
    log("Collection:")
    count, note = vespa_document_count()
    log(f"   {DOC_TYPE}: " + (f"{count:,} documents (expected {DATASET_SIZE:,})" if count is not None
                               else f"{note} (fine for a load run; needed for --search-only)"))
    log("Collection RAM metric:")
    try:
        metrics = fetch_metrics()
        value = collection_ram_from_metrics(metrics)
        if value is not None:
            log(f"   {VESPA_COLLECTION_RAM_METRICS[0]} for {DOC_TYPE}: {gib(value)} GiB")
        else:
            names = sorted({k for _, vals in _metric_entries(metrics) for k in vals if "memory_usage" in k})
            types = sorted({str(v) for dims, _ in _metric_entries(metrics) for k, v in dims.items()
                            if "document" in k.lower()})
            log(f"   [PROBLEM] not found for {DOC_TYPE}. Document types in the metrics: {types[:10] or 'none'}")
            log(f"   memory metrics available: {names[:15] or 'none'}")
            log("   (set VESPA_COLLECTION_RAM_METRICS / --doc-type accordingly; empty is normal before a load)")
    except Exception as e:  # noqa: BLE001
        log(f"   [PROBLEM] could not read metrics from {VESPA_METRICS_URL} inside the container: {e}")
    log("Data dir:")
    h, g, path = data_dir_size()
    log(f"   {path}: {h} ({g} GiB)")
    log("Server disk:")
    check_disk_space()
    log("Restart setup (needed for --reboots):")
    check_restart_setup(fail=False)
    if EF_SEARCH < K:
        log(f"[PROBLEM] ef_search {EF_SEARCH} < k {K}")
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
    log(f"Run {TASK_LABEL}: Vespa {mode} | {COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, dataset "
        f"{DATASET_NAME}, case {CASE_TYPE or 'PerformanceCustomDataset'}) | quantization {QUANTIZATION} | "
        f"k={K} ef_search={EF_SEARCH} | {args.searches} searches x {rounds} round(s)"
        f"{', server restart between rounds' if args.reboots else ''}")
    log(f"Server {VESPA_URI}:{VESPA_PORT} | CSV {OUTPUT_CSV} | log {log_path or '(terminal only)'}")
    if QUANTIZATION == "int8" and not INT8_EXTRA_ARGS:
        log("[NOTE] int8: INT8_EXTRA_ARGS is empty; add the extra int8 options at the top if they are needed")

    log("Pre-flight checks")
    check_endee_stopped()
    if not config_server_ready():
        fatal(f"Vespa's config server is not up at {VESPA_URI}:{VESPA_CONFIG_PORT}; start Vespa first")
    if args.search_only and not vespa_ready():
        fatal(f"Vespa's query port {VESPA_URI}:{VESPA_PORT} is not up (it only serves once a schema is "
              f"deployed, which a load does)")
    check_dataset(fail=True)
    if args.reboots:
        check_restart_setup(fail=True)
    check_deployed_schema(load=not args.search_only, replace=args.replace, fail=True)
    if args.search_only:
        check_collection_for_search(fail=True, force=args.force)
    else:
        log(f"   loading drops any existing {COLLECTION_NAME} first (--drop-old)")
        check_disk_space()

    log("[0] RAM and temperature at start")
    ram_start = measure_ram()
    temp_start = measure_temp()
    log("   " + describe_ram(ram_start))
    log("   " + describe_temp(temp_start))

    base = {
        "started": started, "task_label": TASK_LABEL, "db": "vespa", "mode": mode,
        "collection": COLLECTION_NAME, "doc_type": DOC_TYPE, "dataset": DATASET_NAME,
        "case_type": CASE_TYPE or "PerformanceCustomDataset", "size": DATASET_SIZE, "dim": DATASET_DIM,
        "quantization": QUANTIZATION, "m": M, "ef_construction": EF_CONSTRUCTION, "ef_search": EF_SEARCH,
        "k": K, "concurrency": CONCURRENCY, "searches_per_round": args.searches, "reboots": args.reboots,
        **ram_columns("before_load" if not args.search_only else "at_start", ram_start),
        "temp_at_start_c": temp_value(temp_start), "governor_at_start": temp_start["governor"],
        "cooldown_threshold_c": COOLDOWN_THRESHOLD_C, "log_file": log_path,
    }

    xlsx_path = os.path.join(SUMMARY_XLSX_DIR, f"{TASK_LABEL}.xlsx")
    summary = {"load_duration": None, "dir_h": None, "dir_gib": None, "dir_path": None,
               "collection_ram": None, "ram": None, "ram_when": None, "xlsx_written": False}
    results = []

    def save_summary():
        title = (f"Vespa {COLLECTION_NAME} ({DATASET_SIZE:,} x {DATASET_DIM}D, {QUANTIZATION}) | k={K}, "
                 f"ef_search={EF_SEARCH}, M={M}, ef_construction={EF_CONSTRUCTION}, concurrency={CONCURRENCY} | "
                 f"{mode} | {TASK_LABEL} | {started}")
        try:
            summary["xlsx_written"] = write_summary_xlsx(xlsx_path, title, summary_header_rows(summary), results)
        except Exception as e:  # noqa: BLE001
            log(f"   [WARN] could not write the Excel summary: {e}")

    if args.search_only:
        # Vespa keeps its index in memory from startup, so measure before the first search
        summary["dir_h"], summary["dir_gib"], summary["dir_path"] = data_dir_size()
        summary.update(collection_ram=collection_ram_gib(), ram=ram_start,
                       ram_when="before the first search (Vespa keeps the index loaded)")
        log(f"   data dir: {summary['dir_path']} {summary['dir_h']} | collection RAM {summary['collection_ram']} GiB")
        save_summary()
    else:
        log(f"[1] Loading {DATASET_SIZE:,} vectors into {COLLECTION_NAME} (no search)")
        load_metrics, load_samples = run_stage("load", True, TEMP_SAMPLE_INTERVAL_LOAD, f"{TASK_LABEL}_load")
        log(f"   load_duration (insert + optimize) = {load_metrics.get('load_duration')} s")
        temp_load_end = try_measure(measure_temp, "temperature")
        log("   temperature at end of load: " + describe_temp(temp_load_end))
        count, note = vespa_document_count()
        log(f"   {DOC_TYPE} after load: " + (f"{count:,} documents" if count is not None else note))
        log("[2] Waiting for Vespa's RAM to settle after load")
        ram_after_load, settle_seconds, settled = wait_for_ram_to_settle()
        log("   " + describe_ram(ram_after_load))
        temp_after_settle = try_measure(measure_temp, "temperature")
        summary["load_duration"] = load_metrics.get("load_duration")
        summary["dir_h"], summary["dir_gib"], summary["dir_path"] = data_dir_size()
        summary.update(collection_ram=collection_ram_gib(), ram=ram_after_load,
                       ram_when="after load, before search")
        log(f"   data dir: {summary['dir_path']} {summary['dir_h']} ({summary['dir_gib']} GiB) | "
            f"collection RAM {summary['collection_ram']} GiB")
        save_summary()
        load_max_temp, load_min_big = summarize_temps(load_samples)
        base.update({
            "load_duration_s": load_metrics.get("load_duration"), "documents_after_load": count,
            **ram_columns("after_load", ram_after_load), "ram_settled": settled, "settle_wait_s": settle_seconds,
            "load_max_temp_c": load_max_temp, "load_min_bigcore_mhz": load_min_big,
            "temp_load_end_c": temp_value(temp_load_end), "temp_after_ram_settle_c": temp_value(temp_after_settle),
        })

    qps_by_round, search_overall = {}, 0
    for rnd in range(1, rounds + 1):
        restart_seconds = None
        if rnd > 1:
            log("=" * 70)
            log(f"[R] Round {rnd}/{rounds}: restarting (docker stop {VESPA_CONTAINER}, sudo reboot, "
                f"docker start {VESPA_CONTAINER})")
            restart_seconds = restart_server()
            count, note = vespa_document_count()
            log(f"   restart took {restart_seconds}s; {DOC_TYPE}: "
                + (f"{count:,} documents" if count is not None else note))
        log(f"[round {rnd}/{rounds}] state at round start")
        ram_round = try_measure(measure_ram, "Vespa RAM")
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
            ram_after = try_measure(measure_ram, "Vespa RAM")
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
                   "collection_ram_gib": summary["collection_ram"], "summary_ram_measured": summary["ram_when"]}
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
    log(f"Done: {TASK_LABEL} (Vespa {mode}, {COLLECTION_NAME}, quantization {QUANTIZATION}, k={K})")
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