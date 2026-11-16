#!/usr/bin/env python3
"""
Runs a VectorDBBench custom-dataset benchmark against Endee and records Endee's RAM and the
Endee server's temperature / CPU speed.

  [0] RAM + temperature before load (baseline)
  [1] load only   (--drop-old --load, no search)    -> insert + optimize, load_duration
                  temperature logged every TEMP_SAMPLE_INTERVAL_LOAD seconds while loading
  [2] wait until Endee's RAM stops changing, then record it  <- "everything inserted, before search"
  Then SEARCH_REPEATS times (search 1..N on the same loaded collection):
  [3] cool-down: if the hottest sensor is at or above COOLDOWN_THRESHOLD_C, wait and re-check every
      COOLDOWN_CHECK_INTERVAL seconds; after COOLDOWN_MAX_WAIT seconds the search starts anyway
      (before every search, or only the first if COOLDOWN_BEFORE_EACH_SEARCH = False)
  [4] search only (--skip-drop-old --skip-load)     -> recall, qps, serial p99
                  temperature logged every TEMP_SAMPLE_INTERVAL_SEARCH seconds while searching
  [5] RAM + temperature after that search
One row per search is added to OUTPUT_CSV right after that search finishes (search_run = 1..N; the
load columns repeat on each row). New columns are merged into an existing file's header.
Every line this script prints starts with a timestamp, so the log shows when each reading was taken.

Where RAM and temperature are read: on the machine that runs Endee.
  * SERVER_SSH = "user@host" -> this script SSHes to the Endee server for each reading.
    Needs key-based login without a password prompt:  ssh-copy-id user@host
  * SERVER_SSH = ""          -> Endee runs on this same machine.
Endee's RAM is read from /proc/<pid>/status of the ENDEE_PROCESS_NAME process (works inside Docker too):
  endee_res      = htop RES  (VmRSS): everything Endee has in RAM
  endee_private  = RES - SHR (RssAnon): Endee's own memory (graph, buffers); cannot be given back
  endee_shared_mdbx = htop SHR (RssFile + RssShmem): mostly MDBX data pages mapped from disk; the kernel
                   can reclaim these under memory pressure, but searches slow down when it does
  endee_peak_res = highest RES since the Endee process started (VmHWM); restart Endee before a run if you
                   want it to reflect only this run
  board_used / board_buff_cache / board_available = the same columns as `free` for the whole board
  docker_mem     = `docker stats` for the container (kept for comparison with earlier runs)
Temperature comes from /sys/class/thermal (the hottest sensor is used for decisions), CPU speed from
/sys/devices/system/cpu/cpufreq. "min big-core MHz" is the lowest current speed of the fastest CPU
cluster seen during a stage; well below its maximum under load means the board was throttling.

Check the RAM + temperature setup before a long run:   python run_benchmark_with_ram.py --check
Run the benchmark:   export ENDEE_TOKEN="..."; python run_benchmark_with_ram.py >> ~/run_4M_output.log 2>&1
"""
import csv
import glob
import json
import os
import re
import subprocess
import sys
import statistics
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
COLLECTION_NAME = "4M_index"
TASK_LABEL = "4M_run1"
NUM_PER_BATCH = 1000
M = 16
EF_CON = 128
EF_SEARCH = 128
SPACE_TYPE = "cosine"
PRECISION = "int16"
K = 30
CONCURRENCY = 16
CONCURRENCY_DURATION = 30
SEARCH_REPEATS = 5                   # the search stage runs this many times after one load
COOLDOWN_BEFORE_EACH_SEARCH = True   # False = cool-down check only before the first search

# --- Custom dataset (folder: DATASET_LOCAL_DIR/DATASET_NAME/DATASET_NAME) ---
CUSTOM_CASE_NAME = "Custom 768D 4M"
DATASET_NAME = "custom_768d_4m"
DATASET_SIZE = 4_000_000
DATASET_DIM = 768
LOAD_TIMEOUT = 360000        # seconds
OPTIMIZE_TIMEOUT = 360000    # seconds

# --- RAM measurement on the Endee server ---
SERVER_SSH = "radxa@172.16.23.51"   # "" if Endee runs on this machine
ENDEE_PROCESS_NAME = "ndd-neon"      # Endee server process name as htop shows it (`pgrep -o -x`, oldest match)
ENDEE_DOCKER_CONTAINER = "endee"     # if Endee runs in Docker, `docker stats` is also recorded; "" to skip
SETTLE_INTERVAL = 10                 # seconds between RAM samples after load
SETTLE_SAMPLES = 3                   # settled = this many samples in a row within SETTLE_TOLERANCE
SETTLE_TOLERANCE = 0.01              # 1%
SETTLE_MAX_WAIT = 900                # record anyway after this many seconds

# --- Temperature on the Endee server ---
COOLDOWN_THRESHOLD_C = 70.0          # wait before search while the hottest sensor is at or above this
COOLDOWN_CHECK_INTERVAL = 120        # seconds between checks while waiting
COOLDOWN_MAX_WAIT = 300              # start the search after this many seconds, whatever the temperature
TEMP_SAMPLE_INTERVAL_LOAD = 300      # seconds between temperature log lines during load (0 = off)
TEMP_SAMPLE_INTERVAL_SEARCH = 10     # seconds between temperature log lines during search (0 = off)

RESULTS_DIR = ""                     # "" = VectorDBBench's results dir (vectordb_bench/results)
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ram_benchmark_results.csv")

GIB = 1024 ** 3


# ============================================================
# LOGGING
# ============================================================
def log(msg: str = ""):
    """Timestamped line, flushed at once so it lands in the right place in a redirected log file."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


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
def run_on_server(script: str) -> str:
    """Runs a shell snippet on the Endee machine (over SSH, or locally) and returns its stdout."""
    if SERVER_SSH:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SERVER_SSH, "bash -s"]
    else:
        cmd = ["bash", "-s"]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed (exit {proc.returncode}) using {' '.join(cmd)}\n"
            f"{proc.stdout.strip()}\n{proc.stderr.strip()}"
        )
    return proc.stdout


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
    """Bytes for: res (htop RES), private (RES - SHR: heap/graph), shared (htop SHR: mostly mmap'd MDBX
    pages, reclaimable but needed for fast search), peak_res, docker_mem, and the board's
    sys_used / sys_buff_cache / sys_available / sys_total (same columns as `free`)."""
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
# TEMPERATURE / CPU SPEED MEASUREMENT
# ============================================================
_TEMP_SHELL_SCRIPT = (
    "for z in /sys/class/thermal/thermal_zone*; do "
    "echo \"TEMP $(cat $z/type) $(cat $z/temp)\"; done\n"
    "for p in /sys/devices/system/cpu/cpufreq/policy*; do "
    "echo \"FREQ $(basename $p) $(cat $p/scaling_cur_freq) $(cat $p/cpuinfo_max_freq)\"; done\n"
)


def parse_temp_output(stdout: str) -> dict:
    zones = {}   # sensor name -> degrees C
    freqs = {}   # cpufreq policy -> (current MHz, max MHz)
    for line in stdout.splitlines():
        parts = line.split()
        try:
            if len(parts) == 3 and parts[0] == "TEMP":
                zones[parts[1].replace("-thermal", "")] = int(parts[2]) / 1000
            elif len(parts) == 4 and parts[0] == "FREQ":
                freqs[parts[1]] = (int(parts[2]) // 1000, int(parts[3]) // 1000)
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
    return f"hottest {t['max_c']:.1f}C | {zones} | CPU MHz now/max: {freqs}"


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


# ============================================================
# BENCHMARK RUNNER
# ============================================================
def results_dir() -> str:
    if RESULTS_DIR:
        return RESULTS_DIR
    from vectordb_bench import config  # same default VectorDBBench writes to
    return str(config.RESULTS_LOCAL_DIR)


def check_dataset():
    nested = os.path.join(DATASET_LOCAL_DIR, DATASET_NAME, DATASET_NAME)
    flat = os.path.join(DATASET_LOCAL_DIR, DATASET_NAME)
    for folder in (nested, flat):
        if glob.glob(os.path.join(folder, "*.parquet")):
            log(f"Dataset files found in {folder}")
            return
    sys.exit(f"[ERROR] no .parquet files in {nested} or {flat}; check DATASET_NAME / DATASET_LOCAL_DIR")


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


def run_stage(name: str, load: bool, temp_interval: int, label: str):
    """Runs vectordbbench; logs the server temperature every temp_interval seconds while it runs."""
    rdir = results_dir()
    pattern = os.path.join(rdir, "**", "*.json")
    before = set(glob.glob(pattern, recursive=True))
    env = {**os.environ, "NUM_PER_BATCH": str(NUM_PER_BATCH), "DATASET_LOCAL_DIR": DATASET_LOCAL_DIR}

    samples = []
    proc = subprocess.Popen(build_command(load, label), env=env)
    next_sample = time.time() + temp_interval if temp_interval else None
    while proc.poll() is None:
        time.sleep(1)
        if next_sample is not None and time.time() >= next_sample:
            t = try_measure(measure_temp, "temperature")
            if t is not None:
                samples.append(t)
                log(f"   [{name}] {describe_temp(t)}")
            next_sample = time.time() + temp_interval
    if proc.returncode != 0:
        sys.exit(f"[ERROR] vectordbbench {name} run exited with code {proc.returncode}; stopping")

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


def main():
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    if "--check" in sys.argv or "--check-ram" in sys.argv:
        print(describe_ram(measure_ram()))
        print(describe_temp(measure_temp()))
        return
    if TOKEN.startswith("<") or SERVER_SSH.startswith("<"):
        sys.exit("Set the token (export ENDEE_TOKEN=...) and SERVER_SSH at the top of this script first")

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("=" * 70)
    log(f"Run {TASK_LABEL}: {DATASET_SIZE:,} x {DATASET_DIM}D -> {COLLECTION_NAME} at {BASE_URL}")
    check_dataset()

    log("[0] RAM and temperature before load")
    ram_before = measure_ram()      # also fails fast if SSH / process settings are wrong
    temp_before = measure_temp()    # fails fast if the thermal sensors cannot be read
    log("   " + describe_ram(ram_before))
    log("   " + describe_temp(temp_before))

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

    load_max_temp, load_min_big = summarize_temps(load_samples)
    base = {
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
        **ram_columns("before_load", ram_before),
        **ram_columns("after_load", ram_after_load),
        "board_total_gib": ram_value(ram_after_load or ram_before, "sys_total"),
        "ram_settled": settled,
        "settle_wait_s": settle_seconds,
        "temp_before_load_c": temp_value(temp_before),
        "load_max_temp_c": load_max_temp,
        "load_min_bigcore_mhz": load_min_big,
        "temp_load_end_c": temp_value(temp_load_end),
        "temp_after_ram_settle_c": temp_value(temp_after_settle),
        "cooldown_threshold_c": COOLDOWN_THRESHOLD_C,
        "endee_process": ram_after_load["source"] if ram_after_load else None,
    }

    qps_list = []
    for i in range(1, SEARCH_REPEATS + 1):
        tag = f"search {i}/{SEARCH_REPEATS}"
        label = f"{TASK_LABEL}_search{i}"
        if i == 1 or COOLDOWN_BEFORE_EACH_SEARCH:
            log(f"[3] {tag}: cool-down check (threshold {COOLDOWN_THRESHOLD_C}C, check every "
                f"{COOLDOWN_CHECK_INTERVAL}s, max wait {COOLDOWN_MAX_WAIT}s)")
            temp_search_start, cooldown_seconds, cooled = cool_down()
        else:
            temp_search_start = try_measure(measure_temp, "temperature")
            cooldown_seconds, cooled = 0, None
            log(f"[3] {tag}: no cool-down, {describe_temp(temp_search_start)}")

        log(f"[4] {tag}: search (serial + concurrent), task label {label}")
        search_metrics, search_samples = run_stage(f"search{i}", load=False,
                                                   temp_interval=TEMP_SAMPLE_INTERVAL_SEARCH, label=label)
        log(f"   {tag}: recall={search_metrics.get('recall')} qps={search_metrics.get('qps')} "
            f"serial_p99={search_metrics.get('serial_latency_p99')}")

        log(f"[5] {tag}: RAM and temperature after search")
        ram_after_search = try_measure(measure_ram, "Endee RAM")
        temp_after_search = try_measure(measure_temp, "temperature")
        log("   " + describe_ram(ram_after_search))
        log("   " + describe_temp(temp_after_search))

        search_max_temp, search_min_big = summarize_temps(search_samples)
        row = {
            **base,
            "search_run": i,
            "search_task_label": label,
            "cooldown_wait_s": cooldown_seconds,
            "cooled_below_threshold": cooled,
            "temp_search_start_c": temp_value(temp_search_start),
            "search_max_temp_c": search_max_temp,
            "search_min_bigcore_mhz": search_min_big,
            "temp_after_search_c": temp_value(temp_after_search),
            "recall": search_metrics.get("recall"),
            "qps": search_metrics.get("qps"),
            "serial_latency_p99": search_metrics.get("serial_latency_p99"),
            "conc_latency_p99": first(search_metrics.get("conc_latency_p99_list")),
            **ram_columns("after_search", ram_after_search),
        }
        append_csv(row)  # saved now, so finished searches are kept even if a later one fails
        log(f"   {tag}: saved to {OUTPUT_CSV}")
        if search_metrics.get("qps") is not None:
            qps_list.append(search_metrics["qps"])

    log("=" * 70)
    log(f"Done: {TASK_LABEL}. Endee RAM after load (before search): RES {base['endee_res_after_load_gib']} GiB"
        f" = private {base['endee_private_after_load_gib']} + shared/MDBX {base['endee_shared_mdbx_after_load_gib']}")
    log("QPS per search: " + ", ".join(f"{q:.1f}" for q in qps_list))
    if qps_list:
        log(f"QPS median {statistics.median(qps_list):.1f} | min {min(qps_list):.1f} | max {max(qps_list):.1f}")

if __name__ == "__main__":
    main()