#!/usr/bin/env python3
"""
Measures the RAM each Endee collection uses, for collections that are already loaded
(no insert, no benchmark run). Reads Endee's own per-collection report:

    GET /api/v2/collection/<name>/memory
      mem.graph_bytes         HNSW graph (links + labels)          } Endee process memory, held for as
      mem.vector_cache_bytes  in-RAM cache of quantized vectors    } long as the collection is loaded
      mem.mdbx_bytes          the collection's data file currently in the OS page cache
                              (kernel file cache: used when RAM is free, dropped under memory pressure)
      disk.graph_bytes, disk.mdbx_bytes   size on disk

  endee_ram = graph + vector cache. This is per collection, unlike `docker stats`, which is the whole
  container: every collection Endee has in memory, plus the server itself.

Endee only holds a collection in memory after a search or object fetch has used it since Endee
started (describe and /memory calls don't load it); until then its graph and cache read 0.

  python measure_collection_ram.py                      every collection, as they are now (loads nothing)
  python measure_collection_ram.py 2M_index 3M_index    only these; one that isn't in memory is loaded
                                                        first, then measured
Loading uses the get-objects API (POST /collection/<name>/objects/query) for one id, not a search: Endee
loads the collection from disk (HNSW graph and vector cache in full), and no search runs.
What that measures is "after loading from disk, before any search". Graph, vector cache and the process's
private memory are the same as right after an insert; the process's shared/MDBX pages, RES and the board's
used/available are not (right after an insert, much more of the data file is still in RAM), so label those
"after restart + object fetch", not "after load". For a fresh reading, restart the endee container first
(never while a benchmark is running).
Loaded collections stay in memory until Endee restarts, so only name collections that fit in the
server's RAM together (restart the container afterwards to free them).

board_used_free / board_available / board_swap_used: the "Board used / available / swap used" of the benchmark
summaries (free's used, which on this board's Debian is total - available). The machine_* columns below are
htop's view. endee_process_swap: the Endee process's own pages in swap (VmSwap; not part of RES), and
endee_process_private_plus_swap: the memory it holds that cannot be dropped.

Machine memory (whole server, all processes) is read from /proc/meminfo and split the way htop does:
  used       htop's green bar: total - free - buffers - cache (Cached + SReclaimable)
  shared     htop's magenta bar (Shmem)
  buffers    htop's blue bar
  cache      htop's yellow bar: file cache (Cached + SReclaimable - Shmem), reclaimable
  htop_mem   the number in htop's "Mem" bar text (htop 3.x shows used + shared)
  buff_cache free's "buff/cache" column (buffers + Cached + SReclaimable)
  free, available, total, swap_used   as in free / htop

Needs: export ENDEE_TOKEN="..."  (the same token the benchmark uses; collections belong to its db).
The container total, the Endee process's RES / private / shared (htop's view, whole process: all
loaded collections + the server) and machine memory are read over SSH with the benchmark script's
settings (run_benchmark_with_ram_and_temp.py, or run_benchmark_with_ram.py if that isn't present);
if that fails, the per-collection numbers are still recorded.
Results are appended to OUTPUT_CSV, one row per collection.
"""
import csv
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import requests
from endee import endee

# ============================================================
# CONFIGURATION
# ============================================================
TOKEN = os.environ.get("ENDEE_TOKEN", "<ENDEE_API_TOKEN>")  # export ENDEE_TOKEN="..."
BASE_URL = "http://172.16.23.51:8080/api/v2"
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collection_ram_results.csv")

GIB = 1024 ** 3
MEMINFO_KEYS = ("MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached", "SReclaimable", "Shmem",
                "SwapTotal", "SwapFree")
MEMINFO_SNIPPET = "grep -E '^(" + "|".join(MEMINFO_KEYS) + "):' /proc/meminfo | sed 's/^/MEMINFO /'\n"
MACHINE_COLUMNS = ("htop_mem", "used", "shared", "buffers", "cache", "buff_cache", "free", "available",
                   "total", "swap_used")


def gib(n):
    return None if n is None else round(n / GIB, 3)


# ============================================================
# ENDEE API
# ============================================================
class EndeeApi:
    def __init__(self):
        # The SDK resolves the token the same way the benchmark does ("a:b:region" -> "a:b")
        self.nd = endee.Endee(token=TOKEN)
        self.nd.set_base_url(BASE_URL)

    def _get(self, path):
        return requests.get(f"{self.nd.base_url}{path}",
                            headers={"Authorization": self.nd.token}, timeout=300)

    def _post(self, path, body):
        # Loading a big collection from disk can take a while, hence the long timeout
        return requests.post(f"{self.nd.base_url}{path}",
                             headers={"Authorization": self.nd.token}, json=body, timeout=1800)

    def list_names(self):
        return [c["name"] for c in self.nd.list_collections()]

    def describe(self, name):
        resp = self._get(f"/collection/{name}")
        if resp.status_code == 404:
            raise LookupError(f"collection '{name}' not found (is ENDEE_TOKEN the benchmark's token?)")
        resp.raise_for_status()
        return resp.json()

    def memory(self, name):
        resp = self._get(f"/collection/{name}/memory")
        if resp.status_code == 404:
            # describe() already confirmed the collection exists, so the route itself is missing
            sys.exit("This Endee server has no GET /collection/<name>/memory endpoint (added in "
                     "ndd #105, 2026-08-04). Update the server build, or compare `docker stats` "
                     "before and after loading one collection on a freshly restarted container.")
        resp.raise_for_status()
        return resp.json()

    def load(self, name):
        """Fetches one object by id, which makes Endee load the collection from disk without running
        a search (the id doesn't need to exist; missing ids are skipped). Returns seconds taken."""
        start = time.time()
        resp = self._post(f"/collection/{name}/objects/query", {"ids": ["0"]})
        if resp.status_code != 200:
            raise RuntimeError(f"loading '{name}' failed: HTTP {resp.status_code} {resp.text[:300]}")
        return time.time() - start


def vector_field(info):
    """First dense (or multi-vector) field; None for a sparse-only collection (no graph)."""
    for f in info.get("fields", []):
        if f.get("type") in ("vector", "multi_vector"):
            return f
    return None


# ============================================================
# SERVER MEMORY (container + machine)
# ============================================================
def htop_memory(meminfo):
    """/proc/meminfo values in bytes -> the machine memory split htop shows (same formula as htop)."""
    total, free = meminfo["MemTotal"], meminfo["MemFree"]
    buffers, shared = meminfo.get("Buffers", 0), meminfo.get("Shmem", 0)
    cached = meminfo.get("Cached", 0) + meminfo.get("SReclaimable", 0)
    used_diff = free + cached + buffers
    used = total - used_diff if total >= used_diff else total - free
    return {
        "htop_mem": used + shared,
        "used": used,
        "shared": shared,
        "buffers": buffers,
        "cache": cached - shared,
        "buff_cache": buffers + cached,
        "free": free,
        "available": meminfo.get("MemAvailable", free),
        "total": total,
        "swap_used": meminfo.get("SwapTotal", 0) - meminfo.get("SwapFree", 0),
    }


def parse_meminfo(stdout):
    meminfo = {}
    for line in stdout.splitlines():
        m = re.match(r"MEMINFO (\w+):\s+(\d+)\s*kB", line)
        if m:
            meminfo[m.group(1)] = int(m.group(2)) * 1024
    return meminfo


def server_ram():
    """Container total (docker stats) + machine memory (/proc/meminfo) in one SSH call, using
    run_benchmark_with_ram.py's SERVER_SSH / ENDEE_DOCKER_CONTAINER. None if it fails."""
    try:
        try:
            import run_benchmark_with_ram_and_temp as rb  # current benchmark script
        except ImportError:
            import run_benchmark_with_ram as rb
        cmd = (["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", rb.SERVER_SSH, "bash -s"]
               if rb.SERVER_SSH else ["bash", "-s"])
        proc = subprocess.run(cmd, input=rb._ram_shell_script() + MEMINFO_SNIPPET,
                              text=True, capture_output=True, timeout=60)
        if proc.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} exited {proc.returncode}: "
                               f"{proc.stdout.strip()} {proc.stderr.strip()}")
        ram = rb.parse_ram_output(proc.stdout)
        # Newer benchmark script: docker stats in 'docker_mem', process RES/private/shared in
        # 'res'/'private'/'shared'. Older one: docker stats in 'endee_rss'.
        ram["container"] = ram.get("docker_mem") if ram.get("docker_mem") is not None \
            else ram.get("endee_rss")
        meminfo = parse_meminfo(proc.stdout)
        ram["machine"] = htop_memory(meminfo) if "MemTotal" in meminfo else None
        return ram
    except Exception as e:
        print(f"\n[WARN] container/machine RAM not recorded: {e}")
        return None


# ============================================================
# MAIN
# ============================================================
def measure(api, name, load_if_cold):
    info = api.describe(name)
    field = vector_field(info)
    usage = api.memory(name)
    in_memory_before = usage["mem"]["graph_bytes"] > 0

    load_s = None
    if not in_memory_before and load_if_cold and field:
        print(f" {name}: not in memory, loading it (object fetch, no search) ...", flush=True)
        load_s = api.load(name)
        usage = api.memory(name)
        print(f" {name}: loaded in {load_s:.1f}s", flush=True)

    mem, disk = usage["mem"], usage["disk"]
    p = field["params"] if field else {}
    return {
        "measured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "collection": name,
        "elements": info.get("total_elements"),
        "dim": p.get("dimension"),
        "precision": p.get("precision"),
        "m": p.get("M"),
        "ef_con": p.get("ef_con"),
        "in_memory": mem["graph_bytes"] > 0,
        "loaded_by_script": load_s is not None,
        "measured": ("after restart + object fetch (no search)" if load_s is not None
                     else "as found (collection was already in memory)"),
        "load_from_disk_s": None if load_s is None else round(load_s, 1),
        "graph_gib": gib(mem["graph_bytes"]),
        "vector_cache_gib": gib(mem["vector_cache_bytes"]),
        "endee_ram_gib": gib(mem["graph_bytes"] + mem["vector_cache_bytes"]),
        "page_cache_gib": gib(mem["mdbx_bytes"]),
        "disk_gib": gib(disk["graph_bytes"] + disk["mdbx_bytes"]),
    }


def print_table(rows):
    print(f"\n{'collection':<24}{'elements':>12}{'dim':>6}{'prec':>7}{'in RAM':>8}"
          f"{'graph':>8}{'cache':>8}{'ENDEE':>8}{'pg cache':>10}{'disk':>8}   (GiB)")
    for r in rows:
        elements = f"{r['elements']:,}" if isinstance(r["elements"], int) else str(r["elements"])
        print(f"{r['collection']:<24}{elements:>12}{str(r['dim']):>6}{str(r['precision']):>7}"
              f"{'yes' if r['in_memory'] else 'no':>8}{r['graph_gib']:>8.2f}{r['vector_cache_gib']:>8.2f}"
              f"{r['endee_ram_gib']:>8.2f}{r['page_cache_gib']:>10.2f}{r['disk_gib']:>8.2f}")
    print("ENDEE = graph + cache: RAM the collection holds while loaded. "
          "pg cache = its data file in the OS page cache (reclaimable).")


def append_csv(rows):
    """Appends rows; if OUTPUT_CSV was written with other columns, writes a new timestamped file."""
    fieldnames = list(rows[0])
    path = OUTPUT_CSV
    if os.path.exists(path):
        with open(path, newline="") as f:
            header = next(csv.reader(f), None)
        if header != fieldnames:
            root, ext = os.path.splitext(path)
            path = f"{root}_{datetime.now():%Y%m%d_%H%M%S}{ext}"
            print(f"\n[NOTE] {OUTPUT_CSV} has other columns (older version of this script), "
                  f"writing to a new file")
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)
    return path


def main():
    if TOKEN.startswith("<"):
        sys.exit('Set the token first: export ENDEE_TOKEN="..."')

    api = EndeeApi()
    names = sys.argv[1:]
    load_if_cold = bool(names)  # only load collections the user named
    if not names:
        names = api.list_names()
        if not names:
            sys.exit("No collections found for this token")
    print(f"Measuring {len(names)} collection(s) on {BASE_URL}")

    rows = []
    for name in names:
        try:
            rows.append(measure(api, name, load_if_cold))
        except LookupError as e:
            print(f" [SKIP] {e}")
    if not rows:
        sys.exit("Nothing measured")

    ram = server_ram()
    machine = ram.get("machine") if ram else None
    for r in rows:
        r["container_mem_gib"] = gib(ram["container"]) if ram else None
        r["endee_process_res_gib"] = gib(ram.get("res")) if ram else None
        r["endee_process_private_gib"] = gib(ram.get("private")) if ram else None
        r["endee_process_shared_mdbx_gib"] = gib(ram.get("shared")) if ram else None
        r["endee_process_swap_gib"] = gib(ram.get("swap")) if ram else None
        r["endee_process_private_plus_swap_gib"] = gib(ram.get("private_plus_swap")) if ram else None
        for key in MACHINE_COLUMNS:
            r[f"machine_{key}_gib"] = gib(machine[key]) if machine else None
        # The same "Board used / available" as the benchmark summaries: free's columns (used = total - available)
        board_used = ram.get("sys_used") if ram else None
        if board_used is None and machine:
            board_used = machine["total"] - machine["available"]
        r["board_used_free_gib"] = gib(board_used)
        r["board_available_gib"] = gib(machine["available"]) if machine else None
        r["board_swap_used_gib"] = gib(machine["swap_used"]) if machine else None

    print_table(rows)
    loaded = [r for r in rows if r["in_memory"]]
    if ram:
        print(f"\nIn memory: {len(loaded)} collection(s), ENDEE total "
              f"{sum(r['endee_ram_gib'] for r in loaded):.2f} GiB | whole container "
              f"{gib(ram['container'])} GiB")
        if ram.get("res") is not None:
            print(f"Endee process (htop): RES {gib(ram['res'])} GiB = private {gib(ram['private'])}"
                  f" + shared/MDBX {gib(ram['shared'])}")
            print(f"Endee process swap: {gib(ram.get('swap'))} GiB (VmSwap, not counted in RES) | private + swap "
                  f"{gib(ram.get('private_plus_swap'))} GiB (memory that cannot be dropped)")
    if machine:
        g = {k: gib(v) for k, v in machine.items()}
        print(f"Machine (htop): Mem {g['htop_mem']} / {g['total']} GiB = used {g['used']} + shared "
              f"{g['shared']} | buffers {g['buffers']} | cache {g['cache']} (reclaimable) | "
              f"free {g['free']} | available {g['available']} | swap used {g['swap_used']}")
        print(f"Machine (free): buff/cache {g['buff_cache']} | available {g['available']} GiB")
        r0 = rows[0]
        print(f"For the summary table: Board used {r0['board_used_free_gib']} GiB (free's used = total - available)"
              f" | Board available {r0['board_available_gib']} GiB | Board swap used {r0['board_swap_used_gib']} GiB"
              f" | Endee swap {r0['endee_process_swap_gib']} GiB, private + swap "
              f"{r0['endee_process_private_plus_swap_gib']} GiB")
    cold = [r["collection"] for r in rows if not r["in_memory"]]
    if cold and not load_if_cold:
        print(f"\nNot in memory (read 0): {', '.join(cold)}\n"
              f"To load and measure them: python {os.path.basename(__file__)} {' '.join(cold)}")

    path = append_csv(rows)
    print(f"\nSaved to {path}")


if __name__ == "__main__":
    main()