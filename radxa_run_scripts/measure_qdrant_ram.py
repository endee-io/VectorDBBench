#!/usr/bin/env python3
"""
Measures the RAM Qdrant uses for collections that are already loaded (no insert, no benchmark run): the
Qdrant equivalent of measure_collection_ram.py (Endee) and measure_vespa_ram.py (Vespa).

  python measure_qdrant_ram.py                                  every collection as found now (no restart)
  python measure_qdrant_ram.py --restart                        docker restart qdrant, wait until it is ready,
                                                                every collection is green and its RAM has
                                                                settled, then measure ("after restart, before
                                                                any search")
  python measure_qdrant_ram.py --restart --drop-caches          also empty the board's file cache first
  python measure_qdrant_ram.py VectorDBBench768D1MTurbo16       only these collections

Like Vespa, Qdrant opens all of its collections when it starts, so no fetch is needed after a restart. Data
that Qdrant keeps on disk (memory-mapped) is only read into RAM as it is used, so it shows up as the process's
shared / file-backed memory and grows during searches; measuring right after a restart gives the comparable
"loaded, before any search" figure. Qdrant keeps every collection open, so the process figures cover all of
them: for per-collection numbers keep only the collection you measure (the script warns if there are more).

Per collection (from Qdrant's REST API): points, status (green = indexing finished), indexed vectors, segments,
the vector datatype / on-disk setting / quantization from its config, and the data dir size (du of
collections/<name> in the storage folder). Qdrant has no per-collection memory API, so there is no
"collection RAM" figure.
Whole Qdrant (from /proc of the container's processes, same as the benchmark script):
  qdrant process   RES, private (RES - SHR), shared / file-backed (memory-mapped segments)
  all processes    sum over everything in the container
Board (free): used (= total - available), available, buff/cache, swap used.

Uses the settings and code of run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot.py (same folder):
SERVER_SSH, QDRANT_URL, DB_CONTAINER, QDRANT_STORAGE_DIR. Results are added to OUTPUT_CSV, one row per
collection.
"""
import argparse
import csv
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run_qdrant_benchmark_with_ram_and_temp_with_sudo_reboot as qb  # noqa: E402

OUTPUT_CSV = os.path.join(HERE, "qdrant_collection_ram_results.csv")


def vector_config(info):
    """(datatype, on_disk, quantization) from a collection's config; None where not reported."""
    config = (info or {}).get("config") or {}
    vectors = (config.get("params") or {}).get("vectors") or {}
    if vectors and all(isinstance(v, dict) for v in vectors.values()):   # named vectors: take the first
        vectors = next(iter(vectors.values()))
    quant = config.get("quantization_config") or vectors.get("quantization_config")
    quant_name = next(iter(quant)) if isinstance(quant, dict) and quant else ("none" if not quant else str(quant))
    return vectors.get("datatype"), vectors.get("on_disk"), quant_name


def drop_caches():
    try:
        qb.run_on_server("sync\necho 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null\n", timeout=120)
        qb.log("   board file cache emptied")
        return True
    except Exception as e:  # noqa: BLE001
        qb.log("   [WARN] could not empty the file cache (needs passwordless sudo for tee). Run it yourself on "
               "the server first: sync; echo 3 | sudo tee /proc/sys/vm/drop_caches")
        qb.log(f"   ({str(e).splitlines()[0]})")
        return False


def append_csv(rows):
    existing, fields = [], []
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="") as f:
            reader = csv.DictReader(f)
            fields = list(reader.fieldnames or [])
            existing = list(reader)
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    tmp = OUTPUT_CSV + ".tmp"
    with open(tmp, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing + rows)
    os.replace(tmp, OUTPUT_CSV)


def main():
    p = argparse.ArgumentParser(description="RAM of already-loaded Qdrant collections (see the top of this file).")
    p.add_argument("collections", nargs="*", help="collections to measure (default: all)")
    p.add_argument("--restart", action="store_true",
                   help="docker restart qdrant and wait until it is ready, green and its RAM has settled first")
    p.add_argument("--drop-caches", action="store_true", help="with --restart: empty the board's file cache too")
    args = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)

    qb.log(f"Qdrant at {qb.QDRANT_URL}, container {qb.DB_CONTAINER}, storage {qb.QDRANT_STORAGE_DIR}")
    measured, emptied = "as found, no restart", False
    if args.restart:
        qb.log(f"Restarting: docker restart {qb.DB_CONTAINER}")
        qb.run_on_server(f"docker restart -t 120 {qb.DB_CONTAINER} 2>&1\n", timeout=300)
        if args.drop_caches:
            emptied = drop_caches()
        qb.wait_for_qdrant()
    elif not qb.qdrant_ready():
        sys.exit(f"Qdrant is not ready at {qb.QDRANT_URL}")

    all_collections = qb.list_collections()
    if all_collections is None:
        sys.exit(f"Could not list Qdrant's collections at {qb.QDRANT_URL}")
    names = args.collections or all_collections
    missing = [n for n in names if n not in all_collections]
    if missing:
        sys.exit(f"Not in Qdrant: {', '.join(missing)} (collections: {', '.join(all_collections) or 'none'})")
    if not names:
        sys.exit("Nothing to measure: Qdrant has no collections")
    if len(all_collections) > 1:
        qb.log(f"[NOTE] Qdrant has {len(all_collections)} collections ({', '.join(all_collections)}) and keeps all "
               "of them open, so the qdrant-process and board figures cover all of them together.")

    if args.restart:
        for name in all_collections:          # every open collection affects the process RAM
            qb.COLLECTION_NAME = name
            qb.wait_for_green()
        qb.log("Waiting for Qdrant's RAM to settle")
        qb.wait_for_ram_to_settle()
        measured = "after restart, before any search" + (", file cache emptied" if emptied else "")

    per_collection = []
    for name in names:
        qb.COLLECTION_NAME = name
        info, note = qb.collection_info()
        datatype, on_disk, quant = vector_config(info)
        dir_h, dir_gib, dir_path = qb.data_dir_size()
        per_collection.append({
            "collection": name,
            "points": (info or {}).get("points_count"),
            "status": (info or {}).get("status", note),
            "indexed_vectors": (info or {}).get("indexed_vectors_count"),
            "segments": (info or {}).get("segments_count"),
            "datatype": datatype, "on_disk": on_disk, "quantization": quant,
            "data_dir_size": dir_h, "data_dir_gib": dir_gib, "data_dir_path": dir_path,
        })

    ram = qb.measure_ram()
    measured_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'collection':<34}{'points':>12}{'status':>8}{'datatype':>10}{'on disk':>9}{'quant':>8}{'data dir':>10}")
    for c in per_collection:
        points = f"{c['points']:,}" if isinstance(c["points"], int) else "?"
        print(f"{c['collection']:<34}{points:>12}{str(c['status']):>8}{str(c['datatype']):>10}"
              f"{str(c['on_disk']):>9}{str(c['quantization']):>8}{str(c['data_dir_size']):>10}")
    print("status green = indexing finished; data dir = du -sh of collections/<name> in the storage folder.")
    print("\n" + qb.describe_ram(ram))
    print("\nFor the summary table (" + measured + "):")
    print(f"  Process RES / private / shared (GiB): {qb.ram_value(ram, 'node', 'res')} / "
          f"{qb.ram_value(ram, 'node', 'private')} / {qb.ram_value(ram, 'node', 'shared')}")
    print(f"  Board used / available (GiB):         {qb.ram_value(ram, None, 'sys_used')} / "
          f"{qb.ram_value(ram, None, 'sys_available')}   (swap used {qb.ram_value(ram, None, 'sys_swap_used')})")
    print("  Collection RAM: n/a (Qdrant has no per-collection memory API)")

    rows = [{"measured_at": measured_at, "measured": measured, **c, "collections_in_qdrant": len(all_collections),
             **qb.ram_columns("now", ram)} for c in per_collection]
    append_csv(rows)
    print(f"\nSaved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()