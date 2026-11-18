#!/usr/bin/env python3
"""
Measures the RAM Vespa uses for data that is already loaded (no feed, no benchmark run): the Vespa
equivalent of measure_collection_ram.py for Endee.

  python measure_vespa_ram.py                          every document type as found now (no restart)
  python measure_vespa_ram.py --restart                docker restart vespa, wait until it is up and its RAM
                                                       has settled, then measure ("after restart, before any
                                                       search": the same idea as Endee's restart + object fetch)
  python measure_vespa_ram.py --restart --drop-caches  also empty the board's file cache first, so Board
                                                       used/available start from a clean state
  python measure_vespa_ram.py VectorDBBench768D2MBf16  only these document types

Unlike Endee, Vespa loads all of its data when it starts and keeps it in memory, so no fetch is needed after
a restart. Vespa has one schema deployed at a time; by default the script measures the deployed one (read from
the config server) and ignores data folders left over from schemas that were replaced. It also means the process figures always include every document type deployed in Vespa: for
per-collection process numbers, have only one collection deployed (the script warns if it finds several).

Per document type (from Vespa):
  documents       number of documents (hits=0 query)
  collection RAM  Vespa's memory metric for the document type (VESPA_COLLECTION_RAM_METRICS in the benchmark
                  script; see its --check if this reads empty)
  data dir        du of the document type's folder in the Vespa data dir
Whole Vespa (from /proc of the container's processes, same as the benchmark script):
  content node    vespa-proton-bin: RES, private (RES - SHR), shared/file-backed, swap (VmSwap, not part
                  of RES) and private + swap (the memory that cannot be dropped)
  all Vespa       sum over every process in the container (incl. the Java processes)
Board (free): used (= total - available), available, buff/cache, swap used.

Uses the settings and code of run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot.py (same folder):
SERVER_SSH, VESPA_URI/PORT, VESPA_CONTAINER, VESPA_DATA_DIR, the metric names. Results are added to
OUTPUT_CSV, one row per document type.
"""
import argparse
import csv
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run_vespa_benchmark_with_ram_and_temp_with_sudo_reboot as vb  # noqa: E402

OUTPUT_CSV = os.path.join(HERE, "vespa_collection_ram_results.csv")


def list_doc_types():
    """Document types that have a folder in the Vespa data dir."""
    out = vb.run_on_server(
        f"find {vb.VESPA_DATA_DIR}/db/vespa/search -maxdepth 6 -mindepth 1 -type d -path '*/documents/*' "
        "-prune 2>/dev/null | xargs -r -n1 basename | sort -u\n", timeout=120)
    return [line.strip() for line in out.splitlines() if line.strip()]


def drop_caches():
    try:
        vb.run_on_server("sync\necho 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null\n", timeout=120)
        vb.log("   board file cache emptied")
        return True
    except Exception as e:  # noqa: BLE001
        vb.log("   [WARN] could not empty the file cache (needs passwordless sudo for tee). Run it yourself on "
               "the server first: sync; echo 3 | sudo tee /proc/sys/vm/drop_caches")
        vb.log(f"   ({str(e).splitlines()[0]})")
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
    p = argparse.ArgumentParser(description="RAM of already-loaded Vespa data (see the top of this file).")
    p.add_argument("doc_types", nargs="*", help="document types to measure (default: all found)")
    p.add_argument("--restart", action="store_true",
                   help="docker restart vespa and wait until it is up and its RAM has settled first")
    p.add_argument("--drop-caches", action="store_true", help="with --restart: empty the board's file cache too")
    args = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)

    vb.log(f"Vespa at {vb.VESPA_URI}:{vb.VESPA_PORT}, container {vb.VESPA_CONTAINER}, data {vb.VESPA_DATA_DIR}")
    measured = "as found, no restart"
    emptied = False
    if args.restart:
        vb.log(f"Restarting: docker restart {vb.VESPA_CONTAINER}")
        vb.run_on_server(f"docker restart -t 120 {vb.VESPA_CONTAINER} 2>&1\n", timeout=300)
        if args.drop_caches:
            emptied = drop_caches()
        vb.wait_for_vespa()
        vb.log("Waiting for Vespa's RAM to settle")
        vb.wait_for_ram_to_settle()
        measured = "after restart, before any search" + (", file cache emptied" if emptied else "")
    elif not (vb.config_server_ready() and vb.vespa_ready()):
        sys.exit(f"Vespa is not up at {vb.VESPA_URI} (ports {vb.VESPA_CONFIG_PORT} and {vb.VESPA_PORT})")

    deployed = vb.deployed_schemas()
    folders = list_doc_types()
    if deployed is None:
        vb.log("[WARN] could not read the deployed schema from the config server; using the data folders")
        doc_types = args.doc_types or folders
    else:
        vb.log(f"Deployed schema(s): {', '.join(deployed) or 'none'}")
        leftovers = [f for f in folders if f.lower() not in {d.lower() for d in deployed}]
        if leftovers:
            vb.log(f"[NOTE] data folders of schemas that are no longer deployed (not measured): {', '.join(leftovers)}")
        doc_types = args.doc_types or deployed
        not_deployed = [d for d in doc_types if d.lower() not in {x.lower() for x in deployed}]
        if not_deployed:
            sys.exit(f"Not the deployed schema: {', '.join(not_deployed)} (deployed: {', '.join(deployed) or 'none'})")
    if not doc_types:
        sys.exit("Nothing to measure: no schema is deployed in Vespa")
    if len(doc_types) > 1:
        vb.log(f"[NOTE] {len(doc_types)} document types found ({', '.join(doc_types)}). Vespa keeps all of them "
               f"in memory, so the content-node / all-Vespa / board figures cover all of them together.")

    try:
        metrics = vb.fetch_metrics()
    except Exception as e:  # noqa: BLE001
        vb.log(f"[WARN] could not read Vespa's metrics ({e}); collection RAM will be empty")
        metrics = None

    per_type = []
    for doc_type in doc_types:
        vb.DOC_TYPE = doc_type
        count, note = vb.vespa_document_count()
        coll = vb.collection_ram_from_metrics(metrics) if metrics is not None else None
        dir_h, dir_gib, dir_path = vb.data_dir_size()
        per_type.append({"doc_type": doc_type, "documents": count, "count_note": note,
                         "collection_ram_gib": vb.gib(coll), "data_dir_size": dir_h, "data_dir_gib": dir_gib,
                         "data_dir_path": dir_path})

    ram = vb.measure_ram()
    measured_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'document type':<34}{'documents':>12}{'coll. RAM':>11}{'data dir':>10}   (GiB)")
    for t in per_type:
        docs = f"{t['documents']:,}" if isinstance(t["documents"], int) else "?"
        coll = "?" if t["collection_ram_gib"] is None else f"{t['collection_ram_gib']:.3f}"
        print(f"{t['doc_type']:<34}{docs:>12}{coll:>11}{str(t['data_dir_size']):>10}")
    print("coll. RAM = Vespa's memory metric for the document type; data dir = du -sh of its folder.")
    print("\n" + vb.describe_ram(ram))
    print("\nFor the summary table (" + measured + "):")
    print(f"  Content node RES / private / shared (GiB): {vb.ram_value(ram, 'node', 'res')} / "
          f"{vb.ram_value(ram, 'node', 'private')} / {vb.ram_value(ram, 'node', 'shared')}")
    print(f"  Content node swap / private + swap (GiB):  {vb.ram_value(ram, 'node', 'swap')} / "
          f"{vb.ram_value(ram, 'node', 'private_plus_swap')}   (VmSwap is not counted in RES)")
    print(f"  All Vespa processes RES / private / swap (GiB): {vb.ram_value(ram, 'total', 'res')} / "
          f"{vb.ram_value(ram, 'total', 'private')} / {vb.ram_value(ram, 'total', 'swap')}")
    print(f"  Board used / available (GiB):              {vb.ram_value(ram, None, 'sys_used')} / "
          f"{vb.ram_value(ram, None, 'sys_available')}   (swap used {vb.ram_value(ram, None, 'sys_swap_used')})")

    rows = []
    for t in per_type:
        rows.append({"measured_at": measured_at, "measured": measured, **t,
                     "document_types_deployed": len(doc_types), **vb.ram_columns("now", ram)})
    append_csv(rows)
    print(f"\nSaved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()