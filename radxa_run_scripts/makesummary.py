#!/usr/bin/env python3
"""
Rebuilds the Excel summary of a run from ram_benchmark_results.csv, e.g. for a run that finished while
openpyxl was not installed. Same layout as run_benchmark_with_ram_and_temp.py writes (which must be in
the same folder): top block + Run Count table with SUDO REBOOT rows, and the per-search details sheet.

  python make_summary_xlsx.py --list                          labels in the CSV
  python make_summary_xlsx.py 2M_k100_search-only_run1        rebuild one run (several labels allowed)
"""
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run_benchmark_with_ram_and_temp_with_sudo_reboot as rb  # noqa: E402


def num(value):
    """CSV text -> int / float where it is a number, else the text (None for empty)."""
    if value in (None, ""):
        return None
    try:
        f = float(value)
    except ValueError:
        return value
    return int(f) if f.is_integer() and "." not in value else f


def load_rows():
    if not os.path.exists(rb.OUTPUT_CSV):
        sys.exit(f"No CSV at {rb.OUTPUT_CSV}")
    with open(rb.OUTPUT_CSV, newline="") as f:
        return list(csv.DictReader(f))


def rebuild(label, rows):
    rows = [r for r in rows if r.get("task_label") == label]
    if not rows:
        print(f"[SKIP] no rows with task_label {label} in {rb.OUTPUT_CSV}")
        return
    rows.sort(key=lambda r: num(r.get("search_overall")) or num(r.get("search_run")) or 0)
    first = rows[0]
    rb.COLLECTION_NAME = first.get("collection")

    measured = first.get("summary_ram_measured") or ""
    stage = "after_load" if measured.startswith("after load") else "after_search"
    ram_gib = {
        "res": num(first.get(f"endee_res_{stage}_gib")),
        "private": num(first.get(f"endee_private_{stage}_gib")),
        "shared": num(first.get(f"endee_shared_mdbx_{stage}_gib")),
        "sys_used": num(first.get(f"board_used_{stage}_gib")),
        "sys_available": num(first.get(f"board_available_{stage}_gib")),
    }
    header_rows = rb.summary_header_rows(
        num(first.get("load_duration_s")), first.get("data_dir_size") or None, num(first.get("data_dir_gib")),
        num(first.get("collection_ram_gib")), ram_gib, measured or None)

    results = []
    for n, r in enumerate(rows, start=1):
        results.append({
            "run_count": num(r.get("search_overall")) or n,
            "round": num(r.get("round")) or 1,
            "recall": num(r.get("recall")), "ndcg": num(r.get("ndcg")), "qps": num(r.get("qps")),
            "serial_p99": num(r.get("serial_latency_p99")), "serial_p95": num(r.get("serial_latency_p95")),
            "conc_p99": num(r.get("conc_latency_p99")), "conc_p95": num(r.get("conc_latency_p95")),
            "temp_start": num(r.get("temp_search_start_c")), "temp_max": num(r.get("search_max_temp_c")),
            "min_big_mhz": num(r.get("search_min_bigcore_mhz")), "governor": r.get("governor_search_start"),
            "res_after": num(r.get("endee_res_after_search_gib")),
            "private_after": num(r.get("endee_private_after_search_gib")),
            "shared_after": num(r.get("endee_shared_mdbx_after_search_gib")),
            "available_after": num(r.get("board_available_after_search_gib")),
        })

    size = num(first.get("size"))
    title = (f"{first.get('collection')} ({size:,} x {first.get('dim')}D, {first.get('precision')}) | "
             f"k={first.get('k')}, ef_search={first.get('ef_search')}, M={first.get('m')}, "
             f"ef_con={first.get('ef_con')}, concurrency={first.get('concurrency')} | {first.get('mode')} | "
             f"{label} | {first.get('started')}")
    path = os.path.join(rb.SUMMARY_XLSX_DIR, f"{label}.xlsx")
    if rb.write_summary_xlsx(path, title, header_rows, results):
        print(f"[OK] {path}  ({len(results)} searches)")


def main():
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    rows = load_rows()
    if args == ["--list"]:
        counts = {}
        for r in rows:
            counts[r.get("task_label")] = counts.get(r.get("task_label"), 0) + 1
        for label, count in counts.items():
            print(f"{label}  ({count} searches)")
        return
    for label in args:
        rebuild(label, rows)


if __name__ == "__main__":
    main()