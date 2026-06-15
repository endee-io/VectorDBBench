import os
import time
import subprocess

# ==========================================================
# Note:
# Collections should already be created.
# (Create collection names like collection_prefix_1, collection_prefix_2..)
# This script only runs queries for performance testing.
# ==========================================================

# ==========================================================
# Run the following script before executing this file:
# python3 stress_tests/A_create_random_test_parquet_file.py
# ==========================================================

# ==========================================
# USER CONFIGURATION: CASE 4 (RANDOM QUERIES)
# ==========================================
CONFIG = {
    # --- SCALING PARAMETERS ---
    "NUM_CLIENTS":         10,
    "COLLECTION_PREFIX":   "1M_int16_",

    # --- BENCHMARK SETTINGS ---
    "TOKEN":             "TOKEN",
    "BASE_URL":          "http://localhost:8080/api/v2",

    # Set this to the base directory where your client folders live
    "BASE_DATASET_DIR":  "/home/admin/vectordataset_clients",

    "M":                 16,
    "EF_CON":            128,
    "EF_SEARCH":         128,
    "SPACE_TYPE":        "cosine",
    "PRECISION":         "int16",
    "CASE_TYPE":         "Performance768D1M",
    "K":                 30,
    "CONCURRENCY":       "1",
    "CONCURRENCY_DUR":   30
}
# ==========================================

def run_distributed_stress_test():
    num_clients = CONFIG["NUM_CLIENTS"]
    prefix = CONFIG["COLLECTION_PREFIX"]

    target_collections = [f"{prefix}{i}" for i in range(1, num_clients + 1)]

    print(f"Starting CASE 4 (Cache Busting): {num_clients} Clients -> {num_clients} Unique Collections")
    print(f"Targets: {target_collections}\n")

    os.makedirs("stress_logs", exist_ok=True)
    processes = []

    for i, collection_name in enumerate(target_collections):
        client_num = i + 1
        client_id = f"client_{client_num}"
        log_file = f"stress_logs/{client_id}_{collection_name}.log"
        task_label = f"stress_{collection_name}"

        # Each client gets its own dataset folder with unique queries
        client_dataset_dir = f"{CONFIG['BASE_DATASET_DIR']}/{client_id}"

        bash_cmd = f"""
        NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="{client_dataset_dir}" \\
        vectordbbench endee \\
          --token "{CONFIG['TOKEN']}" \\
          --region location \\
          --base-url "{CONFIG['BASE_URL']}" \\
          --collection-name "{collection_name}" \\
          --task-label "{task_label}" \\
          --m {CONFIG['M']} \\
          --ef-con {CONFIG['EF_CON']} \\
          --ef-search {CONFIG['EF_SEARCH']} \\
          --space-type {CONFIG['SPACE_TYPE']} \\
          --precision {CONFIG['PRECISION']} \\
          --case-type {CONFIG['CASE_TYPE']} \\
          --k {CONFIG['K']} \\
          --num-concurrency "{CONFIG['CONCURRENCY']}" \\
          --concurrency-duration {CONFIG['CONCURRENCY_DUR']} \\
          --concurrency-timeout 3600 \\
          --skip-drop-old \\
          --skip-load \\
          --search-concurrent \\
          --search-serial >> {log_file} 2>&1
        """

        p = subprocess.Popen(bash_cmd, shell=True)
        processes.append((client_id, collection_name, p, log_file))
        print(f"Spawned {client_id} targeting [{collection_name}] (Queries: {client_dataset_dir})")

    print(f"\nAll {num_clients} benchmarks launched simultaneously!")
    print("Waiting for all tests to finish...")

    start_time = time.time()
    success_count = 0

    for client_id, collection_name, p, log_file in processes:
        p.wait()
        if p.returncode == 0:
            print(f"{client_id} ({collection_name}) completed successfully.")
            success_count += 1
        else:
            print(f"{client_id} ({collection_name}) FAILED (Exit Code {p.returncode}). Check {log_file}")

    print(f"\nDistributed Stress Test Complete in {time.time() - start_time:.2f}s")
    print(f"Success Rate: {success_count}/{num_clients} collections survived.")

if __name__ == "__main__":
    run_distributed_stress_test()
