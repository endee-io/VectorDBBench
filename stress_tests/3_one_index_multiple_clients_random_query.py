import os
import time
import subprocess

# ==========================================================
# Note:
# Index should already be created.
# This script only runs queries for performance testing.
# ==========================================================

# ==========================================================
# Run the following script before executing this file:
# python3 stress_tests/A_create_random_test_parquet_file.py
# ==========================================================

# ==========================================
# USER CONFIGURATION: CASE 3 (RANDOM QUERIES)
# ==========================================
CONFIG = {
    "NUM_CLIENTS":       10,    
    "INDEX_NAME":        "1M_int16_1", # All hitting the exact same index!
    
    "TOKEN":             "TOKEN",  
    "BASE_URL":          "http://localhost:8080/api/v1",
    
    # Notice this is now the BASE path of our newly generated client folders
    "BASE_DATASET_DIR":  "/home/admin/vectordataset_clients", 
    
    "M":                 16,
    "EF_CON":            128,
    "EF_SEARCH":         128,
    "SPACE_TYPE":        "cosine",
    "PRECISION":         "int16",
    "VERSION":           1,
    "CASE_TYPE":         "Performance768D1M",
    "K":                 30,
    "CONCURRENCY":       "1", 
    "CONCURRENCY_DUR":   30
}
# ==========================================

def run_stress_test():
    print(f"Starting True Cache-Busting Stress Test: {CONFIG['NUM_CLIENTS']} Clients -> 1 Index")
    
    os.makedirs("stress_logs", exist_ok=True)
    processes = []
    
    # Spawn all clients simultaneously
    for i in range(CONFIG["NUM_CLIENTS"]):
        client_id = f"client_{i+1}"
        log_file = f"stress_logs/{client_id}.log"
        task_label = f"stress_{CONFIG['INDEX_NAME']}_{client_id}"
        
        # CRITICAL CHANGE: Each client gets its own dataset folder!
        client_dataset_dir = f"{CONFIG['BASE_DATASET_DIR']}/{client_id}"
        
        bash_cmd = f"""
        NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="{client_dataset_dir}" \\
        vectordbbench endee \\
          --token "{CONFIG['TOKEN']}" \\
          --region location \\
          --base-url "{CONFIG['BASE_URL']}" \\
          --index-name "{CONFIG['INDEX_NAME']}" \\
          --task-label "{task_label}" \\
          --m {CONFIG['M']} \\
          --ef-con {CONFIG['EF_CON']} \\
          --ef-search {CONFIG['EF_SEARCH']} \\
          --space-type {CONFIG['SPACE_TYPE']} \\
          --precision {CONFIG['PRECISION']} \\
          --version {CONFIG['VERSION']} \\
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
        processes.append((client_id, p, log_file))
        print(f"Spawned {client_id} (Using unique queries from {client_dataset_dir})")

    print(f"\nAll {CONFIG['NUM_CLIENTS']} clients are actively hammering the server with unique queries!")
    print("Waiting for all tests to finish...")

    start_time = time.time()
    success_count = 0
    
    for client_id, p, log_file in processes:
        p.wait()  
        if p.returncode == 0:
            print(f"{client_id} completed successfully.")
            success_count += 1
        else:
            print(f"{client_id} FAILED (Exit Code {p.returncode}). Check {log_file}")

    print(f"\nStress Test Complete in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    run_stress_test()
