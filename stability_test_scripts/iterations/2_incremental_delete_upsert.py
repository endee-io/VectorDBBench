import time
import os
import glob
import random
import pandas as pd
import pyarrow.parquet as pq
import subprocess
from endee import Endee
from tqdm import tqdm

# python3 -u stability_test_scripts/iterations/2_incremental_delete_upsert.py 2>&1 | tee -a logs/log2_1M_int16_m16_efcon128.log

# ==========================================
# USER CONFIGURATION
# ==========================================
CONFIG = {
    # --- Global Settings ---
    "DISABLE_TQDM":   True,  # SET TO TRUE if piping to a .log file!
    "TOKEN":          "TOKEN",  
    "BASE_URL":       "http://localhost:8080/api/v1",
    "INDEX_NAME":     "1M_int16_m16_efcon128_1",
    "PRECISION":      "int16",
    
    # --- Deletion/Upsert Settings ---
    "PARQUET_PATH":   "/home/debian/ssd/vectordataset/cohere/cohere_medium_1m/shuffle_train.parquet", 
    "TOTAL_VECTORS":  1000000,  # Used to calculate 10%, 20% etc.
    "BATCH_SIZE":     1000,
    
    # --- Benchmark Settings ---
    "DATASET_LOCAL_DIR": "/home/debian/ssd/vectordataset",
    "M":                 16,
    "EF_CON":            128,
    "EF_SEARCH":         128,
    "SPACE_TYPE":        "cosine",
    "VERSION":           1,
    "CASE_TYPE":         "Performance768D1M",
    "K":                 30,
    "CONCURRENCY":       "16",
    "CONCURRENCY_DUR":   30
}

TEMP_SAVE_FILE = f"./deleted_{CONFIG['INDEX_NAME']}_temp.parquet"
# ==========================================

def get_endee_index():
    token = CONFIG["TOKEN"] if CONFIG["TOKEN"] != "TOKEN" else None
    client = Endee(token=token)
    client.set_base_url(CONFIG["BASE_URL"])
    return client.get_index(name=CONFIG["INDEX_NAME"])


def run_delete(step_label):
    print(f"\n[{step_label}] Connecting to Endee for Deletion...")
    try:
        index = get_endee_index()
    except Exception as e:
        print(f"Connection Failed: {e}")
        return False

    path = CONFIG["PARQUET_PATH"]
    files_to_read = [path] if os.path.isfile(path) else sorted(glob.glob(os.path.join(path, "shuffle_train*.parquet")))
    
    if not files_to_read:
        print("No valid parquet files found.")
        return False

    print(f"[{step_label}] Calculating total rows via Parquet Metadata...")
    
    # 1. Find total rows without loading data
    total_rows = 0
    file_metadata = []
    for f in files_to_read:
        pf = pq.ParquetFile(f)
        total_rows += pf.metadata.num_rows
        file_metadata.append((f, pf.metadata.num_rows))
        
    print(f"[{step_label}] Found {total_rows} total vectors across files.")
    
    if CONFIG["DELETE_COUNT"] > total_rows:
        print(f"Cannot delete {CONFIG['DELETE_COUNT']} vectors from a dataset of {total_rows}.")
        return False

    # 2. Pick TRUE RANDOM indices globally
    print(f"[{step_label}] Generating {CONFIG['DELETE_COUNT']} true random indices globally...")
    random_indices = set(random.sample(range(total_rows), CONFIG["DELETE_COUNT"]))

    # 3. Stream and extract ONLY the winning indices
    print(f"[{step_label}] Streaming chunks to extract true random sample...")
    collected_dfs = []
    current_offset = 0

    for f, _ in file_metadata:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(batch_size=10000):
            chunk_length = len(batch)
            chunk_start = current_offset
            chunk_end = current_offset + chunk_length
            
            # Find which random lottery numbers fall inside this specific chunk
            needed_in_chunk = [idx - chunk_start for idx in random_indices if chunk_start <= idx < chunk_end]
            
            if needed_in_chunk:
                df_chunk = batch.to_pandas()
                # Pluck only the exact rows we randomly chose
                collected_dfs.append(df_chunk.iloc[needed_in_chunk])
                
            current_offset += chunk_length

    # Combine the extracted rows
    df_to_delete = pd.concat(collected_dfs, ignore_index=True)
    
    df_to_delete.to_parquet(TEMP_SAVE_FILE)
    print(f"[{step_label}] Saved {len(df_to_delete)} true random vectors to {TEMP_SAVE_FILE}")

    ids_to_delete = df_to_delete['id'].astype(str).tolist()
    print(f"[{step_label}] Deleting {len(ids_to_delete)} vectors serially...")

    start_time = time.time()
    success, fail = 0, 0
    for vec_id in tqdm(ids_to_delete, desc=f"{step_label} - Deleting", unit="del", disable=CONFIG["DISABLE_TQDM"]):
        try:
            index.delete_vector(str(vec_id))
            success += 1
        except Exception:
            fail += 1

    print(f"\n[{step_label}] Deletion API Calls Complete in {time.time() - start_time:.2f}s (Success: {success}, Fail: {fail})")

    # print(f"[{step_label}] Verifying deletions...")
    # present_count, deleted_count = 0, 0
    # start_verify = time.time()
    # for vec_id in tqdm(ids_to_delete, desc=f"{step_label} - Verifying Del", unit="chk", disable=CONFIG["DISABLE_TQDM"]):
    #     try:
    #         if index.get_vector(str(vec_id)):
    #             present_count += 1
    #         else:
    #             deleted_count += 1
    #     except Exception:
    #         deleted_count += 1

    # print(f"[{step_label}] Verify Complete in {time.time() - start_verify:.2f}s (Deleted: {deleted_count}, Still Present: {present_count})")
    return True


def run_upsert(step_label):
    print(f"\n[{step_label}] Connecting to Endee for Upsertion...")
    try:
        index = get_endee_index()
    except Exception as e:
        print(f"Connection Failed: {e}")
        return False

    if not os.path.exists(TEMP_SAVE_FILE):
        return False

    print(f"[{step_label}] Streaming deleted vectors from: {TEMP_SAVE_FILE}")
    
    pf = pq.ParquetFile(TEMP_SAVE_FILE)
    
    start_time = time.time()
    success_count, fail_count = 0, 0
    ids_to_verify = []

    for batch in tqdm(pf.iter_batches(batch_size=CONFIG["BATCH_SIZE"]), desc=f"{step_label} - Upserting", disable=CONFIG["DISABLE_TQDM"]):
        df_chunk = batch.to_pandas()
        vector_col = 'emb' if 'emb' in df_chunk.columns else 'vector'
        
        records = []
        for _, row in df_chunk.iterrows():
            vec_id = str(row['id'])
            ids_to_verify.append(vec_id)
            records.append({
                "id": vec_id,
                "vector": row[vector_col].tolist(), 
                "meta": {"id": vec_id}  
            })
            
        try:
            index.upsert(records)
            success_count += len(records)
        except Exception:
            fail_count += 1

    print(f"\n[{step_label}] Upsert Complete in {time.time() - start_time:.2f}s (Upserted: {success_count}, Failed Batches: {fail_count})")

    # print(f"[{step_label}] Verifying upserts...")
    # present_count, missing_count = 0, 0
    # start_verify = time.time()
    # for vec_id in tqdm(ids_to_verify, desc=f"{step_label} - Verifying Up", unit="chk", disable=CONFIG["DISABLE_TQDM"]):
    #     try:
    #         if index.get_vector(str(vec_id)):
    #             present_count += 1
    #         else:
    #             missing_count += 1
    #     except Exception:
    #         missing_count += 1

    # print(f"[{step_label}] Verify Complete in {time.time() - start_verify:.2f}s (Confirmed: {present_count}, Missing: {missing_count})")

    # Cleanup: Only check if file exists since verification is skipped
    if os.path.exists(TEMP_SAVE_FILE):
        os.remove(TEMP_SAVE_FILE)
        print(f"[{step_label}] Cleanup: Deleted temp file '{TEMP_SAVE_FILE}'")
    else:
        print(f"[{step_label}] Cleanup Skipped: Temp file not found.")
        
    return True


def run_benchmark(step_label, phase):
    print(f"\n[{step_label}] Starting VectorDBBench Benchmark ({phase})...")
    
    token_arg = f'--token "{CONFIG["TOKEN"]}"' if CONFIG["TOKEN"] else ''
    task_label = f"{CONFIG['INDEX_NAME']}_{step_label}_{phase}"
    
    bash_cmd = f"""
    NUM_PER_BATCH=10000 DATASET_LOCAL_DIR="{CONFIG['DATASET_LOCAL_DIR']}" \\
    vectordbbench endee \\
      {token_arg} \\
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
      --search-serial
    """
    
    try:
        process = subprocess.run(bash_cmd, shell=True, check=True)
        print(f"[{step_label}] Benchmark ({phase}) completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[{step_label}] Benchmark ({phase}) failed with exit code {e.returncode}.")


def main():
    print(f"Starting Endee Incremental Stability Test.")
    print(f"Target Index: {CONFIG['INDEX_NAME']}\n")
    
    # Define the percentage steps: 10, 20, 30 ... 100
    percentages = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    
    for pct in percentages:
        # Update the global CONFIG for the current percentage
        current_delete_count = int(CONFIG["TOTAL_VECTORS"] * (pct / 100))
        CONFIG["DELETE_COUNT"] = current_delete_count
        
        step_label = f"{pct}pct"
        
        print(f"\n{'#'*60}")
        print(f"### RUNNING {pct}% INCREMENT ({current_delete_count} vectors) ###")
        print(f"{'#'*60}")
        
        # 1. Delete the current percentage
        del_success = run_delete(step_label)
        if not del_success:
            print(f"Stopping at {pct}% due to deletion failure.")
            break
        
        # 2. Benchmark after Deletion
        run_benchmark(step_label, phase="post_delete")
        
        # 3. Upsert back the deleted vectors
        up_success = run_upsert(step_label)
        if not up_success:
            print(f"Stopping at {pct}% due to upsert failure.")
            break
        
        # 4. Benchmark after Upsert
        run_benchmark(step_label, phase="post_upsert")
        
        print(f"\n{pct}% Increment complete. Quick rest for 5s...")
        time.sleep(5)

    print("\nALL INCREMENTAL TESTS COMPLETE!")

if __name__ == "__main__":
    main()
