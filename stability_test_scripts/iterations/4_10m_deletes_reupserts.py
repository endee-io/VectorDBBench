import time
import os
import glob
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import subprocess
from endee import Endee
from tqdm import tqdm

# ==========================================
# USER CONFIGURATION
# ==========================================
CONFIG = {
    # --- Global Settings ---
    "NUM_ITERATIONS": 3,
    "DISABLE_TQDM":   False, # SET TO TRUE if piping to a .log file!
    "TOKEN":          "TOKEN",
    "BASE_URL":       "http://localhost:8080/api/v2",
    "COLLECTION_NAME": "10M_int16_1",
    "PRECISION":      "int16",

    # --- Seed Settings ---
    "USE_FIXED_SEED": False, # False = Pick new random dataset every iteration. True = Same vectors every iteration.
    "SEED":           42,    # Only used if USE_FIXED_SEED is True

    # --- Deletion/Upsert Settings ---
    "PARQUET_PATH":   "/home/admin/vectordataset/cohere/cohere_large_10m/",
    "DELETE_COUNT":   3000000,
    "BATCH_SIZE":     1000,

    # --- Benchmark Settings ---
    "DATASET_LOCAL_DIR": "/home/admin/vectordataset",
    "M":                 16,
    "EF_CON":            128,
    "EF_SEARCH":         128,
    "SPACE_TYPE":        "cosine",
    "CASE_TYPE":         "Performance768D10M",
    "K":                 30,
    "CONCURRENCY":       "16",
    "CONCURRENCY_DUR":   30
}

TEMP_SAVE_FILE = f"./deleted_{CONFIG['COLLECTION_NAME']}_temp.parquet"
# ==========================================

def get_endee_collection():
    token = CONFIG["TOKEN"] if CONFIG["TOKEN"] != "TOKEN" else None
    client = Endee(token=token)
    client.set_base_url(CONFIG["BASE_URL"])
    return client.get_collection(name=CONFIG["COLLECTION_NAME"])


def prepare_deletion_dataset(iteration):
    """Generates the random subset. Streams directly to disk for low RAM."""
    if CONFIG["USE_FIXED_SEED"] and os.path.exists(TEMP_SAVE_FILE):
        if iteration == 1:
            print(f"[*] Found existing temp file '{TEMP_SAVE_FILE}'. Reusing for fixed iterations.")
        return True
    elif os.path.exists(TEMP_SAVE_FILE):
        # Clean up previous iteration's temp file if dynamic seed is used
        os.remove(TEMP_SAVE_FILE)

    seed_type = f"Fixed Seed: {CONFIG['SEED']}" if CONFIG["USE_FIXED_SEED"] else "Dynamic Seed (Random per Iteration)"
    print(f"[*] Generating deletion dataset ({seed_type})...")

    path = CONFIG["PARQUET_PATH"]
    # Handle both direct file paths and directories with multiple chunks
    if os.path.isfile(path):
        files_to_read = [path]
    else:
        files_to_read = sorted(glob.glob(os.path.join(path, "shuffle_train*.parquet")))

    if not files_to_read:
        print("No valid parquet files found in PARQUET_PATH.")
        return False

    total_rows = 0
    file_metadata = []
    for f in files_to_read:
        pf = pq.ParquetFile(f)
        total_rows += pf.metadata.num_rows
        file_metadata.append((f, pf.metadata.num_rows))

    print(f"[*] Found {total_rows} total vectors across {len(files_to_read)} files.")

    if CONFIG["DELETE_COUNT"] > total_rows:
        print(f"Cannot delete {CONFIG['DELETE_COUNT']} vectors from a dataset of {total_rows}.")
        return False

    # Apply seeding logic
    if CONFIG["USE_FIXED_SEED"]:
        random.seed(CONFIG["SEED"])
    else:
        random.seed() # Uses current system time

    random_indices = set(random.sample(range(total_rows), CONFIG["DELETE_COUNT"]))

    print(f"[*] Streaming chunks to extract sample directly to disk...")
    current_offset = 0
    writer = None
    vectors_saved = 0

    try:
        for f, _ in file_metadata:
            pf = pq.ParquetFile(f)

            # Streaming in batches to keep RAM usage negligible
            for batch in pf.iter_batches(batch_size=10000):
                chunk_length = len(batch)
                chunk_start = current_offset

                # O(1) Set lookup: massive speedup for large datasets (e.g. 10M+)
                needed_in_chunk = [i for i in range(chunk_length) if (chunk_start + i) in random_indices]

                if needed_in_chunk:
                    df_chunk = batch.to_pandas()
                    extracted_df = df_chunk.iloc[needed_in_chunk]

                    # Convert straight to pyarrow table
                    table = pa.Table.from_pandas(extracted_df)

                    if writer is None:
                        writer = pq.ParquetWriter(TEMP_SAVE_FILE, table.schema)

                    # Flush immediately to disk
                    writer.write_table(table)
                    vectors_saved += len(extracted_df)

                current_offset += chunk_length

                if current_offset % 1000000 == 0:
                    print(f"  -> Scanned {current_offset} vectors... (Extracted: {vectors_saved})")

    finally:
        if writer is not None:
            writer.close()

    print(f"[*] Saved {vectors_saved} vectors to {TEMP_SAVE_FILE}")
    return True


def run_delete(iteration):
    print(f"\n[{iteration}] Connecting to Endee for Deletion...")
    try:
        collection = get_endee_collection()
    except Exception as e:
        print(f"Connection Failed: {e}")
        return False

    if not os.path.exists(TEMP_SAVE_FILE):
        print("Deletion dataset missing!")
        return False

    # Low RAM Deletion: Read from Parquet in chunks instead of loading all to a list
    pf = pq.ParquetFile(TEMP_SAVE_FILE)
    total_deletes = pf.metadata.num_rows

    print(f"[{iteration}] Deleting {total_deletes} vectors serially...")

    start_time = time.time()
    success, fail = 0, 0

    with tqdm(total=total_deletes, desc=f"It {iteration} - Deleting", disable=CONFIG["DISABLE_TQDM"]) as pbar:
        for batch in pf.iter_batches(columns=['id'], batch_size=CONFIG["BATCH_SIZE"]):
            ids_to_delete = batch['id'].to_pylist()

            for vec_id in ids_to_delete:
                try:
                    collection.delete_object(str(vec_id))
                    success += 1
                except Exception:
                    fail += 1

            pbar.update(len(ids_to_delete))

    print(f"\n[{iteration}] Deletion API Calls Complete in {time.time() - start_time:.2f}s (Success: {success}, Fail: {fail})")
    return True


def run_upsert(iteration):
    print(f"\n[{iteration}] Connecting to Endee for Upsertion...")
    try:
        collection = get_endee_collection()
    except Exception as e:
        print(f"Connection Failed: {e}")
        return False

    if not os.path.exists(TEMP_SAVE_FILE):
        return False

    print(f"[{iteration}] Streaming deleted vectors from: {TEMP_SAVE_FILE}")

    pf = pq.ParquetFile(TEMP_SAVE_FILE)
    start_time = time.time()
    success_count, fail_count = 0, 0

    for batch in tqdm(pf.iter_batches(batch_size=CONFIG["BATCH_SIZE"]), desc=f"It {iteration} - Upserting", disable=CONFIG["DISABLE_TQDM"]):
        df_chunk = batch.to_pandas()
        vector_col = 'emb' if 'emb' in df_chunk.columns else 'vector'

        records = []
        for _, row in df_chunk.iterrows():
            vec_id = str(row['id'])
            records.append({
                "id": vec_id,
                "fields": {"dense": row[vector_col].tolist()},
                "meta": {"id": vec_id}
            })

        try:
            collection.upsert(records)
            success_count += len(records)
        except Exception:
            fail_count += 1

    print(f"\n[{iteration}] Upsert Complete in {time.time() - start_time:.2f}s (Upserted: {success_count}, Failed Batches: {fail_count})")
    return True


def run_benchmark(iteration, phase):
    print(f"\n[{iteration}] Starting VectorDBBench Benchmark ({phase})...")

    token_arg = f'--token "{CONFIG["TOKEN"]}"' if CONFIG["TOKEN"] else ''
    task_label = f"{CONFIG['COLLECTION_NAME']}_iter_{iteration}_{phase}"

    bash_cmd = f"""
    NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="{CONFIG['DATASET_LOCAL_DIR']}" \\
    vectordbbench endee \\
      {token_arg} \\
      --region location \\
      --base-url "{CONFIG['BASE_URL']}" \\
      --collection-name "{CONFIG['COLLECTION_NAME']}" \\
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
      --search-serial
    """

    try:
        subprocess.run(bash_cmd, shell=True, check=True)
        print(f"[{iteration}] Benchmark ({phase}) completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[{iteration}] Benchmark ({phase}) failed with exit code {e.returncode}.")


def main():
    print(f"Starting Test Cycle. Total Iterations configured: {CONFIG['NUM_ITERATIONS']}")
    print(f"Target Collection: {CONFIG['COLLECTION_NAME']}\n")

    for i in range(1, CONFIG["NUM_ITERATIONS"] + 1):
        print(f"\n{'='*50}")
        print(f"ITERATION {i} OF {CONFIG['NUM_ITERATIONS']}")
        print(f"{'='*50}")

        # Prepare dataset is inside the loop so it can rotate vectors if USE_FIXED_SEED=False
        if not prepare_deletion_dataset(i):
            print(f"Failed to prepare deletion dataset for iteration {i}. Exiting.")
            return

        del_success = run_delete(i)
        if not del_success:
            print(f"Skipping remainder of iteration {i} due to deletion failure.")
            continue

        run_benchmark(i, phase="post_delete")

        up_success = run_upsert(i)
        if not up_success:
            print(f"Skipping remainder of iteration {i} due to upsert failure.")
            continue

        run_benchmark(i, phase="post_upsert")

        if i < CONFIG["NUM_ITERATIONS"]:
            print(f"\nIteration {i} complete. Waiting 5 seconds before next iteration...")
            time.sleep(5)

    if os.path.exists(TEMP_SAVE_FILE):
        os.remove(TEMP_SAVE_FILE)
        print(f"\n[*] Final Cleanup: Deleted temp file '{TEMP_SAVE_FILE}'")

    print("\nALL CONFIGURED ITERATIONS COMPLETE!")

if __name__ == "__main__":
    main()
