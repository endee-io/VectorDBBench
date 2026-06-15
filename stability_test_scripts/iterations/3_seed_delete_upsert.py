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

# python3 -u stability_test_scripts/iterations/3_seed_delete_upsert.py 2>&1 | tee -a logs/log3_1M_stability.log

# ==========================================
# USER CONFIGURATION
# ==========================================
CONFIG = {
    # --- Global Settings ---
    "NUM_ITERATIONS":  3,
    "DISABLE_TQDM":    True,  # SET TO TRUE if piping to a .log file!
    "TOKEN":           "TOKEN",
    "BASE_URL":        "http://localhost:8080/api/v2",
    "COLLECTION_NAME": "1M_int16_1",
    "PRECISION":       "int16",
    "SEED":            42,

    # --- Deletion/Upsert Settings ---
    "PARQUET_PATH":   "/home/admin/vectordataset/cohere/cohere_medium_1m/shuffle_train.parquet",
    "DELETE_COUNT":   500000,
    "BATCH_SIZE":     1000,

    # --- Benchmark Settings ---
    "DATASET_LOCAL_DIR": "/home/admin/vectordataset",
    "M":                 16,
    "EF_CON":            128,
    "EF_SEARCH":         128,
    "SPACE_TYPE":        "cosine",
    "CASE_TYPE":         "Performance768D1M",
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


def prepare_deletion_dataset():
    """Generates the random subset ONCE using a fixed seed, streaming directly to disk."""
    if os.path.exists(TEMP_SAVE_FILE):
        print(f"[*] Found existing temp file '{TEMP_SAVE_FILE}'. Reusing for fixed iterations.")
        return True

    print(f"[*] Generating fixed random deletion dataset (Seed: {CONFIG['SEED']})...")

    path = CONFIG["PARQUET_PATH"]
    files_to_read = [path] if os.path.isfile(path) else sorted(glob.glob(os.path.join(path, "shuffle_train*.parquet")))

    if not files_to_read:
        print("No valid parquet files found.")
        return False

    total_rows = 0
    file_metadata = []
    for f in files_to_read:
        pf = pq.ParquetFile(f)
        total_rows += pf.metadata.num_rows
        file_metadata.append((f, pf.metadata.num_rows))

    print(f"[*] Found {total_rows} total vectors across files.")

    if CONFIG["DELETE_COUNT"] > total_rows:
        print(f"Cannot delete {CONFIG['DELETE_COUNT']} vectors from a dataset of {total_rows}.")
        return False

    random.seed(CONFIG["SEED"])
    random_indices = set(random.sample(range(total_rows), CONFIG["DELETE_COUNT"]))

    print(f"[*] Streaming chunks to extract true random sample directly to disk...")
    current_offset = 0
    writer = None
    vectors_saved = 0

    try:
        for f, _ in file_metadata:
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=5000):
                chunk_length = len(batch)
                chunk_start = current_offset
                chunk_end = current_offset + chunk_length

                needed_in_chunk = [idx - chunk_start for idx in random_indices if chunk_start <= idx < chunk_end]

                if needed_in_chunk:
                    df_chunk = batch.to_pandas()
                    extracted_df = df_chunk.iloc[needed_in_chunk]
                    table = pa.Table.from_pandas(extracted_df)

                    if writer is None:
                        writer = pq.ParquetWriter(TEMP_SAVE_FILE, table.schema)

                    writer.write_table(table)
                    vectors_saved += len(extracted_df)

                current_offset += chunk_length

                if current_offset % 100000 == 0:
                    print(f"  -> Scanned {current_offset} vectors... (Extracted: {vectors_saved})")

    finally:
        if writer is not None:
            writer.close()

    print(f"[*] Saved {vectors_saved} fixed random vectors to {TEMP_SAVE_FILE}")
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

    df_to_delete = pd.read_parquet(TEMP_SAVE_FILE, columns=['id'])
    ids_to_delete = df_to_delete['id'].astype(str).tolist()

    print(f"[{iteration}] Deleting {len(ids_to_delete)} exact same vectors serially...")

    start_time = time.time()
    success, fail = 0, 0
    for vec_id in tqdm(ids_to_delete, desc=f"It {iteration} - Deleting", unit="del", disable=CONFIG["DISABLE_TQDM"]):
        try:
            collection.delete_object(str(vec_id))
            success += 1
        except Exception:
            fail += 1

    print(f"\n[{iteration}] Deletion Complete in {time.time() - start_time:.2f}s (Success: {success}, Fail: {fail})")
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

    if not prepare_deletion_dataset():
        print("Failed to prepare deletion dataset. Exiting.")
        return

    for i in range(1, CONFIG["NUM_ITERATIONS"] + 1):
        print(f"\n{'='*50}")
        print(f"ITERATION {i} OF {CONFIG['NUM_ITERATIONS']}")
        print(f"{'='*50}")

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
