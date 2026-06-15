import time
import os
import pandas as pd
from endee import Endee
from tqdm import tqdm

# ==========================================
# USER CONFIGURATION
# ==========================================
CONFIG = {
    "TEMP_SAVE_FILE":  "./deleted_vectors_temp.parquet",
    "COLLECTION_NAME": "1M_int16d_m16_efcon128_1",
    "TOKEN":           None,
    "BASE_URL":        "http://localhost:8080/api/v2",
    "BATCH_SIZE":      1000
}
# ==========================================

def run_upsert():
    print(f"Connecting to Endee at {CONFIG['BASE_URL']}...")
    try:
        token = CONFIG["TOKEN"] if CONFIG["TOKEN"] != "TOKEN" else None
        client = Endee(token=token)
        client.set_base_url(CONFIG["BASE_URL"])
        collection = client.get_collection(name=CONFIG["COLLECTION_NAME"])
        print(f"Connected to Collection: {CONFIG['COLLECTION_NAME']}")
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    if not os.path.exists(CONFIG["TEMP_SAVE_FILE"]):
        print(f"Saved vectors not found! Run the deletion script first.")
        return

    print(f"Reading deleted vectors from: {CONFIG['TEMP_SAVE_FILE']}")
    df = pd.read_parquet(CONFIG["TEMP_SAVE_FILE"])

    vector_col = 'emb' if 'emb' in df.columns else 'vector'

    print(f"Formatting {len(df)} rows...")

    # --- 1. Format for v2 API ---
    records = []
    for _, row in df.iterrows():
        vec_id = str(row['id'])
        records.append({
            "id": vec_id,
            "fields": {"dense": row[vector_col].tolist()},
            "meta": {"id": vec_id}
        })

    # --- 2. Chunk into batches ---
    batches = [records[i:i + CONFIG["BATCH_SIZE"]] for i in range(0, len(records), CONFIG["BATCH_SIZE"])]
    print(f"Created {len(batches)} batches of {CONFIG['BATCH_SIZE']} vectors.")

    # --- 3. Sequential Upserting ---
    start_time = time.time()
    print("Upserting serially...")

    success_count, fail_count = 0, 0

    for batch in tqdm(batches, desc="Upserting Batches", unit="batch"):
        try:
            collection.upsert(batch)
            success_count += len(batch)
        except Exception:
            fail_count += 1

    duration = time.time() - start_time
    print(f"\nUpsert Complete in {duration:.2f}s")
    print(f"   API Reported Upserted Vectors: {success_count}")
    if fail_count > 0:
        print(f"   API Failed Batches: {fail_count}")

    # Note: get_vector is not available in the v2 API.
    # Use collection.describe() to check overall collection stats if needed.

    # --- CLEANUP ---
    if fail_count == 0 and os.path.exists(CONFIG["TEMP_SAVE_FILE"]):
        os.remove(CONFIG["TEMP_SAVE_FILE"])
        print(f"Cleanup: Successfully deleted temporary file '{CONFIG['TEMP_SAVE_FILE']}'")
    elif fail_count > 0:
        print(f"Cleanup Skipped: Temp file kept because there were failed batches.")

if __name__ == "__main__":
    run_upsert()
