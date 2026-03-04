import time
import os
import pandas as pd
from endee import Endee
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# ⚙️ USER CONFIGURATION
# ==========================================
CONFIG = {
    "TEMP_SAVE_FILE": "./deleted_vectors_temp.parquet", 
    "INDEX_NAME":     "1M_int16d_m16_efcon128_1",
    "TOKEN":          None,  
    "BASE_URL":       "http://57.129.55.56:8050/api/v1",
    "CONCURRENCY":    10, 
    "BATCH_SIZE":     1000  
}
# ==========================================

def upsert_worker(index, batch):
    try:
        index.upsert(batch)
        return True, len(batch)
    except Exception as e:
        return False, str(e)

def run_upsert():
    print(f"🔹 Connecting to Endee at {CONFIG['BASE_URL']}...")
    try:
        token = CONFIG["TOKEN"] if CONFIG["TOKEN"] != "TOKEN" else None
        client = Endee(token=token)
        client.set_base_url(CONFIG["BASE_URL"])
        index = client.get_index(name=CONFIG["INDEX_NAME"])
        print(f"✅ Connected to Index: {CONFIG['INDEX_NAME']}")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    if not os.path.exists(CONFIG["TEMP_SAVE_FILE"]):
        print(f"❌ Saved vectors not found! Run the deletion script first.")
        return

    print(f"📂 Reading deleted vectors from: {CONFIG['TEMP_SAVE_FILE']}")
    df = pd.read_parquet(CONFIG["TEMP_SAVE_FILE"])
    
    vector_col = 'emb' if 'emb' in df.columns else 'vector' 

    print(f"🔄 Formatting {len(df)} rows to exactly match benchmark structure...")
    
    # --- 1. Format exactly like the DB benchmark ---
    records = []
    for _, row in df.iterrows():
        vec_id = str(row['id'])
        records.append({
            "id": vec_id,
            "vector": row[vector_col].tolist(), 
            "meta": {"id": vec_id}  # Perfectly matches your DB benchmark format
        })

    # --- 2. Chunk into batches ---
    batches = [records[i:i + CONFIG["BATCH_SIZE"]] for i in range(0, len(records), CONFIG["BATCH_SIZE"])]
    print(f"📦 Created {len(batches)} batches of {CONFIG['BATCH_SIZE']} vectors.")

    # --- 3. Multithreaded Upserting ---
    start_time = time.time()
    print(f"🚀 Upserting using {CONFIG['CONCURRENCY']} threads...")

    with ThreadPoolExecutor(max_workers=CONFIG["CONCURRENCY"]) as executor:
        futures = [executor.submit(upsert_worker, index, batch) for batch in batches]
        
        success_count, fail_count = 0, 0
        with tqdm(total=len(batches), unit="batch") as pbar:
            for future in as_completed(futures):
                success, result = future.result()
                if success:
                    success_count += result
                else:
                    fail_count += 1
                pbar.update(1)

    duration = time.time() - start_time
    print(f"\n✅ Upsert Complete in {duration:.2f}s")
    print(f"   Successfully Upserted Vectors: {success_count}")
    if fail_count > 0:
        print(f"   Failed Batches: {fail_count}")

    # --- CLEANUP CODE (REMOVE THIS IF THAT DELETED VECTORS ARE REQUIRED) ---
    if fail_count == 0 and os.path.exists(CONFIG["TEMP_SAVE_FILE"]):
        os.remove(CONFIG["TEMP_SAVE_FILE"])
        print(f"🗑️  Cleanup: Successfully deleted temporary file '{CONFIG['TEMP_SAVE_FILE']}'")
    elif fail_count > 0:
        print(f"⚠️  Cleanup Skipped: Temp file kept because there were failed batches.")

if __name__ == "__main__":
    run_upsert()