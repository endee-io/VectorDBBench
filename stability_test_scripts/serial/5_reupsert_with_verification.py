import time
import os
import pandas as pd
from endee import Endee
from tqdm import tqdm

# ==========================================
# ⚙️ USER CONFIGURATION
# ==========================================
CONFIG = {
    "TEMP_SAVE_FILE": "./deleted_vectors_temp.parquet", 
    "INDEX_NAME":     "1M_int16d_m16_efcon128_1",
    "TOKEN":          None,  
    "BASE_URL":       "http://57.129.55.56:8050/api/v1",
    "BATCH_SIZE":     1000  
}
# ==========================================

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
    ids_to_verify = [] # Keep a flat list of IDs for the verification step
    
    for _, row in df.iterrows():
        vec_id = str(row['id'])
        ids_to_verify.append(vec_id)
        records.append({
            "id": vec_id,
            "vector": row[vector_col].tolist(), 
            "meta": {"id": vec_id}  
        })

    # --- 2. Chunk into batches ---
    batches = [records[i:i + CONFIG["BATCH_SIZE"]] for i in range(0, len(records), CONFIG["BATCH_SIZE"])]
    print(f"📦 Created {len(batches)} batches of {CONFIG['BATCH_SIZE']} vectors.")

    # --- 3. Sequential Upserting ---
    start_time = time.time()
    print("🚀 Upserting serially...")

    success_count, fail_count = 0, 0
    
    # Simple sequential loop directly wrapped in tqdm
    for batch in tqdm(batches, desc="Upserting Batches", unit="batch"):
        try:
            index.upsert(batch)
            success_count += len(batch)
        except Exception:
            fail_count += 1

    duration = time.time() - start_time
    print(f"\n✅ Upsert API Calls Complete in {duration:.2f}s")
    print(f"   API Reported Upserted Vectors: {success_count}")
    if fail_count > 0:
        print(f"   API Failed Batches: {fail_count}")

    # --- 4. Sequential Verification Step ---
    print(f"\n🔍 Verifying actual insertions using get_vector...")
    present_count = 0
    missing_count = 0

    start_verify = time.time()
    
    # Simple sequential loop for verification
    for vec_id in tqdm(ids_to_verify, desc="Verifying Upserts", unit="chk"):
        try:
            res = index.get_vector(str(vec_id))
            if res:
                present_count += 1
            else:
                missing_count += 1
        except Exception:
            # Assuming an exception means the vector wasn't found
            missing_count += 1

    verify_duration = time.time() - start_verify
    print(f"\n✅ Verification Complete in {verify_duration:.2f}s")
    print(f"   Successfully Upserted (Confirmed): {present_count}")
    print(f"   Missing (Failed to Index):         {missing_count}")

    # --- CLEANUP CODE ---
    if missing_count == 0 and os.path.exists(CONFIG["TEMP_SAVE_FILE"]):
        os.remove(CONFIG["TEMP_SAVE_FILE"])
        print(f"🗑️  Cleanup: Successfully deleted temporary file '{CONFIG['TEMP_SAVE_FILE']}'")
    elif missing_count > 0:
        print(f"⚠️  Cleanup Skipped: Temp file kept because {missing_count} vectors are missing.")

if __name__ == "__main__":
    run_upsert()