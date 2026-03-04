import time
import os
import glob
import pandas as pd
from endee import Endee
from tqdm import tqdm

# ==========================================
# ⚙️ USER CONFIGURATION
# ==========================================
CONFIG = {
    # Provide EITHER a single file OR the directory containing partitioned files
    "PARQUET_PATH":   "/home/debian/ssd/vectordataset/cohere/cohere_medium_1m/shuffle_train.parquet", 
    "TEMP_SAVE_FILE": "./deleted_vectors_temp.parquet", 
    "INDEX_NAME":     "1M_int16d_m16_efcon128_1",
    "TOKEN":          None,  
    "BASE_URL":       "http://57.129.55.56:8050/api/v1",
    "DELETE_COUNT":   100000          
}
# ==========================================

def run_delete():
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

    path = CONFIG["PARQUET_PATH"]
    
    # --- 1. Reading & FULLY RANDOM Sampling Logic ---
    if os.path.isfile(path) and path.endswith('.parquet'):
        print(f"📂 Reading single Parquet file: {path}")
        df = pd.read_parquet(path)
        
        # 🎲 NO SEED: Completely random selection every time
        print(f"🔀 Randomly sampling {CONFIG['DELETE_COUNT']} vectors...")
        df_to_delete = df.sample(n=CONFIG["DELETE_COUNT"])
        
    elif os.path.isdir(path):
        print(f"📂 Reading from Parquet directory: {path}")
        all_files = sorted(glob.glob(os.path.join(path, "shuffle_train*.parquet")))
        
        if not all_files:
            print("❌ No 'shuffle_train*.parquet' files found in directory.")
            return
            
        collected_dfs = []
        collected_rows = 0
        
        for f in reversed(all_files):
            print(f"   -> Loading {os.path.basename(f)}...")
            temp_df = pd.read_parquet(f)
            collected_dfs.insert(0, temp_df)
            collected_rows += len(temp_df)
            if collected_rows >= CONFIG["DELETE_COUNT"]:
                break
                
        df_combined = pd.concat(collected_dfs, ignore_index=True)
        # NO SEED: Randomly sample from the collected chunk
        print(f"🔀 Randomly sampling {CONFIG['DELETE_COUNT']} vectors from collected files...")
        df_to_delete = df_combined.sample(n=CONFIG["DELETE_COUNT"])
        
    else:
        print(f"❌ Invalid path: {path}")
        return

    # --- 2. Save for Re-Upserting ---
    df_to_delete.to_parquet(CONFIG["TEMP_SAVE_FILE"])
    print(f"\n💾 Saved {CONFIG['DELETE_COUNT']} vectors to {CONFIG['TEMP_SAVE_FILE']} for later upsert.")

    ids_to_delete = df_to_delete['id'].astype(str).tolist()
    print(f"🚀 Deleting {len(ids_to_delete)} vectors serially...")

    # --- 3. Sequential Deletion ---
    start_time = time.time()
    success, fail = 0, 0
    
    for vec_id in tqdm(ids_to_delete, desc="Deleting Vectors", unit="del"):
        try:
            index.delete_vector(str(vec_id))
            success += 1
        except Exception:
            fail += 1

    duration = time.time() - start_time
    print(f"\n✅ Deletion API Calls Complete in {duration:.2f}s")
    print(f"   API Success: {success}")
    print(f"   API Failed:  {fail}")

    # --- 4. Sequential Verification Step ---
    print(f"\n🔍 Verifying actual deletions using get_vector...")
    present_count = 0
    deleted_count = 0

    start_verify = time.time()
    
    for vec_id in tqdm(ids_to_delete, desc="Verifying Deletions", unit="chk"):
        try:
            res = index.get_vector(str(vec_id))
            if res:
                present_count += 1
            else:
                deleted_count += 1
        except Exception:
            # Assuming an exception means the vector wasn't found (deleted)
            deleted_count += 1

    verify_duration = time.time() - start_verify
    print(f"\n✅ Verification Complete in {verify_duration:.2f}s")
    print(f"   Successfully Deleted: {deleted_count}")
    print(f"   Still Present:  {present_count}")

if __name__ == "__main__":
    run_delete()