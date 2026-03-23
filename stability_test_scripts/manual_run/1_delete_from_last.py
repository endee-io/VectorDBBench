import time
import os
import glob
import pandas as pd
from endee import Endee
from tqdm import tqdm

# ==========================================
# USER CONFIGURATION
# ==========================================
CONFIG = {
    # Provide EITHER a single file OR the directory containing partitioned files
    "PARQUET_PATH":   "/home/debian/ssd/vectordataset/cohere/cohere_medium_1m/", 
    "TEMP_SAVE_FILE": "./deleted_vectors_temp.parquet", 
    "INDEX_NAME":     "1M_int16d_m16_efcon128_1",
    "TOKEN":          None,  
    "BASE_URL":       "http://57.129.55.56:8050/api/v1",
    "DELETE_COUNT":   100000          
}
# ==========================================

def run_delete():
    print(f"Connecting to Endee at {CONFIG['BASE_URL']}...")
    try:
        token = CONFIG["TOKEN"] if CONFIG["TOKEN"] != "TOKEN" else None
        client = Endee(token=token)
        client.set_base_url(CONFIG["BASE_URL"])
        index = client.get_index(name=CONFIG["INDEX_NAME"])
        print(f"Connected to Index: {CONFIG['INDEX_NAME']}")
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    path = CONFIG["PARQUET_PATH"]
    
    # --- 1. Memory-Efficient Reading Logic ---
    if os.path.isfile(path) and path.endswith('.parquet'):
        print(f"Reading single Parquet file: {path}")
        df = pd.read_parquet(path)
        df_to_delete = df.tail(CONFIG["DELETE_COUNT"])
        
    elif os.path.isdir(path):
        print(f"Reading from Parquet directory: {path}")
        # Find all data files, ignore metadata/test/neighbors files
        all_files = sorted(glob.glob(os.path.join(path, "shuffle_train*.parquet")))
        
        if not all_files:
            print("No 'shuffle_train*.parquet' files found in directory.")
            return
            
        # Read files backwards so we don't load 10M vectors into RAM!
        collected_dfs = []
        collected_rows = 0
        
        for f in reversed(all_files):
            print(f"   -> Loading {os.path.basename(f)}...")
            temp_df = pd.read_parquet(f)
            collected_dfs.insert(0, temp_df) # Insert at beginning to keep original order
            collected_rows += len(temp_df)
            if collected_rows >= CONFIG["DELETE_COUNT"]:
                break
                
        df_combined = pd.concat(collected_dfs, ignore_index=True)
        df_to_delete = df_combined.tail(CONFIG["DELETE_COUNT"])
        
    else:
        print(f"Invalid path: {path}")
        return

    # --- 2. Save for Re-Upserting ---
    df_to_delete.to_parquet(CONFIG["TEMP_SAVE_FILE"])
    print(f"\nSaved the last {CONFIG['DELETE_COUNT']} vectors to {CONFIG['TEMP_SAVE_FILE']} for later.")

    ids_to_delete = df_to_delete['id'].astype(str).tolist()
    print(f"Deleting {len(ids_to_delete)} vectors serially...")

    # --- 3. Sequential Deletion ---
    start_time = time.time()
    success = 0
    fail = 0
    
    # Wrap the list directly in tqdm for the progress bar
    for vec_id in tqdm(ids_to_delete, desc="Deleting Vectors", unit="del"):
        try:
            index.delete_vector(str(vec_id))
            success += 1
        except Exception as e:
            # We don't print the error here to avoid spamming the console 100,000 times
            fail += 1

    duration = time.time() - start_time
    print(f"\nDeletion Complete in {duration:.2f}s")
    print(f"   Success: {success}")
    print(f"   Failed:  {fail}")

if __name__ == "__main__":
    run_delete()