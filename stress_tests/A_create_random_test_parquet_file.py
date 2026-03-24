import os
import random
import shutil
import pandas as pd
import pyarrow.parquet as pq

# ==========================================
# CONFIGURATION
# ==========================================
NUM_CLIENTS = 10
QUERIES_PER_CLIENT = 1000

# Your original dataset location
ORIGINAL_DATASET_DIR = "/home/admin/vectordataset/cohere/cohere_medium_1m"
ORIGINAL_TRAIN_FILE = os.path.join(ORIGINAL_DATASET_DIR, "shuffle_train.parquet")
ORIGINAL_NEIGHBORS = os.path.join(ORIGINAL_DATASET_DIR, "neighbors.parquet")
ORIGINAL_LABELS = os.path.join(ORIGINAL_DATASET_DIR, "scalar_labels.parquet")

# Where to build the fake client folders
BASE_WORKING_DIR = "/home/admin/vectordataset_clients"
# ==========================================

def prepare_unique_datasets():
    print(f"Preparing {NUM_CLIENTS} isolated datasets with unique queries...")
    
    # 1. Get total rows to sample from
    pf = pq.ParquetFile(ORIGINAL_TRAIN_FILE)
    total_rows = pf.metadata.num_rows
    total_queries_needed = NUM_CLIENTS * QUERIES_PER_CLIENT
    
    print(f"Sampling {total_queries_needed} unique vectors from {total_rows} total rows...")
    random_indices = set(random.sample(range(total_rows), total_queries_needed))
    
    # 2. Extract the random vectors
    collected_dfs = []
    current_offset = 0
    for batch in pf.iter_batches(batch_size=20000):
        chunk_length = len(batch)
        chunk_start = current_offset
        chunk_end = current_offset + chunk_length
        
        needed_in_chunk = [idx - chunk_start for idx in random_indices if chunk_start <= idx < chunk_end]
        if needed_in_chunk:
            df_chunk = batch.to_pandas()
            collected_dfs.append(df_chunk.iloc[needed_in_chunk])
        current_offset += chunk_length

    # Combine and shuffle
    all_unique_queries = pd.concat(collected_dfs, ignore_index=True).sample(frac=1).reset_index(drop=True)
    
    # 3. Distribute to client folders
    for i in range(NUM_CLIENTS):
        client_id = i + 1
        # VectorDBBench expects exactly this sub-folder structure
        client_dir = f"{BASE_WORKING_DIR}/client_{client_id}/cohere/cohere_medium_1m"
        os.makedirs(client_dir, exist_ok=True)
        
        # Soft link the heavy training data
        os.symlink(ORIGINAL_TRAIN_FILE, os.path.join(client_dir, "shuffle_train.parquet"))
        
        # Soft link the scalar labels (if they exist)
        if os.path.exists(ORIGINAL_LABELS):
            os.symlink(ORIGINAL_LABELS, os.path.join(client_dir, "scalar_labels.parquet"))
            
        # Copy the original neighbors file just so VectorDBBench doesn't crash (Recall will be 0)
        shutil.copy2(ORIGINAL_NEIGHBORS, os.path.join(client_dir, "neighbors.parquet"))
        
        # Save the UNIQUE test queries for this specific client
        start_idx = i * QUERIES_PER_CLIENT
        end_idx = start_idx + QUERIES_PER_CLIENT
        client_queries = all_unique_queries.iloc[start_idx:end_idx]
        
        test_file_path = os.path.join(client_dir, "test.parquet")
        client_queries.to_parquet(test_file_path)
        
        print(f"Created isolated dataset for Client {client_id} (Soft links + Unique test.parquet)")

    print("\nAll 10 isolated datasets are ready!")

if __name__ == "__main__":
    prepare_unique_datasets()
