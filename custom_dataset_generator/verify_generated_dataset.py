import os
import time
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import faiss
import gc

# --- CONFIGURATION ---
DATASET_DIR = "/home/ubuntu/vectordataset/custom_512d_1M/custom_512d_1M"
TARGET_DIM = 512
TOP_K = 100
CHUNK_SIZE = 100000  # Safe for 16GB RAM

def verify_full_dataset():
    print("[1] Loading Test Queries and Ground Truth...")
    # Load Queries
    test_df = pd.read_parquet(os.path.join(DATASET_DIR, "test.parquet"))
    xq = np.vstack(test_df['emb'].values).astype('float32')
    test_ids = test_df['id'].values
    faiss.normalize_L2(xq)
    num_queries = len(xq)

    # Load Ground Truth Answer Key
    gt_df = pd.read_parquet(os.path.join(DATASET_DIR, "neighbors.parquet"))
    ground_truth_map = dict(zip(gt_df['id'], gt_df['neighbors_id']))

    print(f"\n[2] Scanning Train Vectors in chunks of {CHUNK_SIZE} (Low RAM Mode)...")
    
    # Track the global best distances and indices across all chunks
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices = np.full((num_queries, TOP_K), -1, dtype=np.int64)
    
    train_file = pq.ParquetFile(os.path.join(DATASET_DIR, "train.parquet"))
    total_chunks = (train_file.metadata.num_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    start_time = time.perf_counter()
    
    chunk_idx = 1
    for batch in train_file.iter_batches(batch_size=CHUNK_SIZE):
        print(f" -> Processing chunk {chunk_idx}/{total_chunks}...")
        
        # Extract batch without pandas overhead
        xb = batch['emb'].values.to_numpy().reshape(-1, TARGET_DIM).astype('float32')
        ids = batch['id'].to_numpy().astype('int64')
        faiss.normalize_L2(xb)
        
        # Build a temporary Flat (Brute Force) index for just this chunk
        local_index = faiss.IndexIDMap(faiss.IndexFlatIP(TARGET_DIM))
        local_index.add_with_ids(xb, ids)
        local_dist, local_ind = local_index.search(xq, TOP_K)
        
        # Merge local chunk results with the global best results
        combined_dist = np.concatenate([global_distances, local_dist], axis=1) 
        combined_ind = np.concatenate([global_indices, local_ind], axis=1)
        
        sort_order = np.argsort(-combined_dist, axis=1)[:, :TOP_K]
        row_indices = np.arange(num_queries)[:, None]
        
        global_distances = combined_dist[row_indices, sort_order]
        global_indices = combined_ind[row_indices, sort_order]
        
        # Memory Cleanup
        del xb, ids, local_index, local_dist, local_ind, combined_dist, combined_ind
        gc.collect()
        
        chunk_idx += 1

    scan_time = time.perf_counter() - start_time

    print("\n[3] Calculating Final Recall...")
    recalls = []
    for i in range(num_queries):
        q_id = test_ids[i]
        returned_ids = global_indices[i]
        true_ids = ground_truth_map.get(q_id, [])
        
        hits = len(set(returned_ids).intersection(set(true_ids)))
        recalls.append(hits / TOP_K)

    avg_recall = np.mean(recalls)
    
    print("\n=== VERIFICATION RESULTS ===")
    print(f"Total Vectors Scanned : {train_file.metadata.num_rows}")
    print(f"Total Scan Time       : {scan_time:.2f} seconds")
    print(f"Final Recall          : {avg_recall:.4f}")
    
    if avg_recall == 1.0:
        print("SUCCESS: Your dataset is 100% mathematically perfect!")
    else:
        print("WARNING: Recall is not 1.0. The ground truth does not perfectly match the dataset.")

if __name__ == "__main__":
    verify_full_dataset()
