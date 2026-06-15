import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import faiss
from tqdm import tqdm

# --- CONFIGURATION ---
# --- Destination Directory should contain nested directory (Format of vectordbbench, ex. custom_512d_10m/custom_512d_10m)
SOURCE_DIR = "/home/debian/ssd/vectordataset/cohere/cohere_large_10m"
DEST_DIR = "/home/debian/ssd/vectordataset/custom_512d_10m_single/custom_512d_10m_single"
TARGET_DIM = 512
TOP_K = 100
BATCH_SIZE = 25000       # For safely writing to disk
FAISS_CHUNK = 1000000    # Read 1M at a time for FAISS (Uses ~3GB RAM)
NUM_FILES = 10

def slice_test_file():
    """Slices the queries to 512D."""
    print("\n[1] Slicing Test Queries...")
    in_path = os.path.join(SOURCE_DIR, "test.parquet")
    out_path = os.path.join(DEST_DIR, "test.parquet")
    
    df = pd.read_parquet(in_path)
    df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
    df.to_parquet(out_path)
    print(f" Saved to {out_path}")

def merge_and_slice_train_files():
    """Reads all 10 files in batches and streams them into ONE train.parquet file."""
    print(f"\n[2] Merging 10 files into a single 10M {TARGET_DIM}D train.parquet...")
    out_path = os.path.join(DEST_DIR, "train.parquet")
    
    writer = None
    try:
        for i in range(NUM_FILES):
            in_path = os.path.join(SOURCE_DIR, f"shuffle_train-{i:02d}-of-10.parquet")
            if not os.path.exists(in_path):
                print(f" Warning: {in_path} not found. Skipping.")
                continue
                
            parquet_file = pq.ParquetFile(in_path)
            total_batches = (parquet_file.metadata.num_rows + BATCH_SIZE - 1) // BATCH_SIZE
            
            # Stream the current file into the master writer
            for batch in tqdm(parquet_file.iter_batches(batch_size=BATCH_SIZE), 
                              total=total_batches, 
                              desc=f"Processing Chunk {i+1}/{NUM_FILES}"):
                
                df = batch.to_pandas()
                df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
                table = pa.Table.from_pandas(df)
                
                # Initialize writer on the very first batch
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema)
                
                writer.write_table(table)
    finally:
        # Guarantee the footer is written, preventing file corruption!
        if writer is not None:
            writer.close()
            print(f" Master file securely saved to {out_path}")

def calculate_ground_truth_from_master():
    """Reads the master 10M file in safe chunks to calculate exact neighbors."""
    print("\n[3] Calculating Ground Truth from master file...")
    
    test_df = pd.read_parquet(os.path.join(DEST_DIR, "test.parquet"))
    xq = np.vstack(test_df['emb'].values).astype('float32')
    faiss.normalize_L2(xq)
    num_queries = len(xq)
    
    # Track the global best distances and indices
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices = np.full((num_queries, TOP_K), -1, dtype=np.int64)
    
    master_file = pq.ParquetFile(os.path.join(DEST_DIR, "train.parquet"))
    total_rows = master_file.metadata.num_rows
    total_chunks = (total_rows + FAISS_CHUNK - 1) // FAISS_CHUNK
    
    chunk_idx = 1
    # Read in larger chunks (1M rows) for FAISS efficiency
    for batch in master_file.iter_batches(batch_size=FAISS_CHUNK):
        print(f" -> FAISS Search on chunk {chunk_idx}/{total_chunks}...")
        df = batch.to_pandas()
        xb = np.vstack(df['emb'].values).astype('float32')
        ids = df['id'].values.astype('int64')
        faiss.normalize_L2(xb)
        
        # Local index for this chunk
        local_index = faiss.IndexIDMap(faiss.IndexFlatIP(TARGET_DIM))
        local_index.add_with_ids(xb, ids)
        local_dist, local_ind = local_index.search(xq, TOP_K)
        
        # Merge with global results
        for q_idx in range(num_queries):
            combined_dist = np.concatenate([global_distances[q_idx], local_dist[q_idx]])
            combined_ind = np.concatenate([global_indices[q_idx], local_ind[q_idx]])
            
            sort_order = np.argsort(combined_dist)[::-1]
            
            global_distances[q_idx] = combined_dist[sort_order][:TOP_K]
            global_indices[q_idx] = combined_ind[sort_order][:TOP_K]
            
        chunk_idx += 1

    # Save exactly how VectorDBBench expects it
    neighbors_df = pd.DataFrame({
        'id': test_df['id'],
        'neighbors_id': global_indices.tolist() 
    })
    
    out_path = os.path.join(DEST_DIR, "neighbors.parquet")
    neighbors_df.to_parquet(out_path)
    print(f" Saved true neighbors to {out_path}")

if __name__ == "__main__":
    os.makedirs(DEST_DIR, exist_ok=True)
    
    slice_test_file()
    merge_and_slice_train_files()
    calculate_ground_truth_from_master()
    
    print("\ng 10M Single-File Dataset is complete and ready!")