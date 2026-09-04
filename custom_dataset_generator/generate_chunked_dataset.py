import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import faiss
from tqdm import tqdm
import gc

# --- CONFIGURATION ---
SOURCE_DIR = "/home/ubuntu/vectordataset/cohere/cohere_large_10m"
DEST_DIR = "/home/ubuntu/vectordataset/custom_512d_3M/custom_512d_3M"

TARGET_DIM = 512
TARGET_ROW_COUNT = 3_000_000  # Set to None if you want all vectors (10M), else set your limit like 3_000_000

TOP_K = 100
BATCH_SIZE = 25000       # For safely writing to disk
FAISS_CHUNK = 100000     # Read at a time for FAISS (Uses more RAM)
NUM_FILES = 10

def slice_test_file():
    """Slices the queries to TARGET_DIM."""
    print("\n[1] Slicing Test Queries...")
    in_path = os.path.join(SOURCE_DIR, "test.parquet")
    out_path = os.path.join(DEST_DIR, "test.parquet")
    
    df = pd.read_parquet(in_path)
    df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
    df.to_parquet(out_path)
    print(f" Saved to {out_path}")

def merge_and_slice_train_files():
    """Reads files in batches, streams them into ONE train.parquet file, and stops at TARGET_ROW_COUNT."""
    print(f"\n[2] Merging files into a single {TARGET_DIM}D train.parquet...")
    if TARGET_ROW_COUNT is not None:
        print(f" -> Target size limit set to: {TARGET_ROW_COUNT} vectors.")
        
    out_path = os.path.join(DEST_DIR, "train.parquet")
    
    writer = None
    total_rows_written = 0
    
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
                              desc=f"Processing File {i+1}/{NUM_FILES}"):
                
                df = batch.to_pandas()
                
                # Check if this batch pushes us over the TARGET_ROW_COUNT limit
                if TARGET_ROW_COUNT is not None:
                    rows_needed = TARGET_ROW_COUNT - total_rows_written
                    if len(df) > rows_needed:
                        df = df.head(rows_needed) # Slice the dataframe to exactly what is needed
                
                # Slice dimensionality
                df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
                table = pa.Table.from_pandas(df)
                
                # Initialize writer on the very first batch
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema)
                
                writer.write_table(table)
                total_rows_written += len(df)
                
                # Stop if we have hit the configured target
                if TARGET_ROW_COUNT is not None and total_rows_written >= TARGET_ROW_COUNT:
                    print(f"\n Reached target row count of {TARGET_ROW_COUNT}. Stopping merge early.")
                    return # Exits the function entirely

    finally:
        # Guarantee the footer is written, preventing file corruption!
        if writer is not None:
            writer.close()
            print(f" Master file securely saved to {out_path} with {total_rows_written} rows.")

def calculate_ground_truth_from_master():
    """Reads the master file in safe chunks to calculate exact neighbors."""
    print("\n[3] Calculating Ground Truth from master file...")
    
    # LOWER THIS to 100,000 or 50,000 for 16GB RAM
    SAFE_FAISS_CHUNK = 100000 
    
    test_df = pd.read_parquet(os.path.join(DEST_DIR, "test.parquet"))
    xq = np.vstack(test_df['emb'].values).astype('float32')
    faiss.normalize_L2(xq)
    num_queries = len(xq)
    
    # Track the global best distances and indices
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices = np.full((num_queries, TOP_K), -1, dtype=np.int64)
    
    master_file = pq.ParquetFile(os.path.join(DEST_DIR, "train.parquet"))
    total_rows = master_file.metadata.num_rows
    total_chunks = (total_rows + SAFE_FAISS_CHUNK - 1) // SAFE_FAISS_CHUNK
    
    chunk_idx = 1
    for batch in master_file.iter_batches(batch_size=SAFE_FAISS_CHUNK):
        print(f" -> FAISS Search on chunk {chunk_idx}/{total_chunks}...")
        
        xb = batch['emb'].values.to_numpy().reshape(-1, TARGET_DIM).astype('float32')
        ids = batch['id'].to_numpy().astype('int64')
        
        faiss.normalize_L2(xb)
        
        # Local index for this chunk
        local_index = faiss.IndexIDMap(faiss.IndexFlatIP(TARGET_DIM))
        local_index.add_with_ids(xb, ids)
        local_dist, local_ind = local_index.search(xq, TOP_K)
        
        # --- VECTORIZED MERGE (100x Faster, uses almost 0 extra RAM) ---
        # Combine global and local results horizontally
        combined_dist = np.concatenate([global_distances, local_dist], axis=1) 
        combined_ind = np.concatenate([global_indices, local_ind], axis=1)
        
        # Sort and take the top TOP_K for all queries simultaneously
        sort_order = np.argsort(-combined_dist, axis=1)[:, :TOP_K]
        row_indices = np.arange(num_queries)[:, None]
        
        global_distances = combined_dist[row_indices, sort_order]
        global_indices = combined_ind[row_indices, sort_order]
        
        # --- EXPLICIT MEMORY CLEANUP ---
        # Force Python and FAISS to release the gigabytes of RAM immediately
        del xb
        del ids
        del local_index
        del local_dist
        del local_ind
        del combined_dist
        del combined_ind
        gc.collect()
            
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
    
    print("\nDataset preparation is complete and ready!")
