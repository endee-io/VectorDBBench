import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import faiss
from tqdm import tqdm

# --- CONFIGURATION ---
# --- Destination Directory should contain nested directory (Format of vectordbbench, ex. custom_512d_1m/custom_512d_1m)
SOURCE_DIR = "/home/debian/ssd/vectordataset/cohere/cohere_medium_1m"
DEST_DIR = "/home/debian/ssd/vectordataset/custom_512d_1m/custom_512d_1m"
TARGET_DIM = 512           # Change this to whatever dimension you need!
TOP_K = 100                # Ground truth depth
BATCH_SIZE = 25000         # Safe disk writing batch size
FAISS_CHUNK = 200000       # Safe RAM chunk for FAISS (~1GB RAM usage)

def slice_test_file():
    """Reads the queries, slices them to TARGET_DIM, and saves them."""
    print(f"\n[1] Slicing Test Queries to {TARGET_DIM}D...")
    in_path = os.path.join(SOURCE_DIR, "test.parquet")
    out_path = os.path.join(DEST_DIR, "test.parquet")
    
    df = pd.read_parquet(in_path)
    df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
    df.to_parquet(out_path)
    print(f" Saved to {out_path}")

def slice_and_rename_train_file():
    """Streams shuffle_train.parquet, slices it, and writes as train.parquet."""
    print(f"\n[2] Slicing and renaming training data to {TARGET_DIM}D...")
    in_path = os.path.join(SOURCE_DIR, "shuffle_train.parquet")
    out_path = os.path.join(DEST_DIR, "train.parquet") # Auto-renamed for CLI!
    
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Could not find {in_path}")
        
    parquet_file = pq.ParquetFile(in_path)
    total_batches = (parquet_file.metadata.num_rows + BATCH_SIZE - 1) // BATCH_SIZE
    
    writer = None
    try:
        for batch in tqdm(parquet_file.iter_batches(batch_size=BATCH_SIZE), 
                          total=total_batches, 
                          desc="Processing Vectors"):
            
            df = batch.to_pandas()
            df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
            table = pa.Table.from_pandas(df)
            
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            
            writer.write_table(table)
    finally:
        # Ensures file footer is written to prevent ArrowInvalid corruption
        if writer is not None:
            writer.close()
            print(f" Training data securely saved to {out_path}")

def calculate_ground_truth():
    """Calculates true neighbors using memory-safe chunking."""
    print("\n[3] Calculating Ground Truth...")
    
    # Load sliced queries
    test_df = pd.read_parquet(os.path.join(DEST_DIR, "test.parquet"))
    xq = np.vstack(test_df['emb'].values).astype('float32')
    faiss.normalize_L2(xq) # Required for Cosine Similarity
    num_queries = len(xq)
    
    # Matrices to track the best neighbors found so far
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices = np.full((num_queries, TOP_K), -1, dtype=np.int64)
    
    train_file = pq.ParquetFile(os.path.join(DEST_DIR, "train.parquet"))
    total_rows = train_file.metadata.num_rows
    total_chunks = (total_rows + FAISS_CHUNK - 1) // FAISS_CHUNK
    
    chunk_idx = 1
    for batch in train_file.iter_batches(batch_size=FAISS_CHUNK):
        print(f" -> FAISS Search on chunk {chunk_idx}/{total_chunks}...")
        df = batch.to_pandas()
        xb = np.vstack(df['emb'].values).astype('float32')
        ids = df['id'].values.astype('int64')
        faiss.normalize_L2(xb)
        
        # Exact search on this specific chunk
        local_index = faiss.IndexIDMap(faiss.IndexFlatIP(TARGET_DIM))
        local_index.add_with_ids(xb, ids)
        local_dist, local_ind = local_index.search(xq, TOP_K)
        
        # Merge results to keep the absolute Top K globally
        for q_idx in range(num_queries):
            combined_dist = np.concatenate([global_distances[q_idx], local_dist[q_idx]])
            combined_ind = np.concatenate([global_indices[q_idx], local_ind[q_idx]])
            
            sort_order = np.argsort(combined_dist)[::-1]
            
            global_distances[q_idx] = combined_dist[sort_order][:TOP_K]
            global_indices[q_idx] = combined_ind[sort_order][:TOP_K]
            
        chunk_idx += 1

    # Format exactly for VectorDBBench
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
    slice_and_rename_train_file()
    calculate_ground_truth()
    
    print("\n Custom Dimension Dataset is complete and ready for the CLI!")