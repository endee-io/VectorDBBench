import os
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import faiss
import gc

# PyArrow's default allocator (mimalloc) keeps freed pages around, so RSS grew
# ~300MB per chunk until the OOM killer hit. The system allocator returns
# large buffers to the OS immediately.
pa.set_memory_pool(pa.system_memory_pool())

# --- CONFIGURATION ---
DATASET_DIR = "/home/admin/vectordataset/custom_768d_8m/custom_768d_8m"
TARGET_DIM = 768
TOP_K = 40
MAX_CHUNKS = int(os.environ.get("MAX_CHUNKS", "0"))  # 0 = all; set for quick tests


def rss_gb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024 / 1024
    return 0.0

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

    # Track the global best distances and indices across all chunks
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices = np.full((num_queries, TOP_K), -1, dtype=np.int64)

    train_file = pq.ParquetFile(os.path.join(DATASET_DIR, "train.parquet"))
    total_chunks = train_file.metadata.num_row_groups
    if MAX_CHUNKS:
        total_chunks = min(total_chunks, MAX_CHUNKS)

    print(f"\n[2] Scanning Train Vectors one row group at a time ({total_chunks} chunks)...")

    row_indices = np.arange(num_queries)[:, None]
    pool = pa.default_memory_pool()
    scanned = 0
    start_time = time.perf_counter()

    for rg in range(total_chunks):
        # Read exactly one row group (~25k rows) -> bounded memory per step
        tbl = train_file.read_row_group(rg, columns=['id', 'emb'])
        ids = tbl.column('id').to_numpy().astype(np.int64)
        # flatten() respects list offsets (.values does not)
        flat = tbl.column('emb').combine_chunks().flatten().to_numpy(zero_copy_only=False)
        del tbl
        if flat.size != len(ids) * TARGET_DIM:
            raise ValueError(f"Row group {rg}: expected {len(ids) * TARGET_DIM} floats, got {flat.size}")
        xb = flat.astype(np.float32).reshape(-1, TARGET_DIM)
        del flat
        faiss.normalize_L2(xb)

        # Exact brute-force inner-product search against this chunk
        local_dist, local_pos = faiss.knn(xq, xb, TOP_K, metric=faiss.METRIC_INNER_PRODUCT)
        local_ind = ids[local_pos]

        # Merge local chunk results with the global best results
        combined_dist = np.concatenate([global_distances, local_dist], axis=1)
        combined_ind = np.concatenate([global_indices, local_ind], axis=1)
        sort_order = np.argsort(-combined_dist, axis=1)[:, :TOP_K]
        global_distances = combined_dist[row_indices, sort_order]
        global_indices = combined_ind[row_indices, sort_order]

        scanned += len(ids)

        # Memory Cleanup
        del xb, ids, local_dist, local_pos, local_ind, combined_dist, combined_ind
        gc.collect()
        pool.release_unused()

        if (rg + 1) % 10 == 0 or rg + 1 == total_chunks:
            elapsed = time.perf_counter() - start_time
            print(f" -> chunk {rg + 1}/{total_chunks} | RSS {rss_gb():.2f} GB | {elapsed:.0f}s", flush=True)

    scan_time = time.perf_counter() - start_time

    print("\n[3] Calculating Final Recall...")
    recalls = []
    for i in range(num_queries):
        q_id = test_ids[i]
        returned_ids = global_indices[i]
        # neighbors.parquet stores 1000 neighbors per query; compare against the top-K only
        true_ids = ground_truth_map.get(q_id, [])[:TOP_K]

        hits = len(set(returned_ids).intersection(set(true_ids)))
        recalls.append(hits / TOP_K)

    avg_recall = np.mean(recalls)

    print("\n=== VERIFICATION RESULTS ===")
    print(f"Total Vectors Scanned : {scanned} / {train_file.metadata.num_rows}")
    print(f"Total Scan Time       : {scan_time:.2f} seconds")
    print(f"Final Recall          : {avg_recall:.4f}")

    if avg_recall == 1.0:
        print("SUCCESS: Your dataset is 100% mathematically perfect!")
    else:
        print("WARNING: Recall is not 1.0. The ground truth does not perfectly match the dataset.")

if __name__ == "__main__":
    verify_full_dataset()