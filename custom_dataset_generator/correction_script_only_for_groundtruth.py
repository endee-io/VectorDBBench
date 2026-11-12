"""
Recomputes ground truth (neighbors.parquet) for an already generated dataset, using low, flat RAM.

Use this when train.parquet + test.parquet already exist in DEST_DIR (e.g. generate_chunked_dataset.py
finished steps [1] and [2] but step [3] was "Killed" by the OOM killer).

Why the original step [3] runs out of memory:
  * pyarrow's ParquetFile.iter_batches() keeps decoded data alive as it moves through the file, so
    RAM keeps growing pass after pass no matter how small FAISS_CHUNK is (killed near the end);
  * on top of that, every pass makes several full copies (pandas DataFrame, np.vstack, astype,
    FAISS index), and the previous pass's copies are still alive while the next one loads.

This script instead:
  * reads train.parquet one row group at a time (flat memory), straight from Arrow, no pandas,
  * copies rows into ONE float32 buffer of FAISS_CHUNK rows that is allocated once and reused,
  * searches that buffer with faiss.knn (no index copy) and merges top-k with vectorized numpy.
Peak RAM ~= FAISS_CHUNK x dim x 4 bytes + one row group + a few hundred MB.
"""
import os
import time

import faiss
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# --- CONFIGURATION ---
# --- DEST_DIR is the nested VectorDBBench folder that already holds train.parquet and test.parquet
DEST_DIR = "/home/admin/vectordataset/custom_768d_2m/custom_768d_2m"
TOP_K = 1000             # Ground truth depth; must be >= the --k you benchmark with
FAISS_CHUNK = 500000     # Train rows per FAISS pass (~1.5GB buffer at 768D, ~1GB at 512D)


def emb_column_to_numpy(emb_column, dim):
    """Arrow list<float|double> column -> (rows, dim) numpy array, without one Python object per row."""
    flat = emb_column.flatten().to_numpy(zero_copy_only=False)
    if flat.size != len(emb_column) * dim:
        raise ValueError(
            f"Expected {len(emb_column)} vectors of {dim}D but got {flat.size} values: "
            "train and test dims differ, or some embeddings are null/ragged"
        )
    return flat.reshape(len(emb_column), dim)


def calculate_ground_truth():
    print("\n[GT] Recomputing ground truth...")
    start = time.time()

    test_df = pd.read_parquet(os.path.join(DEST_DIR, "test.parquet"))
    xq = np.ascontiguousarray(np.vstack(test_df['emb'].values), dtype=np.float32)
    faiss.normalize_L2(xq)  # Cosine = inner product on normalized vectors
    num_queries, dim = xq.shape

    train_file = pq.ParquetFile(os.path.join(DEST_DIR, "train.parquet"))
    total_rows = train_file.metadata.num_rows
    chunk_rows = min(FAISS_CHUNK, total_rows)
    total_chunks = (total_rows + chunk_rows - 1) // chunk_rows
    print(f" {total_rows:,} train vectors x {dim}D, {num_queries:,} queries, top_k={TOP_K}")
    print(f" {total_chunks} pass(es) of up to {chunk_rows:,} rows "
          f"(~{chunk_rows * dim * 4 / 1e9:.1f} GB buffer)")

    # Best neighbors found so far, per query
    best_dist = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    best_ids = np.full((num_queries, TOP_K), -1, dtype=np.int64)

    # Allocated once, refilled for every pass
    xb = np.empty((chunk_rows, dim), dtype=np.float32)
    chunk_ids = np.empty(chunk_rows, dtype=np.int64)

    def search_and_merge(n, chunk_no):
        nonlocal best_dist, best_ids
        print(f" -> FAISS Search on chunk {chunk_no}/{total_chunks} ({n:,} rows)...")
        vectors = xb[:n]
        faiss.normalize_L2(vectors)
        dist, pos = faiss.knn(xq, vectors, TOP_K, metric=faiss.METRIC_INNER_PRODUCT)
        # Map positions in this chunk back to dataset ids (-1 only if a chunk has fewer than TOP_K rows)
        found_ids = np.where(pos >= 0, chunk_ids[:n][pos], -1)

        all_dist = np.concatenate([best_dist, dist], axis=1)
        all_ids = np.concatenate([best_ids, found_ids], axis=1)
        order = np.argsort(-all_dist, axis=1, kind="stable")[:, :TOP_K]
        best_dist = np.take_along_axis(all_dist, order, axis=1)
        best_ids = np.take_along_axis(all_ids, order, axis=1)

    filled = 0
    chunk_no = 0
    # Read one row group at a time: ParquetFile.iter_batches() keeps decoded data alive across the
    # whole file, so its memory grows to more than the file's full size.
    for rg in range(train_file.num_row_groups):
        for batch in train_file.read_row_group(rg, columns=['id', 'emb']).to_batches():
            vecs = emb_column_to_numpy(batch.column('emb'), dim)
            ids = batch.column('id').to_numpy(zero_copy_only=False)
            pos = 0
            while pos < batch.num_rows:
                take = min(batch.num_rows - pos, chunk_rows - filled)
                xb[filled:filled + take] = vecs[pos:pos + take]  # also casts float64 -> float32
                chunk_ids[filled:filled + take] = ids[pos:pos + take]
                filled += take
                pos += take
                if filled == chunk_rows:
                    chunk_no += 1
                    search_and_merge(filled, chunk_no)
                    filled = 0
    if filled:
        chunk_no += 1
        search_and_merge(filled, chunk_no)

    # Save exactly how VectorDBBench expects it
    neighbors_df = pd.DataFrame({
        'id': test_df['id'],
        'neighbors_id': best_ids.tolist()
    })
    out_path = os.path.join(DEST_DIR, "neighbors.parquet")
    neighbors_df.to_parquet(out_path)
    print(f" Saved true neighbors to {out_path} in {time.time() - start:.0f}s")


if __name__ == "__main__":
    calculate_ground_truth()
    print("\nGround truth is complete and ready!")
