"""
Builds a VectorDBBench custom dataset from Cohere-style shards in one run:
  [1] slices test.parquet queries to TARGET_DIM
  [2] merges the first NUM_FILES shuffle_train-XX-of-10.parquet shards into ONE train.parquet (sliced to TARGET_DIM)
  [3] computes exact ground truth (neighbors.parquet) with low, flat RAM

Steps [1] and [2] are the same as generate_chunked_dataset.py. Step [3] is the memory-safe version from
correction_script.py (same results as the original step [3], which can get "Killed" by the OOM killer):
  * reads train.parquet one row group at a time (ParquetFile.iter_batches() keeps decoded data alive
    across the whole file, so its memory keeps growing),
  * copies rows into ONE float32 buffer of FAISS_CHUNK rows that is allocated once and reused,
  * searches that buffer with faiss.knn (no index copy) and merges top-k with vectorized numpy.
Peak RAM in step [3] ~= FAISS_CHUNK x TARGET_DIM x 4 bytes + one row group + a few hundred MB.

Each Cohere 10M shard holds 1M rows, so NUM_FILES = 2 -> 2M, 3 -> 3M, ... 10 -> 10M.
Set GT_ONLY = True to skip [1] and [2] and only recompute neighbors.parquet from the files already in DEST_DIR.
"""
import os
import time

import faiss
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# --- CONFIGURATION ---
# --- Destination Directory should contain nested directory (Format of vectordbbench, ex. custom_768d_2m/custom_768d_2m)
SOURCE_DIR = "/home/admin/vectordataset/cohere/cohere_large_10m"
DEST_DIR  = "/home/admin/vectordataset/custom_768d_8m/custom_768d_8m"   # new folder, so the 2M dataset isn't overwritten
TARGET_DIM = 768          # Cohere is 768D; a smaller value (e.g. 512) keeps the first TARGET_DIM values of each vector
NUM_FILES = 8             # How many shuffle_train-XX-of-10 shards to merge (1M rows each)
TOP_K = 1000              # Ground truth depth; must be >= the --k you benchmark with
BATCH_SIZE = 25000        # Rows per read/write batch in steps [1]-[2]
FAISS_CHUNK = 500000      # Train rows per FAISS pass in step [3] (~1.5GB buffer at 768D, ~1GB at 512D)
GT_ONLY = False           # True = skip [1] and [2], only recompute neighbors.parquet


def slice_test_file():
    """Slices the queries to TARGET_DIM."""
    print(f"\n[1] Slicing Test Queries to {TARGET_DIM}D...")
    in_path = os.path.join(SOURCE_DIR, "test.parquet")
    out_path = os.path.join(DEST_DIR, "test.parquet")

    df = pd.read_parquet(in_path)
    df['emb'] = df['emb'].apply(lambda x: x[:TARGET_DIM] if x is not None else x)
    df.to_parquet(out_path)
    print(f" Saved to {out_path}")


def merge_and_slice_train_files():
    """Streams the first NUM_FILES shards in batches into ONE train.parquet file."""
    print(f"\n[2] Merging {NUM_FILES} shard file(s) into a single {TARGET_DIM}D train.parquet...")
    out_path = os.path.join(DEST_DIR, "train.parquet")

    writer = None
    rows_written = 0
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
                rows_written += table.num_rows
    finally:
        # Guarantee the footer is written, preventing file corruption!
        if writer is not None:
            writer.close()
            print(f" Master file securely saved to {out_path} ({rows_written:,} rows)")


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
    """Exact top-K neighbors (cosine) over train.parquet with flat, bounded RAM."""
    print("\n[3] Calculating Ground Truth from master file...")
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

    if (best_ids == -1).any():
        print(f" Warning: train has fewer than TOP_K={TOP_K} vectors, some neighbor slots are -1")

    # Save exactly how VectorDBBench expects it
    neighbors_df = pd.DataFrame({
        'id': test_df['id'],
        'neighbors_id': best_ids.tolist()
    })
    out_path = os.path.join(DEST_DIR, "neighbors.parquet")
    neighbors_df.to_parquet(out_path)
    print(f" Saved true neighbors to {out_path} in {time.time() - start:.0f}s")


def main():
    os.makedirs(DEST_DIR, exist_ok=True)

    if GT_ONLY:
        for name in ("test.parquet", "train.parquet"):
            path = os.path.join(DEST_DIR, name)
            if not os.path.exists(path):
                raise FileNotFoundError(f"GT_ONLY = True needs an existing {path}")
        print(f"\nGT_ONLY = True: skipping [1] and [2], using the files in {DEST_DIR}")
    else:
        slice_test_file()
        merge_and_slice_train_files()

    calculate_ground_truth()

    print("\nDataset is complete and ready!")


if __name__ == "__main__":
    main()
