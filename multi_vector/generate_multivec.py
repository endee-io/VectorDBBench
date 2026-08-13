"""
Multi-vector dataset generator for VectorDBBench.

For each document we store NUM_VECTORS vectors whose noises sum to zero,
so mean pooling always recovers the original embedding regardless of how
many vectors you choose.

  noise_1 .. noise_{N-1}  — independent random perturbations
  noise_N                 — ( -sum of all previous noises )   ← guarantees cancellation
  multivec_i              = normalize(emb + noise_i)

With N=2 this is the classic (emb+ε, emb-ε) pair.
With N=3 you get three perturbed vectors that still average back to emb.
With any N the ground-truth recall should be high when mean pooling is used.

We also keep an `emb` column (the original normalised embedding) so the
standard framework pipeline has a vector field to read.  endee.py uses
multivec1..multivecN for the actual DB fields; `emb` is only used as the
query vector in test.parquet.

Ground truth is computed as:
  For each test query q (original emb, normalised), find top-K train rows
  ranked by cosine-similarity to normalize(mean(multivec1 .. multivecN)).
  Because the noises cancel, the pooled vector ≈ original emb.

Output files (DEST_DIR):
  train.parquet     — id, emb, multivec1 .. multivecN
  test.parquet      — id, emb  (original query embeddings)
  neighbors.parquet — id, neighbors_id  (top-K ground truth)

Usage:
  1. Edit SOURCE_DIR, DEST_DIR, and NUM_VECTORS at the top.
  2. pip install numpy pandas pyarrow faiss-cpu tqdm
  3. python3 generate_multivec_50k.py
"""

import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import faiss
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_DIR = "/home/debian/ssd/vectordataset/openai/openai_small_50k"

# Destination must follow VectorDBBench's nested convention:
#   DATASET_LOCAL_DIR/<dataset-name>/<dataset-dir>/
# Use the same string for both <dataset-name> and <dataset-dir>.
DEST_DIR = "/home/debian/ssd/vectordataset/multivec_1536d_50k/multivec_1536d_50k"

NUM_VECTORS = 2         # how many vectors to store per document (≥ 2)
DIM         = 1536      # embedding dimension (OpenAI text-embedding-3-small)
TOP_K       = 100       # number of ground-truth neighbours to store
NOISE_SCALE = 0.01      # noise magnitude as a fraction of the embedding's L2 norm
BATCH_SIZE  = 5_000     # rows per write batch (disk I/O)
FAISS_CHUNK = 5_000     # rows per FAISS search pass (RAM safety)
SEED        = 42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize(x: np.ndarray) -> np.ndarray:
    """L2-normalise each row in-place and return the array."""
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    return x / norms


def _make_noise_vectors(emb_batch: np.ndarray, n: int, rng: np.random.Generator) -> list[np.ndarray]:
    """
    Return a list of n L2-normalised (B, D) arrays where each array is
    normalize(emb + noise_i) and the noises are constructed so they sum
    to zero — guaranteeing that mean pooling recovers the original emb.

    Construction:
      noise_1 .. noise_{n-1}  ← independent random perturbations
      noise_n                 ← −(noise_1 + … + noise_{n-1})

    For n=2 this reduces to the classic (emb+ε, emb−ε) pair.
    """
    if n < 2:
        raise ValueError("NUM_VECTORS must be ≥ 2")

    B, D = emb_batch.shape
    noises = []
    noise_sum = np.zeros((B, D), dtype=np.float32)

    for _ in range(n - 1):
        noise = rng.standard_normal((B, D)).astype(np.float32)
        # Normalise each noise vector to unit length first, then scale by
        # NOISE_SCALE.  Without this, ||noise|| ≈ NOISE_SCALE * sqrt(D) which
        # at D=1536 is ~3.9× the embedding norm — noise overwhelms the signal
        # and mean(v_i) collapses to near-zero, destroying ground truth.
        noise /= np.linalg.norm(noise, axis=1, keepdims=True)
        noise *= NOISE_SCALE
        noises.append(noise)
        noise_sum += noise

    # Last noise cancels all previous ones so sum = 0
    noises.append(-noise_sum)

    return [_normalize((emb_batch + ni).copy()) for ni in noises]


# ---------------------------------------------------------------------------
# Step 1 — Build train.parquet
# ---------------------------------------------------------------------------
def create_train_file():
    print(f"\n[1] Building train.parquet  →  {DIM}D  |  noise_scale={NOISE_SCALE}")
    in_path  = os.path.join(SOURCE_DIR, "shuffle_train.parquet")
    out_path = os.path.join(DEST_DIR,   "train.parquet")

    rng = np.random.default_rng(SEED)
    parquet_file = pq.ParquetFile(in_path)
    total_rows   = parquet_file.metadata.num_rows
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE

    writer = None
    try:
        for batch in tqdm(
            parquet_file.iter_batches(batch_size=BATCH_SIZE),
            total=total_batches,
            desc="  Generating noise pairs",
        ):
            df = batch.to_pandas()
            ids  = df["id"].values.astype(np.int64)
            embs = np.vstack(df["emb"].values).astype(np.float32)

            # Normalise source embeddings (they may already be normalised,
            # but we guarantee it here for consistent noise scaling).
            embs = _normalize(embs)

            mvecs = _make_noise_vectors(embs, NUM_VECTORS, rng)

            # Pack all N vectors per document into a single list-of-lists column.
            # Each row: [[v1_d1,...,v1_dD], [v2_d1,...,v2_dD], ...]
            # The DB mean-pools within this single field at search time.
            B = len(ids)
            multivec_col = [[mvecs[n][b].tolist() for n in range(NUM_VECTORS)] for b in range(B)]

            table = pa.table({
                "id":       pa.array(ids,           type=pa.int64()),
                "emb":      pa.array(embs.tolist(), type=pa.list_(pa.float32())),
                "multivec": pa.array(multivec_col,  type=pa.list_(pa.list_(pa.float32()))),
            })

            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()
    print(f"  Saved → {out_path}  ({total_rows:,} rows)")


# ---------------------------------------------------------------------------
# Step 2 — Build test.parquet (pass-through, just copy the query vectors)
# ---------------------------------------------------------------------------
def create_test_file():
    print(f"\n[2] Building test.parquet  (original query embeddings)")
    in_path  = os.path.join(SOURCE_DIR, "test.parquet")
    out_path = os.path.join(DEST_DIR,   "test.parquet")

    df   = pd.read_parquet(in_path)
    embs = np.vstack(df["emb"].values).astype(np.float32)
    embs = _normalize(embs)
    ids  = df["id"].values.astype(np.int64)

    table = pa.table({
        "id":  pa.array(ids,           type=pa.int64()),
        "emb": pa.array(embs.tolist(), type=pa.list_(pa.float32())),
    })
    pq.write_table(table, out_path)
    print(f"  Saved → {out_path}  ({len(ids):,} rows)")

    return embs, ids   # return for use in GT computation


# ---------------------------------------------------------------------------
# Step 3 — Compute ground truth
# ---------------------------------------------------------------------------
def compute_ground_truth(query_embs: np.ndarray, query_ids: np.ndarray):
    """
    For each test query (already L2-normalised), find the top-K train rows
    ranked by cosine similarity to normalize(mean(multivec1 .. multivecN)).

    Because the noises sum to zero, the pooled vector ≈ original emb, so
    ground-truth neighbours match what the DB returns with mean pooling.
    """
    print(f"\n[3] Computing ground truth  (top-{TOP_K}, cosine sim to mean of {NUM_VECTORS} vectors)")

    num_queries = len(query_embs)
    global_distances = np.full((num_queries, TOP_K), -np.inf, dtype=np.float32)
    global_indices   = np.full((num_queries, TOP_K), -1,      dtype=np.int64)

    train_file   = pq.ParquetFile(os.path.join(DEST_DIR, "train.parquet"))
    total_rows   = train_file.metadata.num_rows
    total_chunks = (total_rows + FAISS_CHUNK - 1) // FAISS_CHUNK
    chunk_idx    = 1

    for batch in train_file.iter_batches(batch_size=FAISS_CHUNK):
        print(f"  FAISS pass {chunk_idx}/{total_chunks} …")

        # Read directly from the RecordBatch — to_pylist() correctly
        # converts nested list<list<float>> into plain Python lists,
        # avoiding the conversion issues that arise via pandas.
        ids = np.array(batch.column("id").to_pylist(), dtype=np.int64)
        raw = batch.column("multivec").to_pylist()
        # raw[i] = [[v1_d1,...,v1_dD], [v2_d1,...,v2_dD]] → shape (B, N, D)
        stacked = np.array(raw, dtype=np.float32)
        pooled  = _normalize(stacked.mean(axis=1).copy())

        local_index = faiss.IndexIDMap(faiss.IndexFlatIP(DIM))
        local_index.add_with_ids(pooled, ids)
        local_dist, local_ind = local_index.search(query_embs, TOP_K)

        for q in range(num_queries):
            combined_dist = np.concatenate([global_distances[q], local_dist[q]])
            combined_ind  = np.concatenate([global_indices[q],   local_ind[q]])
            order = np.argsort(combined_dist)[::-1]
            global_distances[q] = combined_dist[order][:TOP_K]
            global_indices[q]   = combined_ind[order][:TOP_K]

        chunk_idx += 1

    out_path = os.path.join(DEST_DIR, "neighbors.parquet")
    neighbors_df = pd.DataFrame({
        "id":           query_ids.tolist(),
        "neighbors_id": global_indices.tolist(),
    })
    neighbors_df.to_parquet(out_path)
    print(f"  Saved → {out_path}  ({len(query_ids):,} queries × top-{TOP_K})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs(DEST_DIR, exist_ok=True)

    create_train_file()
    query_embs, query_ids = create_test_file()
    compute_ground_truth(query_embs, query_ids)

    print(f"\n Multi-vector 50K dataset ready!")
    print(f"  Location    : {DEST_DIR}")
    print(f"  Num vectors : {NUM_VECTORS} per document  (single 'multivec' field)")
    print(f"  Train schema: id | emb | multivec  (multivec = list of {NUM_VECTORS} vectors)")
    print(f"  Test schema : id | emb  (original query embeddings)")
    print(f"  Noise       : {NOISE_SCALE * 100:.0f}% perturbation, noises sum to zero → mean-pool ≈ original")
    print(f"\n  Run script params: --multivec-fields multivec --multivec-count {NUM_VECTORS}")
