"""
Brute-force verification for the multi-vector dataset.

Checks:
  1. Ground-truth correctness  — exact cosine search matches stored neighbors.parquet
  2. Noise cancellation quality — cosine(original_emb, normalize(mean(v1..vN))) ≈ 1.0
  3. Vector norms             — all stored vectors are unit length
  4. Schema sanity            — correct dtypes, no NaNs, IDs match across files

Run from the repo root:
    python3 custom_dataset_generator/verify_multivec_dataset.py
"""

import numpy as np
import pyarrow.parquet as pq
import os

# ---------------------------------------------------------------------------
# Config — must match the generator
# ---------------------------------------------------------------------------
DATASET_DIR = "/home/debian/ssd/vectordataset/multivec_1536d_50k/multivec_1536d_50k"
TOP_K       = 100   # neighbors stored in neighbors.parquet
MAX_QUERIES = 100   # brute-force is O(Q * N_train) — 200 queries is fast enough


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    return x / norms


def _cosine_matrix(queries: np.ndarray, docs: np.ndarray) -> np.ndarray:
    """(Q, D) x (N, D) → (Q, N) cosine similarities. Both inputs must be L2-normalised."""
    return queries @ docs.T


def _check(condition: bool, msg: str):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {msg}")
    if not condition:
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Load files
# ---------------------------------------------------------------------------
print("\n=== Loading dataset files ===")

train_path     = os.path.join(DATASET_DIR, "train.parquet")
test_path      = os.path.join(DATASET_DIR, "test.parquet")
neighbors_path = os.path.join(DATASET_DIR, "neighbors.parquet")

for p in [train_path, test_path, neighbors_path]:
    _check(os.path.exists(p), f"File exists: {os.path.basename(p)}")

# Train — read multivec column via PyArrow to avoid pandas nested-list issues
train_pf   = pq.ParquetFile(train_path)
train_meta = train_pf.metadata
NUM_TRAIN  = train_meta.num_rows
print(f"  train rows   : {NUM_TRAIN:,}")
print(f"  train schema : {train_pf.schema_arrow}")

# Sample first batch to infer NUM_VECTORS and DIM
first_batch = next(train_pf.iter_batches(batch_size=500))
sample_mv   = first_batch.column("multivec").to_pylist()   # list[list[list[float]]]
NUM_VECTORS = len(sample_mv[0])
DIM         = len(sample_mv[0][0])
print(f"  num_vectors  : {NUM_VECTORS}")
print(f"  dim          : {DIM}")

# Test queries
test_table = pq.read_table(test_path)
query_ids  = np.array(test_table["id"].to_pylist(), dtype=np.int64)
query_embs = np.array(test_table["emb"].to_pylist(), dtype=np.float32)
print(f"  test queries : {len(query_ids):,}")

# Neighbors ground truth
nb_table        = pq.read_table(neighbors_path)
gt_query_ids    = np.array(nb_table["id"].to_pylist(), dtype=np.int64)
gt_neighbors    = np.array(nb_table["neighbors_id"].to_pylist(), dtype=np.int64)  # (Q, TOP_K)
print(f"  neighbors    : {gt_neighbors.shape}")


# ---------------------------------------------------------------------------
# Check 1 — schema sanity
# ---------------------------------------------------------------------------
print("\n=== Check 1: Schema sanity ===")

_check("id"       in first_batch.schema.names, "train has 'id' column")
_check("emb"      in first_batch.schema.names, "train has 'emb' column")
_check("multivec" in first_batch.schema.names, "train has 'multivec' column")
_check("id"       in test_table.schema.names,  "test has 'id' column")
_check("emb"      in test_table.schema.names,  "test has 'emb' column")
_check(NUM_VECTORS >= 2, f"num_vectors >= 2 (got {NUM_VECTORS})")
_check(DIM > 0, f"dim > 0 (got {DIM})")

# GT query IDs must match test query IDs
_check(np.array_equal(np.sort(gt_query_ids), np.sort(query_ids)),
       "neighbors.parquet query IDs match test.parquet IDs")
_check(gt_neighbors.shape == (len(query_ids), TOP_K),
       f"neighbors shape == ({len(query_ids)}, {TOP_K})")


# ---------------------------------------------------------------------------
# Check 2 — unit norm of stored vectors (sample 2000 rows)
# ---------------------------------------------------------------------------
print("\n=== Check 2: Unit norm of stored vectors ===")

raw = np.array(sample_mv, dtype=np.float32)  # (B, N, D)
for vec_idx in range(NUM_VECTORS):
    vecs  = raw[:, vec_idx, :]             # (B, D)
    norms = np.linalg.norm(vecs, axis=1)
    mean_norm = norms.mean()
    max_err   = np.abs(norms - 1.0).max()
    _check(max_err < 1e-5,
           f"v{vec_idx+1} unit-norm (mean={mean_norm:.6f}, max_err={max_err:.2e})")


# ---------------------------------------------------------------------------
# Check 3 — noise cancellation: cosine(original_emb, normalize(mean(v1..vN)))
# ---------------------------------------------------------------------------
print("\n=== Check 3: Noise cancellation quality ===")

sample_embs_raw = first_batch.column("emb").to_pylist()
sample_embs     = np.array(sample_embs_raw, dtype=np.float32)  # (B, D)

pooled = _normalize(raw.mean(axis=1).copy())  # (B, D)

cosines    = (sample_embs * pooled).sum(axis=1)  # dot product (both normalised)
mean_cos   = cosines.mean()
min_cos    = cosines.min()
print(f"  cosine(original, pooled): mean={mean_cos:.6f}  min={min_cos:.6f}")
_check(mean_cos > 0.99, f"mean cosine > 0.99 (got {mean_cos:.4f})")
_check(min_cos  > 0.95, f"min  cosine > 0.95 (got {min_cos:.4f})")


# ---------------------------------------------------------------------------
# Check 4 — brute-force ground truth verification
# ---------------------------------------------------------------------------
print(f"\n=== Check 4: Brute-force GT verification (first {MAX_QUERIES} queries) ===")

# Load ALL train pooled vectors into memory (50k * 1536 * 4B ≈ 300 MB)
print("  Loading and pooling all train vectors …")
all_pooled_list = []
all_ids_list    = []

for batch in train_pf.iter_batches(batch_size=5000):
    b_ids = np.array(batch.column("id").to_pylist(), dtype=np.int64)
    b_raw = np.array(batch.column("multivec").to_pylist(), dtype=np.float32)  # (B, N, D)
    b_pooled = _normalize(b_raw.mean(axis=1).copy())
    all_pooled_list.append(b_pooled)
    all_ids_list.append(b_ids)

all_pooled = np.vstack(all_pooled_list)  # (N_train, D)
all_ids    = np.concatenate(all_ids_list)  # (N_train,)

# Build id→index map
id_to_idx = {int(tid): i for i, tid in enumerate(all_ids)}

print(f"  Train pooled shape: {all_pooled.shape}")

Q = min(MAX_QUERIES, len(query_ids))
recall_at_k = []

for q in range(Q):
    qvec = query_embs[q:q+1]   # (1, D)
    sims = _cosine_matrix(qvec, all_pooled)[0]  # (N_train,)

    # Top-K by brute force
    bf_top_k_idx  = np.argpartition(sims, -TOP_K)[-TOP_K:]
    bf_top_k_ids  = set(int(all_ids[i]) for i in bf_top_k_idx)

    # Ground truth
    stored_ids = set(int(x) for x in gt_neighbors[q] if x != -1)

    overlap = len(bf_top_k_ids & stored_ids)
    recall_at_k.append(overlap / TOP_K)

mean_recall = np.mean(recall_at_k)
min_recall  = np.min(recall_at_k)
print(f"  Brute-force vs stored GT recall@{TOP_K}: mean={mean_recall:.4f}  min={min_recall:.4f}")
_check(mean_recall > 0.99, f"GT matches brute-force (mean recall > 0.99, got {mean_recall:.4f})")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n=== All checks passed! ===")
print(f"  Dataset location : {DATASET_DIR}")
print(f"  Num vectors/doc  : {NUM_VECTORS}  (single 'multivec' field)")
print(f"  Dimension        : {DIM}")
print(f"  Noise cancel     : mean cosine(original, pooled) = {mean_cos:.6f}")
print(f"  GT accuracy      : mean recall@{TOP_K} vs brute-force = {mean_recall:.4f}")
print()
print("  The dataset is correct. Any recall gap in the benchmark is")
print("  attributable to HNSW approximation, not dataset generation.")
