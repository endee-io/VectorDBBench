import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import os

base_path = "/home/debian/latest_VDB/VectorDBBench/vectordataset"
dataset_folder = "cohere/cohere_medium_1m"
dataset_path = os.path.join(base_path, dataset_folder)

# Output path - new folder for LT ground truth
lt_base_path = "/home/debian/latest_VDB/VectorDBBench/vectordataset_lt"
lt_dataset_path = os.path.join(lt_base_path, dataset_folder)
os.makedirs(lt_dataset_path, exist_ok=True)

# ---- INPUTS ----
int_rate = "99p"   # change to 1p, 50p, 80p, 99p
top_k = 1000

# LT threshold map (strictly less than)
# LT "1p":  id < 9999  → ~9,999 vectors pass  (1 fewer than LTE id <= 9999)
# LT "99p": id < 989999 → ~989,999 vectors pass (1 fewer than LTE id <= 989999)
lt_map = {
    "1p":   9999,
    "50p":  499999,
    "80p":  799999,
    "99p":  989999,
}
lt_threshold = lt_map[int_rate]
print(f"Filter: id < {lt_threshold} ({int_rate})")

# ---- STEP 1: Load filtered train vectors ----
print("Loading filtered train vectors...")
train_file = pq.ParquetFile(os.path.join(dataset_path, "shuffle_train.parquet"))

filtered_ids = []
filtered_vecs = []

for batch in train_file.iter_batches(batch_size=10000):
    df = batch.to_pandas()
    df = df[df["id"] < lt_threshold]  # Apply LT filter (strictly less than)
    if not df.empty:
        filtered_ids.extend(df["id"].tolist())
        filtered_vecs.extend(df["emb"].tolist())

filtered_ids = np.array(filtered_ids)
filtered_vecs = np.array(filtered_vecs, dtype=np.float32)

# Normalize for cosine similarity
norms = np.linalg.norm(filtered_vecs, axis=1, keepdims=True)
filtered_vecs = filtered_vecs / norms

print(f"Filtered vectors: {len(filtered_ids)} (expected ~{lt_threshold})")

# ---- STEP 2: Load test queries ----
print("Loading test queries...")
test_df = pq.ParquetFile(os.path.join(dataset_path, "test.parquet")).read().to_pandas()
test_vecs = np.array(test_df["emb"].tolist(), dtype=np.float32)

# Normalize test queries too
norms = np.linalg.norm(test_vecs, axis=1, keepdims=True)
test_vecs = test_vecs / norms

print(f"Test queries: {len(test_df)}")

# ---- STEP 3: Brute force exact nearest neighbors ----
print("Computing exact nearest neighbors (brute force)...")
results = []

CHUNK = 100  # Process test queries in chunks to save memory
for i in range(0, len(test_vecs), CHUNK):
    chunk = test_vecs[i:i+CHUNK]
    # Cosine similarity = dot product (since both are normalized)
    sims = chunk @ filtered_vecs.T  # (CHUNK, num_filtered)
    top_k_idx = np.argsort(-sims, axis=1)[:, :top_k]
    top_k_ids = filtered_ids[top_k_idx]
    results.extend(top_k_ids.tolist())
    print(f"  Processed {min(i+CHUNK, len(test_vecs))}/{len(test_vecs)}")

# ---- STEP 4: Save as parquet ----
output_df = test_df[["id"]].copy()
output_df["neighbors_id"] = results

table = pa.Table.from_pandas(output_df, preserve_index=False)
output_path = os.path.join(lt_dataset_path, f"neighbors_int_{int_rate}.parquet")
pa.parquet.write_table(table, output_path)
print(f"\n✅ Saved: {output_path}")
print(f"Shape: {output_df.shape}")
print(f"Sample (first row): {output_df['neighbors_id'].iloc[0][:5]}")
