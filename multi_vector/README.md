# Multi-Vector Benchmarking

---

## What is Multi-Vector?

Normally each document has **one vector**. Multi-vector means each document stores **N vectors** in a single field. When you search, the database averages those N vectors (mean pooling) and finds the closest match to your query.

The goal of this benchmark is to verify that the database's mean pooling gives the same results as if you had just stored the original single vector - i.e., recall should be close to dense.

---

## Files Changed

### New files (all inside `multi_vector/`)

| File | Purpose |
|------|---------|
| `generate_multivec.py` | Creates the multi-vector dataset from an existing dense dataset |
| `verify_multivec_dataset.py` | Checks the generated dataset is mathematically correct, independent of the DB |
| `run_multivec.sh` | Run script for the benchmark |

### Modified files

| File | What changed |
|------|-------------|
| `vectordb_bench/backend/runner/concurrent_runner.py` | Detects extra vector columns in the dataset and passes them through to the DB client during insert |
| `vectordb_bench/backend/clients/endee/endee.py` | Handles inserting and searching multi-vector fields |

---

## How the Dataset is Generated

### What is noise?

A noise vector is just a random direction in the same vector space as your embeddings. Think of it like adding a small random nudge to your original vector.

When you add noise to a vector and then normalise it, you get a new unit vector that points in a slightly different direction - but is still close to the original.

### The process

Say you have an original embedding `emb` for a document, and you want to store 2 vectors per document (N=2):

1. **Generate a random noise vector** with a small magnitude (controlled by `NOISE_SCALE`)
2. **Add the noise** to get `v1 = normalize(emb + noise)`
3. **Subtract the same noise** to get `v2 = normalize(emb - noise)`
4. Store `[v1, v2]` in the `multivec` field

Now when the DB mean-pools at search time:

```
mean(v1, v2) = mean(normalize(emb + noise), normalize(emb - noise)) ≈ emb
```

The noises cancel out and the average points back to the original direction. So searching with the original query vector against the mean-pooled document vectors should give the same results as plain dense search.

For N > 2, the same idea extends: generate N-1 independent random noises, make the last noise equal to `-(sum of all others)` so they all sum to zero. Mean pooling still recovers the original.

### Why normalise the noise first?

In high dimensions (e.g. D=1536), a raw random vector has magnitude `≈ sqrt(D) ≈ 39`. If you just scale it by `NOISE_SCALE=0.10`, the noise magnitude is still `0.10 * 39 = 3.9` - nearly 4x the embedding's magnitude of 1. The noise completely overwhelms the signal and the stored vectors point in random directions.

The fix: normalise the random vector to unit length first, then scale by `NOISE_SCALE`. Now the noise magnitude is exactly `0.10` regardless of dimension.

### Choosing NOISE_SCALE

`NOISE_SCALE` controls how far each stored vector is from the original. Formula: `cosine(emb, v_i) = 1 / sqrt(1 + NOISE_SCALE²)`

| NOISE_SCALE | cosine(emb, v_i) | What it means |
|---|---|---|
| 0.01 | ~0.9999 | Vectors almost identical to original |
| 0.10 | ~0.995  | 10% off - good default |
| 0.30 | ~0.958  | Noticeably different vectors |
| 1.00 | ~0.707  | 45° away from original |
| 3.00+ | ~0.3   | Near-random direction |

Recommended range: **0.05 – 0.50**. Too small and it's not a meaningful multi-vector test. Too large and the stored vectors are so far from the original that HNSW struggles to build a good index.

---

## Step 1 - Generate the Dataset

Edit the config block at the top of `generate_multivec.py`:

```python
SOURCE_DIR  = "/path/to/source/dense/dataset"   # must contain shuffle_train.parquet and test.parquet
DEST_DIR    = "/path/to/vectordataset/multivec_768d_1m/multivec_768d_1m"
NUM_VECTORS = 2        # how many vectors to store per document (≥ 2)
DIM         = 768      # embedding dimension of your source dataset
NOISE_SCALE = 0.10     # noise magnitude - see table above
TOP_K       = 100      # how many ground-truth neighbours to store per query
```

Then run:

```bash
python3 multi_vector/generate_multivec.py
```

Three files are written to `DEST_DIR`:

```
train.parquet      - one row per document: id, emb (original), multivec (N vectors)
test.parquet       - one row per query: id, emb (original query vector)
neighbors.parquet  - top-K nearest train IDs for each query (ground truth)
```

> **Note on `emb` in train.parquet:** VectorDBBench requires every dataset to have a standard vector field (`emb`), so it is included. For multi-vector runs, `emb` is not inserted into the DB — the insert pipeline uses only the `multivec` column. For test queries, `emb` is used as the query vector as usual.

> `DEST_DIR` must follow VectorDBBench's nested convention: `DATASET_LOCAL_DIR/<name>/<name>/` - use the same string for both levels.

---

## Step 2 - Verify the Dataset (Optional but Recommended)

```bash
python3 multi_vector/verify_multivec_dataset.py
```

Runs 4 checks without touching the DB:

1. **Schema** - all columns exist, shapes are correct, IDs match across files
2. **Unit norm** - every stored vector is properly L2-normalised
3. **Noise cancellation** - `cosine(original_emb, mean(v1..vN)) > 0.99` confirming noises cancel
4. **Ground truth** - brute-force exact search matches `neighbors.parquet` at recall > 0.99

If all 4 pass, the dataset is correct and any recall gap in the benchmark is purely HNSW approximation.

---

## Step 3 - Run the Benchmark

```bash
NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/path/to/vectordataset" \
vectordbbench endee \
  --token "YOUR_TOKEN" \
  --base-url "http://localhost:8080/api/v2" \
  --collection-name multivec_768d_1m \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision int16 \
  --field-type multi_vector \
  --multivec-fields multivec \
  --multivec-count 2 \
  --multivec-pooling mean \
  --case-type PerformanceCustomDataset \
  --custom-dataset-name multivec_768d_1m \
  --custom-dataset-dir multivec_768d_1m \
  --custom-dataset-size 1000000 \
  --custom-dataset-dim 768 \
  --custom-dataset-metric-type COSINE \
  --custom-dataset-file-count 1 \
  --custom-dataset-with-gt \
  --k 30 \
  --num-concurrency "8" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --drop-old \
  --load \
  --search-concurrent \
  --search-serial
```

### Key flags

| Flag | Description |
|------|-------------|
| `--field-type multi_vector` | Switch the client to multi-vector mode (instead of default dense) |
| `--multivec-fields multivec` | The field name in the DB - must match the column name in `train.parquet` |
| `--multivec-count 2` | Number of vectors per document - must match `NUM_VECTORS` in the generator |
| `--multivec-pooling mean` | How the DB combines stored vectors at search time |
| `--custom-dataset-with-gt` | Load `neighbors.parquet` to compute recall |

### Search-only (skip re-inserting data)

Replace `--drop-old --load` with `--skip-drop-old --skip-load`.

---

## Compatibility

The generator is universal - it works with any dimension and any dataset size. The only requirement is that the source dataset has `shuffle_train.parquet` and `test.parquet` files with `id` and `emb` columns.

**Tested datasets:**

| Dataset | Rows | Dim | Status |
|---------|------|-----|--------|
| OpenAI Small 50K | 50,000 | 1536 | Verified |
| Cohere Medium 1M | 1,000,000 | 768 | Compatible - `id` + `emb` columns present, both list and large_list emb types are handled |

When switching datasets, only these values need to change:

| Where | What to update |
|-------|---------------|
| `generate_multivec.py` config | `SOURCE_DIR`, `DIM`, `DEST_DIR` |
| Run command | `--custom-dataset-name`, `--custom-dataset-dir`, `--custom-dataset-size`, `--custom-dataset-dim` |
