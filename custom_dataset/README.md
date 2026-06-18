# Custom Dataset Usage with VectorDBBench

This document explains how to prepare and use a custom dataset with `vectordbbench`.

## Dataset Directory Structure

The dataset must follow this structure and file naming convention exactly (`train.parquet`, `test.parquet`, and `neighbors.parquet`):

```text
custom_dataset_name/
└── custom_dataset_dir/
    ├── train.parquet
    ├── test.parquet
    └── neighbors.parquet
```

---

# Required Dataset Files

## 1. train.parquet

Contains the base vectors used for indexing.

Example schema:

```python
id: int64
emb: list<float>
```

* Total rows = dataset size
* Vector dimension must match `--custom-dataset-dim`

---

## 2. test.parquet

Contains query vectors used during search benchmarking.

Example schema:

```python
id: int64
emb: list<float>
```

---

## 3. neighbors.parquet

Contains ground truth nearest neighbors for each query vector.

Example schema:

```python
id: int64
neighbors_id: list<int64>
```

Required only when:

```bash
--custom-dataset-with-gt
```

is enabled.

---

## Dataset Generation Scripts

If you need to test lower dimensions (e.g., slicing a 768D dataset down to 512D) or format an existing dataset to match VectorDBBench's strict requirements, use the provided generation scripts. These scripts slice the vectors, rename the files correctly, and recalculate the exact Ground Truth (neighbors) using FAISS in a low-RAM, memory-safe way.

### 1. `generate_single_file_dataset.py`
Use this script for datasets that consist of a **single** training file (e.g., 50K, 500K, 1M).
* **Input:** `shuffle_train.parquet`, `test.parquet`
* **Output:** `train.parquet`, `test.parquet`, `neighbors.parquet`

### 2. `generate_chunked_dataset.py`
Use this script for massive datasets that are partitioned into **multiple** chunks (e.g., 10M, 100M).
* **Input:** `shuffle_train-00-of-10.parquet`, `test.parquet`
* **Output:** `train.parquet`, `test.parquet`, `neighbors.parquet`

**How to Use:**
1. Open the script you need to use.
2. Edit the `SOURCE_DIR`, `DEST_DIR`, and `TARGET_DIM` variables at the top of the file.
3. Run the script: `python3 10m_dataset_generator.py`

---

# Example Benchmark Command for Endee VectorDB:

```bash
NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/debian/ssd/vectordataset" \
vectordbbench endee \
  --token "TOKEN" \
  --region location \
  --base-url "http://localhost:8080/api/v1" \
  --index-name 10M_int16_1 \
  --task-label "10M_int16_1" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision int16 \
  --version 1 \
  --case-type PerformanceCustomDataset \
  --custom-case-name "Custom 512D 10M" \
  --custom-dataset-name custom_512d_10m \
  --custom-dataset-dir custom_512d_10m \
  --custom-dataset-size 10000000 \
  --custom-dataset-dim 512 \
  --custom-dataset-metric-type COSINE \
  --custom-case-load-timeout 360000 \
  --custom-case-optimize-timeout 360000 \
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

---

# Custom Dataset Parameters

| Parameter                      | Description                            |
| ------------------------------ | -------------------------------------- |
| `--custom-dataset-name`        | Dataset identifier                     |
| `--custom-dataset-dir`         | Dataset directory name                 |
| `--custom-dataset-size`        | Number of train vectors                |
| `--custom-dataset-dim`         | Vector dimension                       |
| `--custom-dataset-metric-type` | Distance metric (`COSINE`, `L2`, etc.) |
| `--custom-dataset-file-count`  | Number of dataset files                |
| `--custom-dataset-with-gt`     | Enables ground truth evaluation        |

---
