#!/bin/bash
# =============================================================================
# Multi-vector benchmark
#
# Run from the repo root. Dataset must be generated first:
#   python3 multi_vector/generate_multivec.py
#
# Each document stores NUM_VECTORS perturbed vectors whose noises sum to zero,
# so mean pooling ≈ the original embedding → high recall expected.
#
# Set NUM_VECTORS to match what you used in the generator script.
# Adjust TOKEN, BASE_URL, DATASET_LOCAL_DIR before running.
# =============================================================================

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/path/to/folder/vectordataset" \
vectordbbench endee \
  --token "TOKEN" \
  --region location \
  --base-url "http://localhost:8080/api/v2" \
  --collection-name multivec_2_fields \
  --task-label "multivec_2_fields" \
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
  --custom-case-name "MultiVec 1536D 50K" \
  --custom-dataset-name multivec_1536d_50k \
  --custom-dataset-dir multivec_1536d_50k \
  --custom-dataset-size 50000 \
  --custom-dataset-dim 1536 \
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


# ============================================
# =============== SEARCH ONLY ================
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/path/to/folder/vectordataset" \
# vectordbbench endee \
#   --token "TOKEN" \
#   --region location \
#   --base-url "http://localhost:8080/api/v2" \
#   --collection-name multivec_2_fields \
#   --task-label "multivec_2_fields" \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision int16 \
#   --field-type multi_vector \
#   --multivec-fields multivec \
#   --multivec-count 2 \
#   --multivec-pooling mean \
#   --case-type PerformanceCustomDataset \
#   --custom-case-name "MultiVec 1536D 50K" \
#   --custom-dataset-name multivec_1536d_50k \
#   --custom-dataset-dir multivec_1536d_50k \
#   --custom-dataset-size 50000 \
#   --custom-dataset-dim 1536 \
#   --custom-dataset-metric-type COSINE \
#   --custom-dataset-file-count 1 \
#   --custom-dataset-with-gt \
#   --k 30 \
#   --num-concurrency "8" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --skip-drop-old \
#   --skip-load \
#   --search-concurrent \
#   --search-serial