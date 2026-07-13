#!/bin/bash

# turbovec is an embedded, in-process index (no server to point at), so
# --path is where VectorDBBench persists the .tvim index file between its
# load / optimize / search subprocesses. Swap --case-type for a bigger
# dataset (e.g. Performance768D1M, Performance1536D5M, Performance768D10M) -
# see `vectordbbench turbovec --help` for the full list.

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/User/vectordataset" \
vectordbbench turbovec \
  --bit-width 4 \
  --path "/home/User/VectorDBBench/vectordbbench_turbovec" \
  --collection-name "turbovec_bench" \
  --task-label "task_detail" \
  --case-type Performance1536D50K \
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

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/User/vectordataset" \
# vectordbbench turbovec \
#   --bit-width 4 \
#   --path "/home/User/VectorDBBench/vectordbbench_turbovec" \
#   --collection-name "turbovec_bench" \
#   --task-label "task_detail" \
#   --case-type Performance1536D50K \
#   --k 30 \
#   --num-concurrency "8" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --skip-drop-old \
#   --skip-load \
#   --search-concurrent \
#   --search-serial
