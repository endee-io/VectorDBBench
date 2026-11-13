#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench vespa \
  --uri http://172.31.14.84 \
  --port 8080 \
  --config-port 19071 \
  --collection-name v1m_float \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --quantization none \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_vespa_float_load \
  --db-label vespa_float \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench vespa \
#   --uri http://172.31.14.84 \
#   --port 8080 \
#   --config-port 19071 \
#   --collection-name v1m_float \
#   --m 16 \
#   --ef-construction 128 \
#   --ef-search 128 \
#   --quantization none \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_vespa_float_search \
#   --db-label vespa_float \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
