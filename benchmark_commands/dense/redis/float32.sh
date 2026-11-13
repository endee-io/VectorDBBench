#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench redis \
  --host 172.31.14.84 \
  --port 6379 \
  --password 1234 \
  --no-ssl \
  --cmd \
  --collection-name r1m_float32 \
  --m 16 \
  --ef-construction 128 \
  --ef-runtime 128 \
  --vector-type FLOAT32 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_redis_float32_load \
  --db-label redis_float32 \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench redis \
#   --host 172.31.14.84 \
#   --port 6379 \
#   --password 1234 \
#   --no-ssl \
#   --cmd \
#   --collection-name r1m_float32 \
#   --m 16 \
#   --ef-construction 128 \
#   --ef-runtime 128 \
#   --vector-type FLOAT32 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_redis_float32_search \
#   --db-label redis_float32 \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
