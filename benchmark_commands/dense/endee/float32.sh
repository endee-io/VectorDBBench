#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench endee \
  --token root/bench:1234 \
  --region location \
  --base-url http://172.31.14.84:8080/api/v2 \
  --collection-name e1M_float32 \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision float32 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_endee_float32_load \
  --db-label endee_float32 \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench endee \
#   --token root/bench:1234 \
#   --region location \
#   --base-url http://172.31.14.84:8080/api/v2 \
#   --collection-name e1M_float32 \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision float32 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_endee_float32_search \
#   --db-label endee_float32 \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
