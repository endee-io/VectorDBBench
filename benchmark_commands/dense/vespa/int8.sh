#!/bin/bash
#   --int8-max-abs 5.5046 = largest absolute value in the Cohere 1M vectors (needed for int8)

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench vespa \
  --uri http://172.31.14.84 \
  --port 8080 \
  --config-port 19071 \
  --collection-name v1m_int8 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --quantization int8 \
  --int8-max-abs 5.5046 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_vespa_int8_load \
  --db-label vespa_int8 \
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
#   --collection-name v1m_int8 \
#   --m 16 \
#   --ef-construction 128 \
#   --ef-search 128 \
#   --quantization int8 \
#   --int8-max-abs 5.5046 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_vespa_int8_search \
#   --db-label vespa_int8 \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
