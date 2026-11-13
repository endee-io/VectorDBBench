#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvusivfflat \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_ivf_flat \
  --lists 1024 \
  --probes 64 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_ivf_flat_load \
  --db-label milvus_ivf_flat \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench milvusivfflat \
#   --uri http://172.31.14.84:19530 \
#   --collection-name m1m_ivf_flat \
#   --lists 1024 \
#   --probes 64 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_milvus_ivf_flat_search \
#   --db-label milvus_ivf_flat \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
