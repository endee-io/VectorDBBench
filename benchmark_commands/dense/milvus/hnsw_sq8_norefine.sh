#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvushnswsq \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_hnsw_sq8_norefine \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --sq-type sq8 \
  --refine false \
  --refine-type fp32 \
  --refine-k 1 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_hnsw_sq8_norefine_load \
  --db-label milvus_hnsw_sq8_norefine \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench milvushnswsq \
#   --uri http://172.31.14.84:19530 \
#   --collection-name m1m_hnsw_sq8_norefine \
#   --m 16 \
#   --ef-construction 128 \
#   --ef-search 128 \
#   --sq-type sq8 \
#   --refine false \
#   --refine-type fp32 \
#   --refine-k 1 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_milvus_hnsw_sq8_norefine_search \
#   --db-label milvus_hnsw_sq8_norefine \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
