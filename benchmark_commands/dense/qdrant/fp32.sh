#!/bin/bash
#   --prefer-grpc true = gRPC (use this: much faster);  --prefer-grpc false = REST (slow at top_k 30)

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench qdrantlocal \
  --url http://172.31.14.84:6333 \
  --collection-name q1m_fp32 \
  --m 16 \
  --ef-construct 128 \
  --hnsw-ef 128 \
  --prefer-grpc true \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_qdrant_fp32_load \
  --db-label qdrant_fp32 \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent


# ============================================
# ============== SEARCH ONLY ===============
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
# vectordbbench qdrantlocal \
#   --url http://172.31.14.84:6333 \
#   --collection-name q1m_fp32 \
#   --m 16 \
#   --ef-construct 128 \
#   --hnsw-ef 128 \
#   --prefer-grpc true \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_qdrant_fp32_search \
#   --db-label qdrant_fp32 \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
