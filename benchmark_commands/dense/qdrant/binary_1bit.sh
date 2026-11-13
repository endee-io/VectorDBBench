#!/bin/bash
#   --prefer-grpc true = gRPC (use this: much faster);  --prefer-grpc false = REST (slow at top_k 30)

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench qdrantlocal \
  --url http://172.31.14.84:6333 \
  --collection-name q1m_binary_1bit \
  --m 16 \
  --ef-construct 128 \
  --hnsw-ef 128 \
  --quantization binary \
  --prefer-grpc true \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_qdrant_binary_1bit_load \
  --db-label qdrant_binary_1bit \
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
#   --collection-name q1m_binary_1bit \
#   --m 16 \
#   --ef-construct 128 \
#   --hnsw-ef 128 \
#   --quantization binary \
#   --prefer-grpc true \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency 16 \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --task-label 1m_qdrant_binary_1bit_search \
#   --db-label qdrant_binary_1bit \
#   --skip-drop-old \
#   --skip-load \
#   --search-serial \
#   --search-concurrent
