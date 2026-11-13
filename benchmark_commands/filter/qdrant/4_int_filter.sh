#!/bin/bash
#   --filter-rate = share of vectors REMOVED (0.99 = only 1 % of the vectors are searched)
#   Values the team ran: 0.01 0.5 0.8 0.99
#   --prefer-grpc true = gRPC (use this: much faster);  --prefer-grpc false = REST (slow at top_k 30)

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench qdrantlocal \
  --url http://172.31.14.84:6333 \
  --collection-name q1m_filter_turbo16 \
  --m 16 \
  --ef-construct 128 \
  --hnsw-ef 128 \
  --datatype turbo16 \
  --prefer-grpc true \
  --case-type NewIntFilterPerformanceCase \
  --filter-rate 0.99 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_qdrant_filter_int_0.99 \
  --db-label qdrant_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
