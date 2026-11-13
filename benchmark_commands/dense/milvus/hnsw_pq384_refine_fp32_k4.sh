#!/bin/bash
#   Search only: refine_k 4 is a search setting - load hnsw_pq384_refine_fp32.sh first, this searches its collection

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvushnswpq \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_hnsw_pq384_refine_fp32 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --pq-m 384 \
  --nbits 8 \
  --refine true \
  --refine-type fp32 \
  --refine-k 4 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_hnsw_pq384_refine_fp32_k4_search \
  --db-label milvus_hnsw_pq384_refine_fp32_k4 \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
