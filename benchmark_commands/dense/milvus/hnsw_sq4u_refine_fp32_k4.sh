#!/bin/bash
#   --cosine-via-ip is needed for SQ4U: vectors are normalised and searched with IP (without it recall collapses)
#   Search only: refine_k 4 is a search setting - load hnsw_sq4u_refine_fp32.sh first, this searches its collection

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvushnswsq \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_hnsw_sq4u_refine_fp32 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --sq-type sq4u \
  --refine true \
  --refine-type fp32 \
  --cosine-via-ip \
  --refine-k 4 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_hnsw_sq4u_refine_fp32_k4_search \
  --db-label milvus_hnsw_sq4u_refine_fp32_k4 \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
