#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvushnsw \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_filter_hnsw_vecfield_bf16 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --vector-dtype bfloat16 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_filter_dense \
  --db-label milvus_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
