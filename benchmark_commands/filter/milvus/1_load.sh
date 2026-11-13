#!/bin/bash
#   Load once: this one collection serves the label filters, the int filters and the plain dense search

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench milvushnsw \
  --uri http://172.31.14.84:19530 \
  --collection-name m1m_filter_hnsw_vecfield_bf16 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --vector-dtype bfloat16 \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_milvus_filter_load \
  --db-label milvus_filter \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent
