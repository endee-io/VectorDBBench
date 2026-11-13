#!/bin/bash
#   Load once: this one collection serves the label filters, the int filters and the plain dense search

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench redis \
  --host 172.31.14.84 \
  --port 6379 \
  --password 1234 \
  --no-ssl \
  --cmd \
  --collection-name r1m_filter_float16 \
  --m 16 \
  --ef-construction 128 \
  --ef-runtime 128 \
  --vector-type FLOAT16 \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_redis_filter_load \
  --db-label redis_filter \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent
