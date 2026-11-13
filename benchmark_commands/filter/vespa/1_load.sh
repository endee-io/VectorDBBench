#!/bin/bash
#   Load once: this one collection serves the label filters, the int filters and the plain dense search
#   --label-field keeps the label field in Vespa's schema - needed when int filters / dense search run on this collection

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench vespa \
  --uri http://172.31.14.84 \
  --port 8080 \
  --config-port 19071 \
  --collection-name v1m_filter_bfloat16 \
  --m 16 \
  --ef-construction 128 \
  --ef-search 128 \
  --quantization bfloat16 \
  --label-field \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_vespa_filter_load \
  --db-label vespa_filter \
  --drop-old \
  --load \
  --search-serial \
  --search-concurrent
