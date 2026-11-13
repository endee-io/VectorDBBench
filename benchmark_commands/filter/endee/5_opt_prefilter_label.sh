#!/bin/bash
#   Endee optimisation: prefilter threshold for LABEL filters
#   Below this many matching vectors Endee searches the matches exactly (default 10000).
#   Run with --label-percentage 0.01, 0.02, 0.05  and  --prefilter-cardinality-threshold 100000

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/admin/vectordataset" \
vectordbbench endee \
  --token root/bench:1234 \
  --region location \
  --base-url http://172.31.14.84:8080/api/v2 \
  --collection-name e1m_int16_labels \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision int16 \
  --prefilter-cardinality-threshold 100000 \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_endee_filter_prefilter_label_label_0.01 \
  --db-label endee_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
