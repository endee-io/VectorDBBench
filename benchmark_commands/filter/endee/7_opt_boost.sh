#!/bin/bash
#   Endee optimisation: filter boost (widens the search when a filter is applied; 0 = normal run)
#   Run with --filter-boost-percentage 25, 50, 75, 100 for every label and int rate
#   (for an int filter: replace the two label lines by the two lines of int_filter.sh)

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
  --filter-boost-percentage 50 \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_endee_filter_boost_label_0.01 \
  --db-label endee_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
