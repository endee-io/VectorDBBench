#!/bin/bash
#   --filter-rate = share of vectors REMOVED (0.99 = only 1 % of the vectors are searched)
#   Values the team ran: 0.01 0.5 0.8 0.99

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
  --case-type NewIntFilterPerformanceCase \
  --filter-rate 0.99 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_endee_filter_int_0.99 \
  --db-label endee_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
