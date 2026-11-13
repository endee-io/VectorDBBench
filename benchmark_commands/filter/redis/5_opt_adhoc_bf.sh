#!/bin/bash
#   Redis optimisation: HYBRID_POLICY ADHOC_BF (exact search over the matches; default = Redis chooses)
#   Only useful for small filtered sets: run with label 0.05 0.02 0.01 0.002 0.001 and int 0.99

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
  --hybrid-policy ADHOC_BF \
  --case-type LabelFilterPerformanceCase \
  --label-percentage 0.01 \
  --dataset-with-size-type "Medium Cohere (768dim, 1M)" \
  --k 30 \
  --num-concurrency 16 \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --task-label 1m_redis_filter_adhoc_bf_label_0.01 \
  --db-label redis_filter \
  --skip-drop-old \
  --skip-load \
  --search-serial \
  --search-concurrent
