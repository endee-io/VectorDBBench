#!/bin/bash

NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/launchx/vectordataset" \
vectordbbench endee \
  --token "radxa_benchmark:ndH3I4a4HrWzDIblkQPf4dn53x4hOBA7" \
  --region bellandur \
  --base-url "http://172.16.23.51:8080/api/v2" \
  --collection-name 2M_index \
  --task-label "2M_run5" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision int16 \
  --case-type PerformanceCustomDataset \
  --custom-case-name "Custom 768D 2M" \
  --custom-dataset-name custom_768d_2m \
  --custom-dataset-dir custom_768d_2m \
  --custom-dataset-size 2000000 \
  --custom-dataset-dim 768 \
  --custom-dataset-metric-type COSINE \
  --custom-dataset-file-count 1 \
  --custom-dataset-with-gt \
  --custom-case-load-timeout 360000 \
  --custom-case-optimize-timeout 360000 \
  --k 30 \
  --num-concurrency "16" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --skip-drop-old \
  --skip-load \
  --search-concurrent \
  --search-serial