#!/bin/bash

NUM_PER_BATCH=100 DATASET_LOCAL_DIR="/home/debian/vectordataset" \
vectordbbench cosmosdb \
  --endpoint "https://cosmos.azure.com:443/" \
  --key "your_key" \
  --task-label "cosmos_1m" \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency "16" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600000 \
  --drop-old \
  --load \
  --search-concurrent \
  --search-serial
