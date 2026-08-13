#!/bin/bash

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/debian/vectordataset" \
# vectordbbench endee \
#   --token "mydb:mNwwdcYackwwg8B6tEQxBlE1xh7WFjpN" \
#   --region mumbai \
#   --base-url "http://148.113.58.83:8080/api/v2" \
#   --collection-name 1M_index_multivector_rebuild \
#   --task-label "fresh2rebuildm16ef128_rebuildmultivector_26j_try2" \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision int16 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency "16" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --skip-drop-old \
#   --skip-load \
#   --search-concurrent \
#   --search-serial


# ============================================
# =============== SEARCH ONLY ================
# ============================================

# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/User/vectordataset" \
# vectordbbench endee \
#   --token "TOKEN" \
#   --region location \
#   --base-url "http://127.0.0.1:8080/api/v2" \
#   --collection-name 1M_index \
#   --task-label "task_detail" \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision int16 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency "8" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --skip-drop-old \
#   --skip-load \
#   --search-concurrent \
#   --search-serial


# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/debian/vectordataset" \
# vectordbbench endee \
#   --token "mydb:mNwwdcYackwwg8B6tEQxBlE1xh7WFjpN" \
#   --region mumbai \
#   --base-url "http://148.113.58.83:8080/api/v2" \
#   --collection-name 1M_multivec_count5 \
#   --task-label "1M_multivec_singlefield_count5" \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision int16 \
#   --field-type multi_vector \
#   --multivec-fields multivec \
#   --multivec-pooling mean \
#   --multivec-count 5 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency "16" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --drop-old \
#   --load \
#   --search-concurrent \
#   --search-serial


# NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/debian/vectordataset" \
# vectordbbench endee \
#   --token "mydb:X338qP0DT8IJDa3LRT3QShKZixgqufHe" \
#   --region mumbai \
#   --base-url "http://148.113.59.156:8080/api/v2" \
#   --collection-name 1M_multivec_count5 \
#   --task-label "restart1_count5_m16efcon128" \
#   --m 16 \
#   --ef-con 128 \
#   --ef-search 128 \
#   --space-type cosine \
#   --precision int16 \
#   --field-type multi_vector \
#   --multivec-pooling mean \
#   --multivec-count 5 \
#   --case-type Performance768D1M \
#   --k 30 \
#   --num-concurrency "16" \
#   --concurrency-duration 30 \
#   --concurrency-timeout 3600 \
#   --skip-drop-old \
#   --skip-load \
#   --search-concurrent \
#   --search-serial


NUM_PER_BATCH=1000 DATASET_LOCAL_DIR="/home/debian/vectordataset" \
vectordbbench endee \
  --token "mydb:X338qP0DT8IJDa3LRT3QShKZixgqufHe" \
  --region mumbai \
  --base-url "http://148.113.59.156:8080/api/v2" \
  --collection-name 1M_multivec_5fields_count5 \
  --task-label "st4rebuild2same_searchfield5" \
  --m 16 \
  --ef-con 128 \
  --ef-search 128 \
  --space-type cosine \
  --precision int16 \
  --field-type multi_vector \
  --multivec-fields multivec1 \
  --multivec-fields multivec2 \
  --multivec-fields multivec3 \
  --multivec-fields multivec4 \
  --multivec-fields multivec5 \
  --multivec-count 5 \
  --search-field multivec5 \
  --case-type Performance768D1M \
  --k 30 \
  --num-concurrency "16" \
  --concurrency-duration 30 \
  --concurrency-timeout 3600 \
  --skip-drop-old \
  --skip-load \
  --search-concurrent \
  --search-serial
