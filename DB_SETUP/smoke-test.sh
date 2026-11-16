#!/usr/bin/env bash
# End-to-end vector smoke test for the four release containers:
# create a 4-dim collection/index, insert 2 vectors, KNN search for the 1st one.
# Expect "PASS" for each. Usage: smoke-test.sh [milvus|qdrant|redis|vespa ...]
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
J='Content-Type: application/json'
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1: $2"; FAILED=1; }
FAILED=0

qdrant() {
  local u=localhost:6333
  curl -s -X DELETE $u/collections/smoke >/dev/null
  curl -s -X PUT $u/collections/smoke -H "$J" -d '{"vectors":{"size":4,"distance":"Cosine"}}' >/dev/null
  curl -s -X PUT "$u/collections/smoke/points?wait=true" -H "$J" \
    -d '{"points":[{"id":1,"vector":[1,0,0,0]},{"id":2,"vector":[0,1,0,0]}]}' >/dev/null
  local r; r=$(curl -s -X POST $u/collections/smoke/points/query -H "$J" -d '{"query":[1,0,0,0],"limit":1}')
  local v; v=$(curl -s $u/ | jq -r .version)
  [[ $(jq -r '.result.points[0].id' <<<"$r") == 1 ]] && pass "qdrant $v" || fail qdrant "$r"
  curl -s -X DELETE $u/collections/smoke >/dev/null
}

milvus() {
  local u=localhost:19530/v2/vectordb
  curl -s -X POST $u/collections/drop -H "$J" -d '{"collectionName":"smoke"}' >/dev/null
  curl -s -X POST $u/collections/create -H "$J" \
    -d '{"collectionName":"smoke","dimension":4,"metricType":"COSINE"}' >/dev/null
  curl -s -X POST $u/entities/insert -H "$J" \
    -d '{"collectionName":"smoke","data":[{"id":1,"vector":[1,0,0,0]},{"id":2,"vector":[0,1,0,0]}]}' >/dev/null
  sleep 2
  local r; r=$(curl -s -X POST $u/entities/search -H "$J" \
    -d '{"collectionName":"smoke","data":[[1,0,0,0]],"limit":1,"consistencyLevel":"Strong"}')
  local v; v=$(docker inspect -f '{{.Config.Image}}' milvus-standalone)
  [[ $(jq -r '.data[0].id' <<<"$r") == 1 ]] && pass "milvus $v" || fail milvus "$r"
  curl -s -X POST $u/collections/drop -H "$J" -d '{"collectionName":"smoke"}' >/dev/null
}

redis() {
  local c="docker exec -i redis redis-cli"
  $c FT.DROPINDEX smoke DD >/dev/null 2>&1
  $c FT.CREATE smoke ON HASH PREFIX 1 smoke: SCHEMA v VECTOR HNSW 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC COSINE >/dev/null
  # binary float32 vectors go through stdin (-x) because shell args can't carry NUL bytes
  python3 -c 'import struct,sys;sys.stdout.buffer.write(struct.pack("4f",1,0,0,0))' | $c -x HSET smoke:1 v >/dev/null
  python3 -c 'import struct,sys;sys.stdout.buffer.write(struct.pack("4f",0,1,0,0))' | $c -x HSET smoke:2 v >/dev/null
  local r; r=$(python3 -c 'import struct,sys;sys.stdout.buffer.write(struct.pack("4f",1,0,0,0))' \
    | $c -x FT.SEARCH smoke '*=>[KNN 1 @v $q]' NOCONTENT DIALECT 2 PARAMS 2 q)
  local v; v=$($c INFO server | grep -oP 'redis_version:\K\S+')
  [[ $(sed -n 2p <<<"$r") == smoke:1 ]] && pass "redis $v (Query Engine HNSW)" || fail redis "$r"
  # Vector Sets (built-in VADD/VSIM)
  $c DEL smokevs >/dev/null
  $c VADD smokevs VALUES 4 1 0 0 0 a >/dev/null; $c VADD smokevs VALUES 4 0 1 0 0 b >/dev/null
  [[ $($c VSIM smokevs VALUES 4 1 0 0 0 COUNT 1) == a ]] && pass "redis $v (Vector Sets)" || fail redis-vset "VSIM"
  $c FT.DROPINDEX smoke DD >/dev/null; $c DEL smokevs >/dev/null
}

vespa() {
  # Deploy the minimal app package in vespa-app/ (schema with HNSW tensor field)
  local zip; zip=$(mktemp --suffix=.zip); rm -f "$zip"
  (cd "$HERE/vespa/app" && zip -qr "$zip" .)
  curl -s -X POST -H 'Content-Type: application/zip' --data-binary @"$zip" \
    localhost:19071/application/v2/tenant/default/prepareandactivate >/dev/null
  rm -f "$zip"
  for _ in $(seq 60); do curl -sf localhost:8080/state/v1/health | grep -q '"up"' && break; sleep 2; done
  for i in 1 2; do
    local vec; [[ $i == 1 ]] && vec='[1,0,0,0]' || vec='[0,1,0,0]'
    curl -s -X POST -H "$J" localhost:8080/document/v1/smoke/doc/docid/$i \
      -d "{\"fields\":{\"id\":$i,\"embedding\":$vec}}" >/dev/null
  done
  local r; r=$(curl -s -H "$J" localhost:8080/search/ -d '{
    "yql":"select id from doc where {targetHits:1}nearestNeighbor(embedding,q)",
    "input.query(q)":"[1,0,0,0]","ranking":"closeness","hits":1}')
  local v; v=$(curl -s localhost:19071/state/v1/version | jq -r .version)
  [[ $(jq -r '.root.children[0].fields.id' <<<"$r") == 1 ]] && pass "vespa $v" || fail vespa "$r"
}

TARGETS=("$@"); [[ ${#TARGETS[@]} -eq 0 ]] && TARGETS=(qdrant milvus redis vespa)
for t in "${TARGETS[@]}"; do "$t"; done
exit $FAILED
