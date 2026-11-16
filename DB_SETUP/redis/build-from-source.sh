#!/usr/bin/env bash
# Build Redis (core + bundled modules: Query Engine/RediSearch, JSON, TimeSeries,
# Bloom, Vector Sets) inside the repo's official build container (docker/Dockerfile.noble),
# stage an install tree, and package it into a runtime image `redis-local:<tag>`.
# Usage: build-redis.sh [IMAGE_TAG]
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SRC=${REDIS_SRC:-$HOME/redis}
OUT=${REDIS_OUT:-$HERE/.build}
cd "$SRC"
TAG=${1:-$(git rev-parse --abbrev-ref HEAD)-$(git rev-parse --short=8 HEAD)-local}
trap 'sudo chown -R "$(id -u):$(id -g)" "$SRC" "$OUT"' EXIT

# 1. Fetch module sources at the versions pinned in modules/modules.yaml
[ -d modules/redisearch/src ] || make modules-update

# 2. Build environment image (Ubuntu 24.04 + LLVM/Rust/CMake for the modules)
docker build -f docker/Dockerfile.noble -t redis-build:noble .

# 3. Compile + stage install tree into $OUT/stage/usr/local
rm -rf "$OUT/stage" && mkdir -p "$OUT/stage"
docker run --rm -v "$SRC":/workspace -v "$OUT":/out -w /workspace redis-build:noble bash -lc '
  set -e
  git config --global --add safe.directory "*"
  make distclean >/dev/null 2>&1 || true
  make -j"$(nproc)" BUILD_TLS=yes
  make install BUILD_TLS=yes PREFIX=/usr/local DESTDIR=/out/stage
  cp redis-full.conf /out/stage/redis-full.conf
'

# 4. Runtime image
docker build -f "$HERE/Dockerfile.runtime" -t "redis-local:$TAG" "$OUT/stage"
echo "Built image: redis-local:$TAG"
