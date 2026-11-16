#!/usr/bin/env bash
# Build Milvus from source the same way Milvus CI publishes images
# (ci/jenkins/PublishImages.groovy): compile inside the pinned milvus-env builder
# container, then package bin/ + lib/ + configs/ into the runtime image.
# Usage: build-milvus.sh [IMAGE_TAG]
set -euo pipefail

SRC=${MILVUS_SRC:-$HOME/milvus}
cd "$SRC"
SHA=$(git rev-parse --short=8 HEAD)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
TAG=${1:-${BRANCH}-$(date +%Y%m%d)-${SHA}-local}

# Builder runs as root and writes into the repo; hand ownership back when done.
trap 'sudo chown -R "$(id -u):$(id -g)" "$SRC"' EXIT

# 1. Compile (C++ core via Conan+CMake, then Go) -> bin/milvus, lib/*.so
#    Caches (conan, ccache, go-mod) persist in $SRC/.docker/ so rebuilds are fast.
export IS_NETWORK_MODE_HOST=true
build/builder.sh /bin/bash -c \
  "git config --global --add safe.directory /go/src/github.com/milvus-io/milvus && make install"
# builder.sh edits docker-compose.yml for host networking - undo that edit
git checkout -- docker-compose.yml

# 2. Package into a runtime image (build/docker/milvus/ubuntu22.04/Dockerfile)
MILVUS_IMAGE_REPO=milvus-local MILVUS_IMAGE_TAG="$TAG" DOCKER_BUILDKIT=1 build/build_image.sh

echo "Built image: milvus-local:$TAG"
