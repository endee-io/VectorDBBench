#!/usr/bin/env bash
# Build Qdrant from source: (a) native release binary, (b) Docker image using the
# repo's own Dockerfile (identical recipe to qdrant/qdrant on Docker Hub).
# Usage: build-qdrant.sh [native|image|both]   (default both)
set -euo pipefail

SRC=${QDRANT_SRC:-$HOME/qdrant}
MODE=${1:-both}
cd "$SRC"
TAG=$(git describe --tags --always)-local

if [[ $MODE == native || $MODE == both ]]; then
  source "$HOME/.cargo/env"
  cargo build --release --bin qdrant          # -> target/release/qdrant
  ./target/release/qdrant --version
fi

if [[ $MODE == image || $MODE == both ]]; then
  # Same build args as the published image (mold linker, --features=stacktrace)
  DOCKER_BUILDKIT=1 docker build . --tag "qdrant-local:$TAG"
  echo "Built image: qdrant-local:$TAG"
fi
