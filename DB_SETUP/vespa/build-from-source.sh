#!/usr/bin/env bash
# Build Vespa from source exactly like Vespa's CI (.buildkite/Makefile), inside the
# official AlmaLinux 9 build container, then produce RPMs.
# Usage: build-vespa.sh [VESPA_VERSION]   (default 8.999.1 = Vespa CI's dev version)
set -euo pipefail

SRC=${VESPA_SRC:-$HOME/vespa}
WORK=${VESPA_WORK:-$(cd "$(dirname "$0")" && pwd)/.build}
VESPA_VERSION=${1:-8.999.1}
JOBS=${NUM_CPU_LIMIT:-$(nproc)}
IMAGE=vespaengine/vespa-build-almalinux-9:latest

mkdir -p "$WORK"/{m2,ccache,logs}
docker pull -q "$IMAGE"

# The RPM step (.copr/Makefile srpm) reads the version from a git tag on HEAD,
# exactly like Vespa CI does. Add a temporary local tag (removed at the end).
git -C "$SRC" tag -f "v$VESPA_VERSION" HEAD >/dev/null
cleanup() {
  sudo chown -R "$(id -u):$(id -g)" "$SRC" "$WORK" 2>/dev/null || true
  git -C "$SRC" tag -d "v$VESPA_VERSION" >/dev/null 2>&1 || true
  # prepare.sh rewrites <version> in every pom.xml - revert so the clone stays clean
  git -C "$SRC" diff --name-only -- '*pom.xml' | (cd "$SRC" && xargs -r git checkout --) || true
}
trap cleanup EXIT

docker run --rm --name vespa-build \
  -v "$SRC":/src/vespa \
  -v "$WORK":/work \
  -v "$WORK/m2":/root/.m2 \
  -v "$WORK/ccache":/root/.ccache \
  -e SOURCE_DIR=/src/vespa -e WORKDIR=/work -e LOG_DIR=/work/logs \
  -e VESPA_VERSION="$VESPA_VERSION" -e NUM_CPU_LIMIT="$JOBS" \
  -e VESPA_BUILDOS_LABEL=alma9 -e BUILDKITE=false -e VERBOSE= \
  -w /src/vespa "$IMAGE" bash -lc '
    set -e
    git config --global --add safe.directory "*"
    ccache -M 20G >/dev/null
    make -f .buildkite/Makefile build-rpms
  '

ls -lh "$WORK"/artifacts/*/rpms/
