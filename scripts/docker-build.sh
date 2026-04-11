#!/usr/bin/env bash
# Build and push multi-arch Docker image for Fuse.
# Usage: ./scripts/docker-build.sh [TAG] [--push]
set -euo pipefail

TAG="${1:-latest}"
PUSH="${2:-}"
REPO="ghcr.io/seraphjiang/fuse-server"

echo "=== Building Fuse multi-arch image ==="
echo "Tag: $REPO:$TAG"
echo "Platforms: linux/amd64, linux/arm64"

# Ensure buildx builder exists
docker buildx inspect fuse-builder >/dev/null 2>&1 || \
  docker buildx create --name fuse-builder --use

ARGS="--platform linux/amd64,linux/arm64 -t $REPO:$TAG"

if [ "$PUSH" = "--push" ]; then
  echo "Will push to registry"
  ARGS="$ARGS --push"
else
  echo "Local build only (use --push to push)"
  ARGS="$ARGS --load"
  # --load only supports single platform
  ARGS="--platform linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') -t $REPO:$TAG --load"
fi

docker buildx build $ARGS .

echo "=== Done ==="
