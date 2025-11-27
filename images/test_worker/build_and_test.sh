#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE_NAME="wavespeed/test-worker:dev"

echo "=== Building Docker image ==="
docker build -f "$SCRIPT_DIR/Dockerfile" -t "$IMAGE_NAME" "$REPO_ROOT"

echo ""
echo "=== Running test with test_input.json ==="
docker run --rm \
    -v "$SCRIPT_DIR/test_input.json:/opt/work_dir/test_input.json:ro" \
    "$IMAGE_NAME"

echo ""
echo "=== Test completed ==="
