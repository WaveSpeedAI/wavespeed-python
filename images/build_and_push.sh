#!/bin/bash
# Build and push Docker images for wavespeed-python.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default values
DIRECTORY=
DOCKERFILE=Dockerfile
VARIANT=
LOCAL=0

# Parse command line args
while getopts ":hd:f:v:l" opt; do
  case $opt in
    d) DIRECTORY="$OPTARG" ;;
    f) DOCKERFILE="$OPTARG" ;;
    v) VARIANT="$OPTARG" ;;
    l) LOCAL=1 ;;
    h) echo "Usage: $0 -d <directory> [-f dockerfile] [-v variant] [-l (local only)]"
       echo ""
       echo "Options:"
       echo "  -d  Directory name under images/ (required)"
       echo "  -f  Dockerfile name (default: Dockerfile)"
       echo "  -v  Variant suffix for tag"
       echo "  -l  Local build only, skip push"
       exit 0 ;;
    :) echo "Option -$OPTARG requires an argument" >&2
       exit 1 ;;
    \?) echo "Invalid option -$OPTARG" >&2
       exit 1 ;;
  esac
done

if [ -z "$DIRECTORY" ]; then
  echo "Error: Missing directory argument (-d)" >&2
  echo "Usage: $0 -d <directory> [-f dockerfile] [-v variant] [-l]" >&2
  exit 1
fi

# Extract dockerfile postfix if any (e.g., Dockerfile.cuda -> cuda)
if [[ "$DOCKERFILE" =~ Dockerfile\.(.+) ]]; then
  POSTFIX="${BASH_REMATCH[1]}"
else
  POSTFIX="default"
fi

# Build tag
IMAGE_NAME="wavespeed-python"
DOCKERHUB_USERNAME="wavespeed"
TIMESTAMP="$(date -u +%Y%m%d%H%M)"

if [ -z "$VARIANT" ]; then
  TAG="${DIRECTORY}-${POSTFIX}-${TIMESTAMP}"
else
  TAG="${DIRECTORY}-${POSTFIX}-${VARIANT}-${TIMESTAMP}"
fi

FULL_TAG="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"
DOCKERFILE_PATH="${SCRIPT_DIR}/${DIRECTORY}/${DOCKERFILE}"
PLATFORM="linux/amd64"

# Verify dockerfile exists
if [ ! -f "$DOCKERFILE_PATH" ]; then
  echo "Error: Dockerfile not found at $DOCKERFILE_PATH" >&2
  exit 1
fi

echo "=== Building Docker image ===" >&2
echo "Directory: $DIRECTORY" >&2
echo "Dockerfile: $DOCKERFILE_PATH" >&2
echo "Tag: $FULL_TAG" >&2
echo "" >&2

# Build the Docker image
if [ -z "$VARIANT" ]; then
  docker buildx build \
    --platform "$PLATFORM" \
    -t "$FULL_TAG" \
    -f "$DOCKERFILE_PATH" \
    "$REPO_ROOT"
else
  docker buildx build \
    --platform "$PLATFORM" \
    -t "$FULL_TAG" \
    -f "$DOCKERFILE_PATH" \
    --build-arg VARIANT="$VARIANT" \
    "$REPO_ROOT"
fi

echo "" >&2
echo "Docker image built: $FULL_TAG" >&2

# Push the Docker image to Docker Hub
if [ $LOCAL -eq 0 ]; then
  echo "" >&2
  echo "=== Pushing to Docker Hub ===" >&2
  docker push "$FULL_TAG"
  echo "Pushed: $FULL_TAG" >&2
else
  echo "" >&2
  echo "Skipping push (local mode)" >&2
fi

# Output the full tag (useful for scripting)
echo "$FULL_TAG"
