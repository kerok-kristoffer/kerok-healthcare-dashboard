#!/usr/bin/env bash
set -euo pipefail

# Load .env if present
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

docker run --rm -p 8501:8501 \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_PROFILE \
  -e AWS_DEFAULT_REGION \
  -e ATHENA_S3_OUTPUT \
  -e ATHENA_WORKGROUP \
  -e ATHENA_DATABASE \
  -e ATHENA_CATALOG \
  -v "$HOME/.aws:/root/.aws:ro" \
  healthcare-dashboard:latest
