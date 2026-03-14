#!/usr/bin/env bash
set -euo pipefail

# Load credentials from .env
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
set -a
source "$SCRIPT_DIR/.env"
set +a

export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION=auto

echo "Uploading parquet + sidecar JSON files..."
aws s3 cp "$SCRIPT_DIR/data/sbac_data.parquet" "s3://$R2_BUCKET/sbac_data.parquet" \
  --endpoint-url "$R2_ENDPOINT" --no-progress
aws s3 cp "$SCRIPT_DIR/data/sbac_entities.json" "s3://$R2_BUCKET/sbac_entities.json" \
  --endpoint-url "$R2_ENDPOINT" --no-progress
aws s3 cp "$SCRIPT_DIR/data/sbac_subgroups.json" "s3://$R2_BUCKET/sbac_subgroups.json" \
  --endpoint-url "$R2_ENDPOINT" --no-progress

echo "Syncing per-entity JSON files..."
aws s3 sync "$SCRIPT_DIR/data/sbac_data/" "s3://$R2_BUCKET/sbac_data/" \
  --endpoint-url "$R2_ENDPOINT" --no-progress

echo "Done."
