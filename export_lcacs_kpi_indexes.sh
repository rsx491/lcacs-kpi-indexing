#!/usr/bin/env bash
set -euo pipefail

LOCAL_ES="${LOCAL_ES:-http://localhost:9200}"
OUT_DIR="${OUT_DIR:-kpi_index_exports}"
ARCHIVE_NAME="${ARCHIVE_NAME:-lcacs-kpi-index-exports.tgz}"

INDEXES=(
  "lcacs-kpi-api-calls-30d-v2"
  "lcacs-kpi-public-repo-downloads-30d-v1"
  "lcacs-kpi-estimated-process-downloads-30d-v7"
  "lcacs-kpi-public-process-inventory-v2"
  "lcacs-kpi-total-repositories-published-30d-v1"
)

echo "Using local Elasticsearch: $LOCAL_ES"
echo "Export directory: $OUT_DIR"
echo

if ! command -v elasticdump >/dev/null 2>&1; then
  echo "ERROR: elasticdump is not installed or not in PATH."
  echo "Install with: npm install -g elasticdump"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "WARNING: jq not found. Count checks will still run but output may be raw."
fi

echo "Checking local Elasticsearch..."
curl -fsS "$LOCAL_ES/_cluster/health?pretty" >/dev/null
echo "Local Elasticsearch reachable."
echo

mkdir -p "$OUT_DIR"

echo "Exporting KPI indexes..."
echo

for idx in "${INDEXES[@]}"; do
  echo "========================================"
  echo "Index: $idx"

  echo "Checking index exists..."
  if ! curl -fsS "$LOCAL_ES/$idx" >/dev/null; then
    echo "ERROR: Index not found: $idx"
    exit 1
  fi

  echo "Document count:"
  if command -v jq >/dev/null 2>&1; then
    curl -s "$LOCAL_ES/$idx/_count" | jq
  else
    curl -s "$LOCAL_ES/$idx/_count"
    echo
  fi

  echo "Exporting mapping..."
  elasticdump \
    --input="$LOCAL_ES/$idx" \
    --output="$OUT_DIR/$idx-mapping.json" \
    --type=mapping

  echo "Exporting data..."
  elasticdump \
    --input="$LOCAL_ES/$idx" \
    --output="$OUT_DIR/$idx-data.json" \
    --type=data

  echo "Finished: $idx"
  echo
done

echo "Creating archive: $ARCHIVE_NAME"
tar -czf "$ARCHIVE_NAME" "$OUT_DIR"

echo
echo "Export complete."
echo "Archive created:"
ls -lh "$ARCHIVE_NAME"

echo
echo "Files exported:"
ls -lh "$OUT_DIR"
