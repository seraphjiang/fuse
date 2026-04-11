#!/usr/bin/env bash
# Generate API clients from OpenAPI spec.
# Usage: ./scripts/generate-clients.sh [--lang python|typescript|go|rust]
set -euo pipefail

SPEC="docs/api/openapi.yaml"
OUT_DIR="sdks/generated"
LANG="${1:-all}"

if ! command -v openapi-generator-cli &>/dev/null && ! command -v npx &>/dev/null; then
  echo "Requires openapi-generator-cli or npx. Install via:"
  echo "  npm install -g @openapitools/openapi-generator-cli"
  echo "  # or use npx @openapitools/openapi-generator-cli"
  exit 1
fi

GEN="openapi-generator-cli"
command -v "$GEN" &>/dev/null || GEN="npx @openapitools/openapi-generator-cli"

generate() {
  local lang="$1" out="$OUT_DIR/$lang"
  echo "=== Generating $lang client ==="
  mkdir -p "$out"
  $GEN generate \
    -i "$SPEC" \
    -g "$lang" \
    -o "$out" \
    --additional-properties=packageName=fuse_client,projectName=fuse-client \
    2>&1 | tail -5
  echo "  → $out"
}

case "$LANG" in
  python)     generate python ;;
  typescript) generate typescript-fetch ;;
  go)         generate go ;;
  rust)       generate rust ;;
  all)
    generate python
    generate typescript-fetch
    generate go
    generate rust
    ;;
  *) echo "Unknown language: $LANG. Use: python, typescript, go, rust, all"; exit 1 ;;
esac

echo ""
echo "=== Done. Generated clients in $OUT_DIR ==="
