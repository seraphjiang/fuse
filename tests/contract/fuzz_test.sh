#!/usr/bin/env bash
# Fuzz Testing — random SQL/PPL input generation
# Verifies the server never returns 500 on malformed input.
# Usage: ./fuzz_test.sh [URL] [ITERATIONS]
set -euo pipefail

FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
ITERS="${2:-200}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "🎲 Fuzz Test — $FUSE ($ITERS iterations)"
echo ""
python3 "$DIR/fuzz_queries.py" "$FUSE" "$ITERS"
