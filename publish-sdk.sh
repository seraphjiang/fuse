#!/usr/bin/env bash
# Publish fuse-connector-sdk to crates.io (dry-run by default)
set -euo pipefail

cd "$(dirname "$0")"

echo "=== Checking package contents ==="
cargo package -p fuse-connector-sdk --list

echo ""
echo "=== Dry-run publish ==="
cargo publish -p fuse-connector-sdk --dry-run

echo ""
echo "Dry-run succeeded. To publish for real, run:"
echo "  cargo publish -p fuse-connector-sdk"
