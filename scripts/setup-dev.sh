#!/usr/bin/env bash
# Fuse development environment setup
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }

MISSING=0

echo "Fuse — Dev Environment Check"
echo "=============================="
echo

# Rust
echo "Checking prerequisites..."
if command -v rustc &>/dev/null; then
    ok "Rust $(rustc --version | awk '{print $2}')"
else
    fail "Rust not found"
    warn "Install: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    MISSING=1
fi

# Cargo
if command -v cargo &>/dev/null; then
    ok "Cargo $(cargo --version | awk '{print $2}')"
else
    fail "Cargo not found"
    MISSING=1
fi

# Docker
if command -v docker &>/dev/null; then
    ok "Docker $(docker --version | awk '{print $3}' | tr -d ',')"
else
    fail "Docker not found"
    warn "Install: https://docs.docker.com/engine/install/"
    MISSING=1
fi

# docker compose
if docker compose version &>/dev/null 2>&1; then
    ok "Docker Compose $(docker compose version --short 2>/dev/null || echo 'available')"
elif command -v docker-compose &>/dev/null; then
    ok "docker-compose (legacy)"
else
    fail "Docker Compose not found"
    MISSING=1
fi

# openssl-dev
if pkg-config --exists openssl 2>/dev/null; then
    ok "OpenSSL dev headers ($(pkg-config --modversion openssl))"
elif [ -f /usr/include/openssl/ssl.h ]; then
    ok "OpenSSL dev headers"
else
    fail "OpenSSL dev headers not found"
    if command -v apt-get &>/dev/null; then
        warn "Install: sudo apt-get install -y libssl-dev pkg-config"
        read -rp "  Install now? [y/N] " ans
        if [[ "$ans" =~ ^[Yy]$ ]]; then
            sudo apt-get install -y libssl-dev pkg-config
            ok "Installed libssl-dev"
        fi
    elif command -v dnf &>/dev/null; then
        warn "Install: sudo dnf install -y openssl-devel pkg-config"
    elif command -v brew &>/dev/null; then
        warn "Install: brew install openssl pkg-config"
    fi
    MISSING=1
fi

echo

if [ "$MISSING" -ne 0 ]; then
    fail "Some prerequisites are missing. Install them and re-run."
    exit 1
fi

# Build check
echo "Running cargo check..."
cd "$(dirname "$0")/.."
cargo check 2>&1 | tail -5
ok "Workspace compiles"

echo
echo "=============================="
echo -e "${GREEN}Dev environment ready!${NC}"
echo
echo "Quick start:"
echo "  docker compose up -d          # Start OpenSearch cluster"
echo "  cargo run -p fuse-server      # Start Fuse server on :9400"
echo "  curl localhost:9400/api/fuse/health  # Health check"
echo
echo "Run tests:"
echo "  cargo test --all-targets      # Unit + integration tests"
echo "  ./scripts/test-local.sh       # Full local test with Docker"
