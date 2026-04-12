# Fuse — common dev commands

# Build
build:
    cargo build --all-targets

release:
    cargo build --release --bin fuse-server

# Test
test:
    cargo nextest run --all-targets 2>/dev/null || cargo test --all-targets

test-chaos:
    cargo test --test chaos_test -- --test-threads=1

test-fuzz:
    cargo test --test sql_injection_fuzz_test

# Lint
fmt:
    cargo fmt --all

check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings

# Run
run:
    cargo run -p fuse-server

# Docker
docker-build:
    docker build -t fuse-server:dev .

docker-up:
    docker compose up -d

docker-down:
    docker compose down

# Helm
helm-lint:
    helm lint deploy/helm/fuse/

helm-template:
    helm template fuse deploy/helm/fuse/ --debug

# Coverage
coverage:
    cargo llvm-cov --all-targets --lcov --output-path lcov.info
    cargo llvm-cov report --summary-only

# E2E (requires running server)
e2e BASE="http://localhost:9400":
    bash tests/e2e/playground_test.sh {{BASE}}
    bash tests/e2e/sprint18_pages_test.sh {{BASE}}

# Clean
clean:
    cargo clean
