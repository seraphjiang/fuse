# Multi-arch Dockerfile for Fuse (AMD64 + ARM64)
# Build: docker buildx build --platform linux/amd64,linux/arm64 -t fuse-server .

# Stage 1: Build
FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/rust:1.85-bookworm AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM

RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

# Install cross-compilation toolchains for ARM64 when building on AMD64
RUN case "$TARGETPLATFORM" in \
      "linux/arm64") \
        dpkg --add-architecture arm64 && \
        apt-get update && \
        apt-get install -y gcc-aarch64-linux-gnu libssl-dev:arm64 && \
        rustup target add aarch64-unknown-linux-gnu && \
        rm -rf /var/lib/apt/lists/* ;; \
      "linux/amd64") \
        ;; \
    esac

WORKDIR /usr/src/fuse
COPY . .

# Build for target architecture
RUN case "$TARGETPLATFORM" in \
      "linux/arm64") \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
        export PKG_CONFIG_SYSROOT_DIR=/usr/aarch64-linux-gnu && \
        cargo build --release --bin fuse-server --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/fuse-server /usr/src/fuse/fuse-server ;; \
      *) \
        cargo build --release --bin fuse-server && \
        cp target/release/fuse-server /usr/src/fuse/fuse-server ;; \
    esac

# Stage 2: Runtime (multi-arch base image)
FROM public.ecr.aws/docker/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 1000 -m fuse
COPY --from=builder /usr/src/fuse/fuse-server /usr/local/bin/fuse-server
COPY --from=builder /usr/src/fuse/fuse.toml /etc/fuse/fuse.toml

ENV FUSE_CONFIG=/etc/fuse/fuse.toml
ENV FUSE_LOG_FORMAT=json
ENV RUST_LOG=info
EXPOSE 9400
USER fuse

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:9400/api/fuse/health || exit 1

ENTRYPOINT ["/usr/local/bin/fuse-server"]
