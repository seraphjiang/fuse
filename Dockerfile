# Stage 1: Build
FROM public.ecr.aws/docker/library/rust:1.85-bookworm AS builder
RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/fuse
COPY . .
RUN cargo build --release --bin fuse-server

# Stage 2: Runtime
FROM public.ecr.aws/docker/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/fuse/target/release/fuse-server /usr/local/bin/fuse-server
COPY --from=builder /usr/src/fuse/fuse.toml /etc/fuse/fuse.toml

ENV FUSE_CONFIG=/etc/fuse/fuse.toml
ENV FUSE_LOG_FORMAT=json
EXPOSE 9400

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:9400/api/fuse/health || exit 1

ENTRYPOINT ["/usr/local/bin/fuse-server"]
