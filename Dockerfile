# Stage 1: Build
FROM public.ecr.aws/docker/library/rust:1.85-bookworm AS builder
RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/fuse
COPY . .
RUN cargo build --release --bin fuse-server

# Stage 2: Runtime
FROM public.ecr.aws/docker/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/fuse/target/release/fuse-server /usr/local/bin/fuse-server
COPY --from=builder /usr/src/fuse/fuse.toml /etc/fuse/fuse.toml

ENV FUSE_CONFIG=/etc/fuse/fuse.toml
EXPOSE 9400

ENTRYPOINT ["/usr/local/bin/fuse-server"]
