# Stage 1: Generate dependency recipe
FROM public.ecr.aws/docker/library/rust:1.85-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /usr/src/fuse
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached unless Cargo.toml/Cargo.lock change)
FROM public.ecr.aws/docker/library/rust:1.85-bookworm AS builder
RUN cargo install cargo-chef
RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/fuse
COPY --from=chef /usr/src/fuse/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build application (only this reruns on source changes)
COPY . .
RUN cargo build --release --bin fuse-server

# Stage 4: Runtime
FROM public.ecr.aws/docker/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/fuse/target/release/fuse-server /usr/local/bin/fuse-server
COPY --from=builder /usr/src/fuse/fuse.toml /etc/fuse/fuse.toml

ENV FUSE_CONFIG=/etc/fuse/fuse.toml
EXPOSE 9400

ENTRYPOINT ["/usr/local/bin/fuse-server"]
