# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY cc-blackbox-cli/Cargo.toml cc-blackbox-cli/Cargo.toml
COPY cc-blackbox-core/Cargo.toml cc-blackbox-core/Cargo.toml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo fetch --locked

COPY cc-blackbox-cli/src/ cc-blackbox-cli/src/
COPY cc-blackbox-core/build.rs cc-blackbox-core/build.rs
COPY cc-blackbox-core/proto/ cc-blackbox-core/proto/
COPY cc-blackbox-core/src/ cc-blackbox-core/src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release -p cc-blackbox-core \
    && cp /app/target/release/cc-blackbox-core /tmp/cc-blackbox-core

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/cc-blackbox-core /usr/local/bin/cc-blackbox-core
EXPOSE 50051 9090
CMD ["cc-blackbox-core"]
