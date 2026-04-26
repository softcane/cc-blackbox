# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY clauditor-cli/Cargo.toml clauditor-cli/Cargo.toml
COPY clauditor-core/Cargo.toml clauditor-core/Cargo.toml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo fetch --locked

COPY clauditor-cli/src/ clauditor-cli/src/
COPY clauditor-core/build.rs clauditor-core/build.rs
COPY clauditor-core/proto/ clauditor-core/proto/
COPY clauditor-core/src/ clauditor-core/src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release -p clauditor-core \
    && cp /app/target/release/clauditor-core /tmp/clauditor-core

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/clauditor-core /usr/local/bin/clauditor-core
EXPOSE 50051 9090
CMD ["clauditor-core"]
