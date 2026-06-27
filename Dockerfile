# syntax=docker/dockerfile:1

# ---- builder: compile the centrifugo binary (release) ----
FROM rust:1-bookworm AS builder
WORKDIR /src

# openssl-sys (reqwest native-tls) needs these to build; protobuf is compiled in
# pure Rust (protox), so no protoc is required.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Build only the server binary (not the conformance test crate). A cargo registry
# cache mount speeds repeat builds without leaving target/ in a cache (so the
# binary stays copyable in the next stage).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p centrifugo-server

# ---- runtime: minimal image with just the binary ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group centrifugo

COPY --from=builder /src/target/release/centrifugo /usr/local/bin/centrifugo

USER centrifugo
# 8000 = HTTP (WebSocket/SockJS, API, admin, metrics); 10000 = gRPC API.
EXPOSE 8000 10000

ENTRYPOINT ["centrifugo"]
# Sensible default for `docker run`; compose overrides `command:` with the cluster config.
CMD ["serve", "--address", "0.0.0.0", "--client_insecure"]
