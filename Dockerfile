# syntax=docker/dockerfile:1
#
# Produces a FULLY STATIC binary (musl libc, rustls TLS with bundled CA roots —
# no OpenSSL, no glibc, no system cert store) and ships it on `scratch`, so the
# final image is just the self-contained binary.

# ---- builder: static binary via musl ----
FROM rust:1-bookworm AS builder
WORKDIR /src

# musl toolchain for the static target. ring's C bits compile with musl-gcc.
# (No pkg-config/libssl-dev needed — rustls replaced OpenSSL.)
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Build the static binary for the builder's NATIVE arch (no cross-compile).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    set -eux; \
    case "$(uname -m)" in \
      x86_64)  TARGET=x86_64-unknown-linux-musl ;; \
      aarch64) TARGET=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    export CC_x86_64_unknown_linux_musl=musl-gcc CC_aarch64_unknown_linux_musl=musl-gcc; \
    rustup target add "$TARGET"; \
    cargo build --release --target "$TARGET" -p centrifugo-server; \
    cp "target/$TARGET/release/centrifugo" /centrifugo

# ---- runtime: scratch (nothing but the static binary) ----
FROM scratch
COPY --from=builder /centrifugo /centrifugo
# Numeric UID (no /etc/passwd on scratch); ports 8000 HTTP, 10000 gRPC.
USER 10001
EXPOSE 8000 10000
ENTRYPOINT ["/centrifugo"]
# Default for `docker run`; compose overrides `command:` with the cluster config.
CMD ["serve", "--address", "0.0.0.0", "--client_insecure"]
