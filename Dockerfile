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

# Add the musl std target in its OWN layer, BEFORE copying source — so the (often
# slow) `rust-std` download is cached and is NOT repeated on every source change.
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64)  TARGET=x86_64-unknown-linux-musl ;; \
      aarch64) TARGET=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    rustup target add "$TARGET"

COPY . .

# Build the static binary for the builder's NATIVE arch (no cross-compile). With
# the target cached above and a target/registry cache mount, source-only changes
# rebuild in seconds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    case "$(uname -m)" in \
      x86_64)  TARGET=x86_64-unknown-linux-musl ;; \
      aarch64) TARGET=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    export CC_x86_64_unknown_linux_musl=musl-gcc CC_aarch64_unknown_linux_musl=musl-gcc; \
    cargo build --release --target "$TARGET" -p centrifugo-server; \
    cp "target/$TARGET/release/centrifugo" /centrifugo

# ---- runtime: scratch (nothing but the static binary) ----
FROM scratch
# Put the binary on PATH as `centrifugo` so the official launch contract works
# verbatim: `docker run IMG centrifugo -c config.json`, `centrifugo --client_insecure`.
COPY --from=builder /centrifugo /usr/local/bin/centrifugo
ENV PATH=/usr/local/bin
# Go's image reads ./config.json from the working dir; WORKDIR creates the
# (mountable) /centrifugo dir so `-v cfg:/centrifugo/config.json` is auto-discovered.
WORKDIR /centrifugo
# Numeric UID (no /etc/passwd on scratch); ports 8000 HTTP, 10000 gRPC.
USER 10001
EXPOSE 8000 10000
# No entrypoint (like the official image): the bare root command IS the server, so
# `docker run IMG centrifugo [flags]` and `docker run IMG centrifugo <subcommand>` work.
ENTRYPOINT []
# Bare `docker run IMG` starts the server (binds all interfaces by default, like Go).
# Insecure default for local use; compose overrides `command:` for the cluster.
CMD ["centrifugo", "--client_insecure"]
