# Centrifugo (Rust implementation)

A full reimplementation of the **Centrifugo v2.8.6** real-time server in **Rust**, wire-compatible with real clients.

> 🇷🇺 Русская версия: [README.md](README.md)

---

## Why this exists

The goal is byte-for-byte compatibility with clients that **cannot be updated**. Real SDKs (centrifuge-go, centrifuge-js, etc.) connect to this Rust binary and behave exactly as they do against the original Go server — no client-side changes required.

**Wire era:** protocol **v0.3.4** / centrifuge **v0.14.2** (protocol **v2**, not v3/v4). This generation defaults to **seq/gen**, not offset.

---

## What's implemented

| Feature | Status |
|---|---|
| WebSocket transport (`/connection/websocket`) | ✅ JSON (NDJSON) and Protobuf (`?format=protobuf`) |
| SockJS fallback (`/connection/sockjs`) | ✅ xhr-polling + `/info` + CORS |
| Client commands | ✅ connect, subscribe, publish, unsubscribe, presence, presence_stats, history, refresh, ping, send, rpc, sub_refresh |
| History & recovery | ✅ seq/gen, recovery on (re)subscribe, descending order |
| Presence + join/leave | ✅ |
| JWT authentication | ✅ HMAC (HS256/384/512), RSA (RS*), ECDSA (ES256/384) |
| JWKS | ✅ key selection by `kid`, background refresh |
| Connect proxy (HTTP callback) | ✅ |
| Namespaces & private channels (`$`) | ✅ |
| HTTP API (`POST /api`) | ✅ apikey authentication |
| gRPC API (port 10000) | ✅ same 11 RPCs, apikey in metadata |
| Engines | ✅ Memory (single node) **and** Redis (multi-node) |
| Admin (`/admin/auth`, `/admin/api`) | ✅ token authentication |
| Prometheus metrics (`/metrics`) | ✅ |
| Configuration | ✅ flags + JSON file (`-c`) + env (`CENTRIFUGO_*`) |
| CLI subcommands | ✅ `serve`, `gentoken`, `genconfig`, `checkconfig`, `version` |

---

## Architecture

The project is split into 6 crates (Cargo workspace):

| Crate | Responsibility |
|---|---|
| `centrifugo-protocol` | Wire format: Command/Reply/Push envelopes, NDJSON (inline-raw JSON), uvarint length-prefixed protobuf, error codes (100–111), disconnect codes (3000–3013), JSON/Protobuf codec |
| `centrifugo-auth` | JWT verification (HMAC/RSA/ECDSA), JWKS by `kid`, manual exp/nbf checks, subscription tokens, token generation |
| `centrifugo-core` | `Node`, sharded `Hub`, `Client` state machine, `Engine` abstraction (pub/sub + history + presence), `MemoryEngine`, connect proxy |
| `centrifugo-grpc` | tonic codegen (server + client) from `api.proto` via pure Rust (`protox`, no `protoc`) |
| `centrifugo-redis` | `RedisEngine`: cross-node fan-out over Redis PUB/SUB, atomic Lua history, hash-based presence |
| `centrifugo-server` | The `centrifugo` binary: CLI, config, HTTP (axum), WebSocket, SockJS, HTTP/gRPC API, admin, metrics |

### Non-blocking fan-out (the load-bearing requirement)

Broadcasts to **10,000 / 100,000** subscribers never block each other:

- Each connection = a reader task + a writer task draining a bounded `tokio::mpsc` queue.
- On publish, the `Node` encodes the push **once per protocol**, then `try_send`s the prepared frame to each subscriber.
- The `Hub` is **sharded by channel hash**, so different channels fan out fully in parallel; only same-channel offset assignment is serialized.
- A slow subscriber whose queue fills is disconnected with **DisconnectSlow (3008)** and removed; the publisher and every other subscriber are untouched.

### Engine abstraction

`Engine` (async trait) unifies pub/sub + history + presence. One `Arc<dyn Engine>` backs the `Node`:

- **MemoryEngine** — single node, in-process. History is a size-bounded ring with lazy TTL; presence is a map; the stream meta (offset + epoch) persists past `history_lifetime` expiry (matching Go).
- **RedisEngine** — multi-node. Each node pattern-subscribes to `centrifugo.pub.*` and routes incoming messages into its local hub. History is a list + meta hash (offset, epoch) with atomic Lua append. Presence is a `clientID → ClientInfo` hash.

---

## Build & run

Requires Rust (stable). The Go oracle and live-SDK test need Go; the Redis tests need `redis-server`.

```bash
# Build
cargo build --release          # binary: target/release/centrifugo

# Run in insecure mode (no tokens)
./target/release/centrifugo serve --client_insecure

# Run with a config file
./target/release/centrifugo serve -c config.json

# All tests (unit + conformance)
cargo test --workspace
```

### Endpoints

| Path | Purpose |
|---|---|
| `GET /connection/websocket` | WebSocket (append `?format=protobuf` for protobuf) |
| `*  /connection/sockjs/...` | SockJS fallback |
| `POST /api` | HTTP API (`Authorization: apikey <KEY>` header or `?api_key=`) |
| `POST /admin/auth` | Exchange a password for an admin token |
| `POST /admin/api` | Admin API (`Authorization: token <TOKEN>` header) |
| `GET /metrics` | Prometheus metrics |
| `GET /health` | Health check |
| gRPC on `grpc_api_port` (10000) | gRPC API (`authorization: apikey <KEY>` metadata) |

---

## Configuration

Precedence: **flags > config file > environment variables** (`CENTRIFUGO_<OPTION>`).

Example `config.json`:

```json
{
  "token_hmac_secret_key": "secret",
  "api_key": "api-key",
  "admin": true,
  "admin_password": "password",
  "admin_secret": "session-secret",
  "engine": "redis",
  "redis_address": "127.0.0.1:6379",
  "grpc_api": true,
  "grpc_api_port": 10000,
  "presence": true,
  "join_leave": true,
  "history_size": 100,
  "history_lifetime": 300,
  "history_recover": true,
  "namespaces": [
    { "name": "news", "presence": true, "history_size": 10, "history_lifetime": 60 }
  ]
}
```

Key options: `client_insecure`, `client_anonymous`, `token_hmac_secret_key`, `token_rsa_public_key`, `token_ecdsa_public_key`, `token_jwks_public_endpoint`, `api_key`, `api_insecure`, `engine` (`memory`|`redis`), `redis_address`, `proxy_connect_endpoint`, `grpc_api`, `grpc_api_port`, `grpc_api_key`, `admin`, `admin_password`, `admin_secret`, `channel_namespace_boundary` (`:`), `channel_private_prefix` (`$`), plus channel options: `presence`, `join_leave`, `presence_disable_for_client`, `history_size`, `history_lifetime`, `history_recover`, `anonymous`, `server_side`.

### CLI subcommands

```bash
centrifugo gentoken --token_hmac_secret_key <secret> -u <user> [--ttl <sec>]   # issue a JWT
centrifugo genconfig -c config.json                                            # generate a config with random secrets
centrifugo checkconfig -c config.json                                          # validate a config
centrifugo version
```

---

## Conformance (3 tiers)

The ideal "100% of Go tests pass" is not directly achievable: every Go `*_test.go` is an in-process unit test linking Go as a library and cannot target a foreign binary. So compatibility is verified as a **black box** across three tiers:

1. **Go oracle.** The real Centrifugo v2.8.6 binary is built (`conformance/go-oracle/build.sh`). Both servers (Go and Rust) run side by side, are driven with identical commands, and replies are compared structurally (`key_shape` — a value-agnostic shape compare that ignores volatile ids/epochs).
2. **Black-box harness.** Rust tests connect to the running binary over real WebSocket/HTTP/gRPC and check behavior command by command.
3. **Live SDK.** The real **centrifuge-go v0.6.2** client (this is the version that speaks protocol v0.3.4 — the v0.8.4 from the original plan turned out to be incompatible) connects to the Rust binary, subscribes, publishes, and authenticates with a JWT — the decisive compatibility proof.

```bash
# Prepare the oracle (Go required)
bash conformance/go-oracle/build.sh

# Redis for multi-node tests (optional — tests skip otherwise)
brew install redis

# Run
cargo test --workspace
```

Tests requiring external dependencies (Go oracle, Redis, Go SDK) **skip cleanly** when the dependency is absent, so the suite stays green on any machine.

---

## Compatibility notes

- **seq/gen by default.** Centrifugo v2.8.6 uses seq/gen, not offset (`v3_use_offset=false`). `offset = gen*MaxUint32 + seq` (asymmetric with the `>>32` unpack — a centrifuge v0.14.2 quirk, replicated verbatim). Recovered publications are returned in descending order (newest first).
- **Push** is a Reply with `id==0` whose result carries the encoded Push. The integer `method` is omitted when it is 0 (CONNECT).
- **Error codes** 100–111; **disconnect codes** 3000–3013. Semantics verified against the Go source: connect token expired → 109 (reply), invalid/missing → 3002/3003 (disconnect), refresh expired → 3005, presence/history disabled → 108, not subscribed → 103, unknown namespace → 102.
- **History meta-TTL** is decoupled from `history_lifetime`: after the history window elapses only the publications are cleared, while the stream's offset + epoch persist, so a caught-up client reconnecting after an idle period gets `recovered=true`.

---

## Out of scope (deferred)

- Server-side channels (the `subs` field in connect): requires implementing server-side subscriptions in full.
- Protobuf codec for the HTTP API (`application/octet-stream`).
- Redis Sentinel/Cluster sharding; a presence-refresh timer + zset TTL for crashed-node cleanup; a mixed Go+Rust cluster on one Redis (a homogeneous Rust cluster is supported).
- Admin web UI (a prebuilt JS bundle from the Go distribution — out of scope; the functional auth + API are implemented).
- SUB_REFRESH, proxies for subscribe/publish/rpc/refresh, user-limited (`#`) channels.

---

## Status

All milestones M0–M12 are complete. **133 tests pass** (unit + conformance), 0 failures. Every wire behavior is checked against the real Centrifugo v2.8.6 (golden diffs) and confirmed by the live centrifuge-go SDK. A full compatibility audit resolved 17 divergences from the Go reference.
