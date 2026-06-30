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
| History & recovery | ✅ seq/gen, recovery on (re)subscribe, descending order; `history_disable_for_client` → 108 |
| Presence + join/leave | ✅ presence TTL (Redis) + per-connection refresh timer |
| Token expiry enforcement | ✅ timer disconnects expired connections (3005) / subscriptions (3006) after a grace window |
| Server-side channels | ✅ `subs` in connect, JWT `channels` → auto-subscribe |
| User-limited (`#`) channels | ✅ `name#u1,u2` membership check |
| Publish permission | ✅ `publish` / `subscribe_to_publish` channel options |
| Origin allow-listing (`allowed_origins`) | ✅ WS upgrade + SockJS; glob patterns (`*`, `https://*.example.com`), case-insensitive; 403 on mismatch |
| Connection & channel limits | ✅ `channel_max_length` (255), `client_channel_limit` (128) → 106; `client_user_connection_limit` → 3013 |
| JWT authentication | ✅ HMAC (HS256/384/512), RSA (RS*), ECDSA (ES256/384) |
| JWKS | ✅ key selection by `kid`, background refresh |
| Proxies (HTTP callbacks) | ✅ connect, refresh, subscribe, publish, rpc |
| Namespaces & private channels (`$`) | ✅ |
| HTTP API (`POST /api`) | ✅ apikey auth; JSON (NDJSON) **and** Protobuf (`application/octet-stream`); echoes request Content-Type |
| Server API `unsubscribe` / `disconnect` | ✅ force a user off a channel / close a user's connections (HTTP + gRPC, cluster-wide) |
| gRPC API (port 10000) | ✅ same 11 RPCs, apikey in metadata |
| Personal channels | ✅ `user_subscribe_to_personal` auto-subscribe to `#<user>` |
| Engines | ✅ Memory (single node) **and** Redis (multi-node), incl. **Sentinel** with mid-flight failover re-resolution |
| Go ⇄ Rust Redis interop | ✅ live pub/sub **+ history + presence + control + node-info** across Go + Rust nodes on one Redis (centrifuge wire format) — each side's `info` lists the other's nodes |
| Admin (`/admin/auth`, `/admin/api`) | ✅ token auth + vendored web UI at `/` |
| Prometheus metrics (`/metrics`) | ✅ node gauges + per-command/per-message/per-transport counters |
| Configuration | ✅ flags + JSON file (`-c`) + env (`CENTRIFUGO_*`) |
| CLI subcommands | ✅ root command = server; `gentoken`, `checktoken`, `genconfig`, `checkconfig`, `version` |

---

## Architecture

The project is split into 6 crates (Cargo workspace):

| Crate | Responsibility |
|---|---|
| `centrifugo-protocol` | Wire format: Command/Reply/Push envelopes, NDJSON (inline-raw JSON), uvarint length-prefixed protobuf, error codes (100–111), disconnect codes (3000–3013), JSON/Protobuf codec |
| `centrifugo-auth` | JWT verification (HMAC/RSA/ECDSA), JWKS by `kid`, manual exp/nbf checks, subscription tokens, token generation |
| `centrifugo-core` | `Node`, sharded `Hub`, `Client` state machine, per-subscription state, `Engine` abstraction (pub/sub + history + presence + control), `MemoryEngine`, proxy traits (connect/refresh/subscribe/publish/rpc), metrics registry |
| `centrifugo-grpc` | tonic codegen (server + client) from `api.proto` via pure Rust (`protox`, no `protoc`) |
| `centrifugo-redis` | `RedisEngine`: cross-node fan-out in **centrifuge v0.14.2's wire format** (Go⇄Rust interop), Lua list-history + zset/hash presence, Sentinel discovery + mid-flight failover |
| `centrifugo-server` | The `centrifugo` binary: CLI, config, HTTP (axum), WebSocket, SockJS, HTTP/gRPC API, admin, metrics; outbound TLS (JWKS/proxies) via rustls (no OpenSSL) |

### Non-blocking fan-out (the load-bearing requirement)

Broadcasts to **10,000 / 100,000** subscribers never block each other:

- Each connection = a reader task + a writer task draining a bounded `tokio::mpsc` queue.
- On publish, the `Node` encodes the push **once per protocol**, then `try_send`s the prepared frame to each subscriber.
- The `Hub` is **sharded by channel hash**, so different channels fan out fully in parallel; only same-channel offset assignment is serialized.
- A slow subscriber whose queue fills is disconnected with **DisconnectSlow (3008)** and removed; the publisher and every other subscriber are untouched.

### Engine abstraction

`Engine` (async trait) unifies pub/sub + history + presence. One `Arc<dyn Engine>` backs the `Node`:

- **MemoryEngine** — single node, in-process. History is a size-bounded ring with lazy TTL; presence is a map; the stream meta (offset + epoch) persists past `history_lifetime` expiry (matching Go).
- **RedisEngine** — multi-node, **byte-compatible with centrifuge v0.14.2's Redis format**, so Go and Rust nodes can share one Redis. Each node pattern-subscribes to `centrifugo.client.*` and routes incoming messages — protobuf `Publication`, plus `__j__`/`__l__`-framed joins/leaves — into its local hub. History is a list (`centrifugo.list.<ch>`, `__<offset>__<protobuf>` entries, LPUSH) + meta hash (`s`=offset, `e`=epoch) appended by the verbatim centrifuge Lua; presence is a `clientID → protobuf ClientInfo` data hash plus an expiry zset, with atomic Lua add/prune-by-score read so crashed-node entries expire. The master can be discovered via **Redis Sentinel** (`redis_master_name` + `redis_sentinels`) with mid-flight failover re-resolution. Cross-node control (unsubscribe/disconnect) and periodic NODE-info pings ride `centrifugo.control` as centrifuge `controlpb` protobuf, so cluster membership and control commands interoperate with Go nodes too.

---

## Build & run

Requires Rust (stable). The Go oracle and live-SDK test need Go; the Redis tests need `redis-server`.

```bash
# Build
cargo build --release          # binary: target/release/centrifugo

# Fully static binary (no glibc/OpenSSL) — see the Docker section, or directly:
#   rustup target add x86_64-unknown-linux-musl   # + musl-tools
#   cargo build --release --target x86_64-unknown-linux-musl -p centrifugo-server

# Run the server — root command (like Go centrifugo; `serve` kept as a hidden alias)
./target/release/centrifugo --client_insecure

# With a config file (without -c it auto-reads ./config.json from the working dir)
./target/release/centrifugo -c config.json

# All tests (unit + conformance)
cargo test --workspace
```

### Docker

The multi-stage `Dockerfile` builds a **fully static binary** (musl libc + rustls TLS with bundled CA roots — no OpenSSL, no glibc, no system cert store) and ships it on `scratch`, so the image is just the self-contained binary with zero runtime dependencies. `compose.yml` brings up a **two-node cluster sharing one Redis** (the Redis engine fans publications across nodes):

```bash
docker compose up --build
# node-1 admin UI:  http://localhost:8000/   (password: password)
# node-2 admin UI:  http://localhost:8001/
# HTTP API:         POST http://localhost:8000/api   (Authorization: apikey api-secret-key)
# gRPC API:         localhost:10000
```

A client subscribed on node-1 receives messages published via node-2's API — demonstrating the cross-node engine. `.dockerignore` keeps the build context lean (no `target/`, vendored Go oracle, or docs).

### Drop-in compatibility with the official image

The image is meant as a substitute for `centrifugo/centrifugo:v2.8.6`: the root command runs the server, `centrifugo` is on `PATH`, the working dir is `/centrifugo` with `./config.json` auto-discovery, every flag is also read from `CENTRIFUGO_<NAME>`, the official flags/aliases (incl. `redis_host`/`redis_port`/`redis_url`) are accepted, and `checktoken` exists. So official commands work as-is:

```bash
docker run -p 8000:8000 centrifugo-rust:local centrifugo --port 8000 --client_insecure
docker run -p 8000:8000 -v ./config.json:/centrifugo/config.json centrifugo-rust:local
```

Unimplemented features (in-process TLS, NATS broker, separate internal port, Redis/gRPC TLS) are **accepted with a warning** rather than aborting startup. Full per-surface breakdown (launch, flags, env, subcommands, config) and a migration checklist: [`docs/COMPATIBILITY_v2.8.6.md`](docs/COMPATIBILITY_v2.8.6.md).

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

Key options: `client_insecure`, `client_anonymous`, `token_hmac_secret_key`, `token_rsa_public_key`, `token_ecdsa_public_key`, `token_jwks_public_endpoint`, `api_key`, `api_insecure`, `engine` (`memory`|`redis`), `redis_address`, `redis_master_name`, `redis_sentinels`, `client_presence_ping_interval`, `client_presence_expire_interval`, `proxy_connect_endpoint`, `proxy_refresh_endpoint`, `proxy_subscribe_endpoint`, `proxy_publish_endpoint`, `proxy_rpc_endpoint`, `grpc_api`, `grpc_api_port`, `grpc_api_key`, `admin`, `admin_insecure`, `admin_password`, `admin_secret`, `admin_web_path`, `user_subscribe_to_personal`, `user_personal_channel_namespace`, `channel_namespace_boundary` (`:`), `channel_private_prefix` (`$`), plus channel options: `presence`, `join_leave`, `presence_disable_for_client`, `publish`, `subscribe_to_publish`, `proxy_subscribe`, `proxy_publish`, `history_size`, `history_lifetime`, `history_recover`, `anonymous`, `server_side`. Plus the Go-compatible Redis aliases `redis_host`/`redis_port`/`redis_url` (mapped into `redis_address`), `redis_db`, `redis_password`, `redis_prefix`, and `name`, `log_level`, `pid_file`. Every server flag is also read from `CENTRIFUGO_<NAME>` (like Go's viper).

### CLI subcommands

```bash
centrifugo --client_insecure                                                   # run the server (root command)
centrifugo gentoken --token_hmac_secret_key <secret> -u <user> [-t <sec>]      # issue a JWT (7-day TTL by default)
centrifugo checktoken --token_hmac_secret_key <secret> <JWT>                   # verify a JWT
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

### What each test file covers

The conformance suite lives in `conformance/tests/` (one file per stage). Plus crate unit and integration tests (protocol codec, auth/JWT, core `Client`/`Hub`/`Node`/`NodeRegistry`/metrics, redis helpers, generated Protobuf contracts). **275 test entries in `cargo test --workspace -- --list`**; `perf` is a Go-vs-Rust benchmark marked `#[ignore]` and is not run by a plain `cargo test --workspace` (see [Performance](#performance)).

**Core wire compatibility — M0–M12**

| File | Covers |
|---|---|
| `m0_smoke` | binary starts and becomes healthy |
| `m1_golden`, `m1_vertical` | golden diff vs the Go oracle; connect → subscribe → publish → receive over the wire |
| `m2_disconnect`, `m2_protobuf` | pre-CONNECT disconnect codes; the `?format=protobuf` transport end-to-end |
| `m3_jwt` | JWT auth — HMAC (HS*), RSA (RS*), ECDSA (ES*), exp/nbf, golden connect reply |
| `m4_presence` | presence, presence_stats, join/leave (golden) |
| `m5_history` | history + recovery (seq/gen, descending order; golden) |
| `m6_api`, `m6_namespaces`, `m6_private` | HTTP API (apikey); namespace resolution; private `$`-channels (subscription token) |
| `m7_grpc` | gRPC server API (apikey metadata) |
| `m8_redis`, `m8_slow_consumer` | Redis multi-node (cross-node publish/presence/history); slow-consumer → DisconnectSlow 3008 |
| `m9_sockjs` | SockJS fallback (xhr-polling + `/info` + CORS) |
| `m10_jwks`, `m10_proxy` | JWKS key-by-`kid`; connect proxy |
| `m11_admin`, `m11_cli`, `m11_env`, `m11_metrics` | admin auth + insecure; CLI subcommands; `CENTRIFUGO_*` env; Prometheus `/metrics` |
| `m12_live_sdk` | the real **centrifuge-go v0.6.2** SDK drives the Rust binary (decisive proof) |

**Full Go-parity phases — m13–m21**

| File | Covers |
|---|---|
| `m13_user_channels` | user-limited (`#`) channels |
| `m14_sub_refresh` | SUB_REFRESH (method 11) |
| `m15_server_side` | server-side channels (JWT `channels` → auto-subscribe + `subs`) |
| `m16_presence_ttl` | Redis presence TTL + per-connection refresh timer |
| `m17_proxies` | refresh / subscribe / publish / rpc proxies |
| `m18_protobuf_api` | Protobuf HTTP API (`application/octet-stream`) |
| `m19_publish_permission` | client publish permission (`publish` / `subscribe_to_publish`) |
| `m20_redis_sentinel` | Redis Sentinel master discovery |
| `m21_admin_ui` | admin web UI + `admin_web_path` |

**Audit fixes, post-audit features, drop-in, Go⇄Rust interop & round-2 config/security — m22–m32**

| File | Covers |
|---|---|
| `m22_subscribe_validation` | SUBSCRIBE/UNSUBSCRIBE/SUB_REFRESH validation (105/3003/107 parity) |
| `m23_server_api` | server-side `unsubscribe` / `disconnect` API (cluster-wide) |
| `m24_personal` | personal channels (`user_subscribe_to_personal`) |
| `m25_go_rust_cluster` | **Go ⇄ Rust on one Redis** — pub/sub, history, presence, control (unsubscribe/disconnect), and node-info, both directions |
| `m26_dropin` | drop-in launch parity (unknown flags don't abort startup, admin via env, gentoken TTL, checktoken) |
| `m27_protocol_semantics` | post-audit error/disconnect semantics (malformed params → 3003, unknown method → 104, 2nd CONNECT/id==0/empty frame → 3003, RPC → 108, bare PING reply) |
| `m28_frame_coalescing` | WS writer coalesces up to 4 messages per frame without loss/reorder |
| `m29_channel_limits` | `channel_max_length` (255) + `client_channel_limit` (128) enforced at SUBSCRIBE → 106 |
| `m30_user_connection_limit` | `client_user_connection_limit` → DisconnectConnectionLimit (3013), per authenticated user |
| `m31_history_disable_for_client` | `history_disable_for_client` → ErrorNotAvailable (108) even when history is stored |
| `m32_allowed_origins` | `allowed_origins` enforced on the WS upgrade + SockJS (403 on mismatch; case-insensitive; absent Origin allowed) |

**Generated Protobuf contract tests**

| File | Covers |
|---|---|
| `crates/centrifugo-protocol/tests/generated_apipb.rs` | generated `prost` client-protocol types (`client.proto`): encode/decode round-trip, `encoded_len`, enum wire values, fixed golden bytes, domain ⇄ protobuf conversions, and `Raw` byte semantics |
| `crates/centrifugo-grpc/tests/generated_apipb.rs` | generated `prost`/tonic server-API types (`api.proto`): request/response/result wrappers, error responses, `NodeResult`/`Metrics`, enum wire values, and fixed golden bytes |

---

## Performance

A comparison of this implementation against the real Centrifugo v2.8.6 (Go) under identical load. The benchmark is `conformance/tests/perf.rs` (marked `#[ignore]`):

```bash
# release binary — for a fair comparison vs the optimized Go
cargo build --release -p centrifugo-server
CENTRIFUGO_TEST_BIN="$PWD/target/release/centrifugo" \
  cargo test --test perf -- --ignored --nocapture
```

Two metrics (identical methodology for both backends, so the *ratio* is what matters, not the absolute numbers):

- **Fan-out throughput** — 100 subscribers on one channel, 500 publishes via the HTTP API → 50,000 deliveries; rate = deliveries / wall-clock.
- **Broadcast latency** — single subscriber, median/p95 of publish-call → delivery.

Measured on a **MacBook (Apple M4 Pro, 12 cores, 24 GB RAM)**, median of 3 runs, memory engine (single-node fan-out, apples-to-apples):

| Metric | Rust | Go v2.8.6 | Ratio |
|---|---|---|---|
| Fan-out, deliveries/s | **~235,000** (100% delivered) | ~66,000 (100%) | **Rust ≈ 3.5× faster** |
| Broadcast latency, median | **0.13 ms** | 0.14 ms | ≈ parity (Rust marginally lower) |
| Broadcast latency, p95 | **0.16 ms** | 0.16 ms | ≈ parity |

Both backends deliver 100% at this load; Rust's edge is fan-out throughput. This is a single-machine microbenchmark — it shows the order-of-magnitude fan-out difference, not absolute production numbers. (The bench counts messages, not WS frames: both servers coalesce up to 4 messages per frame, so `perf.rs` sums NDJSON lines — earlier figures here undercounted coalesced deliveries.)

---

## Compatibility notes

- **seq/gen by default.** Centrifugo v2.8.6 uses seq/gen, not offset (`v3_use_offset=false`). `offset = gen*MaxUint32 + seq` (asymmetric with the `>>32` unpack — a centrifuge v0.14.2 quirk, replicated verbatim). Recovered publications are returned in descending order (newest first).
- **Push** is a Reply with `id==0` whose result carries the encoded Push. The integer `method` is omitted when it is 0 (CONNECT).
- **Error codes** 100–111; **disconnect codes** 3000–3013. Semantics verified against the Go source: connect token expired → 109 (reply), invalid/missing → 3002/3003 (disconnect), refresh expired → 3005, presence/history disabled → 108, not subscribed → 103, unknown namespace → 102.
- **History meta-TTL** is decoupled from `history_lifetime`: after the history window elapses only the publications are cleared, while the stream's offset + epoch persist, so a caught-up client reconnecting after an idle period gets `recovered=true`.

---

## Out of scope (deferred)

- **Redis Cluster / sharding.** Only single-master Redis (directly or via Sentinel) is supported — no consistent-hash sharding across multiple Redis shards.
- **A live Sentinel-failover integration test.** Mid-flight master re-resolution is implemented, but a CI test that actually fails a master over needs a replica + Sentinel-promotion harness (the live scenario is verified manually).

---

## Status

Development ran across stages **M0–M32** (see the test-file breakdown above):

- **M0–M12** — core wire compatibility (transports, commands, history/recovery, presence, JWT/JWKS, namespaces, HTTP/gRPC API, Redis, SockJS, admin, metrics, CLI, live-SDK proof).
- **m13–m21** — full Go-parity phases (`#`-channels, SUB_REFRESH, server-side channels, presence TTL, granular proxies, Protobuf HTTP API, publish permission, Redis Sentinel, admin web UI).
- **m22–m25** — adversarial-audit fixes + post-audit features (server-side unsubscribe/disconnect, personal channels, Sentinel mid-flight failover, per-command metrics) and **full Go⇄Rust Redis interop** (pub/sub + history + presence + control + node-info).
- **m26–m28** — drop-in launch parity, post-audit protocol semantics (see below), and WS frame coalescing.
- **m29–m32** — round-2 config/security audit: channel & per-user connection limits (`channel_max_length`/`client_channel_limit`/`client_user_connection_limit`), `history_disable_for_client`, and `allowed_origins` Origin enforcement on WS + SockJS — plus CLI bool-flag precedence over the config file (viper parity). See `docs/POSTAUDIT_v2.8.6.md` (Round 2).

All complete: **275 test entries** (unit + integration + conformance, including `perf` in the list), 0 failures in the verified suites. Every wire behavior is checked against the real Centrifugo v2.8.6 (golden diffs) and confirmed by the live **centrifuge-go v0.6.2** SDK (connects, subscribes, publishes, authenticates with a JWT — unmodified). The generated Protobuf suites additionally pin the binary contract of `client.proto` and `api.proto`.

A **second adversarial audit** (after the interop, control, and refresh changes) found 13 real divergences from the Go reference — all fixed, each with a test:

- **B1** — live publications on recoverable channels ship seq/gen (offset zeroed), matching Go's `UseSeqGen` (the v2.8.6 default);
- **B2** — cross-node Disconnect carries `reconnect` + `whitelist` (load-bearing for `user_personal_single_connection`);
- **B3** — the refresh proxy is server-side: proactive timer-driven renewal + a client REFRESH is rejected (3003), like Go;
- **B4** — AUTH/db for the Sentinel-resolved master + `redis_password`/`redis_db` config;
- **B5** — server-side unsubscribe/disconnect apply locally before publishing + own-uid loopback skip;
- **B6/B8/B9/B10/B11** — API string/code parity (apikey exactly 2 fields; `rpc` method → 107/104; gRPC message `unauthenticated`; Content-Type echoed; decode_control doc);
- **B7** — node name = `hostname_port`/config `name` (not the UID);
- **B12** — control signals are no longer dropped silently + expiry moved off the 25s presence tick onto a dedicated timer;
- **B13** — `redis_prefix` and `redis_history_meta_ttl` are now configurable.

A **third pass — finding implementation divergences from Go** (adversarial audit against the Go sources + the live Go oracle) found **17 divergences** (all on abnormal-input/config paths; the steady-state pub/sub/recovery path had none). All fixed, each with a test:

- **H2** — the self-Join now arrives *after* the subscribe reply (Go flushes the reply, then publishes Join), not before — the only HIGH on a common path;
- **H3** — fractional/string JWT `exp`/`nbf` (float NumericDate emitted by common JWT libraries) is accepted; an expired such token → 109 (refresh), not 3002;
- **H4** — `redis_url` db/password from the URL win over config (like Go) — otherwise a silent split-brain in a mixed Go/Rust cluster;
- **H1/H5/M1/M2/M3** — error/disconnect semantics: malformed params → close 3003 (not 107); unknown method → reply 104 (connection stays open, not 3003); a 2nd CONNECT → 3003; `id==0` (non-Send) → 3003; RPC with no proxy → 108;
- **M4** — the PING reply is a bare `{"id":N}` with no `result:{}` (JSON parity);
- **M5** — env vars beat the config file (viper precedence `flag > env > file`);
- **M6** — config validation at startup/`checkconfig` (history recovery requires size+lifetime; namespace-name regex/uniqueness; personal-namespace existence) — exit 1, like Go;
- **L1/L2** — empty frame → 3003; explicit `null` for seq/gen/epoch is treated as zero (like `encoding/json`);
- **L3** — `memory_history_meta_ttl` implemented (an idle stream resets → fresh epoch/offset);
- **L4** — JWKS is RSA + `use:sig` only (matching Go, which rejects non-RSA/non-sig keys);
- **L5** — the WS writer coalesces up to 4 messages per frame (like Go);
- **L6** — epoch format is 4 chars `[a-zA-Z]` (like `memstream.genEpoch`).

The full post-audit report is in `docs/POSTAUDIT_v2.8.6.md` (only L7 — comma-sharded Redis — is left out of scope by design, per the Sentinel-only decision).
