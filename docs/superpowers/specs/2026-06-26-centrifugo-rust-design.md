# Centrifugo v2.8.6 → Rust: Design Spec

**Date:** 2026-06-26
**Status:** Approved
**Goal:** A from-scratch Rust reimplementation of Centrifugo v2.8.6 that is wire/contract-compatible with the Go original, so that black-box conformance tests pass against the Rust binary and unmodifiable existing clients keep working.

---

## 1. Context & honest framing

Centrifugo v2.8.6 is a thin server shell (the `centrifugo` repo, `main.go`) wrapping two libraries:

- **`github.com/centrifugal/centrifuge` v0.14.2** — the engine/core (Node, Hub, Client session, Memory/Redis engines, history/recovery, presence).
- **`github.com/centrifugal/protocol` v0.3.4** — the wire codec (this is the *protocol v2* era; NOT the v3/v4 framing of later Centrifugo).

### What "Go tests pass against the Rust binary" really means

Every `*_test.go` in both `centrifugo` v2.8.6 and `centrifuge` v0.14.2 is an **in-process Go unit test**: it constructs a `centrifuge.Node` in memory and calls exported Go methods directly, or boots an in-process `httptest` server wired to Go callbacks. These tests link the Go implementation *as a library*. They **cannot** be re-pointed at a foreign (Rust) binary without being rewritten. There is no language-agnostic conformance corpus and no "boot the server, connect a client" harness shipped in the repos.

Therefore **literal "100% of the Go tests pass on the Rust binary" is impossible by construction.** Compatibility is instead defined as **wire-protocol / contract compatibility**, proven by a black-box conformance gate (Section 7). This framing was explicitly accepted by the user.

---

## 2. Architecture (five layers)

1. **Transport** — accepts client connections.
   - WebSocket at `GET /connection/websocket`. Protocol chosen by query param `?format=protobuf` or `?protocol=protobuf` (else JSON). NOT WebSocket-subprotocol negotiation in this version.
   - SockJS at `/connection/sockjs/*` (JSON-only fallback).
   - Server ping = **native WS control-frame Ping** every 25s (`WebsocketPingInterval`). Write timeout 1s, message size limit 64KB (defaults).
2. **Protocol / codec** — three envelopes: `Command{id,method,params}`, `Reply{id,error,result}`, `Push{type,channel,data}`. 12 client method types.
   - **JSON framing = newline-delimited (NDJSON)**, streaming-decode multiple messages per frame. Raw-bytes fields (`params`/`result`/`data`) are embedded as **INLINE raw JSON, NOT base64**.
   - **Protobuf framing = varint length-prefixed**, multiple messages may pack into one binary frame.
   - A `Reply` with `id==0` carrying a `result` is a Push. A reply has either `error` or `result`, never both.
   - `Error{code,message}` standard codes 100–111. `Disconnect{code,reason,reconnect}` codes 3000–3013, sent as the transport close (no DISCONNECT push type in v0.3.4).
3. **Client / session** — per-connection state machine. Enforces CONNECT-first handshake, dispatches the 12 commands, tracks per-channel flags (presence/joinLeave/recover/serverSide), runs token-expiry/refresh and recovery-position checks (closes with `DisconnectInsufficientState` on an offset gap). Each client owns a dedicated writer task draining a bounded queue; a slow consumer gets `DisconnectSlow` so it never blocks the broadcaster.
4. **Hub / Node** — central registry: conns by clientID, users by userID, subs by channel. The Node routes publications/join/leave to local subscribers, encoding each push **exactly once per protocol** (JSON/Protobuf) into a prepared reply, then enqueues bytes to every subscriber's queue. It never writes sockets directly.
5. **Engine** — `Engine = Broker (pub/sub + history) + PresenceManager (presence)`.
   - **MemoryEngine**: single-node, in-process. Per-channel pub locks serialize offset assignment; in-memory streams trim by size/TTL; presence is a map (no TTL on memory engine).
   - **RedisEngine**: multi-node fan-out via Redis PUB/SUB, atomic publish+history Lua scripts, sharding/Sentinel/Cluster, presence ZSET+HASH with TTL.

**Cross-cutting:** server HTTP API (`POST /api`, apikey auth) + gRPC API (`Centrifugo` service, port 10000); JWT verifier (HMAC/RSA/ECDSA/JWKS) for connection + private-channel tokens; viper-style config (file/env `CENTRIFUGO_*`/flags) driving namespaces and channel options; admin UI, Prometheus `/metrics`, `/health`, `/debug/pprof`; HTTP proxy auth alternative.

**Data flow (publish):** client SUBSCRIBE registers clientID in `hub.subs[channel]` and calls `Engine.Subscribe(channel)`. A client PUBLISH or HTTP-API publish calls `Engine.Publish(channel,data,opts)` which assigns the offset, optionally appends to history, then routes the publication back into the Node; the Node looks up `hub.subs[channel]` and enqueues a prepared Publication push to each subscriber's writer queue. Recovery on (re)subscribe: client sends last `offset`+`epoch`, Node fetches history `Since` that position, merges with live-buffered pubs (PubSubSync), returns `publications` + `recovered`.

---

## 3. Chosen approach: C — faithful contracts, idiomatic concurrency

Observable behavior matches Go byte-for-byte (wire frames, error/disconnect codes, recovery semantics, JWT claim handling) so golden-diff stays valid; concurrency is idiomatic Rust.

**Fan-out (the explicit non-blocking requirement):** a connection = a read task + a writer task draining a bounded `tokio::mpsc`. On publish, the Node encodes the push **once per protocol**, then `try_send`s the shared frame to each subscriber's queue. Queue full → that single client gets `DisconnectSlow`; the publisher and every other subscriber are untouched. The Hub is **sharded by channel hash**, so different channels fan out fully in parallel; only same-channel offset assignment is serialized (required for monotonic offsets). The Redis engine reuses the identical local fan-out on each node.

Rejected: **A** (faithful 1:1 with one global hub lock — serializes 100k fan-out); **B** (full lock-free redesign — highest contract-divergence risk, hard to diff).

---

## 4. Workspace / crate layout

A Cargo workspace of focused crates, each independently testable:

| crate | responsibility |
|---|---|
| `centrifugo-protocol` | Command/Reply/Push envelopes; NDJSON codec (inline-raw-JSON via `serde_json::RawValue`); protobuf framing (`prost` + uvarint length-prefix); error codes 100–111; disconnect codes 3000–3013 |
| `centrifugo-core` | `Node`, sharded `Hub`, per-connection `Client` state machine, `ClientInfo`, `StreamPosition{offset,epoch}`, recovery/PubSubSync, `Engine` trait + **Memory engine** |
| `centrifugo-redis` | Redis engine: PUB/SUB node fan-out, atomic publish+history Lua, presence ZSET+HASH TTL, sharding/Sentinel/Cluster |
| `centrifugo-auth` | JWT verify HMAC/RSA/ECDSA, JWKS (kid cache), connect + subscription token claims |
| `centrifugo-config` | layered config (file JSON/YAML/TOML + `CENTRIFUGO_*` env + CLI flags), namespaces, channel options |
| `centrifugo-server` | the binary: WS + SockJS transports, HTTP `/api`, gRPC (`tonic`), admin/metrics/health, proxy hooks, CLI subcommands (`serve`/`genconfig`/`gentoken`/`checkconfig`/`version`) |
| `conformance/` (tests) | spawn-binary harness, Go-oracle build, golden differential, `centrifuge-go` v0.8.4 runner |

---

## 5. Recommended Rust crates per concern

- Async runtime: **tokio**
- HTTP server/routing: **axum** (on hyper + tower)
- WebSocket: **tokio-tungstenite** / `axum::extract::ws` (need explicit control-frame access for the native-WS ping)
- Protobuf: **prost** + **prost-build**; custom uvarint length-prefix framer
- JSON: **serde_json** with **`RawValue`** (critical for inline-raw-JSON bytes, never base64)
- JWT: **jsonwebtoken** (HS/RS/ES 256/384/512); **reqwest** for JWKS fetch + hand-rolled kid cache
- Config: **config** + **clap** + **serde** (flags > file > env priority)
- Hub maps: std `RwLock<HashMap>` per shard (or **dashmap** if profiling demands)
- Per-client queue: **tokio::sync::mpsc** (bounded; overflow → `DisconnectSlow`)
- gRPC: **tonic** + prost (tower interceptor for `authorization: apikey`)
- Redis: **redis** (tokio + connection-manager) or **fred** (first-class cluster/Sentinel)
- Metrics: **prometheus** (tikv/rust-prometheus)
- Logging: **tracing** + **tracing-subscriber**
- Conformance harness: tokio-tungstenite + reqwest + assert_cmd + a Rust JWT signer

---

## 6. Contract surface (must reproduce)

**Client methods (12):** CONNECT (must be first), SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PRESENCE, PRESENCE_STATS, HISTORY (no since/limit/reverse in v0.3.4), PING, SEND (fire-and-forget), RPC, REFRESH, SUB_REFRESH.

**Async pushes:** Publication{seq,gen,uid,data,info,offset}, Join{info}, Leave{info}, Unsub{resubscribe}, Sub{recoverable,seq,gen,epoch,offset}, Message{data}. ClientInfo{user,client,conn_info,chan_info}.

**JWT — connect token claims:** sub, exp, info, b64info, channels (server-side subs), nbf, iat. Algorithms HS/RS/ES 256/384/512; JWKS (RSA, kid). `client_insecure`/`anonymous`/`client_anonymous` modes (no token) — REQUIRED for the centrifuge-go conformance suite.

**JWT — subscription token claims:** client (required), channel (required), info, b64info, exp, eto. Private-channel prefix `$`; sub-token refresh via SUB_REFRESH.

**Channels / namespaces:** ASCII, max length 255. Separators `:` (namespace), `$` (private), `#` (user-limited), `,` (user list). Namespace options (no inheritance): publish, subscribe_to_publish, anonymous, presence, presence_disable_for_client, join_leave, history_size, history_lifetime, history_recover, history_disable_for_client, server_side. Namespace name regex `^[-a-zA-Z0-9_]{2,}$`.

**History/recovery/presence:** `StreamPosition{offset,epoch}`. History enabled only when `history_size>0 AND history_lifetime>0`; PublishOptions carry TTL/size per publish. Recovery: client sends last offset+epoch → server returns publications `Since` + `recovered`. PubSubSync buffers live pubs during history fetch, merges+dedupes by offset, verifies contiguity or `DisconnectServerError`. Client position check: `pub.offset == current+1` else `DisconnectInsufficientState`; periodic checkPosition (2 failures → disconnect). Presence map clientID→ClientInfo; memory engine no TTL; presence update 25s / expire 60s defaults.

**HTTP API (`POST /api`):** apikey auth (`Authorization: apikey <KEY>` header or `?api_key=`; `api_insecure` disables; empty key → 401). Command JSON `{method,params}`, NDJSON pipelining, reply `{result}|{error}` at HTTP 200. Commands: publish, broadcast, unsubscribe, disconnect, presence, presence_stats, history, history_remove, channels, info, rpc. NO server-side subscribe in v2.

**gRPC API:** proto package `api`, service `Centrifugo`, port 10000, `grpc_api=true`. Unary RPCs Publish/Broadcast/Unsubscribe/Disconnect/Presence/PresenceStats/History/HistoryRemove/Channels/Info/RPC. `grpc_api_key` via per-RPC metadata `authorization: apikey <KEY>`.

**Config:** file (JSON/YAML/TOML) + env `CENTRIFUGO_<OPTION>` + flags (priority flags>file>env). Core keys: address, port (8000), engine (memory|redis), api_key, api_insecure, token_* keys, client_insecure/anonymous, channel_* boundaries, namespaces, endpoint disable flags, internal_port. Subcommands: serve, genconfig, gentoken, checkconfig, version. SIGHUP reload of secrets + channel options.

**Auxiliary:** `/health` (health=true), `/metrics` Prometheus, admin UI `/` + `/admin/auth` + `/admin/api`, `/debug/pprof` (debug=true). HTTP proxy auth: proxy_connect/refresh/rpc endpoints + per-namespace proxy_subscribe/proxy_publish.

---

## 7. Test / conformance gate (3-tier — chosen)

1. **Go oracle.** Build the real Go `centrifugo` v2.8.6 from source (Go toolchain installed locally) as a behavior oracle. Its own Go unit tests are run to confirm the oracle is green; they are not (and cannot be) run against the Rust binary.
2. **Rust black-box harness** (the workhorse, `#[tokio::test]`). Spawn the Rust binary as a child process, wait for `/health`, drive it over the network — mirroring the *behaviors* of `centrifuge`'s `handler_websocket_test.go`, `token_verifier_jwt_test.go`, `internal/api/handler_test.go`, `rule_test.go`, etc.:
   - WS: dial `/connection/websocket` in JSON and `?format=protobuf`; Connect→client id, Subscribe→Reply, Publish→subscriber receives Publication; ping/pong; custom disconnect codes.
   - HTTP API: `POST /api` with apikey for all commands; assert exact JSON result/error shapes.
   - Presence/History/Recovery: publish to a history-enabled channel; read history/presence/presence_stats; resubscribe with stale offset/epoch → assert `recovered` + publication set.
   - JWT: HS256 & RS256 connect tokens (valid/expired/bad-sig/disabled-alg) + refresh flow.
3. **Official SDK suite (headline gate).** Run the era-correct **`centrifuge-go` v0.8.4** live-server test suite (`go test`) unmodified against the Rust binary in `client_insecure` mode on :8000. Optionally a `centrifuge-js` live subset.
4. **Golden differential.** Stand up the Go binary and the Rust binary side by side, drive both with identical scripted commands, byte-diff replies (canonicalize JSON key order; decode protobuf). Redis adds a 2-node fan-out diff.

"Conformant for milestone N" = Tier-2 green for that milestone's surface + relevant Tier-3 subset green + Tier-4 diff clean for the covered commands. Document explicitly which Go test files are NOT runnable (all of them, in-process) vs which behaviors the black-box mirror covers.

---

## 8. Milestone plan

Each milestone = implement → conformance tests for that surface → commit.

- **M0** — Repo scaffold: `git init`, Cargo workspace, `/health`, Go-oracle build script, conformance harness skeleton, CI skeleton.
- **M1** — WS+NDJSON; connect/subscribe/publish/unsubscribe; sharded Hub; memory pub/sub (no history/presence); native 25s WS ping. *Deliverable:* spawn-binary test does Connect→Subscribe→Publish→receive Publication on a second connection.
- **M2** — All 12 methods; protobuf framing (prost from client.proto); full error codes 100–111 + disconnect codes 3000–3013 with correct reconnect flags; CONNECT-first enforcement; message size limit + write timeout. *Deliverable:* JSON+protobuf green; centrifuge-go Connect/Subscribe/Publish/Unsubscribe subset passes.
- **M3** — JWT connect auth (HMAC/RSA/ECDSA, alg-from-header with disabled-alg rejection); token expiry → ErrorTokenExpired + REFRESH flow; anonymous/insecure toggles.
- **M4** — Presence + Join/Leave (PresenceManager in memory engine; PRESENCE/PRESENCE_STATS; flags gated by namespace presence/join_leave; ClientInfo from token info/b64info).
- **M5** — History + recovery (in-memory streams: offset counter + random epoch, size/TTL trim, expire+remove timers; HISTORY; PublishOptions TTL/size; StreamPosition recovery; PubSubSync merge/dedupe; position check → DisconnectInsufficientState). *Hardest correctness work.*
- **M6** — HTTP `/api` (apikey header+query, api_insecure; all 11 commands; NDJSON pipelining) + namespace config & channel-option enforcement + channel name/boundary rules + private-channel `$` + SubscribeToken verification.
- **M7** — gRPC API (`tonic`, port 10000, apikey metadata interceptor; all 11 RPCs).
- **M8** — **Redis engine** (multi-node PUB/SUB, atomic publish+history Lua, presence ZSET+HASH TTL, sharding/Sentinel/Cluster) + 2-node golden diff.
- **M9** — SockJS transport.
- **M10** — JWKS (RSA, kid cache, 1h, HTTPS) + HTTP proxy auth (proxy_connect/refresh/rpc + per-namespace proxy_subscribe/proxy_publish).
- **M11** — Admin UI + Prometheus `/metrics` + `/debug/pprof` equivalent + full config (file/env/flags, genconfig/gentoken/checkconfig) + SIGHUP reload + graceful shutdown + allowed_origins/CheckOrigin + internal_port separation + endpoint disable flags.
- **M12** — Full differential CI; full `centrifuge-go` v0.8.4 suite green against a configured Rust binary; hardening.

---

## 9. Git workflow

`git init` on `main`. A feature branch (or git worktree) per milestone; worktrees let subagents build independent milestones in parallel. Commit per logical unit. Repository name `centrifugo-rust`. Commits carry the Co-Authored-By trailer.

---

## 10. Biggest risks

1. **Recovery + offset/epoch + PubSubSync (M5)** — deepest correctness trap; contiguous-offset merge, isRecovered logic, periodic position check, DisconnectInsufficientState. Golden differential vs Go is the main defense.
2. **Wire-byte fidelity, especially JSON** — inline raw JSON (not base64), key ordering, omitempty, `id==0`-means-Push. A serde mistake silently breaks every SDK. Protobuf varint framing/packing must be byte-identical.
3. **Ping/transport details** — server ping is a native WS control frame in this version (not app-level as in v3+); `?format=protobuf` query selection (not subprotocol negotiation). The centrifuge-go v0.8.4 suite depends on these.
4. **Conformance gate is not turnkey** — must be built; SDK tags must be era-correct (v0.8.4).
5. **Redis engine fidelity (M8)** — atomic Lua, `__offset__payload` wire prefix, consistent sharding, presence ZSET TTL.
6. **Concurrency translation** — preserve slow-consumer (DisconnectSlow) and back-pressure so a slow client never head-of-line-blocks the broadcaster.
