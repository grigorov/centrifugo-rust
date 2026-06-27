# Post-Audit Features Implementation Plan

> **For agentic workers:** implement task-by-task, in order. Verify each with tests before committing. One commit per task. Never run the full suite while editing source.

**Goal:** Close the remaining gaps after the full-parity audit — server-side unsubscribe/disconnect API, then the residual audit items, the optional refinements, and Go+Rust Redis interop.

**Architecture:** Mirror Centrifugo v2.8.6 / centrifuge v0.14.2 wire + API behavior. Verify every behavior against `conformance/go-oracle/src` and `~/go/pkg/mod/github.com/centrifugal/centrifuge@v0.14.2`.

**Tech Stack:** Rust workspace (centrifugo-{protocol,auth,core,grpc,redis,server}); tokio, axum, prost/tonic, redis.

**Order (as directed):** A1 → A2 → B(#16, #19, #24) → C(personal channels, Sentinel failover, admin WS stats, full metrics) → D10.

> **STATUS: COMPLETE** (suite 186 passed / 0 failed). A1 `ef72f76`, A2 `617e8ab`, B1 `ddba6a5`, B2 `52c23bb`, B3 `ce937ae`, C1 `a469abb`, C2 `c5d5862`, C3 `8dc2942` (admin had no WS — SPA polls /admin/api info; verified), C4 metrics, D10 `46e47fe` (live pub/sub interop; history/presence/control stay Rust-native). Remaining deferrals: D9 Redis Cluster/sharding (out of scope), full byte-compat Go interop for history/presence/control, a live Sentinel-failover integration test.

---

## A1 — Server-side `unsubscribe` / `disconnect` via API

**Goal:** The HTTP (JSON + protobuf) and gRPC server APIs can force a user (or a specific client) off a channel and close a user's connections. Currently stubbed (JSON→104, pb/gRPC→no-op).

**Go ref:** `internal/api/api.go` `Executor.Unsubscribe`/`Disconnect`; centrifuge `node.go` `Unsubscribe`/`Disconnect`, `client.go` `Unsubscribe`/`Disconnect`. `unsubscribe`: `user` required (107), channel validated if non-empty (102), empty channel = all channels. `disconnect`: `user` required (107). Both broadcast a control command to all nodes; each node acts on its local clients. Server unsubscribe sends an Unsubscribe push (PushType Unsubscribe) to the client; disconnect closes the transport with the disconnect code.

**Files:**
- `crates/centrifugo-core/src/hub.rs`: add `Control { Unsubscribe(String), Disconnect(Disconnect) }`; `ClientHandle.ctrl: Sender<Control>`; `Hub::user_clients(user) -> Vec<ClientHandle>`; helpers to send control to a user's clients (all channels when channel empty).
- `crates/centrifugo-core/src/client.rs`: a way for the reader to apply a Control — `unsubscribe_channel` already exists; add server-unsubscribe push emission; expose `set_ctrl`/store ctrl receiver wiring via the transport.
- `crates/centrifugo-core/src/engine.rs` + `memory.rs` + `centrifugo-redis/src/lib.rs`: a node-to-node **control channel** so multi-node clusters act cluster-wide. Add `Engine::publish_control(bytes)` + a `Control` arm in the route callback. Memory engine = local only (no-op publish, direct apply). Redis = publish to a `centrifugo.control` pub/sub channel; each node applies. (Rust-native control encoding here; Go-compatible encoding lands in D10.)
- `crates/centrifugo-core/src/node.rs`: `unsubscribe_user(user, channel)` / `disconnect_user(user, disconnect)` — apply locally + broadcast control.
- `crates/centrifugo-server/src/ws.rs` + `sockjs.rs`: the reader loop selects on a per-connection control receiver; on Unsubscribe → `client.server_unsubscribe(channel)`, on Disconnect → send close + break.
- `crates/centrifugo-server/src/api.rs`: implement JSON `unsubscribe`/`disconnect` (currently 104) + pb `Unsubscribe`/`Disconnect` (currently void no-op).
- `crates/centrifugo-server/src/grpc.rs`: implement `unsubscribe`/`disconnect` (currently no-op).
- `crates/centrifugo-protocol`: confirm PushType::Unsubscribe exists; add if missing.

**Approach:**
1. PushType::Unsubscribe (verify/add) + a server-unsubscribe push in client.
2. Per-connection control channel (mpsc) created in `new_client`/transport; store its sender in `ClientHandle`.
3. Reader loops select on the control receiver.
4. `Node::unsubscribe_user`/`disconnect_user` apply to local hub clients + publish a control message for other nodes.
5. Wire the API/gRPC handlers; validation (107/102) mirrors Go.

**Tests:** new `conformance/tests/m23_server_api.rs` — connect+subscribe a client, call `/api unsubscribe {user,channel}` → client receives Unsubscribe push and can no longer publish/receive; call `/api disconnect {user}` → client connection closes. Cover JSON + pb + gRPC paths; empty-user→107, unknown-channel→102. Multi-node (Redis) variant: two nodes, unsubscribe/disconnect crosses nodes.

**Commit:** `feat: server-side unsubscribe/disconnect via HTTP+gRPC API`

---

## A2 — Refresh-proxy malformed base64 → `ErrorInternal(100)`

**Goal:** A refresh-proxy response with malformed `b64info` returns ErrorInternal(100), not the +60s graceful path (which is only for transport errors).

**Go ref:** `internal/proxy/refresh_handler.go` — base64 decode error → `(RefreshReply{}, ErrorInternal)`; only `ProxyRefresh` transport error → `RefreshReply{ExpireAt: now+60}`.

**Files:** `crates/centrifugo-server/src/proxy_http.rs` (HttpRefreshProxy) + `crates/centrifugo-core/src/client.rs` (`refresh_via_proxy`).

**Approach:** Don't let `decode_gated`'s b64 error become the function's transport `Err`. Either decode inside `refresh_via_proxy`, or have the proxy return `ProxyOutcome::Error{code:100}` on decode failure while transport errors stay `Err(_)` → +60s.

**Tests:** extend `m17_proxies` — refresh proxy returns `{"result":{"b64info":"!!notb64!!","expire_at":<future>}}` to a protobuf client → error reply 100 (not a +60s success).

**Commit:** `fix: refresh-proxy malformed base64 -> ErrorInternal (audit A2)`

---

## B1 — `admin_web_path` arbitrary file tree (#16)

**Goal:** When `admin_web_path` is set, serve any file under it (not just the 4 embedded names), with `..` traversal rejected.

**Go ref:** `internal/admin/handlers.go` — `http.FileServer(http.Dir(WebPath))`.

**Files:** `crates/centrifugo-server/src/webui.rs` + `http.rs`. Add a catch-all GET asset route mounted only when admin enabled; normalize/reject `..`; default `/`→index.html; content-type by extension. Keep the embedded bundle when `web_path` empty. Ensure no conflict with API routes (test router builds).

**Tests:** `m21_admin_ui` — with `admin_web_path` pointing at a temp dir containing an extra asset (e.g. `vendor.js`), `GET /vendor.js` returns it; `GET /../etc` is rejected (400/404).

**Commit:** `feat: admin_web_path serves full asset tree (audit #16)`

---

## B2 — Refresh-proxy empty-token disconnect before proxy (#19)

**Goal:** An empty refresh token disconnects with DisconnectBadRequest(3003) before the refresh handler/proxy runs, matching Go `handleRefresh`.

**Go ref:** `client.go handleRefresh` — `if cmd.Token == "" { return DisconnectBadRequest }` unconditionally, before the handler.

**Files:** `crates/centrifugo-core/src/client.rs on_refresh` — move the empty-token check above the refresh-proxy branch. Verify it does NOT break the proxy-refresh-via-command flow (our m17 refresh test sends `params:{}` with no token!). **Decision point:** Go's client-side-refresh requires a token even with a proxy; but our model calls the proxy from a tokenless refresh command. Reconcile: only require a token when NOT in proxy mode, OR change m17 to send a token. Mirror Go faithfully = require token always; then m17 refresh test must send a (dummy) token.

**Tests:** `m17_proxies` — empty-token refresh with a proxy configured → 3003; update existing refresh-proxy tests to send a token.

**Commit:** `fix: empty refresh token disconnects before proxy (audit #19)`

---

## B3 — API response `Content-Type` echo (#24)

**Goal:** The `/api` (and `/admin/api`) response echoes the request `Content-Type`, matching Go.

**Go ref:** `internal/api/handler.go` — `w.Header().Set("Content-Type", r.Header.Get("Content-Type"))`.

**Files:** `crates/centrifugo-server/src/api.rs` — thread the request content-type into `run_protobuf`/`run_commands`; echo it (fallback to the branch default when empty).

**Tests:** `m6_api`/`m18_protobuf_api` — assert the response `Content-Type` matches the request.

**Commit:** `fix: API echoes request Content-Type (audit #24)`

---

## C1 — Personal channels (`user_subscribe_to_personal`)

**Goal:** When `user_subscribe_to_personal` is set, a non-anonymous client is auto-subscribed on connect to its personal channel `<user_personal_channel_namespace>:#<user>` (or `#<user>` with no namespace), as a server-side subscription.

**Go ref:** `internal/client/...` connect handler + `user_subscribe_to_personal` / `user_personal_channel_namespace` config; personal channel = `namespace:#user` (user-limited).

**Files:** `crates/centrifugo-server/src/config.rs` + `cli.rs` (`user_subscribe_to_personal`, `user_personal_channel_namespace`); `centrifugo-core/src/node.rs` (carry the two settings); `client.rs on_connect` (build the personal channel, add to the server-side `channels` list before the subscribe loop, skip for empty user).

**Tests:** `conformance/tests/m24_personal.rs` — connect with a JWT user, assert the connect reply `subs` contains the personal channel; a publish to it reaches the client.

**Commit:** `feat: personal channels (user_subscribe_to_personal)`

---

## C2 — Redis Sentinel mid-flight failover re-resolution

**Goal:** On a Redis connection error, re-resolve the master via Sentinel and reconnect, instead of failing until restart.

**Go ref:** centrifuge redis engine / go-redis FailoverClient (auto master re-resolution).

**Files:** `crates/centrifugo-redis/src/lib.rs` — keep the Sentinel handle; on a command/connection error, re-run `async_master_for` and rebuild the connection manager (or wrap the manager so it reconnects through Sentinel). Add a reconnect/backoff loop in the pub/sub subscriber task too.

**Tests:** `m20_redis_sentinel` — kill the master mid-test (or trigger Sentinel failover), assert pub/sub + presence recover after re-resolution. (If killing the master is too flaky in CI, add a unit test on the re-resolution helper + document the manual scenario.)

**Commit:** `feat: Redis Sentinel mid-flight failover re-resolution`

---

## C3 — Admin live node-stats over the admin WebSocket

**Goal:** The admin UI's real-time node-stats stream works (the SPA subscribes to a control/stats channel over a WebSocket and renders live node info).

**Go ref:** `internal/admin/handlers.go` + the admin SPA: it connects to the admin WebSocket and receives periodic node-info snapshots. Determine the exact endpoint + message shape the vendored bundle expects.

**Files:** `crates/centrifugo-server/src/webui.rs`/a new `admin_ws.rs`; `http.rs` route; reuse the Info data (node uid/clients/users/channels/uptime). Emit periodic snapshots in the format the SPA expects.

**Tests:** `m21_admin_ui` — connect to the admin WS, assert a node-info snapshot arrives with the expected fields. (First: inspect the vendored `bundle.js` to learn the exact endpoint/format.)

**Commit:** `feat: admin live node-stats over WebSocket`

---

## C4 — Full per-command Prometheus metrics

**Goal:** Expose the per-command/per-action counters Go exports (e.g. `centrifugo_client_num_*`, command counts, message sent/received), not just node gauges + build_info.

**Go ref:** centrifuge `metrics.go` (prometheus counters/histograms): client command counts, messages sent/received, etc.

**Files:** `crates/centrifugo-core` — a lightweight metrics registry (atomic counters) incremented in the command dispatch + publish/fan-out paths; `crates/centrifugo-server/src/http.rs metrics` — render them in Prometheus text format. Match Go metric names where they’re client-observable for dashboards.

**Tests:** `m11_metrics` — drive some commands, assert the relevant counters appear and increment in `/metrics`.

**Commit:** `feat: per-command Prometheus metrics`

---

## D10 — Mixed Go+Rust cluster on one Redis

**Goal:** A Rust node and a Go centrifugo v2.8.6 node sharing one Redis interoperate: a publish on either is delivered to subscribers on the other, and history/presence/control are mutually readable.

**Go ref:** centrifuge `engine_redis.go` — the exact Redis schema: pub/sub channel names + the **protobuf** publication/join/leave/control message encoding, history list + meta hash key names + Lua, presence hash/zset key names. Our current Redis engine uses a Rust-native format; this task makes it byte-compatible with Go’s.

**Files:** `crates/centrifugo-redis/src/lib.rs` — align channel names (`centrifugo.<...>`), message encoding (centrifuge `protocol.Push`/control protobuf), history/presence key schemas + Lua scripts to match Go exactly. Reuse `centrifugo-grpc`/`centrifugo-protocol` pb types for the control + push encodings.

**Tests:** `conformance/tests/m25_go_rust_cluster.rs` — start a Go oracle node and a Rust node on the same `redis-server`; subscribe on one, publish via the other’s API, assert delivery; cross-check presence + history. Skips cleanly if `go`/`redis-server` absent.

**Commit:** `feat: Go+Rust interop on a shared Redis cluster`

---

## Self-review notes

- A1 introduces the node-to-node control channel; D10 makes that channel (and pub/sub) Go-wire-compatible — A1 uses a Rust-native control encoding first, D10 swaps it for the centrifuge protobuf control format.
- B2 has a real reconciliation decision (token required vs our proxy-refresh-via-command model) — resolve by mirroring Go and updating the proxy refresh tests to send a token.
- C3 and D10 both require reverse-engineering exact Go formats (bundle WS protocol; Redis schema) — inspect first, implement second.
- Run the full suite only between tasks, never while editing.
