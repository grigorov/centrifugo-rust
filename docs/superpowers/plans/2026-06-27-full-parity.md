# Full Go-parity Implementation Plan

> Execution: incremental on `main`, one commit per phase, each with a Go-oracle golden diff + harness tests.
> Decisions (2026-06-27): Admin UI = vendor `centrifugal/web` MIT bundle; Redis = Sentinel only (no sharding/cluster); process = incremental on main.

Goal: close the remaining gaps to full Centrifugo v2.8.6 parity. Every behavior verified against the Go source in `conformance/go-oracle/src` before changing wire behavior.

---

## Phase 0 — Foundation: per-subscription Client state
`Client.subscriptions: Vec<String>` → `IndexMap<String, SubState{ expire_at, recoverable, presence, join_leave, chan_info: Option<Vec<u8>>, server_side }>` (keep insertion order for deterministic leave-on-disconnect).
- Update: `is_subscribed`, `on_subscribe` (store SubState), `unsubscribe_channel`, `on_disconnect`.
- Unblocks: SUB_REFRESH (expiry), server-side subs (track + flag), per-sub chan_info (from sub token), presence-refresh (iterate subs).
- Tests: existing m4/m5/m6 stay green.

## Phase 1 — Channel semantics
### 1a. `#`-channels (user-limited)
- Channel `name#u1,u2`: only listed users may subscribe, else PermissionDenied(103). Empty user (anonymous) never allowed on `#`.
- Namespace resolution uses the part before `#`. Strip `#...` (and `$`) before namespace lookup.
- Go ref: `rule.Container` channel parsing.
### 1b. Server-side channels + `subs` map
- Add `subs map<string, SubscribeRequest>` (field 3) to ConnectRequest and `subs map<string, SubscribeResult>` (field 6) to ConnectResult in protocol messages + convert.rs (finding #11).
- On connect: for each `channels` claim in the JWT (already parsed) + personal channel when `user_subscribe_to_personal`, resolve options, server-side subscribe (hub + presence + join), build SubscribeResult (recovered/epoch/seq/gen/publications) into ConnectResult.subs.
- Go ref: handler.go OnClientConnecting (token.Channels → subscriptions), connectCmd subscribe loop.
### 1c. SUB_REFRESH (method 11)
- Verify sub token; update SubState.expire_at; return SubRefreshResult{expires, ttl}. Expired sub token → DisconnectSubExpired (3006) or ErrorExpired per Go subRefreshCmd (verify exact: handler.go OnSubRefresh returns SubRefreshReply{Expired} → centrifuge → DisconnectSubExpired 3006).
- Requires per-sub expiry from Phase 0.

## Phase 2 — Presence TTL + refresh timer
- Presence entries gain an expire timestamp. Memory: prune on read by `now > expire_at`. Redis: `zset`(clientID→expire_ms) + `hash`(clientID→info) + Lua prune (the deferred layout).
- Per-client timer every `client_presence_ping_interval` (25s) re-adds presence for all subscribed channels; expire `client_presence_expire_interval` (60s). Spawned in ws.rs/sockjs alongside the connection; stops on disconnect.
- Go ref: client.go addPresenceUpdate / presence ping.

## Phase 3 — Granular proxies (HTTP)
Extend the existing `ConnectProxy` pattern to refresh/subscribe/publish/rpc, mirroring Go `internal/proxy/*` (connect already done):
- `RefreshProxy`: token refresh via HTTP callback (alternative to JWT). Reply {expired|expire_at|info}.
- `SubscribeProxy`: on subscribe, authorize via callback → {error|disconnect|result{info,channel opts}}.
- `PublishProxy`: on publish, authorize/transform → {error|disconnect|result{data,...}}.
- `RpcProxy`: rpc method → callback → {error|disconnect|result{data}} (makes RPC functional; currently method_not_found).
- Config: `proxy_{refresh,subscribe,publish,rpc}_endpoint` (+ timeouts). Core traits in centrifugo-core/proxy.rs; HTTP impls in server/proxy_http.rs.
- Go ref: internal/proxy/{refresh,subscribe,publish,rpc}_handler.go + _http.go.

## Phase 4 — Protobuf HTTP API
- `/api`: when `Content-Type: application/octet-stream` → decode a uvarint-length-prefixed stream of pb `api.Command` (centrifugo-grpc types) → dispatch → encode uvarint-prefixed pb `api.Reply` stream; echo Content-Type. JSON path stays NDJSON.
- Reuse uvarint helpers from centrifugo-protocol::codec + pb types from centrifugo-grpc.
- Go ref: internal/api/handler.go (Content-Type branch) + marshal.go (PutUvarint framing).

## Phase 5 — Redis Sentinel
- Config: `redis_master_name`, `redis_sentinels` (comma-separated), `redis_sentinel_password`.
- Discover master via sentinels; reconnect/rediscover on failover. The `redis` crate has sentinel support (redis::sentinel) — wrap connection acquisition.
- Keep the existing single-instance path; sentinel is an alternative connection source feeding the same RedisEngine.
- (Sharding + Cluster explicitly out of scope per decision.)

## Phase 6 — Admin web UI (vendor bundle)
- Pin the `github.com/centrifugal/web` version referenced by centrifugo v2.8.6's go.mod; vendor its built assets into `crates/centrifugo-server/web/`.
- Serve at `/` (and prefix) via a static file handler (rust-embed or a fs handler). `/admin/auth` + `/admin/api` already exist.
- VERIFY EARLY: what the bundle calls at runtime (it authenticates via /admin/auth, polls /admin/api for info/channels, and may open a websocket as an admin client for live node stats). Ensure parity for whatever it needs (possibly an admin/stats stream).
- Go ref: internal/admin/handlers.go (WebFS served at `/`).

---

## Cross-cutting
- Each phase: golden diff vs `conformance/go-oracle` where observable + black-box harness tests + (where relevant) a centrifuge-go SDK check.
- Keep clippy clean; commit per phase.
- Update README.md / README_EN.md "deferred" sections as items land.
