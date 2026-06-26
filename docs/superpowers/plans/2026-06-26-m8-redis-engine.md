# M8 — Redis Engine (multi-node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a Redis-backed engine so multiple Rust centrifugo nodes form one cluster — cross-node publication fan-out, shared presence, shared history/recovery — without changing the client wire contract.

**Architecture:** Promote the sync `Broker` trait to an async `Engine` trait covering pub/sub + history + presence. `MemoryEngine` keeps today's single-node behavior (presence/history maps move out of `Node` into it). `RedisEngine` (new `centrifugo-redis` crate) uses Redis PUB/SUB for cross-node delivery (a subscriber task routes incoming pubs into the same local fan-out), Redis hashes/zsets for presence with TTL, and per-channel Redis lists + a meta hash for history with monotonic offset + epoch (atomic via Lua). `Client::handle_command` and the touched `Node` methods become `async` (the WS read loop already runs in a task).

**Scope note:** Target is a homogeneous **Rust** cluster (the fleet is being replaced wholesale). The Redis layout is self-consistent and centrifuge-shaped but **not** byte-identical to centrifuge v0.14.2's Lua/keys, so a mixed Go+Rust cluster on one Redis is out of scope. The client-facing wire contract is unchanged and is already pinned by the single-node goldens (Redis is internal).

**Tech Stack:** `redis` crate (tokio async + connection manager), `async-trait`, Lua (`EVAL`), existing tokio/axum stack.

---

## File Structure

- `crates/centrifugo-core/src/engine.rs` — async `Engine` trait (pub/sub + history + presence) + `PublishOptions`, `HistoryResult` types. Replaces sync `Broker`.
- `crates/centrifugo-core/src/memory.rs` — `MemoryEngine` implementing `Engine`; owns the presence + history maps moved out of `Node`.
- `crates/centrifugo-core/src/node.rs` — `Node` delegates to `Arc<dyn Engine>`; presence/history/publish methods become `async` thin delegations; keeps the local fan-out (`deliver_*`) which both engines call.
- `crates/centrifugo-core/src/client.rs` — `handle_command` + per-method handlers become `async`.
- `crates/centrifugo-server/src/{ws.rs,api.rs,grpc.rs}` — `.await` the now-async calls.
- `crates/centrifugo-redis/` — NEW crate: `RedisEngine`, pub/sub subscriber task, Lua scripts, presence/history layout.
- `crates/centrifugo-server/src/{cli.rs,config.rs,main.rs}` — `--engine` (`memory`|`redis`), `--redis_address`; build the chosen engine.
- `conformance/src/lib.rs` — `Redis` helper (spawn `redis-server` on a free port, killed on drop) + `Server::start_cluster(n, config)` returning N nodes sharing one Redis.
- `conformance/tests/m8_redis.rs` — 2-node cross-node publish/presence/history tests.

---

## Task 1: Async `Engine` trait + `MemoryEngine` (no behavior change)

**Files:** Modify `engine.rs`, `memory.rs`, `node.rs`, `client.rs`, `lib.rs`, `ws.rs`, `api.rs`, `grpc.rs`; add `async-trait` to `centrifugo-core/Cargo.toml`.

- [ ] **Step 1:** Define the async `Engine` trait in `engine.rs`:

```rust
use std::collections::HashMap;
use async_trait::async_trait;
use centrifugo_protocol::messages::{ClientInfo, Publication};
use crate::node::StreamPosition;

/// Per-publish history directives (0/0 = history disabled for this channel).
#[derive(Clone, Copy, Default)]
pub struct PublishOptions {
    pub history_size: usize,
    pub history_lifetime: u64,
}

#[async_trait]
pub trait Engine: Send + Sync {
    async fn publish(&self, channel: &str, data: &[u8], info: Option<ClientInfo>, opts: PublishOptions) -> anyhow::Result<()>;
    async fn subscribe(&self, channel: &str) -> anyhow::Result<()>;
    async fn unsubscribe(&self, channel: &str) -> anyhow::Result<()>;

    async fn history(&self, channel: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)>;
    async fn history_since(&self, channel: &str, offset: u64, epoch: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)>;
    async fn remove_history(&self, channel: &str) -> anyhow::Result<()>;

    async fn add_presence(&self, channel: &str, client_id: &str, info: ClientInfo) -> anyhow::Result<()>;
    async fn remove_presence(&self, channel: &str, client_id: &str) -> anyhow::Result<()>;
    async fn presence(&self, channel: &str) -> anyhow::Result<HashMap<String, ClientInfo>>;
    async fn presence_stats(&self, channel: &str) -> anyhow::Result<(u32, u32)>;
}
```

- [ ] **Step 2:** Move presence + history state into `MemoryEngine` in `memory.rs` (the `Stream` struct + maps currently in `node.rs`). `MemoryEngine::new(route_fn)` keeps the publish route callback. `publish` appends to history when `opts` enables it (assign offset) then calls the route fn for local fan-out. Implement all `Engine` methods (logic copied verbatim from current `Node`).

- [ ] **Step 3:** In `node.rs`, replace `broker: Arc<dyn Broker>` + the `presence`/`history` Mutex fields with `engine: Arc<dyn Engine>`. Public methods (`publish`, `presence`, `presence_stats`, `history`, `history_since`, `remove_history`, `add_presence`, `remove_presence`) become `async` and delegate to `self.engine`. `Node::new_with` builds a `MemoryEngine` with the route closure. Keep `deliver_publication`/`deliver_push`/`make_push_frame` in `node.rs` (engines call back via the route fn).

- [ ] **Step 4:** Make `Client::handle_command` and its handlers `async`; add `.await` to every Node call.

- [ ] **Step 5:** Add `.await` at the 10 `handle_command` call sites + Node-method calls in `ws.rs`, `api.rs`, `grpc.rs`, and the `node.rs`/`client.rs` `#[tokio::test]`s.

- [ ] **Step 6:** `cargo test --workspace` — expect the same green count as before (103). Commit: `refactor(core): async Engine trait; MemoryEngine owns presence+history`.

---

## Task 2: `centrifugo-redis` crate — `RedisEngine`

**Files:** Create `crates/centrifugo-redis/{Cargo.toml,src/lib.rs}`; add member to workspace `Cargo.toml`.

Layout (prefix `centrifugo`):
- Pub/sub channel per app-channel: `centrifugo.pub.{channel}` carrying a framed `Publication`/`Join`/`Leave` (bincode/JSON internal envelope with `type`+`channel`+payload).
- History: list `centrifugo.hist.list.{channel}` (each element = serialized `Publication` with its offset) + meta hash `centrifugo.hist.meta.{channel}` fields `offset`,`epoch`. Append via Lua `EVAL` (INCR offset, RPUSH, LTRIM to size, PEXPIRE both keys) returning the new offset+epoch atomically.
- Presence: hash `centrifugo.presence.data.{channel}` (clientID → serialized ClientInfo) + zset `centrifugo.presence.exp.{channel}` (clientID → expire-at ms). Read prunes expired members (ZRANGEBYSCORE cleanup) via Lua.

- [ ] **Step 1:** `RedisEngine::connect(addr, route_fn)`: open `redis::Client`, a `ConnectionManager` for commands, and spawn a subscriber task on a dedicated pubsub connection. The subscriber decodes envelopes and calls `route_fn(channel, kind, payload)` to drive local fan-out. `subscribe`/`unsubscribe` (un)subscribe the pubsub connection to `centrifugo.pub.{channel}` (use `redis`'s `subscribe`/`PubSub` or a command channel to the subscriber task).

- [ ] **Step 2:** Implement `publish`: if `opts` enables history, run the history Lua and build the `Publication` with the returned offset; serialize the envelope; `PUBLISH centrifugo.pub.{channel}`. (Live wire offset is zeroed by the fan-out path as today.)

- [ ] **Step 3:** Implement `history`/`history_since`/`remove_history` against the list+meta keys; `add_presence`/`remove_presence`/`presence`/`presence_stats` against the hash+zset with TTL pruning.

- [ ] **Step 4:** Unit tests in the crate (`#[tokio::test]`, gated on a reachable `REDIS_TEST_URL` or a spawned server — skip if absent) covering presence add/read/expire and history append/since. Commit: `feat(redis): RedisEngine (pubsub fan-out, presence TTL, history)`.

---

## Task 3: Config + engine selection

**Files:** Modify `cli.rs`, `config.rs`, `main.rs`; add `centrifugo-redis` dep to `centrifugo-server`.

- [ ] **Step 1:** Add `--engine` (default `memory`) + `--redis_address` (default `127.0.0.1:6379`) flags and the matching `engine`/`redis_address` config keys.
- [ ] **Step 2:** Add `Node::new_with_engine(engine, verifier, client_insecure, namespaces)`; `main.rs` builds `MemoryEngine` or `RedisEngine` from settings and passes it in. The route closure is identical for both (local fan-out).
- [ ] **Step 3:** `cargo build` + `cargo test --workspace` still green. Commit: `feat(server): engine selection (memory|redis) via config`.

---

## Task 4: 2-node conformance tests

**Files:** Modify `conformance/src/lib.rs`; create `conformance/tests/m8_redis.rs`.

- [ ] **Step 1:** Add a `Redis` harness struct that spawns `redis-server --port <free> --save ''` (skip the whole test, like the Go oracle, if `redis-server` is absent) and is killed on drop. Add `Server::start_redis(redis_addr, config_json)`.
- [ ] **Step 2:** Test `cross_node_publish`: start Redis + node A + node B sharing it; WS-subscribe a client on A to `room`; publish via B's HTTP API; assert A's client receives the Publication.
- [ ] **Step 3:** Test `cross_node_presence`: subscribe a client on A (presence on); query presence_stats via B's HTTP API; assert it sees the client.
- [ ] **Step 4:** Test `cross_node_history_recovery`: publish 3 via A's API; a fresh client subscribes-with-recover on B; assert it recovers all 3 with correct seq/gen.
- [ ] **Step 5:** `cargo test -p conformance --test m8_redis`. Commit: `test(m8): 2-node Redis cross-node publish/presence/recovery`.

---

## Self-Review

- **Spec coverage:** multi-node PUB/SUB ✓ (Task 2.1–2.2, Task 4.2); presence TTL ✓ (2.3); history+recovery across nodes ✓ (2.3, 4.4); engine selection ✓ (Task 3). Sentinel/Cluster sharding from the spec is **deferred** (single-instance Redis only) — noted as a follow-up; not needed for the multi-node fan-out deliverable.
- **Wire contract:** unchanged; existing goldens still pin it (Task 1 must stay at 103 green).
- **Type consistency:** `PublishOptions`, `StreamPosition`, `Engine` method names are used identically in Tasks 1–3.
