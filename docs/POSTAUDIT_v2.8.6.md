# Post-Audit Report: Rust Centrifugo vs Go Centrifugo v2.8.6 Wire Compatibility

## Verdict

The Rust reimplementation is **highly wire-faithful on all common, conformant-SDK paths** — the 205-test conformance suite (golden diffs, JWT, presence, history, interop) passes, and every confirmed divergence below lives in edge cases that real v2-era SDKs do not exercise during normal operation. After deduplication, **8 distinct divergences** remain. There are **3 genuinely high-severity** items, but each is reachable only via abnormal input or specific configuration: (1) **malformed inner-command params return a recoverable 107 reply instead of a fatal 3003 close**, (2) **a self-Join push is emitted before the subscribe reply** (the one high-severity issue on a *common* path — deterministic, every-time ordering inversion for join_leave subscribers), and (3) **fractional/string JWT `exp`/`nbf` claims are mis-rejected as invalid token (3002) instead of accepted or expired (109)**. Two additional high-impact items concern Redis URL credential/db precedence (silent cross-node split-brain) and unknown-method handling (forward-compat). No high-severity divergence exists on the steady-state publish/subscribe/recovery data path itself. Dimensions with **no confirmed findings** (positive results) are noted in the closing section.

Note on dedup: the `protocol-codec` and `client-commands` runs independently confirmed the same four issues (malformed params → 107, id==0 processed, unknown method → 3003, second CONNECT → 107) plus two unique-to-`client-commands` items (RPC error code, PING result shape). These are merged below. The unknown-method finding was filed at both medium and high severity across runs; I take the high verdict (forward-compat + sibling-command loss).

---

## HIGH severity

### H1. Malformed inner-command params return error reply (107) instead of closing with DisconnectBadRequest (3003)
- **Where:** `crates/centrifugo-core/src/client.rs:1052` (`parse_params` → `Error::bad_request()`); wrapped as `Reply::err(cmd.id, e)` in `on_connect`/`on_subscribe`/`on_publish`/`on_history`/`on_presence`/`on_presence_stats`/`on_refresh`/`on_rpc`/`on_unsubscribe` (e.g. lines 228, 412, 591, 618). Only `on_sub_refresh` already returns a 3003.
- **Rust:** On a params decode failure, returns in-band `{"error":{"code":107,"message":"bad request"}}` and keeps the connection open.
- **Go:** Every handler returns `DisconnectBadRequest`; `handleCommand` (client.go:841-848) closes with WS code **3003** `{"reason":"bad request","reconnect":false}`, no error reply (handleConnect client.go:924-927, handleSubscribe 1062-1065, handlePublish 1228-1231, handleHistory 1407-1411, …).
- **Evidence:** After a successful CONNECT, malformed params per method — RUST 18200 → `TEXT {"id":2,"error":{"code":107,"message":"bad request"}}` (open) for subscribe `"oops"`, publish `42`, history `[1,2]`, presence `"x"`; rpc even returned `result:{}`. GO 18201 → `CLOSE 3003 {"reason":"bad request","reconnect":false}` for every case.
- **Why it matters:** v2-era SDKs treat a 3003 `reconnect:false` close as a fatal protocol error and stop reconnecting; against Rust they get a recoverable error and keep the socket. Only fires on corrupt params, not normal traffic.
- **Fix:** In each of the above handlers, return `CommandOutcome::disconnect(Disconnect::bad_request())` on a params decode error (mirror the existing `on_sub_refresh` path).

### H2. Self-Join push precedes the subscribe reply on a client SUBSCRIBE (ordering inverted)
- **Where:** `crates/centrifugo-core/src/client.rs:582-585` — `self.node.publish_join(&req.channel, sub_info).await` runs **before** returning `CommandOutcome::replies(vec![reply])`. `publish_join` → `deliver_push` → `handle.tx.try_send(Out::Frame)` (`node.rs:699-728`) enqueues the Join into the subscriber's own queue synchronously during the await.
- **Rust:** For a join_leave channel, the subscribing connection receives its own **JOIN before the subscribe REPLY**, every time.
- **Go:** client.go:1100-1104 flushes the subscribe reply first (`rw.flush()`), *then* `go func(){ c.node.publishJoin(...) }()` on a detached goroutine — subscriber always sees **REPLY then JOIN**.
- **Evidence:** Oracle (`--presence --join_leave`, :18331) vs Rust (:18332), connect then `{"id":2,"method":1,"params":{"channel":"room"}}`, 4 trials each — ORACLE `[REPLY,JOIN]×4`; RUST `[JOIN,REPLY]×4`. Raw: ORACLE frame0 `{"id":2,"result":{}}` then `{"result":{"type":1,...}}`; RUST frame0 is the `type:1` Join, frame1 the reply.
- **Why it matters:** This is the only high-severity item on a **common path**. SDKs transition the subscription to SUBSCRIBED on the reply and only then dispatch channel pushes; a Join arriving before its reply can be dropped, mis-attributed, or trip an "unexpected push for unknown channel" assertion. Not covered by the passing conformance suite.
- **Fix:** Enqueue/return the subscribe reply before calling `publish_join` — reuse the existing `pending_joins` deferral (`client.rs:90-125`, currently only for server-side subs) for client subscribes, flushing the Join after the reply frame is written.

### H3. Fractional / string-typed JWT `exp`/`nbf` wrongly rejected as Invalid (3002) instead of accepted or Expired (109)
- **Where:** `crates/centrifugo-auth/src/claims.rs:13-15,33-35` (`exp`/`nbf` as `Option<i64>`); `verifier.rs:150-152` does `decode::<T>(...).map_err(|_| VerifyError::Invalid)` **before** `check_expiry`.
- **Rust:** A JSON float `exp` (e.g. `…920.5`) or numeric string `"…920"` fails `i64` decode → whole token → `VerifyError::Invalid` → `Disconnect::invalid_token()` = **3002** (`reconnect:false`). Even an *expired* fractional token is mis-classed Invalid, never reaching the 109 path.
- **Go:** cristalhq/jwt v3.0.9 `NumericDate.UnmarshalJSON` (numeric_date.go:31-44) parses via `json.Number → Float64() → time.Unix(sec, dec*1e9)`, preserving sub-second precision and accepting numeric strings. Valid fractional token connects; expired/not-yet-valid → `ErrTokenExpired` → **109** on connect (handler.go:233-234).
- **Evidence:** HMAC `mysecret`, framing `{"id":1,"method":0,"params":{"token":T}}`. Controls (integer exp valid/expired) match on both. `exp=now+10000.5`: RUST `CLOSE 3002 invalid token` / GO `result ttl 10000`. `exp=now-100.5`: RUST `CLOSE 3002` / GO `error 109 token expired`. `nbf=now+10000.5`: RUST `CLOSE 3002` / GO `error 109`. string `exp`: RUST `CLOSE 3002` / GO connect OK.
- **Why it matters:** RFC-7519-legal float NumericDates (emitted by common PHP/JS/Python JWT and clock libraries) connect on Go but get a no-reconnect 3002 on Rust. An expired such token gets 3002 (connection breaks) instead of 109 (the code SDKs use to trigger a refresh). Same `i64` root cause affects the refresh and subscribe-token paths.
- **Fix:** Deserialize `exp`/`nbf` as `f64` (or `serde_json::Number`), compare with full precision, floor to `i64` for the reported `expire_at` (matching Go's `NumericDate.Unix()`); optionally accept string-encoded numerics for strict parity.

### H4. `redis_url` db-path and URL credentials precedence inverted vs Go (config `redis_db`/`redis_password` override the URL)
- **Where:** `crates/centrifugo-redis/src/lib.rs:421-427` (`connect`) — after parsing the URL, unconditionally applies `if opts.password.filter(non-empty) {…}` and `if opts.db != 0 {…}`, so config beats the URL. `opts` from `main.rs:340-341`.
- **Rust:** Config `redis_db`/`redis_password` win over what the URL carried.
- **Go:** main.go seeds `passwords[i]`/`dbs[i]` from config as **defaults**, then "if URL set then prefer it" — db from URL path, password from URL userinfo override the config (when present). When URL has no path, config db is retained.
- **Evidence:** redis on :6399 (`requirepass testpw`), MONITOR. `--redis_url 'redis://:testpw@127.0.0.1:6399/4' --redis_db 7` → GO `SELECT 4`, RUST `SELECT 7`. URL `//0` + db 7 → GO no SELECT (db 0), RUST `SELECT 7`. URL pw `urlpw` + `redis_password testpw` → GO fatal `WRONGPASS`, RUST stays up (used config pw).
- **Why it matters:** A Go node and a Rust node given the **same** config land on different Redis databases / auth — different keyspace, so live pub/sub, history, presence, and control silently do not interoperate across a mixed cluster (no error, just no cross-node delivery). High because of silent split-brain.
- **Fix:** When `redis_address`/`url` contains `://`, let the URL's db (path) and password (userinfo) win; apply config `redis_db`/`redis_password` only as defaults when the URL omits them (mirror Go's seed-then-URL-overrides ordering).

### H5. Unknown / out-of-range method int closes connection (3003) instead of replying ErrorMethodNotFound (104)
- **Where:** `crates/centrifugo-protocol/src/method.rs:42-58` (`from_u64` returns `None` outside 0..=11) → Deserialize custom error (`method.rs:93-101`) → `decode_commands` fails the whole frame → `crates/centrifugo-server/src/ws.rs:159-164` closes with `Disconnect::bad_request()` (3003).
- **Rust:** A command with an unrecognized method (e.g. `method:99` or `12`) tears down the connection with 3003, and any valid commands batched in the same frame are also lost. Error code 104 is unreachable from the client command path.
- **Go:** protocol v0.3.4 `MethodType.UnmarshalJSON` (decode.go:16-28) does `MethodType(val)` for **any** int; `handleCommand`'s switch `default` (client.go:835-837) returns `ErrorMethodNotFound` as an in-band reply `{"code":104,"message":"method not found"}`, connection stays open.
- **Evidence:** Connect then `{"id":2,"method":99}` — RUST `CLOSE 3003 {"reason":"bad request","reconnect":false}` then EOF; GO `TEXT {"id":2,"error":{"code":104,"message":"method not found"}}`, conn kept open. Identical for `method:12`. Confirmed over protobuf too.
- **Why it matters:** Breaks forward-compat — an SDK sending a method this build doesn't know gets the whole connection dropped (and sibling commands silently discarded) instead of a per-command 104. High for the data-loss aspect.
- **Fix:** Make `MethodType` decode tolerant of unknown ints (e.g. an `Unknown(i32)` sentinel variant) and have `handle_command` return `Reply::err(cmd.id, Error::method_not_found())` (104) for it, rather than failing frame decode.

---

## MEDIUM severity

### M1. Second CONNECT on an already-authenticated connection returns 107 reply instead of closing with 3003
- **Where:** `crates/centrifugo-core/src/client.rs:223-224` — `on_connect` returns `Reply::err(cmd.id, Error::bad_request())` (107), open.
- **Go:** `connectCmd` (client.go:1579-1582) returns `DisconnectBadRequest`; `handleConnect`/`handleCommand` close with **3003**.
- **Evidence:** Two connects on one socket — RUST id:1 result then `{"id":2,"error":{"code":107,...}}` (open); GO id:1 result then `CLOSE 3003`. Reproduced with both `{"connect":{}}` and `{"params":{}}` encodings.
- **Severity note:** Genuine wire divergence but only on the duplicate-CONNECT edge case from a misbehaving client.
- **Fix:** In `on_connect`'s already-authenticated branch return `CommandOutcome::disconnect(Disconnect::bad_request())` (the helper is already used at `client.rs:199`, `:309`).

### M2. Command with id==0 (non-Send, including CONNECT) is processed instead of closing with 3003
- **Where:** `crates/centrifugo-server/src/ws.rs:169-176` and `sockjs.rs:222` — dispatch loop has no id==0 guard; `client.rs` `handle_command` only checks the authenticated/connect-first rule.
- **Go:** `Client.Handle` (client.go:708-713) closes with 3003 for any `cmd.ID==0 && cmd.Method != Send` (applies to CONNECT too), logging "command ID required for commands with reply expected".
- **Evidence:** Connect id:1 then `{"method":7}` (ping, id omitted) → RUST `TEXT {"result":{}}`; GO `CLOSE 3003`. First frame `{"params":{}}` (connect id=0) → RUST connect result; GO `CLOSE 3003`.
- **Severity note:** Unreachable by conformant SDKs (they always send incremental ids for reply-expecting commands); fires only on hand-crafted frames.
- **Fix:** In both `ws.rs` and `sockjs.rs`, before `handle_command`, if `c.id == 0 && c.method != MethodType::Send` enqueue `Out::Close(Disconnect::bad_request())` and stop the read loop.

### M3. RPC with no proxy returns ErrorMethodNotFound (104) instead of ErrorNotAvailable (108)
- **Where:** `crates/centrifugo-core/src/client.rs:924-927` — `on_rpc` no-proxy branch returns `Error::method_not_found()` (104).
- **Go:** centrifugo registers `OnRPC` only when an RPC proxy/extension exists (handler.go:158); otherwise `handleRPC` returns `ErrorNotAvailable` → **108** "not available".
- **Evidence:** Connect then `{"id":2,"method":9,"params":{"method":"foo","data":{}}}` → RUST `{"code":104,"message":"method not found"}`; GO `{"code":108,"message":"not available"}`.
- **Severity note:** Wrong error code, but only on the RPC-not-enabled error path.
- **Fix:** Return `Error::not_available()` (108) in the no-proxy branch (`crates/centrifugo-protocol/src/error.rs:44`).

### M4. PING reply carries empty `result:{}` (JSON) instead of a bare reply with no result
- **Where:** `crates/centrifugo-core/src/client.rs:208-210` — Ping handler builds `ok_reply(..., &PingResult{})` → `{"id":N,"result":{}}`.
- **Go:** `handlePing` (client.go:1466-1475) writes `&protocol.Reply{}` → `{"id":N}` (no result field).
- **Evidence:** Connect then `{"id":5,"method":7}` → RUST `{"id":5,"result":{}}`; GO `{"id":5}`. Protobuf path does **not** diverge (empty `PingResult` encodes to zero bytes, omitted by proto3).
- **Severity note:** Byte-for-byte JSON mismatch on every PING reply (golden-diff relevant); era SDKs parse leniently so runtime impact is low.
- **Fix:** For `MethodType::Ping` return `Reply { id: cmd.id, error: None, result: None }` instead of `ok_reply` with `PingResult`.

### M5. Env-var vs config-file precedence inverted relative to Go/viper (file wins over env)
- **Where:** `crates/centrifugo-server/src/config.rs:154-208` (`apply_env`, `fill()` writes only when field `is_empty()`), called after the file load at `main.rs:281`; comment at `config.rs:151-153` states "flags > file > env" (backwards from viper).
- **Go:** viper precedence is flag > env > config file; env beats the file.
- **Evidence:** Config `{"api_key":"filekey"}` + `CENTRIFUGO_API_KEY=envkey` → POST /api with `apikey envkey` GO 200 / RUST 401, with `apikey filekey` GO 401 / RUST 200. Config `port:18400` + `CENTRIFUGO_PORT` → GO binds env port, RUST binds 18400.
- **Severity note:** Operability/config-resolution bug, not an on-wire protocol divergence; manifests only when both a file value and a conflicting env var (no flag) exist for the same key — the standard baked-config + per-pod-env container pattern, which would pick the stale file secret/port on a Rust node.
- **Fix:** Restructure loading so env overlays the file result *below* explicit flags: precedence flag > env > file > default.

### M6. No startup/checkconfig validation: invalid configs Go rejects (exit 1) are accepted by Rust
- **Where:** `crates/centrifugo-server/src/config.rs:340-343` (`check_config` only does `serde_json::from_str`); startup (`main.rs`) never validates the rule config; namespaces in a `HashMap` so duplicates last-wins.
- **Go:** `rule.Config.Validate()` (rule.go:78-127), fatal at main.go:307 and in checkconfig, rejects `history_recover` without `history_size>0 && history_lifetime>0`, namespace names not matching `^[-a-zA-Z0-9_.]{2,}$`, duplicate namespace names, and `user_personal_channel_namespace` with no matching namespace — all exit 1.
- **Evidence:** checkconfig — `{"history_recover":true}` GO "both history size and history lifetime required…" exit 1 / RUST "is valid" exit 0; `!bad` name GO "wrong namespace name" / RUST valid; duplicate `news` GO "namespace name must be unique: news" / RUST valid; personal-ns `nope` GO "namespace … not found: nope" / RUST valid. Server start: GO `level:fatal` exit 1 / RUST stays running.
- **Severity note:** Config-safety/operability gap. Wire-relevant impact is duplicate namespaces: Go aborts, Rust silently keeps the last entry, so channel-option resolution can differ from a peer Go node.
- **Fix:** Port `rule.Validate()` (history-recovery requires size+lifetime; namespace-name regex; duplicate detection; personal-channel-namespace existence) into `check_config` and the startup path; exit non-zero on failure.

---

## LOW severity

### L1. Empty (zero-length) data frame is ignored instead of closing with 3003
- **Where:** `crates/centrifugo-server/src/ws.rs:153-189` (and `sockjs.rs`) — no `len==0` guard; `decode_commands` returns `Ok(empty)` for an empty frame, so no replies and no disconnect.
- **Go:** `Client.Handle` (client.go:680-684) closes with 3003 on `len(data)==0` before decoding.
- **Evidence:** Empty first frame → RUST timeout (open) / GO `CLOSE 3003`. Connect then empty frame → RUST connect reply only (open) / GO connect reply then `CLOSE 3003`. Whitespace-only `"   "` stays open on both (divergence is specific to truly zero-length).
- **Fix:** Treat `frame.is_empty()` as `Disconnect::bad_request()` before `decode_commands` in `ws.rs` and `sockjs.rs`.

### L2. Subscribe params: explicit JSON `null` for seq/gen/epoch rejected (107) where Go treats null as zero-value
- **Where:** `crates/centrifugo-protocol/src/messages.rs:72-77` — `seq:u32`, `gen:u32`, `epoch:String` with only `#[serde(default)]` (fills missing keys, not explicit null). Decode failure → `Error::bad_request()` (107) at `client.rs:410-412`.
- **Go:** encoding/json tolerates `null` for scalar/string fields, decoding as the zero value; subscribe proceeds.
- **Evidence:** `{"id":2,"method":1,"params":{"channel":"news","epoch":null,"seq":null,"gen":null}}` → RUST `{"code":107,"message":"bad request"}`; ORACLE `{"id":2,"result":{}}`. Control (no null fields) identical on both.
- **Severity note:** Real SDKs serialize from protobuf-derived structs with omitempty and never emit explicit null; only hand-rolled JSON clients trigger it. Same null-intolerance likely spans other request structs (ConnectRequest etc.).
- **Fix (if strict parity desired):** `deserialize_with` that coerces null → default for these scalar fields.

### L3. Memory engine ignores `memory_history_meta_ttl` — stream meta (offset+epoch) never expires/resets
- **Where:** `crates/centrifugo-core/src/memory.rs:93-100` (`evict_if_expired` clears pubs but keeps offset+epoch); no `memory_history_meta_ttl` config field anywhere (only `redis_history_meta_ttl` exists), so the key is silently swallowed.
- **Go:** centrifuge runs a `removeStreams` goroutine (engine_memory.go:281-314) that, after `HistoryMetaTTL` of inactivity, `delete(h.streams, ch)`; the next publish rebuilds `memstream.New()` → fresh epoch and offset restarting at 1. Wired via `memory_history_meta_ttl` (default 0).
- **Evidence:** `memory_history_meta_ttl=3, history_lifetime=1, history_recover=true`; publish 3, idle 6s, publish 1 — RUST top_seq 3→4, epoch unchanged; ORACLE top_seq 3→1, epoch flipped.
- **Severity note:** Identical at the default (0); only manifests when an operator sets `memory_history_meta_ttl>0` (non-default, single-node memory engine). With it set, Rust can tell a long-stale client `recovered=true` against a stream Go treats as fresh, and offsets never recycle (the leak the option exists to prevent).
- **Fix:** Add the config field, thread it into MemoryEngine, and on expiry remove the stream entry (drop offset+epoch) rather than just clearing pubs.

### L4. JWKS: Rust accepts non-RSA / non-`use:sig` JWKs that Go skips or rejects
- **Where:** `crates/centrifugo-auth/src/verifier.rs:94-106` (`set_jwks` loads any kid'd JWK `DecodingKey::from_jwk` parses — EC, oct, `use` absent/enc); verify uses `Validation::new(header.alg)` from the **token** header, not the JWK's declared alg.
- **Go:** `jwks.Manager.fetchKey` (manager.go:167-170) skips `Use != "sig"`; `jwksManager.verify` (token_verifier_jwt.go:94-96) requires `Kty == "RSA"` and derives the algorithm from the JWK's own `alg`.
- **Evidence:** Source comparison; the in-tree test `jwks_verifies_token_by_kid` (verifier.rs:356-385) deliberately uses an `oct` JWK with no `use` — concrete proof Rust accepts what Go rejects.
- **Severity note:** Requires a JWKS endpoint serving non-RSA / non-sig-tagged keys referenced by kid — non-standard for v2-era deployments; the passing m25 interop test uses properly-tagged RSA sig keys.
- **Fix:** In `set_jwks`, skip JWKs whose `use` is not `sig` and `kty` is not `RSA`; verify using the JWK's declared alg.

### L5. Reply+push frame coalescing: Go batches up to 4 messages into one WS frame, Rust sends separate frames
- **Where:** `crates/centrifugo-server/src/ws.rs:70-76` — writer emits one `Message::Text/Binary` per `Out::Frame` (no drain/coalesce loop; contrast `sockjs.rs:93-95` which does coalesce); `node.rs:699-709` one `Out::Frame` per push.
- **Go:** per-connection writer (writer.go:36-99, `defaultMaxMessagesInFrame=4`) drains queued messages and `WriteManyFn` concatenates them (newline-joined) into a single WS frame.
- **Evidence:** Back-to-back publish+presence on a subscribed conn — ORACLE 2 WS frames (frame0 coalesces a push and a reply, `nl=2`); RUST 3 WS frames, each `nl=1`. Per-message bytes and logical order are identical on both.
- **Severity note:** Pure WS-frame-boundary difference; Go's batching is timing-dependent and non-deterministic. All era SDKs split incoming text frames on newline and process each reply independently, so message-for-message parity holds.
- **Fix (only if strict frame-for-frame parity is required):** Add a drain-and-coalesce step to the `ws.rs` writer.

### L6. Stream epoch token format differs (10-char hex UUID vs Go 4-char [a-zA-Z])
- **Where:** `crates/centrifugo-core/src/node.rs:148-150` — `new_epoch()` returns the first 10 chars of a v4 UUID hex string.
- **Go:** `memstream.genEpoch()` returns `randString(4)` over `[a-zA-Z]` (stream.go:11-23,46).
- **Severity note:** Functionally none — the epoch is an opaque token SDKs only store and echo back verbatim, compared by exact string equality. No golden/conformance test can assert a specific value. Listed for completeness; cosmetic.
- **Fix (cosmetic only):** Match the 4-char `[a-zA-Z]` format if exact byte parity of generated tokens is ever required.

### L7. Malformed/comma-separated `redis_host`/`redis_port` not validated; Rust builds an invalid address instead of failing fast or sharding
- **Where:** `crates/centrifugo-server/src/config.rs:17-35` (`effective_redis_address` just `format!("{host}:{port}")`, no numeric validation, no comma handling).
- **Go:** `strconv.Atoi(ports[i])` fails fast with "malformed port: …" before connecting (main.go:1512-1516); comma-separated host/port → sharding ("Redis sharding enabled: N shards").
- **Evidence:** `--redis_port=abc` → GO fatal "malformed port" at config stage (exit 1) / RUST "Redis URL did not parse" at connect stage (exit 1, different message). `--redis_host '127.0.0.1,127.0.0.1'` → GO 2-shard, RUST literal malformed host `127.0.0.1,127.0.0.1:6399`.
- **Severity note:** Both reject a bad port (exit 1) — no silent acceptance of bad data; only a less-clear/later error and the missing sharding feature (plausibly out of scope per the Sentinel-only decision).
- **Fix:** Validate the port is numeric in `effective_redis_address` (mirror Go's message); reject or explicitly warn "sharding not supported" when host/port contain commas.

---

## Dimensions with no confirmed divergences (positive results)

- **Steady-state data path** (publish/subscribe/history/presence happy paths): no confirmed divergence — fully covered by the passing conformance golden diffs. All findings are edge/error/config cases.
- **Recovery correctness** (offset/epoch matching, `recovered` flag on the common path): no confirmed divergence beyond the non-default `memory_history_meta_ttl` (L3) and the cosmetic epoch format (L6). Core recovery logic matches.
- **JWT happy path** (integer `exp`/`nbf`, HMAC/RSA verification, refresh, subscribe tokens with well-formed claims): no confirmed divergence — controls matched exactly in the H3 probe; only the fractional/string NumericDate edge (H3) and the non-standard JWKS permissiveness (L4) diverge.
- **API/HTTP and gRPC surfaces**: no new confirmed divergence in this audit (prior B6–B13 fixes hold).
- **Protobuf wire encoding**: no divergence found — PING (M4) and the empty-frame/unknown-method cases were re-checked over protobuf and behave identically to JSON except where proto3 omission masks the difference (M4).

---

## Recommended fix order

1. **H2 (self-Join ordering)** — the only high-severity divergence on a common path; deterministic; can corrupt SDK subscription state. Reuse the existing `pending_joins` deferral.
2. **H3 (fractional/string JWT exp/nbf)** — broad real-world trigger (common JWT libraries), wrong disconnect code breaks refresh. Single root cause (`Option<i64>` → `f64`/`Number`).
3. **H4 (Redis URL precedence)** — silent cross-node split-brain in mixed Go/Rust clusters; data-correctness-adjacent.
4. **H1 + H5 + M1 + M2 + M3** — batch the protocol error/disconnect-semantics fixes together (they share the `Disconnect::bad_request()` / error-code pattern in `client.rs` and the `ws.rs`/`sockjs.rs` dispatch loop): malformed params → 3003, unknown method → 104, second CONNECT → 3003, id==0 → 3003, RPC → 108.
5. **M5 + M6 (config precedence + validation)** — operability/cluster-consistency; port `rule.Validate()` and invert env/file precedence.
6. **M4 (PING result shape)** — trivial JSON byte fix; do alongside the protocol batch.
7. **L1, L2 (malformed-frame/null hardening)** — cheap input-hardening; fold into the protocol batch.
8. **L3, L4, L5, L7 (non-default config / cosmetic / framing)** — lowest priority; address only if strict parity or the specific config option is required.
9. **L6 (epoch format)** — cosmetic; skip unless exact token byte-parity is mandated.

Relevant source files: `crates/centrifugo-core/src/client.rs`, `crates/centrifugo-core/src/node.rs`, `crates/centrifugo-core/src/memory.rs`, `crates/centrifugo-auth/src/claims.rs`, `crates/centrifugo-auth/src/verifier.rs`, `crates/centrifugo-protocol/src/method.rs`, `crates/centrifugo-protocol/src/messages.rs`, `crates/centrifugo-protocol/src/error.rs`, `crates/centrifugo-redis/src/lib.rs`, `crates/centrifugo-server/src/ws.rs`, `crates/centrifugo-server/src/sockjs.rs`, `crates/centrifugo-server/src/config.rs`, `crates/centrifugo-server/src/main.rs`.
