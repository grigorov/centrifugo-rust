# M2: Protocol Parity (full methods, protobuf framing, error/disconnect semantics)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** Complete the client protocol surface: all 12 methods dispatched, **protobuf framing** alongside JSON, full error/disconnect semantics (CONNECT-first as a hard disconnect, malformed-frame and slow-consumer closes), and message-size/write-timeout limits — verified by golden diff vs Go and the `centrifuge-go` v0.8.4 subset.

**Authority:** `docs/reference/protocol-v0.3.4-wire-format.md`, `crates/centrifugo-protocol/proto/client.proto`, and the real Go source in scratchpad (`centrifuge-v0.14.2/client.go`, `writer.go`, `handler_websocket.go`).

---

## The key design problem: JSON vs Protobuf payloads

In **JSON** mode `Command.params` / `Reply.result` / `Push.data` are **inline JSON**. In **Protobuf** mode those same `bytes` fields hold the **protobuf-encoded sub-message** (e.g. a protobuf `ConnectRequest`), and the outer Command/Reply/Push are protobuf with uvarint framing. So the payload bytes are codec-specific; the envelope must carry raw bytes and a per-connection `ProtocolType` decides how to decode params into a typed request and encode a typed result.

### Decision: `Raw(Vec<u8>)` domain type + two codecs

- Replace `Raw = Box<RawValue>` with a newtype `Raw(Vec<u8>)` holding raw payload bytes (the canonical domain value). Byte fields in all messages become `Raw`.
- **JSON codec** (`serde`): `Raw` serializes its bytes *verbatim as raw JSON* (via `serde_json::value::RawValue` internally) and deserializes by capturing the raw JSON slice — preserving the inline-raw-bytes semantics M1 already proved. Assumes JSON-connection payloads are valid JSON/UTF-8 (true for JSON clients).
- **Protobuf codec** (`prost`): generated `pb` types from `client.proto`; bytes fields are `Vec<u8>` ↔ `Raw`. Convert `pb` ⇄ domain structs at the boundary.
- The `Client` state machine works on domain typed requests/results; a `ProtocolType`-aware `Codec` decodes params → typed request and encodes typed result → bytes.

This keeps one domain type set, isolates wire concerns in two codec impls, and supports arbitrary protobuf payload bytes. Verify against `centrifuge-go` v0.8.4 protobuf mode (Task M2.7); if it sends non-UTF8 binary over JSON paths (it does not), revisit.

---

## File structure (changes)

- `crates/centrifugo-protocol/build.rs` — prost-build compiling `proto/client.proto` into `pb`.
- `crates/centrifugo-protocol/src/raw.rs` — `Raw(Vec<u8>)` newtype + serde (inline-raw JSON) + accessors.
- `crates/centrifugo-protocol/src/command.rs`, `messages.rs` — switch byte fields to `Raw`.
- `crates/centrifugo-protocol/src/pb.rs` — `include!` the prost-generated module; `From`/`Into` conversions domain⇄pb.
- `crates/centrifugo-protocol/src/codec.rs` — `ProtocolType { Json, Protobuf }`; `decode_commands`, `encode_replies`, typed `decode_params`/`encode_result`/`encode_push` per type; protobuf uvarint framer.
- `crates/centrifugo-core/src/client.rs` — full 12-method dispatch; SEND no-reply; PRESENCE/PRESENCE_STATS/HISTORY → `Error::not_available` until their milestones; RPC → `Error::method_not_found` (no handler yet); REFRESH/SUB_REFRESH minimal; CONNECT-first → return a `Disconnect` signal.
- `crates/centrifugo-core` — `handle_command` returns an enum `{ replies: Vec<Reply>, disconnect: Option<Disconnect> }` (or a dedicated `Action`) so the transport can close with the right code.
- `crates/centrifugo-server/src/ws.rs` — select `ProtocolType` from `?format=protobuf`/`?protocol=protobuf`; Text vs Binary frames; 64KB max frame; 1s write timeout; close with disconnect code+reason on bad frame / CONNECT-first / slow queue (`DisconnectSlow`).
- `conformance/` — protobuf WS client; golden diffs for protobuf; `centrifuge-go` v0.8.4 runner (Task M2.7).

---

## Tasks (TDD; commit after each)

### M2.1 — prost codegen + `pb` module
- **No `protoc` on this machine and downloads are flaky — use the pure-Rust `protox` compiler**, not `prost_build::compile_protos` (which shells out to `protoc`). `build.rs`:
  ```rust
  let fds = protox::compile(["proto/client.proto"], ["proto"]).unwrap();
  prost_build::Config::new()
      .compile_fds(fds)  // or .file_descriptor_set + compile
      .unwrap();
  ```
  Build-deps: `prost-build`, `protox`. Runtime dep: `prost`. `pb.rs`: `include!(concat!(env!("OUT_DIR"), "/protocol.rs"));`. (Confirm the exact `protox`/`prost-build` API at execution; alternative: `protox::compile` → write FDS → `prost_build::Config::compile_protos_with_field_attributes`.)
- Test: a unit test encodes a `pb::ConnectResult{client,version}` and decodes it back (prost round-trip).
- Commit.

### M2.2 — `Raw(Vec<u8>)` newtype
- Implement `Raw(Vec<u8>)`, `From<&[u8]>`, `from_json_str`, `as_bytes`, `into_bytes`.
- `Serialize`: emit bytes as raw JSON. Impl: `let rv = serde_json::value::RawValue::from_string(String::from_utf8(self.0.clone()).map_err(ser::Error::custom)?); rv.serialize(serializer)`.
- `Deserialize`: `let rv = Box::<RawValue>::deserialize(d)?; Ok(Raw(rv.get().as_bytes().to_vec()))`.
- Tests: round-trip `{"a":1}` stays inline (not base64); `null` handling for no-omitempty fields (wrap in `Option<Raw>`, `None`→`null`).
- Migrate `command.rs`/`messages.rs` byte fields from `Box<RawValue>` to `Raw`; keep all M1 JSON tests green (adjust construction helpers).
- Commit.

### M2.3 — domain⇄pb conversions
- `From<Command> for pb::Command` and back; same for Reply, Push, and every request/result/inner message. Bytes: `Raw` ⇄ `Vec<u8>`. Enums: `MethodType`/`PushType` ⇄ pb i32. Maps (`subs`, `presence`) handled.
- Tests: round-trip each domain type through pb and assert equality.
- Commit.

### M2.4 — `Codec` / `ProtocolType` + protobuf framing
- `decode_commands(proto, frame) -> Vec<Command>`: JSON = M1 NDJSON streaming; Protobuf = loop `uvarint len` + `pb::Command::decode` then convert.
- `encode_replies(proto, &[Reply]) -> Vec<u8>`: JSON = NDJSON; Protobuf = for each, `pb::Reply::encode` then `uvarint len`-prefix concat.
- `decode_params::<T>(proto, &Raw)`, `encode_result(proto, &T) -> Raw`, `encode_push(proto, &Push)`.
- Tests: protobuf frame with two packed commands decodes to two; uvarint framing matches `binary.PutUvarint` (compare against a known Go-produced frame captured from the oracle if feasible, else against prost+manual uvarint).
- Commit.

### M2.5 — full method dispatch + disconnect signalling
- `Client::handle_command` returns `Action { replies: Vec<Reply>, disconnect: Option<Disconnect> }`.
- CONNECT-first violation → `disconnect: Some(Disconnect::bad_request())` (Go closes the connection). Malformed params → `ErrorBadRequest` reply (not disconnect) where Go does so; match `client.go`.
- SEND → no reply (empty action). RPC (no handler) → `ErrorMethodNotFound`. PRESENCE/PRESENCE_STATS/HISTORY → `ErrorNotAvailable` (until M4/M5). REFRESH/SUB_REFRESH → minimal valid result for insecure (no expiry).
- Tests mirror `client.go`/`handler_websocket_test.go` behaviors (unit, in-process).
- Commit.

### M2.6 — transport: protocol selection, frames, limits, closes
- `ws.rs`: parse `?format=`/`?protocol=` → `ProtocolType`; JSON→Text frames, Protobuf→Binary frames.
- 64KB max frame (reject/close `DisconnectBadRequest`); 1s write timeout (close `DisconnectWriteError`).
- On bad frame decode → close with `DisconnectBadRequest` (code 3003) text. On `Action.disconnect` → close with its code+reason. On writer queue full → close `DisconnectSlow` (3008).
- WebSocket close frame: code = disconnect code; reason = `disconnect.close_text()`.
- Tests: harness asserts close code/reason for bad frame and CONNECT-first; protobuf connect/subscribe/publish round-trip.
- Commit.

### M2.7 — conformance: protobuf golden diff + centrifuge-go subset
- Protobuf WS client in the harness; golden diff connect/subscribe/publish in protobuf vs the Go oracle.
- Add a runner that clones `centrifuge-go` v0.8.4 and runs `go test` against the Rust binary (insecure, :8000); document which subset must pass at M2 (Connect/Subscribe/Publish/Unsubscribe). Skips if Go/network absent.
- Commit.

---

## Risks
- **uvarint framing byte-exactness** — verify against a real Go-produced protobuf frame, not just self-consistency.
- **`Raw` JSON serialize on invalid UTF-8** — guarded by the JSON-connection invariant; protobuf path uses bytes directly.
- **Disconnect close semantics** — code is the WS close code, reason is the JSON text < 127 bytes; confirm tungstenite/axum let us set both.
- **centrifuge-go v0.8.4 era** — must use `?format=protobuf` (this version), not subprotocol negotiation.
