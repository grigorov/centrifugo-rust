# Generated apipb Serialization Test Plan

> Execution: add focused Rust suites instead of mechanically porting every Go
> generated test. Steps use `- [ ]`.

**Goal:** close the `internal/api/apipb_test.go` coverage gap by testing the
same wire-contract classes in Rust: protobuf encode/decode, encoded size,
stable field numbers/types, nested messages, bytes fields, repeated fields, and
maps. The Go file has many generated checks (`Proto`, `MarshalTo`, `JSON`,
`ProtoText`, `Size`); Rust/prost exposes a different API, so the Rust suite
should be compact, table-driven, and contract-focused.

**Authority:**
- Go reference: `conformance/go-oracle/src/internal/api/apipb_test.go`
- Client protocol proto: `crates/centrifugo-protocol/proto/client.proto`
- Server API proto: `crates/centrifugo-grpc/proto/api.proto`
- Rust client protocol types/conversions:
  `crates/centrifugo-protocol/src/{pb.rs,messages.rs,convert.rs,codec.rs}`
- Rust gRPC API types: `crates/centrifugo-grpc/src/lib.rs`

---

## Scope split

The Go `apipb` suite covers two conceptual protobuf surfaces that live in
different Rust crates:

1. **Client/wire protocol** in `centrifugo-protocol`.
   Examples: `Error`, `Command`, `Reply`, `Push`, `ClientInfo`, `Publication`,
   `Connect*`, `Subscribe*`, `Presence*`, `History*`, `RPC*`.

2. **Server API/gRPC protocol** in `centrifugo-grpc`.
   Examples: `Publish*`, `Broadcast*`, `Unsubscribe*`, `Disconnect*`,
   `Presence*`, `History*`, `Channels*`, `Info*`, `NodeResult`, `Metrics`.

Keep the tests in those crates so failures point at the owning schema/module.

---

## Tasks

### 1. Add client protocol prost suite

- [ ] Create `crates/centrifugo-protocol/tests/generated_apipb.rs`.
- [ ] Add a helper:
  - `assert_prost_roundtrip<T>(value: T)`
  - bounds: `T: prost::Message + Default + PartialEq + Debug + Clone`
  - assert `value.encoded_len() == value.encode_to_vec().len()`
  - decode the encoded bytes and assert equality.
- [ ] Cover representative `pb` types from `client.proto`:
  - `Error`
  - `Command`
  - `Reply`
  - `Push`
  - `ClientInfo`
  - `Publication`
  - `ConnectRequest`, `ConnectResult`
  - `SubscribeRequest`, `SubscribeResult`
  - `UnsubscribeRequest`, `UnsubscribeResult`
  - `PublishRequest`, `PublishResult`
  - `PresenceRequest`, `PresenceResult`
  - `PresenceStatsRequest`, `PresenceStatsResult`
  - `HistoryRequest`, `HistoryResult`
  - `RefreshRequest`, `RefreshResult`
  - `SubRefreshRequest`, `SubRefreshResult`
  - `RpcRequest`, `RpcResult`
  - `Join`, `Leave`, `PingResult`
- [ ] Include fixtures for:
  - scalar-only message
  - bytes field containing raw JSON bytes
  - nested `ClientInfo`
  - repeated `Publication`
  - `map<string, ...>` fields (`subs`, `presence`)
  - empty result messages.
- [ ] Keep the existing enum-value checks, or move/expand them here:
  `MethodType::{Connect, SubRefresh}` and `PushType::{Publication, Sub}`.

### 2. Add domain-to-protobuf conversion suite

- [ ] In `crates/centrifugo-protocol/tests/generated_apipb.rs`, add tests for
  `messages.rs` domain structs crossing the `convert.rs` boundary:
  domain -> `pb` -> domain.
- [ ] Cover all domain types that implement conversion:
  - envelopes: `Command`, `Reply`, `Push`
  - inner objects: `Error`, `ClientInfo`, `Publication`
  - request/result payloads for all supported methods.
- [ ] Assert raw byte semantics explicitly:
  - non-empty `Raw` becomes the identical `Vec<u8>`
  - non-empty `Vec<u8>` becomes `Some(Raw)`
  - empty `Vec<u8>` becomes `None` where this is current Rust behavior.
- [ ] Include nested cases:
  - `Publication.info`
  - `SubscribeResult.publications`
  - `ConnectRequest.subs`
  - `ConnectResult.subs`
  - `PresenceResult.presence`.

### 3. Add server API prost suite

- [ ] Create `crates/centrifugo-grpc/tests/generated_apipb.rs`.
- [ ] Reuse the same `assert_prost_roundtrip<T>` helper locally.
- [ ] Cover representative `api.proto` messages:
  - `ClientInfo`
  - `Publication`
  - `Error`
  - `Command`
  - `Reply`
  - `PublishRequest`, `PublishResponse`, `PublishResult`
  - `BroadcastRequest`, `BroadcastResponse`, `BroadcastResult`
  - `UnsubscribeRequest`, `UnsubscribeResponse`, `UnsubscribeResult`
  - `DisconnectRequest`, `DisconnectResponse`, `DisconnectResult`
  - `PresenceRequest`, `PresenceResponse`, `PresenceResult`
  - `PresenceStatsRequest`, `PresenceStatsResponse`, `PresenceStatsResult`
  - `HistoryRequest`, `HistoryResponse`, `HistoryResult`
  - `HistoryRemoveRequest`, `HistoryRemoveResponse`, `HistoryRemoveResult`
  - `ChannelsRequest`, `ChannelsResponse`, `ChannelsResult`
  - `InfoRequest`, `InfoResponse`, `InfoResult`
  - `RpcRequest`, `RpcResponse`, `RpcResult`
  - `NodeResult`
  - `Metrics`.
- [ ] Include fixtures for:
  - response with `error`
  - response with `result`
  - repeated publications
  - repeated channels
  - repeated nodes
  - metrics payload.

### 4. Add a small Go-compatible golden-byte layer

- [ ] Add 10-15 representative protobuf golden fixtures, not one per generated
  Go test.
- [ ] Use hex strings checked into the Rust tests, with comments naming the Go
  source type and field values.
- [ ] Prefer stable non-map messages for byte-for-byte assertions.
- [ ] For map messages, decode and compare semantic equality instead of raw
  bytes because protobuf map entry order is not a good cross-runtime contract.
- [ ] Good candidates:
  - `Error{code,message}`
  - `Command{id,method,params}`
  - `Reply{id,result}`
  - `ClientInfo{user,client,conn_info,chan_info}`
  - `Publication{seq,gen,uid,data,info,offset}`
  - `PublishRequest{channel,data}`
  - `HistoryResult{publications}`
  - `PresenceStatsResult{num_clients,num_users}`
  - `InfoResult{nodes}`
  - `Metrics{...}`.

### 5. Decide what not to port literally

- [ ] Do not port Go `MarshalTo` tests literally. `prost` does not expose the
  same generated API; `encode_to_vec` + `encoded_len` covers the Rust contract.
- [ ] Do not assert generated text/proto string formatting unless a real consumer
  depends on it. Rust `Debug` output is not a wire contract.
- [ ] Do not force byte-for-byte assertions for map messages.
- [ ] Keep JSON parity on the Rust domain/HTTP layers, not on tonic/prost
  generated structs, unless the generated structs are explicitly serialized as
  JSON by production code.

### 6. Optional stretch: generated fixture helper

- [ ] If maintaining golden bytes manually gets annoying, add a small Go helper
  under `conformance/go-oracle/` that prints selected fixture bytes as hex.
- [ ] Keep generated output checked in as Rust constants so the default Rust test
  run does not need Go.
- [ ] The helper must be optional and documented; CI should not depend on it.

---

## Acceptance criteria

- [ ] `cargo test -p centrifugo-protocol generated_apipb`
- [ ] `cargo test -p centrifugo-grpc generated_apipb`
- [ ] `cargo test --workspace -- --list` shows both new suites.
- [ ] A field-number/type regression in either proto causes a focused test
  failure before any server conformance test runs.
- [ ] The suite covers protobuf binary contracts without adding hundreds of
  duplicated generated-method tests.

---

## Suggested implementation order

1. Client protocol prost roundtrips.
2. Client protocol domain <-> pb conversion roundtrips.
3. Server API prost roundtrips.
4. Hand-picked golden byte fixtures.
5. Optional Go fixture generator.

This order gives quick value first: prost schema regressions and conversion
regressions are caught immediately, while byte-for-byte Go fixtures can be added
after the shape of the Rust suite is stable.
