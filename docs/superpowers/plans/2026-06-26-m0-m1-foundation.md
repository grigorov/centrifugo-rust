# M0 + M1: Foundation & Thin Vertical Slice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Cargo workspace, the Go behavior-oracle, and a conformance harness, then deliver a thin vertical slice — a Rust binary that speaks the real Centrifugal v0.3.4 JSON wire protocol over WebSocket well enough that one client can CONNECT + SUBSCRIBE and receive a Publication produced by a second client's PUBLISH.

**Architecture:** Approach C (faithful contracts, idiomatic Rust concurrency). Cargo workspace of focused crates: `centrifugo-protocol` (wire codec), `centrifugo-core` (Hub/Node/Client/Engine + memory broker), `centrifugo-server` (binary: axum HTTP + WS transport, CLI), `conformance` (spawn-binary integration tests + Go oracle). Per-connection = a read task + a writer task draining a bounded `tokio::mpsc`; the Node encodes each push once and `try_send`s the shared frame to every subscriber, so a slow/full client is dropped (`DisconnectSlow`) without blocking the broadcaster. Hub is sharded by channel hash.

**Tech Stack:** Rust (stable), tokio, axum + `axum::extract::ws`, serde + serde_json (`RawValue` for inline-raw bytes), tracing, clap; Go (for the oracle binary); conformance uses tokio-tungstenite + reqwest.

**Authority for all wire bytes:** `docs/reference/protocol-v0.3.4-wire-format.md`. Every JSON shape below is taken from it — do not deviate.

---

## File structure

```
Cargo.toml                              # workspace
rust-toolchain.toml                     # pin stable
.github/workflows/ci.yml                # fmt + clippy + test
crates/
  centrifugo-protocol/
    Cargo.toml
    src/lib.rs                          # re-exports
    src/method.rs                       # MethodType, PushType (int repr, str-or-int decode)
    src/error.rs                        # Error{code,message} + 100..111 constants
    src/disconnect.rs                   # Disconnect{code,reason,reconnect} + 3000..3013
    src/command.rs                      # Command, Reply, Push envelopes
    src/messages.rs                     # *Request / *Result / Publication / ClientInfo / Join / Leave / Sub / Unsub / Message
    src/json.rs                         # JsonCommandDecoder (streaming), encode_reply (NDJSON), push framing helpers
  centrifugo-core/
    Cargo.toml
    src/lib.rs
    src/hub.rs                          # sharded registry: conns/users/subs
    src/engine.rs                       # Broker + PresenceManager traits, Engine alias
    src/memory.rs                       # MemoryBroker (pub/sub only for M1)
    src/client.rs                       # per-connection session: queue, handle_command
    src/node.rs                         # ties Hub + Engine + clients; publish fan-out
  centrifugo-server/
    Cargo.toml
    src/main.rs                         # CLI dispatch
    src/cli.rs                          # clap: serve, version
    src/config.rs                       # minimal: address, port, client_insecure
    src/http.rs                         # axum router: /health, /connection/websocket
    src/ws.rs                           # WS upgrade + read loop + writer task + ping
conformance/
  Cargo.toml                            # test-only crate
  src/lib.rs                            # Harness: spawn binary, await /health, WsJsonClient helper
  src/oracle.rs                         # locate/spawn the Go oracle binary
  tests/m0_smoke.rs                     # /health smoke
  tests/m1_vertical.rs                  # connect→subscribe→publish→receive
  go-oracle/
    build.sh                            # clone+build centrifugo v2.8.6
    README.md
```

---

## M0 — Foundation

### Task 0.1: Cargo workspace + toolchain + CI

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the workspace manifest**

`Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = ["crates/centrifugo-protocol", "crates/centrifugo-core", "crates/centrifugo-server", "conformance"]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "MIT"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = { version = "1", features = ["raw_value"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
axum = { version = "0.7", features = ["ws"] }
clap = { version = "4", features = ["derive"] }
anyhow = "1"
thiserror = "1"
futures-util = "0.3"
```

`rust-toolchain.toml`:
```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 2: Create each crate's directory with a stub so the workspace resolves**

For now create `crates/centrifugo-protocol/Cargo.toml`, `crates/centrifugo-core/Cargo.toml`, `crates/centrifugo-server/Cargo.toml`, `conformance/Cargo.toml`, each with a minimal `[package]` and an empty `src/lib.rs` (or `src/main.rs` for server). Exact contents are filled in by later tasks; for this step each lib crate gets:
```toml
[package]
name = "centrifugo-protocol"   # (and -core, etc.)
version.workspace = true
edition.workspace = true

[dependencies]
```
and `src/lib.rs` containing `// placeholder` (replaced in Task 0.3+). The server crate gets `src/main.rs` with `fn main() {}`. The conformance crate Cargo.toml sets `publish = false`.

- [ ] **Step 3: CI workflow**

`.github/workflows/ci.yml`:
```yaml
name: ci
on: [push, pull_request]
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy }
      - run: cargo fmt --all --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: compiles (empty crates), no errors.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(m0): cargo workspace, toolchain pin, CI skeleton"
```

### Task 0.2: Go oracle build script

**Files:**
- Create: `conformance/go-oracle/build.sh`
- Create: `conformance/go-oracle/README.md`

- [ ] **Step 1: Write the build script**

`conformance/go-oracle/build.sh` (idempotent; clones the tag, builds the binary into `bin/centrifugo`):
```bash
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$DIR/src"
BIN="$DIR/bin/centrifugo"
TAG="v2.8.6"
if [ ! -x "$BIN" ]; then
  rm -rf "$SRC"
  git clone --depth 1 --branch "$TAG" https://github.com/centrifugal/centrifugo "$SRC"
  mkdir -p "$DIR/bin"
  ( cd "$SRC" && go build -o "$BIN" . )
fi
"$BIN" version
```

- [ ] **Step 2: README documents usage**

`conformance/go-oracle/README.md`: explain that `build.sh` produces `bin/centrifugo` (the real Go centrifugo v2.8.6), used as the differential oracle; `bin/` and `src/` are git-ignored (already covered by `.gitignore`).

- [ ] **Step 3: Run it and verify version**

Run: `bash conformance/go-oracle/build.sh`
Expected: prints `Centrifugo v2.8.6` (the `version` subcommand output). If `go` not on PATH, run with `PATH="$(brew --prefix)/bin:$PATH"`.

- [ ] **Step 4: Commit**

```bash
git add conformance/go-oracle/build.sh conformance/go-oracle/README.md
git commit -m "test(m0): build script for the Go centrifugo v2.8.6 oracle"
```

### Task 0.3: `MethodType` / `PushType` with exact JSON semantics

**Files:**
- Modify: `crates/centrifugo-protocol/Cargo.toml`
- Create: `crates/centrifugo-protocol/src/method.rs`
- Modify: `crates/centrifugo-protocol/src/lib.rs`

`Cargo.toml` deps: `serde = { workspace = true }`, `serde_json = { workspace = true }`, `thiserror = { workspace = true }`.

- [ ] **Step 1: Write failing tests**

In `src/method.rs` (bottom):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn method_serializes_as_integer() {
        assert_eq!(serde_json::to_string(&MethodType::Subscribe).unwrap(), "1");
        assert_eq!(serde_json::to_string(&MethodType::SubRefresh).unwrap(), "11");
    }
    #[test]
    fn method_deserializes_from_int_or_string() {
        assert_eq!(serde_json::from_str::<MethodType>("3").unwrap(), MethodType::Publish);
        assert_eq!(serde_json::from_str::<MethodType>("\"publish\"").unwrap(), MethodType::Publish);
        assert_eq!(serde_json::from_str::<MethodType>("\"PUBLISH\"").unwrap(), MethodType::Publish);
    }
    #[test]
    fn connect_is_default_zero() {
        assert_eq!(MethodType::default(), MethodType::Connect);
        assert_eq!(MethodType::Connect as u8, 0);
        assert!(MethodType::Connect.is_default());
    }
    #[test]
    fn push_type_publication_is_zero() {
        assert_eq!(serde_json::to_string(&PushType::Join).unwrap(), "1");
        assert!(PushType::Publication.is_default());
    }
}
```

- [ ] **Step 2: Run, verify fail**

Run: `cargo test -p centrifugo-protocol method`
Expected: FAIL (types not defined).

- [ ] **Step 3: Implement**

`src/method.rs`:
```rust
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MethodType {
    Connect = 0, Subscribe = 1, Unsubscribe = 2, Publish = 3, Presence = 4,
    PresenceStats = 5, History = 6, Ping = 7, Send = 8, Rpc = 9, Refresh = 10, SubRefresh = 11,
}
impl Default for MethodType { fn default() -> Self { MethodType::Connect } }
impl MethodType {
    pub fn is_default(&self) -> bool { *self == MethodType::Connect }
    fn from_u64(n: u64) -> Option<Self> {
        Some(match n {
            0 => Self::Connect, 1 => Self::Subscribe, 2 => Self::Unsubscribe, 3 => Self::Publish,
            4 => Self::Presence, 5 => Self::PresenceStats, 6 => Self::History, 7 => Self::Ping,
            8 => Self::Send, 9 => Self::Rpc, 10 => Self::Refresh, 11 => Self::SubRefresh, _ => return None,
        })
    }
    fn from_name(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "CONNECT" => Self::Connect, "SUBSCRIBE" => Self::Subscribe, "UNSUBSCRIBE" => Self::Unsubscribe,
            "PUBLISH" => Self::Publish, "PRESENCE" => Self::Presence, "PRESENCE_STATS" => Self::PresenceStats,
            "HISTORY" => Self::History, "PING" => Self::Ping, "SEND" => Self::Send, "RPC" => Self::Rpc,
            "REFRESH" => Self::Refresh, "SUB_REFRESH" => Self::SubRefresh, _ => return None,
        })
    }
}
impl Serialize for MethodType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.serialize_u8(*self as u8) }
}
impl<'de> Deserialize<'de> for MethodType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MethodType;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("method int or name") }
            fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<MethodType, E> {
                MethodType::from_u64(n).ok_or_else(|| E::custom("bad method int"))
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<MethodType, E> {
                MethodType::from_name(s).ok_or_else(|| E::custom("bad method name"))
            }
        }
        d.deserialize_any(V)
    }
}

// PushType mirrors MethodType: PUBLICATION=0, JOIN=1, LEAVE=2, UNSUB=3, MESSAGE=4, SUB=5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PushType { Publication = 0, Join = 1, Leave = 2, Unsub = 3, Message = 4, Sub = 5 }
impl Default for PushType { fn default() -> Self { PushType::Publication } }
impl PushType {
    pub fn is_default(&self) -> bool { *self == PushType::Publication }
}
impl Serialize for PushType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.serialize_u8(*self as u8) }
}
impl<'de> Deserialize<'de> for PushType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let n = u8::deserialize(d)?;
        Ok(match n { 0=>Self::Publication,1=>Self::Join,2=>Self::Leave,3=>Self::Unsub,4=>Self::Message,5=>Self::Sub,
            _ => return Err(serde::de::Error::custom("bad push type")) })
    }
}
```

`src/lib.rs`: `pub mod method; pub use method::{MethodType, PushType};`

- [ ] **Step 4: Run, verify pass**

Run: `cargo test -p centrifugo-protocol method`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/centrifugo-protocol
git commit -m "feat(protocol): MethodType/PushType with integer JSON + str-or-int decode"
```

### Task 0.4: Envelopes (`Command`/`Reply`/`Push`) + result/request messages

**Files:**
- Create: `crates/centrifugo-protocol/src/command.rs`
- Create: `crates/centrifugo-protocol/src/messages.rs`
- Modify: `crates/centrifugo-protocol/src/lib.rs`

Key serde rules (from the wire-format reference):
- `Command.id` / `Reply.id`: `#[serde(default, skip_serializing_if = "is_zero")]`.
- `Command.method`: `#[serde(default, skip_serializing_if = "MethodType::is_default")]`.
- bytes fields = `Option<Box<serde_json::value::RawValue>>` (inline raw JSON). For `params`/`result`/`error` use `skip_serializing_if = "Option::is_none"`. For **`data`** on Push/Publication/Message/PublishRequest/RPCRequest/SendRequest: NO skip → `None` serializes as `null`.
- snake_case keys where required via explicit `#[serde(rename = "...")]`: `conn_info`, `chan_info`, `num_clients`, `num_users`.

- [ ] **Step 1: Write failing tests** (`src/command.rs` tests module)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;

    fn raw(s: &str) -> Box<RawValue> { RawValue::from_string(s.to_string()).unwrap() }

    #[test]
    fn connect_command_omits_method_and_uses_inline_params() {
        let cmd = Command { id: 1, method: MethodType::Connect, params: Some(raw("{}")) };
        assert_eq!(serde_json::to_string(&cmd).unwrap(), r#"{"id":1,"params":{}}"#);
    }
    #[test]
    fn subscribe_command_has_integer_method() {
        let cmd = Command { id: 2, method: MethodType::Subscribe, params: Some(raw(r#"{"channel":"news"}"#)) };
        assert_eq!(serde_json::to_string(&cmd).unwrap(), r#"{"id":2,"method":1,"params":{"channel":"news"}}"#);
    }
    #[test]
    fn reply_with_result_no_id_for_push() {
        // a push is a Reply with id==0 carrying an encoded Push in result
        let push = Push { r#type: PushType::Publication, channel: "news".into(), data: Some(raw(r#"{"data":{"x":1}}"#)) };
        let reply = Reply::push(&push).unwrap();
        assert_eq!(serde_json::to_string(&reply).unwrap(),
            r#"{"result":{"channel":"news","data":{"data":{"x":1}}}}"#);
    }
    #[test]
    fn command_reply_has_id_and_result() {
        let reply = Reply { id: 7, error: None, result: Some(raw(r#"{"client":"abc","version":""}"#)) };
        assert_eq!(serde_json::to_string(&reply).unwrap(),
            r#"{"id":7,"result":{"client":"abc","version":""}}"#);
    }
    #[test]
    fn reply_with_error() {
        let reply = Reply { id: 3, error: Some(crate::error::Error::unknown_channel()), result: None };
        assert_eq!(serde_json::to_string(&reply).unwrap(),
            r#"{"id":3,"error":{"code":102,"message":"unknown channel"}}"#);
    }
}
```

- [ ] **Step 2: Run, verify fail**

Run: `cargo test -p centrifugo-protocol command`
Expected: FAIL.

- [ ] **Step 3: Implement `command.rs`**

```rust
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use crate::error::Error;
use crate::method::{MethodType, PushType};

pub type Raw = Box<RawValue>;
fn is_zero(n: &u32) -> bool { *n == 0 }

#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub id: u32,
    #[serde(default, skip_serializing_if = "MethodType::is_default")]
    pub method: MethodType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Raw>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Raw>,
}
impl Reply {
    /// Build a command reply with an already-encoded result object.
    pub fn ok(id: u32, result: Raw) -> Self { Reply { id, error: None, result: Some(result) } }
    pub fn err(id: u32, error: Error) -> Self { Reply { id, error: Some(error), result: None } }
    /// Frame an async push: a Reply with id==0 whose result is the encoded Push.
    pub fn push(push: &Push) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_string(push)?;
        Ok(Reply { id: 0, error: None, result: Some(RawValue::from_string(bytes)?) })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Push {
    #[serde(default, skip_serializing_if = "PushType::is_default")]
    pub r#type: PushType,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    pub data: Option<Raw>, // NO skip: serializes as null when None
}
```

- [ ] **Step 4: Implement `messages.rs`** — the request/result structs and inner objects. Apply the omitempty/rename map from the reference. Representative set needed for M1 (others added in their milestones):

```rust
use serde::{Deserialize, Serialize};
use crate::command::Raw;

#[derive(Debug, Default, Deserialize)]
pub struct ConnectRequest {
    #[serde(default)] pub token: String,
    #[serde(default)] pub data: Option<Raw>,
    #[serde(default)] pub name: String,
    #[serde(default)] pub version: String,
}
#[derive(Debug, Serialize)]
pub struct ConnectResult {
    pub client: String,             // no skip
    pub version: String,            // no skip (emits "version":"")
    #[serde(skip_serializing_if = "is_false")] pub expires: bool,
    #[serde(skip_serializing_if = "is_zero_u32")] pub ttl: u32,
    #[serde(skip_serializing_if = "Option::is_none")] pub data: Option<Raw>,
    // subs map omitted in M1
}

#[derive(Debug, Default, Deserialize)]
pub struct SubscribeRequest {
    #[serde(default)] pub channel: String,
    #[serde(default)] pub token: String,
    #[serde(default)] pub recover: bool,
    #[serde(default)] pub epoch: String,
    #[serde(default)] pub offset: u64,
}
#[derive(Debug, Default, Serialize)]
pub struct SubscribeResult {
    #[serde(skip_serializing_if = "is_false")] pub expires: bool,
    #[serde(skip_serializing_if = "is_zero_u32")] pub ttl: u32,
    #[serde(skip_serializing_if = "is_false")] pub recoverable: bool,
    #[serde(skip_serializing_if = "String::is_empty")] pub epoch: String,
    #[serde(skip_serializing_if = "Vec::is_empty")] pub publications: Vec<Publication>,
    #[serde(skip_serializing_if = "is_false")] pub recovered: bool,
    #[serde(skip_serializing_if = "is_zero_u64")] pub offset: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct PublishRequest {
    #[serde(default)] pub channel: String,
    pub data: Option<Raw>,
}
#[derive(Debug, Default, Serialize)]
pub struct PublishResult {} // -> {}

#[derive(Debug, Default, Deserialize)]
pub struct UnsubscribeRequest { #[serde(default)] pub channel: String }
#[derive(Debug, Default, Serialize)]
pub struct UnsubscribeResult {} // -> {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientInfo {
    pub user: String,
    pub client: String,
    #[serde(rename = "conn_info", skip_serializing_if = "Option::is_none")] pub conn_info: Option<Raw>,
    #[serde(rename = "chan_info", skip_serializing_if = "Option::is_none")] pub chan_info: Option<Raw>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publication {
    #[serde(default, skip_serializing_if = "is_zero_u32")] pub seq: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")] pub gen: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")] pub uid: String,
    pub data: Option<Raw>, // NO skip
    #[serde(default, skip_serializing_if = "Option::is_none")] pub info: Option<ClientInfo>,
    #[serde(default, skip_serializing_if = "is_zero_u64")] pub offset: u64, // zeroed on push in v0.14.2
}

fn is_false(b: &bool) -> bool { !*b }
fn is_zero_u32(n: &u32) -> bool { *n == 0 }
fn is_zero_u64(n: &u64) -> bool { *n == 0 }
```

Add tests asserting `ConnectResult{client:"abc",version:"".."}` → `{"client":"abc","version":""}` and `PublishResult{}` → `{}` and `Publication` with only `data` set → `{"data":{...}}` (note offset zeroed → absent).

- [ ] **Step 5: lib.rs re-exports**

`pub mod command; pub mod messages; pub use command::{Command, Reply, Push, Raw};`

- [ ] **Step 6: Run, verify pass**

Run: `cargo test -p centrifugo-protocol`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/centrifugo-protocol
git commit -m "feat(protocol): Command/Reply/Push envelopes + connect/subscribe/publish messages"
```

### Task 0.5: Error & Disconnect tables

**Files:**
- Create: `crates/centrifugo-protocol/src/error.rs`
- Create: `crates/centrifugo-protocol/src/disconnect.rs`
- Modify: `crates/centrifugo-protocol/src/lib.rs`

- [ ] **Step 1: Write failing tests** (`error.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn error_codes_and_messages() {
        assert_eq!(serde_json::to_string(&Error::unknown_channel()).unwrap(),
            r#"{"code":102,"message":"unknown channel"}"#);
        assert_eq!(serde_json::to_string(&Error::permission_denied()).unwrap(),
            r#"{"code":103,"message":"permission denied"}"#);
        assert_eq!(Error::bad_request().code, 107);
    }
}
```
And in `disconnect.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disconnect_slow_close_text() {
        let d = Disconnect::slow();
        assert_eq!(d.code, 3008);
        assert!(d.reconnect);
        assert_eq!(d.close_text(), r#"{"reason":"slow","reconnect":true}"#);
    }
    #[test]
    fn disconnect_invalid_token_no_reconnect() {
        let d = Disconnect::invalid_token();
        assert_eq!(d.code, 3002);
        assert!(!d.reconnect);
    }
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p centrifugo-protocol error disconnect` → FAIL.

- [ ] **Step 3: Implement `error.rs`** — `Error { code: u32, message: String }` (`#[derive(Serialize, Deserialize)]`, both fields no skip), plus a constructor per code 100..111 with the exact messages from the reference table (`internal`, `unauthorized`, `unknown channel`, `permission denied`, `method not found`, `already subscribed`, `limit exceeded`, `bad request`, `not available`, `token expired`, `expired`, `too many requests`).

- [ ] **Step 4: Implement `disconnect.rs`** — `Disconnect { code: u32, reason: String, reconnect: bool }` with a constructor per code 3000..3013 (exact reason/reconnect from the reference table) and `close_text(&self) -> String` returning `{"reason":"...","reconnect":bool}` via serde (a small struct with just `reason`,`reconnect`). Assert text < 127 bytes in a debug_assert.

- [ ] **Step 5: lib.rs:** `pub mod error; pub mod disconnect; pub use error::Error; pub use disconnect::Disconnect;`

- [ ] **Step 6: Run, verify pass.** `cargo test -p centrifugo-protocol` → PASS.

- [ ] **Step 7: Commit**
```bash
git add crates/centrifugo-protocol
git commit -m "feat(protocol): error codes 100-111 and disconnect codes 3000-3013"
```

### Task 0.6: JSON codec — streaming decode + NDJSON encode

**Files:**
- Create: `crates/centrifugo-protocol/src/json.rs`
- Modify: `crates/centrifugo-protocol/src/lib.rs`

- [ ] **Step 1: Write failing tests** (`json.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, Reply};
    use serde_json::value::RawValue;

    #[test]
    fn decode_multiple_commands_one_frame() {
        // newline-separated AND bare-concatenated must both parse
        let frame = b"{\"id\":1}\n{\"id\":2,\"method\":1}";
        let cmds: Vec<Command> = decode_commands(frame).unwrap();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].id, 1);
        assert_eq!(cmds[1].id, 2);
    }
    #[test]
    fn encode_reply_appends_newline() {
        let r = Reply::ok(1, RawValue::from_string("{}".into()).unwrap());
        let out = encode_reply(&r).unwrap();
        assert_eq!(out, b"{\"id\":1,\"result\":{}}\n");
    }
    #[test]
    fn encode_many_concatenates() {
        let a = Reply::ok(1, RawValue::from_string("{}".into()).unwrap());
        let b = Reply::ok(2, RawValue::from_string("{}".into()).unwrap());
        let out = encode_replies(&[a, b]).unwrap();
        assert_eq!(out, b"{\"id\":1,\"result\":{}}\n{\"id\":2,\"result\":{}}\n");
    }
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p centrifugo-protocol json` → FAIL.

- [ ] **Step 3: Implement `json.rs`**

```rust
use crate::{Command, Reply};

/// Stream-decode consecutive JSON Commands from one WS text frame (NDJSON or bare-concatenated).
pub fn decode_commands(frame: &[u8]) -> Result<Vec<Command>, serde_json::Error> {
    let mut out = Vec::new();
    let de = serde_json::Deserializer::from_slice(frame);
    for cmd in de.into_iter::<Command>() {
        out.push(cmd?);
    }
    Ok(out)
}

/// Encode one Reply with a trailing newline (NDJSON).
pub fn encode_reply(reply: &Reply) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = serde_json::to_vec(reply)?;
    buf.push(b'\n');
    Ok(buf)
}

/// Encode many Replies as one NDJSON buffer (one WS frame).
pub fn encode_replies(replies: &[Reply]) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = Vec::new();
    for r in replies {
        serde_json::to_writer(&mut buf, r)?;
        buf.push(b'\n');
    }
    Ok(buf)
}
```

- [ ] **Step 4: Run, verify pass.** `cargo test -p centrifugo-protocol` → PASS (all protocol tests green).

- [ ] **Step 5: Commit**
```bash
git add crates/centrifugo-protocol
git commit -m "feat(protocol): NDJSON streaming command decoder + reply encoder"
```

### Task 0.7: Server skeleton — CLI + `/health`

**Files:**
- Modify: `crates/centrifugo-server/Cargo.toml`
- Create: `crates/centrifugo-server/src/cli.rs`
- Create: `crates/centrifugo-server/src/config.rs`
- Create: `crates/centrifugo-server/src/http.rs`
- Modify: `crates/centrifugo-server/src/main.rs`

`Cargo.toml` deps: `tokio`, `axum`, `clap`, `serde`, `serde_json`, `tracing`, `tracing-subscriber`, `anyhow`, `centrifugo-protocol = { path = "../centrifugo-protocol" }`, `centrifugo-core = { path = "../centrifugo-core" }`. Binary name must be `centrifugo`:
```toml
[[bin]]
name = "centrifugo"
path = "src/main.rs"
```

- [ ] **Step 1: CLI** (`cli.rs`) — clap derive with subcommands `Serve { #[arg(long, default_value="127.0.0.1")] address: String, #[arg(long, default_value_t=8000)] port: u16, #[arg(long)] client_insecure: bool }` and `Version`. `Version` prints `Centrifugo v2.8.6` (match the oracle's string so differential version checks align — document that this binary impersonates 2.8.6 on the wire).

- [ ] **Step 2: `/health`** (`http.rs`)

```rust
use axum::{routing::get, Router, Json};
use serde_json::json;

pub fn router() -> Router {
    Router::new().route("/health", get(|| async { Json(json!({})) }))
    // /connection/websocket added in Task 1.4
}

pub async fn serve(addr: std::net::SocketAddr, app: Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
```
(Go centrifugo returns `{}` with 200 on `/health`.)

- [ ] **Step 3: `main.rs`** wires clap → on `Serve`, build router, `serve`. Init tracing-subscriber. On `Version`, print and exit.

- [ ] **Step 4: Verify manually**

Run: `cargo run -p centrifugo-server -- version`
Expected: `Centrifugo v2.8.6`.
Run (in background): `cargo run -p centrifugo-server -- serve --port 8400 &` then `curl -s localhost:8400/health`
Expected: `{}` and HTTP 200. Kill the process.

- [ ] **Step 5: Commit**
```bash
git add crates/centrifugo-server
git commit -m "feat(server): CLI (serve/version) + /health endpoint on axum"
```

### Task 0.8: Conformance harness skeleton + `/health` smoke test

**Files:**
- Modify: `conformance/Cargo.toml`
- Create: `conformance/src/lib.rs`
- Create: `conformance/tests/m0_smoke.rs`

`Cargo.toml`: `publish = false`; dev/deps `tokio`, `reqwest = { version="0.12", default-features=false, features=["json"] }`, `tokio-tungstenite = "0.23"`, `futures-util`, `serde_json`, `anyhow`, `assert_cmd`-style spawning via `std::process` (or `escargot` to build the bin). Simplest: build the bin once with `cargo build -p centrifugo-server` and spawn `target/debug/centrifugo`.

- [ ] **Step 1: Write the harness** (`src/lib.rs`)

```rust
use std::process::{Child, Command};
use std::time::Duration;

pub struct Server { child: Child, pub port: u16, pub http: String }

impl Server {
    /// Spawn the centrifugo binary in insecure mode and wait until /health is ready.
    pub async fn start() -> Server {
        let port = pick_port();
        let bin = env!("CARGO_BIN_FILE_CENTRIFUGO_centrifugo", "centrifugo"); // if using artifact deps; else hardcode target path
        let child = Command::new(bin_path())
            .args(["serve", "--port", &port.to_string(), "--client-insecure"])
            .spawn().expect("spawn centrifugo");
        let http = format!("http://127.0.0.1:{port}");
        // poll /health for up to 10s
        let client = reqwest::Client::new();
        for _ in 0..100 {
            if client.get(format!("{http}/health")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
                return Server { child, port, http };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("server did not become healthy");
    }
    pub fn ws_url(&self) -> String { format!("ws://127.0.0.1:{}/connection/websocket", self.port) }
}
impl Drop for Server { fn drop(&mut self) { let _ = self.child.kill(); } }

fn bin_path() -> std::path::PathBuf {
    // workspace target dir; CARGO_MANIFEST_DIR is conformance/
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); p.push("target"); p.push("debug"); p.push("centrifugo"); p
}
fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}
```
(Note: the test run must `cargo build -p centrifugo-server` first; document in README that `cargo test -p conformance` depends on the built binary, or add a build.rs that runs the build. Simplest path: the CI `cargo test --workspace` builds all bins before tests.)

- [ ] **Step 2: Smoke test** (`tests/m0_smoke.rs`)

```rust
#[tokio::test]
async fn health_is_ok() {
    let s = conformance::Server::start().await;
    let body = reqwest::get(format!("{}/health", s.http)).await.unwrap();
    assert!(body.status().is_success());
}
```

- [ ] **Step 3: Run**

Run: `cargo build -p centrifugo-server && cargo test -p conformance m0_smoke`
Expected: PASS (server spawns, /health 200).

- [ ] **Step 4: Commit**
```bash
git add conformance
git commit -m "test(m0): conformance harness (spawn binary, await health) + smoke test"
```

---

## M1 — Thin vertical slice

### Task 1.1: Sharded Hub

**Files:**
- Modify: `crates/centrifugo-core/Cargo.toml`
- Create: `crates/centrifugo-core/src/hub.rs`
- Modify: `crates/centrifugo-core/src/lib.rs`

`Cargo.toml` deps: `tokio`, `centrifugo-protocol = { path = "../centrifugo-protocol" }`, `parking_lot = "0.12"` (or std RwLock). Model: N shards (e.g. 16), each `RwLock<HashMap<String /*channel*/, HashSet<ClientId>>>` plus a global `RwLock<HashMap<ClientId, ClientHandle>>` for conns and `RwLock<HashMap<String /*user*/, HashSet<ClientId>>>`. `ClientId = String` (uuid v4). `ClientHandle` holds the bounded `mpsc::Sender<Vec<u8>>` (encoded frames) + user id.

- [ ] **Step 1: Write failing tests** (`hub.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn handle() -> (ClientHandle, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::channel(16);
        (ClientHandle { user: "u1".into(), tx }, rx)
    }
    #[test]
    fn add_remove_subscriber_and_lookup() {
        let hub = Hub::new();
        let (h, _rx) = handle();
        let id = "c1".to_string();
        hub.add(id.clone(), h);
        hub.subscribe(&id, "news");
        let subs = hub.subscribers("news");
        assert_eq!(subs.len(), 1);
        hub.unsubscribe(&id, "news");
        assert_eq!(hub.subscribers("news").len(), 0);
        hub.remove(&id);
        assert!(hub.get(&id).is_none());
    }
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p centrifugo-core hub` → FAIL.

- [ ] **Step 3: Implement `hub.rs`** — `Hub::new()`, `add(id, handle)`, `remove(&id)`, `get(&id) -> Option<ClientHandle>` (clone of tx+user), `subscribe(&id, channel)`, `unsubscribe(&id, channel)`, `subscribers(channel) -> Vec<ClientHandle>` (clone the senders under read lock). Shard channels by `hash(channel) % N`. Keep the lock scope minimal: `subscribers()` collects sender clones, then the caller does `try_send` OUTSIDE the lock.

- [ ] **Step 4: Run, verify pass.** `cargo test -p centrifugo-core hub` → PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/centrifugo-core
git commit -m "feat(core): sharded Hub (conns/users/subs) with lock-minimal subscriber lookup"
```

### Task 1.2: Engine trait + MemoryBroker (pub/sub only)

**Files:**
- Create: `crates/centrifugo-core/src/engine.rs`
- Create: `crates/centrifugo-core/src/memory.rs`
- Modify: `crates/centrifugo-core/src/lib.rs`

For M1 the broker only needs pub/sub routing; history/presence land in M4/M5. The `Broker` trait method `publish(channel, data) -> Publication-to-route`. In the memory single-node case, publish simply hands the publication straight back to the Node for local fan-out. Define the trait so the Redis impl (M8) can route via PUB/SUB instead.

- [ ] **Step 1: Write failing test** (`memory.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn publish_routes_to_node_callback() {
        let routed = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String,String)>::new()));
        let r2 = routed.clone();
        let broker = MemoryBroker::new(move |ch: String, data: String| {
            r2.lock().unwrap().push((ch, data));
        });
        broker.publish("news", "{\"x\":1}".as_bytes()).await.unwrap();
        assert_eq!(routed.lock().unwrap()[0], ("news".into(), "{\"x\":1}".into()));
    }
}
```
(The callback models the Node's local fan-out entry point; in the real Node the callback enqueues to subscribers.)

- [ ] **Step 2: Run, verify fail.** `cargo test -p centrifugo-core memory` → FAIL.

- [ ] **Step 3: Implement `engine.rs` + `memory.rs`** — `trait Broker { async fn publish(&self, channel: &str, data: &[u8]) -> anyhow::Result<()>; async fn subscribe(&self, channel: &str) -> anyhow::Result<()>; async fn unsubscribe(&self, channel: &str) -> anyhow::Result<()>; }`. `MemoryBroker` holds the route callback `Arc<dyn Fn(String, String) + Send + Sync>`; `publish` invokes it; `subscribe`/`unsubscribe` are no-ops for memory single-node (the Hub tracks local subs). Keep it minimal.

- [ ] **Step 4: Run, verify pass.** PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/centrifugo-core
git commit -m "feat(core): Broker trait + MemoryBroker (single-node pub/sub routing)"
```

### Task 1.3: Client session + Node fan-out

**Files:**
- Create: `crates/centrifugo-core/src/client.rs`
- Create: `crates/centrifugo-core/src/node.rs`
- Modify: `crates/centrifugo-core/src/lib.rs`

The `Node` owns the `Hub` and the `Broker`, wired so the broker's route callback fans out to local subscribers: for each subscriber handle, build the push frame once and `try_send`; on `Err(TrySendError::Full)` mark that client for `DisconnectSlow` (drop its sender so its writer task ends). The `Client` is the per-connection state: id, user, authenticated flag, and the bounded `mpsc::Sender` already registered in the Hub. `Client::handle_command(&Command) -> Vec<Reply>` dispatches:
- CONNECT: in insecure mode assign `client = uuid`, mark authenticated, register in Hub, return `Reply::ok(id, ConnectResult{client, version:""})`. Reject a second CONNECT with `ErrorBadRequest`. Reject any non-CONNECT before CONNECT with `ErrorBadRequest` (CONNECT-first; full disconnect enforcement is M2).
- SUBSCRIBE: parse channel, `hub.subscribe`, `broker.subscribe`, return `Reply::ok(id, SubscribeResult::default())`.
- PUBLISH: parse channel+data, `broker.publish(channel, data)` (routes → fan-out), return `Reply::ok(id, PublishResult{})`.
- UNSUBSCRIBE: `hub.unsubscribe`, return `Reply::ok(id, UnsubscribeResult{})`.
- PING: return `Reply::ok(id, {})`.
- anything else: `Reply::err(id, ErrorMethodNotFound)` (stubs; full set in M2).

- [ ] **Step 1: Write failing test** (`node.rs`) — end-to-end in-process fan-out without the network:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn publish_fans_out_to_local_subscriber() {
        let node = Node::new();
        // subscriber connection
        let (tx_b, mut rx_b) = mpsc::channel::<Vec<u8>>(16);
        let mut sub = node.new_client(tx_b);
        sub.handle_command(&connect_cmd(1)).await;
        sub.handle_command(&subscribe_cmd(2, "news")).await;
        // publisher connection
        let (tx_a, _rx_a) = mpsc::channel::<Vec<u8>>(16);
        let mut pubr = node.new_client(tx_a);
        pubr.handle_command(&connect_cmd(1)).await;
        pubr.handle_command(&publish_cmd(2, "news", r#"{"msg":"hi"}"#)).await;
        // subscriber B receives a push frame (Reply with id==0, result is a Publication push)
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), rx_b.recv()).await.unwrap().unwrap();
        let s = String::from_utf8(frame).unwrap();
        assert!(s.contains("\"channel\":\"news\""));
        assert!(s.contains("\"msg\":\"hi\""));
        assert!(!s.contains("\"id\":")); // push has no id
    }
    // helpers connect_cmd/subscribe_cmd/publish_cmd build Command structs with raw params
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p centrifugo-core node` → FAIL.

- [ ] **Step 3: Implement `client.rs` + `node.rs`** per the dispatch spec above. The push frame for a publication: build `Publication { data: Some(raw(data)), ..default }` (offset zeroed → absent), wrap `Push { type: Publication, channel, data: Some(encoded Publication) }`, then `Reply::push(&push)`, then `encode_reply` → bytes. Encode once, clone bytes per subscriber, `try_send`.

- [ ] **Step 4: Run, verify pass.** PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/centrifugo-core
git commit -m "feat(core): Client session state machine + Node publish fan-out (non-blocking)"
```

### Task 1.4: WebSocket transport (JSON)

**Files:**
- Modify: `crates/centrifugo-server/src/http.rs` (add the route)
- Create: `crates/centrifugo-server/src/ws.rs`
- Modify: `crates/centrifugo-server/src/main.rs` (share a `Node` via state)

Wire `/connection/websocket` (GET upgrade). On upgrade: create a bounded `mpsc::channel(256)`, register a `Client` via `node.new_client(tx)`. Spawn a **writer task** that drains `rx` and writes each `Vec<u8>` as a WS **Text** frame. The **read loop**: on each Text frame, `decode_commands(frame)`, dispatch each through `client.handle_command`, collect the `Vec<Reply>`, `encode_replies`, send back through the same writer channel (so ordering is preserved). Start a `tokio::time::interval(25s)` that sends a WS **Ping** control frame (native ping per the reference). On socket close / writer-channel closed, `node.remove(client_id)`.

- [ ] **Step 1: Implement `ws.rs`** (axum `WebSocketUpgrade`):

```rust
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use centrifugo_core::Node;
use centrifugo_protocol::json::{decode_commands, encode_replies};

pub async fn ws_handler(State(node): State<Arc<Node>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, node))
}

async fn handle_socket(socket: WebSocket, node: Arc<Node>) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let mut client = node.new_client(tx.clone());
    let client_id = client.id.clone();

    // writer task: frames + 25s native ping
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(Duration::from_secs(25));
        ping.tick().await; // skip immediate
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(bytes) => { if sink.send(Message::Text(String::from_utf8_lossy(&bytes).into_owned())).await.is_err() { break; } }
                    None => break,
                },
                _ = ping.tick() => { if sink.send(Message::Ping(Vec::new())).await.is_err() { break; } }
            }
        }
    });

    // read loop
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(t) => {
                match decode_commands(t.as_bytes()) {
                    Ok(cmds) => {
                        let mut replies = Vec::new();
                        for c in &cmds { replies.extend(client.handle_command(c).await); }
                        if !replies.is_empty() {
                            if let Ok(buf) = encode_replies(&replies) { let _ = tx.send(buf).await; }
                        }
                    }
                    Err(_) => break, // bad frame -> close (full disconnect codes in M2)
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    node.remove(&client_id);
    writer.abort();
}
```

- [ ] **Step 2: Add the route + state** in `http.rs`: `Router::new().route("/health", ...).route("/connection/websocket", get(ws::ws_handler)).with_state(node)` where `node: Arc<Node>` is created in `main.rs`.

- [ ] **Step 3: Verify build.** `cargo build -p centrifugo-server` → compiles.

- [ ] **Step 4: Commit**
```bash
git add crates/centrifugo-server
git commit -m "feat(server): WebSocket transport (JSON) with per-conn writer task + 25s native ping"
```

### Task 1.5: M1 conformance test — connect → subscribe → publish → receive

**Files:**
- Modify: `conformance/src/lib.rs` (add `WsJsonClient` helper)
- Create: `conformance/tests/m1_vertical.rs`

- [ ] **Step 1: Add a minimal JSON WS client to the harness**

```rust
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub struct WsJsonClient { ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> }

impl WsJsonClient {
    pub async fn connect(url: &str) -> Self {
        let (ws, _) = connect_async(url).await.expect("ws connect");
        WsJsonClient { ws }
    }
    pub async fn send_raw(&mut self, json: &str) { self.ws.send(Message::Text(json.into())).await.unwrap(); }
    /// Send a CONNECT command (insecure, empty params) and return the client id from the reply.
    pub async fn connect_command(&mut self) -> String {
        self.send_raw(r#"{"id":1,"params":{}}"#).await;
        let v = self.next_json().await;
        v["result"]["client"].as_str().unwrap().to_string()
    }
    pub async fn subscribe(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(r#"{{"id":{id},"method":1,"params":{{"channel":"{channel}"}}}}"#)).await;
        self.next_json().await
    }
    pub async fn publish(&mut self, id: u32, channel: &str, data: &str) -> serde_json::Value {
        self.send_raw(&format!(r#"{{"id":{id},"method":3,"params":{{"channel":"{channel}","data":{data}}}}}"#)).await;
        self.next_json().await
    }
    /// Read the next text frame, parse first JSON line. Ignores Ping/Pong control frames.
    pub async fn next_json(&mut self) -> serde_json::Value {
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(2), self.ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let line = t.lines().next().unwrap_or("{}");
                    return serde_json::from_str(line).unwrap();
                }
                Ok(Some(Ok(_))) => continue, // ping/pong/binary
                other => panic!("ws closed/timeout waiting json: {other:?}"),
            }
        }
    }
}
```

- [ ] **Step 2: Write the vertical test** (`tests/m1_vertical.rs`)

```rust
use conformance::{Server, WsJsonClient};

#[tokio::test]
async fn connect_returns_client_id() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    assert!(!id.is_empty());
}

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    let s = Server::start().await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    let sub_reply = a.subscribe(2, "news").await;
    assert!(sub_reply.get("error").is_none(), "subscribe error: {sub_reply}");

    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    b.connect_command().await;
    let pub_reply = b.publish(2, "news", r#"{"msg":"hello"}"#).await;
    assert!(pub_reply.get("error").is_none(), "publish error: {pub_reply}");

    // A should now receive a publication push (Reply with no id; result is the Push)
    let push = a.next_json().await;
    assert!(push.get("id").is_none(), "push must have no id: {push}");
    let inner = &push["result"];
    assert_eq!(inner["channel"], "news");
    assert_eq!(inner["data"]["data"]["msg"], "hello");
}
```
Note on the assertion path: the push `result` is a `Push{channel,data}` whose `data` is the encoded `Publication{data:{"msg":"hello"}}`. So the nesting is `result.data` (Publication) `.data` (the user payload) `.msg`. Verify the exact nesting against the Go oracle in Task 1.6 and adjust if needed.

- [ ] **Step 3: Run**

Run: `cargo build -p centrifugo-server && cargo test -p conformance m1_vertical`
Expected: both tests PASS.

- [ ] **Step 4: Commit**
```bash
git add conformance
git commit -m "test(m1): vertical slice — connect/subscribe/publish/receive over real WS+JSON"
```

### Task 1.6: Differential check vs Go oracle (connect/subscribe/publish)

**Files:**
- Create: `conformance/tests/m1_golden.rs`

This locks the byte-level shapes against the real Go binary for the M1 surface, catching any framing/nesting mistakes early (the deepest M1 risk).

- [ ] **Step 1: Add a helper to start the Go oracle** in `src/oracle.rs`: spawn `conformance/go-oracle/bin/centrifugo serve` with a temp config enabling `client_insecure`, on a picked port, await `/health`. Skip the test (return early with a logged note) if the oracle binary is absent so the suite stays green on machines without Go.

- [ ] **Step 2: Write the differential test** (`tests/m1_golden.rs`)

Drive the SAME command sequence (connect, subscribe, publish, receive push) against both the Rust server and the Go oracle; capture the JSON replies/pushes from each; canonicalize (parse → sort keys) and assert structural equality for: ConnectResult keys/types, SubscribeResult, PublishResult (`{}`), and the publication push envelope (`id` absent, `result.channel`, `result.data` nesting). Where the Go output legitimately differs (e.g. the random `client` id, epoch), compare shape not value.

```rust
// pseudocode skeleton — see oracle.rs for start_oracle()
#[tokio::test]
async fn connect_reply_matches_go() {
    let Some(go) = conformance::oracle::start_oracle().await else { eprintln!("oracle absent; skipping"); return; };
    let rust = conformance::Server::start().await;
    let go_reply = conformance::ws_connect_reply(&go.ws_url()).await;     // serde_json::Value
    let rust_reply = conformance::ws_connect_reply(&rust.ws_url()).await;
    assert_eq!(key_shape(&go_reply), key_shape(&rust_reply)); // keys+types, ignoring values like client id
}
```
`key_shape` = recursively map a Value to its key set + value types (String/Number/Bool/Null/Array/Object), discarding leaf string/number values, so only structure is compared.

- [ ] **Step 3: Run (only meaningful with Go installed)**

Run: `bash conformance/go-oracle/build.sh && cargo test -p conformance m1_golden`
Expected: PASS (structural match). If a mismatch appears, the Rust shape is wrong — fix the protocol structs/framing to match Go, this is the whole point.

- [ ] **Step 4: Commit**
```bash
git add conformance
git commit -m "test(m1): golden differential vs Go centrifugo v2.8.6 for connect/subscribe/publish"
```

---

## Self-review notes

- **Spec coverage (M0+M1 surface):** workspace ✓ (0.1), Go oracle ✓ (0.2), protocol envelopes/codec/errors/disconnect ✓ (0.3–0.6), `/health`+CLI ✓ (0.7), harness ✓ (0.8), Hub ✓ (1.1), Broker/memory ✓ (1.2), Client/Node fan-out ✓ (1.3), WS+JSON+25s ping ✓ (1.4), vertical conformance ✓ (1.5), golden diff ✓ (1.6). Out of M1 scope by design (later milestones): protobuf (M2), full method set & disconnect-on-error (M2), JWT (M3), presence/join-leave (M4), history/recovery (M5), HTTP `/api` (M6).
- **Wire fidelity guardrails:** every JSON shape cites `docs/reference/protocol-v0.3.4-wire-format.md`; Task 1.6 verifies against the real Go binary.
- **Type consistency:** `Raw = Box<RawValue>`, `ClientId = String`, `MethodType`/`PushType` integer-encoded, `Reply::push/ok/err`, `Node::new_client(tx) -> Client`, `Client::handle_command(&Command) -> Vec<Reply>`, `Hub::subscribers(channel) -> Vec<ClientHandle>` used consistently across tasks.
- **Known follow-ups to confirm during execution:** (a) the exact publication-push nesting (`result.data` = encoded Publication) must be validated against the oracle in 1.6 — adjust if Go nests differently; (b) the conformance crate must build the server bin before tests (CI `cargo test --workspace` does; locally run `cargo build -p centrifugo-server` first or add a build.rs).
