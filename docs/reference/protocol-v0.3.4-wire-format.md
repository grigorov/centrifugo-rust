# Centrifugal Protocol v0.3.4 — Wire Format (source-precise)

Sourced by reading `github.com/centrifugal/protocol@v0.3.4` (`definitions/client.proto`, `client.pb.go`, `encode.go`, `decode.go`, `type.go`, `raw.go`, `error.go`) and `github.com/centrifugal/centrifuge@v0.14.2` (`client.go`, `hub.go`, `errors.go`, `disconnect.go`, `handler_websocket.go`, `writer.go`). This is the authority for the Rust codec; do not guess against it.

**Governing fact:** the JSON codec is plain Go `encoding/json` over the protobuf-generated structs (no easyjson). So JSON keys/omitempty come straight from the Go struct `json:` tags, plus custom `MarshalJSON`/`UnmarshalJSON` on `Raw` and `MethodType`/`PushType`.

## 1. Envelopes (client.proto)

```proto
syntax = "proto3"; package protocol;

message Error   { uint32 code = 1; string message = 2; }
message Command { uint32 id = 1; MethodType method = 2; bytes params = 3; }
message Reply   { uint32 id = 1; Error error = 2; bytes result = 3; }
message Push    { PushType type = 1; string channel = 2; bytes data = 3; }

enum MethodType { CONNECT=0; SUBSCRIBE=1; UNSUBSCRIBE=2; PUBLISH=3; PRESENCE=4;
                  PRESENCE_STATS=5; HISTORY=6; PING=7; SEND=8; RPC=9; REFRESH=10; SUB_REFRESH=11; }
enum PushType   { PUBLICATION=0; JOIN=1; LEAVE=2; UNSUB=3; MESSAGE=4; SUB=5; }
```

There is **no `Disconnect` proto message** at v0.3.4 — Disconnect is a centrifuge Go struct delivered in the WS/SockJS close frame (see §7).

## 2. Request/Result/object messages (proto field numbers)

| Message | Fields (num: type name) |
|---|---|
| ConnectRequest | 1:string token, 2:bytes data, 3:map<string,SubscribeRequest> subs, 4:string name, 5:string version |
| ConnectResult | 1:string client, 2:string version, 3:bool expires, 4:uint32 ttl, 5:bytes data, 6:map<string,SubscribeResult> subs |
| RefreshRequest | 1:string token |
| RefreshResult | 1:string client, 2:string version, 3:bool expires, 4:uint32 ttl |
| SubscribeRequest | 1:string channel, 2:string token, 3:bool recover, 4:uint32 seq, 5:uint32 gen, 6:string epoch, 7:uint64 offset |
| SubscribeResult | 1:bool expires, 2:uint32 ttl, 3:bool recoverable, 4:uint32 seq, 5:uint32 gen, 6:string epoch, 7:repeated Publication publications, 8:bool recovered, 9:uint64 offset |
| SubRefreshRequest | 1:string channel, 2:string token |
| SubRefreshResult | 1:bool expires, 2:uint32 ttl |
| UnsubscribeRequest | 1:string channel |
| UnsubscribeResult | (empty → `{}`) |
| PublishRequest | 1:string channel, 2:bytes data |
| PublishResult | (empty → `{}`) |
| PresenceRequest | 1:string channel |
| PresenceResult | 1:map<string,ClientInfo> presence |
| PresenceStatsRequest | 1:string channel |
| PresenceStatsResult | 1:uint32 num_clients, 2:uint32 num_users |
| HistoryRequest | 1:string channel |
| HistoryResult | 1:repeated Publication publications |
| PingRequest / PingResult | (empty → `{}`) |
| RPCRequest | 1:bytes data, 2:string method |
| RPCResult | 1:bytes data |
| SendRequest | 1:bytes data |
| ClientInfo | 1:string user, 2:string client, 3:bytes conn_info, 4:bytes chan_info |
| Publication | 1:uint32 seq, 2:uint32 gen, 3:string uid, 4:bytes data, 5:ClientInfo info, 6:uint64 offset |
| Join | 1:ClientInfo info |
| Leave | 1:ClientInfo info |
| Unsub | 1:bool resubscribe |
| Sub | 1:bool recoverable, 2:uint32 seq, 3:uint32 gen, 4:string epoch, 5:uint64 offset |
| Message | 1:bytes data |

## 3. JSON encoding (critical)

- **All `bytes` fields are `protocol.Raw` = inline raw JSON, NEVER base64.** `Raw.MarshalJSON` returns the bytes verbatim, or `null` when nil. Applies to: Command.params, Reply.result, Push.data, Publication.data, RPCResult.data, ConnectRequest/Result.data, PublishRequest.data, Message.data, ClientInfo.conn_info/chan_info, SendRequest.data, RPCRequest.data.
- **`method`** is the **integer** enum value, tag `json:"method,omitempty"` → **omitted when 0 (CONNECT)**. Decoder must accept integer OR quoted name. `Push.type` same pattern (omitted when 0=PUBLICATION).
- **`id`** tag `json:"id,omitempty"` on both Command and Reply → omitted when 0.
- **A server→client async Push is framed as a `Reply` with no `id` (id==0) whose `result` contains the fully-encoded `Push`** (double encoding). Client distinguishes push from command-reply by `id==0`. Example (centrifuge hub.go): encode inner Publication → wrap as Push → encode Push → `&protocol.Reply{Result: pushBytes}` (ID left zero).
- **JSON keys are snake_case** from the `json:` tag (not the protobuf camelCase alias): `conn_info`, `chan_info`, `num_clients`, `num_users`.
- **`data` has NO omitempty** on Push, Publication, Message, PublishRequest, RPCRequest, SendRequest → always present; serializes as literal `null` when the Raw is nil.
- omitempty elsewhere: bools omitted when false, ints when 0, strings when "", maps/slices when empty.

Full omitempty map (load-bearing):
- Error: `code`, `message` (neither omitempty)
- Command: `id,omitempty` `method,omitempty` `params,omitempty`
- Reply: `id,omitempty` `error,omitempty` `result,omitempty`
- Push: `type,omitempty` `channel,omitempty` `data` (NO omitempty)
- ClientInfo: `user` `client` (no omitempty) · `conn_info,omitempty` `chan_info,omitempty`
- Publication: `seq,omitempty` `gen,omitempty` `uid,omitempty` `data`(NO) `info,omitempty` `offset,omitempty`
- ConnectResult: `client` `version` (no omitempty) · `expires,omitempty` `ttl,omitempty` `data,omitempty` `subs,omitempty`
- SubscribeResult: all `omitempty` (`expires,ttl,recoverable,seq,gen,epoch,publications,recovered,offset`)
- PresenceStatsResult: `num_clients` `num_users` (no omitempty)
- RPCResult: `data,omitempty`
- UnsubscribeResult/PublishResult/PingResult: no fields → `{}`

Numbers: `id`/`seq`/`gen`/`ttl`/`code` are uint32; `offset` is uint64. All marshal as plain JSON numbers (uint64 NOT quoted).

## 4. JSON framing

NDJSON on write, whitespace-tolerant streaming on read. Encoder appends `\n` after every Reply (`encode.go`). Multiple replies in one WS text frame = concatenation of `...\n...\n`. Decoder = `json.NewDecoder(reader)` looped until `io.EOF` — tolerates newline-separated or bare-concatenated values. **Not length-prefixed in JSON.** WS frame type: **text** for JSON.

Rust: encoder appends `\n` after each command/reply; decoder stream-parses consecutive JSON values tolerating whitespace.

## 5. Protobuf framing

Uvarint length-prefix per message (`binary.PutUvarint`), multiple messages packed per binary frame. Decoder reads `binary.Uvarint` then that many bytes, advancing offset until end. WS frame type: **binary** for protobuf.

## 6. Connect flow

`ConnectResult.client` (string) is the connection/client ID — JSON key `"client"` (not `id`/`client_id`). No `user` field in ConnectResult. v0.3.4 already has `subs map<string,SubscribeResult>` (server-side subs) in both ConnectRequest and ConnectResult.

## 7. Error & Disconnect tables (centrifuge v0.14.2)

**Errors** (`Error{code,message}`):

| Code | Name | Message |
|---|---|---|
| 100 | Internal | internal server error |
| 101 | Unauthorized | unauthorized |
| 102 | UnknownChannel | unknown channel |
| 103 | PermissionDenied | permission denied |
| 104 | MethodNotFound | method not found |
| 105 | AlreadySubscribed | already subscribed |
| 106 | LimitExceeded | limit exceeded |
| 107 | BadRequest | bad request |
| 108 | NotAvailable | not available |
| 109 | TokenExpired | token expired |
| 110 | Expired | expired |
| 111 | TooManyRequests | too many requests |

**Disconnect** (Go struct; JSON `{"reason":"...","reconnect":bool}` in close-frame text < 127 bytes; code is the WS close code):

| Code | Name | Reason | Reconnect |
|---|---|---|---|
| 3000 | Normal | normal | true |
| 3001 | Shutdown | shutdown | true |
| 3002 | InvalidToken | invalid token | false |
| 3003 | BadRequest | bad request | false |
| 3004 | ServerError | internal server error | true |
| 3005 | Expired | expired | true |
| 3006 | SubExpired | subscription expired | true |
| 3007 | Stale | stale | false |
| 3008 | Slow | slow | true |
| 3009 | WriteError | write error | true |
| 3010 | InsufficientState | insufficient state | true |
| 3011 | ForceReconnect | force reconnect | true |
| 3012 | ForceNoReconnect | force disconnect | false |
| 3013 | ConnectionLimit | connection limit | false |

## 8. Implementer caveats

- centrifuge v0.14.2 **zeroes `Publication.Offset` before pushing to clients** (hub.go "Do not send offset to clients for now") → pushed Publications typically have no `offset` field. (History/subscribe-result publications may still carry it — verify against the oracle per-case.)
- Empty/nil `data` on no-omitempty fields → `"data":null` on the wire.
- WS close: code is the close code; reason text is the JSON disconnect body.
