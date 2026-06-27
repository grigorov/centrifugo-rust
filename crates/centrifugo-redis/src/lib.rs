//! Redis engine — multi-node fan-out for a homogeneous Rust centrifugo cluster.
//!
//! - **Pub/sub:** every publication/join/leave is `PUBLISH`ed to
//!   `centrifugo.pub.{channel}`; each node's subscriber task pattern-subscribes
//!   to `centrifugo.pub.*` and routes incoming messages into the local hub. The
//!   hub naturally drops messages for channels with no local subscriber, so
//!   per-channel `SUBSCRIBE` is unnecessary (a targeted-subscribe optimization
//!   for very large clusters is deferred). The publishing node also delivers via
//!   the round-trip, so all nodes see an identical ordered stream.
//! - **History:** list `centrifugo.hist.list.{channel}` (last N publications) +
//!   meta hash `centrifugo.hist.meta.{channel}` (`offset`, `epoch`). Appended
//!   atomically by a Lua script (HINCRBY offset, RPUSH, LTRIM, PEXPIRE). Each
//!   element's absolute offset is derived from its position relative to the top
//!   offset, so payloads need not embed it (centrifuge's list approach).
//! - **Presence:** data hash `centrifugo.presence.data.{channel}` (clientID →
//!   ClientInfo) + expiry zset `centrifugo.presence.exp.{channel}` (clientID →
//!   expire-at ms). Add/read are atomic Lua: add HSET+ZADD+PEXPIRE, read prunes
//!   entries whose score has passed (crashed-node cleanup) then returns the hash.
//!   The per-connection presence timer re-asserts entries before they expire.
//!
//! The wire contract is unchanged — Redis is internal — so the single-node
//! goldens already pin the client-facing bytes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use centrifugo_core::engine::{ControlMessage, Engine, NodeMessage, PublishOptions};
use centrifugo_core::node::StreamPosition;
use centrifugo_core::RouteFn;
use centrifugo_protocol::messages::{ClientInfo, Publication};
use centrifugo_protocol::{pb, Raw};
use futures_util::StreamExt;
use prost::Message as _;
use redis::aio::ConnectionManager;
use redis::sentinel::Sentinel;
use tokio::sync::{Mutex, RwLock};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

const PREFIX: &str = "centrifugo";

/// ClientInfo as carried over Redis (bytes instead of inline-JSON `Raw`).
#[derive(Default, Serialize, Deserialize)]
struct WireInfo {
    user: String,
    client: String,
    conn_info: Vec<u8>,
    chan_info: Vec<u8>,
}

impl From<ClientInfo> for WireInfo {
    fn from(ci: ClientInfo) -> Self {
        WireInfo {
            user: ci.user,
            client: ci.client,
            conn_info: ci.conn_info.map(|r| r.as_bytes().to_vec()).unwrap_or_default(),
            chan_info: ci.chan_info.map(|r| r.as_bytes().to_vec()).unwrap_or_default(),
        }
    }
}

impl From<WireInfo> for ClientInfo {
    fn from(w: WireInfo) -> Self {
        let opt = |b: Vec<u8>| if b.is_empty() { None } else { Some(Raw::from_bytes(b)) };
        ClientInfo {
            user: w.user,
            client: w.client,
            conn_info: opt(w.conn_info),
            chan_info: opt(w.chan_info),
        }
    }
}

/// A publication's payload as stored in history / carried over pub/sub.
#[derive(Default, Serialize, Deserialize)]
struct PubData {
    data: Vec<u8>,
    info: Option<WireInfo>,
}

impl PubData {
    fn into_publication(self, offset: u64) -> Publication {
        Publication {
            data: Some(Raw::from_bytes(self.data)),
            info: self.info.map(Into::into),
            offset,
            ..Default::default()
        }
    }
}

// ---- centrifuge-compatible pub/sub wire format (Go interop) ----
//
// Live messages on `<prefix>.client.<ch>` use centrifuge v0.14.2's framing so a
// Go centrifugo node and a Rust node can share one Redis for live fan-out:
//   - Publication: raw protobuf `Publication` bytes (a `__<offset>__` prefix when
//     history-tracked; we emit live messages without it, offset 0).
//   - Join:  `__j__` + protobuf `ClientInfo`.
//   - Leave: `__l__` + protobuf `ClientInfo`.
// (History/presence/control remain Rust-native — see the module note.)

fn to_pb_info(ci: &ClientInfo) -> pb::ClientInfo {
    pb::ClientInfo {
        user: ci.user.clone(),
        client: ci.client.clone(),
        conn_info: ci.conn_info.as_ref().map(|r| r.as_bytes().to_vec()).unwrap_or_default(),
        chan_info: ci.chan_info.as_ref().map(|r| r.as_bytes().to_vec()).unwrap_or_default(),
    }
}

fn from_pb_info(pi: pb::ClientInfo) -> ClientInfo {
    let opt = |b: Vec<u8>| if b.is_empty() { None } else { Some(Raw::from_bytes(b)) };
    ClientInfo {
        user: pi.user,
        client: pi.client,
        conn_info: opt(pi.conn_info),
        chan_info: opt(pi.chan_info),
    }
}

/// centrifuge `extractPushData`: a `__`-prefixed frame carries a join (`j`),
/// leave (`l`), or offset marker; otherwise the bytes are a raw Publication.
/// Returns `(kind, offset, body)` with kind 0=pub, 1=join, 2=leave.
fn extract_push(data: &[u8]) -> (u8, u64, &[u8]) {
    if let Some(rest) = data.strip_prefix(b"__") {
        if let Some(pos) = rest.windows(2).position(|w| w == b"__") {
            let marker = &rest[..pos];
            let body = &rest[pos + 2..];
            return match marker {
                b"j" => (1, 0, body),
                b"l" => (2, 0, body),
                _ => {
                    let offset = std::str::from_utf8(marker)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    (0, offset, body)
                }
            };
        }
    }
    (0, 0, data)
}

/// Atomic history append: HINCRBY offset, set epoch if absent, RPUSH+LTRIM, then
/// expire ONLY the list with the history lifetime. KEYS[1]=meta KEYS[2]=list;
/// ARGV: 1=payload 2=size 3=candidate_epoch 4=list_ttl_ms.
///
/// The meta hash (offset+epoch) is deliberately NOT expired with the list: Go
/// expires the publication list on `history_lifetime` but keeps the meta (top
/// offset + epoch) until the separate `redis_history_meta_ttl`, which defaults
/// to 0 (never). Expiring the meta with the list would reset epoch/offset and
/// make a caught-up client recover=false after an idle window.
const HIST_ADD: &str = r#"
local offset = redis.call('hincrby', KEYS[1], 'offset', 1)
local epoch = redis.call('hget', KEYS[1], 'epoch')
if not epoch or epoch == false then
  epoch = ARGV[3]
  redis.call('hset', KEYS[1], 'epoch', epoch)
end
redis.call('rpush', KEYS[2], ARGV[1])
local size = tonumber(ARGV[2])
redis.call('ltrim', KEYS[2], -size, -1)
local ttl = tonumber(ARGV[4])
redis.call('pexpire', KEYS[2], ttl)
return {offset, epoch}
"#;

/// Presence add: HSET data + ZADD exp + PEXPIRE both. KEYS[1]=data KEYS[2]=exp;
/// ARGV: 1=clientID 2=info 3=expire_at_ms 4=ttl_ms.
const PRESENCE_ADD: &str = r#"
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
redis.call('zadd', KEYS[2], ARGV[3], ARGV[1])
local ttl = tonumber(ARGV[4])
redis.call('pexpire', KEYS[1], ttl)
redis.call('pexpire', KEYS[2], ttl)
"#;

/// Presence read: prune entries whose exp score <= now, then return the data
/// hash. KEYS[1]=exp zset KEYS[2]=data hash; ARGV[1]=now_ms.
const PRESENCE_READ: &str = r#"
local expired = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1])
for i=1,#expired do
  redis.call('zrem', KEYS[1], expired[i])
  redis.call('hdel', KEYS[2], expired[i])
end
return redis.call('hgetall', KEYS[2])
"#;

pub struct RedisEngine {
    /// Swappable so the Sentinel watchdog can repoint commands at a new master.
    mgr: Arc<RwLock<ConnectionManager>>,
}

/// Subscribe a fresh pub/sub connection to the message pattern + control channel.
/// Uses centrifuge's `<prefix>.client.*` so a Go node's publications are received.
async fn subscribe(client: &redis::Client) -> anyhow::Result<redis::aio::PubSub> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.psubscribe(format!("{PREFIX}.client.*")).await?;
    // Rust-native control channel (distinct from Go's `<prefix>.control`, whose
    // protobuf control protocol is not interop-supported).
    pubsub.subscribe(format!("{PREFIX}.control.rust")).await?;
    Ok(pubsub)
}

/// Route one pub/sub message (centrifuge-framed publication/join/leave or a
/// Rust cross-node control command).
fn dispatch_message(msg: &redis::Msg, route: &RouteFn) {
    let topic = msg.get_channel_name();
    let payload: Vec<u8> = match msg.get_payload() {
        Ok(p) => p,
        Err(_) => return,
    };
    if topic == format!("{PREFIX}.control.rust") {
        match serde_json::from_slice::<ControlMessage>(&payload) {
            Ok(cmd) => route(NodeMessage::Control(cmd)),
            Err(e) => tracing::warn!("redis control decode: {e}"),
        }
        return;
    }
    let Some(channel) = topic.strip_prefix(&format!("{PREFIX}.client.")) else {
        return;
    };
    let channel = channel.to_string();
    let (kind, offset, body) = extract_push(&payload);
    match kind {
        1 => match pb::ClientInfo::decode(body) {
            Ok(ci) => route(NodeMessage::Join {
                channel,
                info: from_pb_info(ci),
            }),
            Err(e) => tracing::warn!("redis join decode {channel}: {e}"),
        },
        2 => match pb::ClientInfo::decode(body) {
            Ok(ci) => route(NodeMessage::Leave {
                channel,
                info: from_pb_info(ci),
            }),
            Err(e) => tracing::warn!("redis leave decode {channel}: {e}"),
        },
        _ => match pb::Publication::decode(body) {
            Ok(p) => route(NodeMessage::Publication {
                channel,
                publication: Publication {
                    data: Some(Raw::from_bytes(p.data)),
                    info: p.info.map(from_pb_info),
                    offset,
                    ..Default::default()
                },
            }),
            Err(e) => tracing::warn!("redis pub decode {channel}: {e}"),
        },
    }
}

/// Drain a pub/sub connection until it ends (a disconnect), routing each message.
async fn run_pubsub(pubsub: redis::aio::PubSub, route: RouteFn) {
    let mut stream = pubsub.into_on_message();
    while let Some(msg) = stream.next().await {
        dispatch_message(&msg, &route);
    }
}

impl RedisEngine {
    /// Connect to Redis at `addr` (`host:port` or a full `redis://` URL) and spawn
    /// the pub/sub subscriber task that routes incoming messages via `route`.
    pub async fn connect(addr: &str, route: RouteFn) -> anyhow::Result<Self> {
        let url = if addr.contains("://") {
            addr.to_string()
        } else {
            format!("redis://{addr}")
        };
        let client = redis::Client::open(url)?;
        let mgr = Arc::new(RwLock::new(client.get_connection_manager().await?));
        let pubsub = subscribe(&client).await?;
        tokio::spawn(run_pubsub(pubsub, route));
        Ok(RedisEngine { mgr })
    }

    /// Connect via Redis Sentinel. The master is resolved at startup, and a
    /// watchdog task re-resolves it via Sentinel on every pub/sub disconnect —
    /// rebuilding the pub/sub subscription AND swapping the command manager — so a
    /// mid-flight failover is handled without a restart.
    pub async fn connect_sentinel(
        master_name: &str,
        sentinels: &str,
        route: RouteFn,
    ) -> anyhow::Result<Self> {
        let mut addrs: Vec<String> = Vec::new();
        for s in sentinels.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            if s.contains("://") {
                addrs.push(s.to_string());
            } else {
                // Go validates each Sentinel address with net.SplitHostPort and
                // fails fast on a malformed (e.g. portless) entry; mirror that.
                if s.rsplit_once(':').map(|(h, p)| h.is_empty() || p.is_empty()) != Some(false) {
                    anyhow::bail!("malformed Sentinel address (want host:port): {s}");
                }
                addrs.push(format!("redis://{s}"));
            }
        }
        if addrs.is_empty() {
            anyhow::bail!("no Sentinel addresses configured");
        }
        let mut sentinel = Sentinel::build(addrs)?;
        let client = sentinel.async_master_for(master_name, None).await?;
        let mgr = Arc::new(RwLock::new(client.get_connection_manager().await?));
        // Subscribe synchronously before returning so an immediate publish isn't
        // missed (no startup race); the watchdog re-subscribes on disconnect.
        let pubsub = subscribe(&client).await?;

        let sentinel = Arc::new(Mutex::new(sentinel));
        let master = master_name.to_string();
        let mgr_watch = mgr.clone();
        tokio::spawn(async move {
            // First run uses the already-subscribed connection.
            run_pubsub(pubsub, route.clone()).await;
            // On disconnect (e.g. master failover), re-resolve via Sentinel, rebuild
            // the pub/sub subscription, and repoint the command manager.
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                match sentinel.lock().await.async_master_for(&master, None).await {
                    Ok(client) => {
                        match client.get_connection_manager().await {
                            Ok(m) => *mgr_watch.write().await = m,
                            Err(e) => tracing::warn!("sentinel manager rebuild: {e}"),
                        }
                        match subscribe(&client).await {
                            Ok(pubsub) => run_pubsub(pubsub, route.clone()).await,
                            Err(e) => tracing::warn!("sentinel subscribe: {e}"),
                        }
                    }
                    Err(e) => tracing::warn!("sentinel resolve master {master}: {e}"),
                }
            }
        });
        Ok(RedisEngine { mgr })
    }

    fn client_key(channel: &str) -> String {
        format!("{PREFIX}.client.{channel}")
    }
    fn meta_key(channel: &str) -> String {
        format!("{PREFIX}.hist.meta.{channel}")
    }
    fn list_key(channel: &str) -> String {
        format!("{PREFIX}.hist.list.{channel}")
    }
    fn presence_data_key(channel: &str) -> String {
        format!("{PREFIX}.presence.data.{channel}")
    }
    fn presence_exp_key(channel: &str) -> String {
        format!("{PREFIX}.presence.exp.{channel}")
    }

    /// Publish raw framed bytes to a channel's centrifuge message channel.
    async fn publish_frame(&self, channel: &str, payload: Vec<u8>) -> anyhow::Result<()> {
        let mut conn = self.mgr.read().await.clone();
        let _: () = conn.publish(Self::client_key(channel), payload).await?;
        Ok(())
    }

    /// Read the full retained history + top position, ensuring the stream has a
    /// stable epoch (created lazily, like the memory engine).
    async fn read_history(&self, channel: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)> {
        let mut conn = self.mgr.read().await.clone();
        let meta_key = Self::meta_key(channel);
        // Ensure an epoch exists so recovery against an empty stream is stable.
        let candidate = new_epoch();
        let _: () = redis::cmd("HSETNX")
            .arg(&meta_key)
            .arg("epoch")
            .arg(&candidate)
            .query_async(&mut conn)
            .await?;
        let meta: HashMap<String, String> = conn.hgetall(&meta_key).await?;
        let top_offset: u64 = meta.get("offset").and_then(|s| s.parse().ok()).unwrap_or(0);
        let epoch = meta.get("epoch").cloned().unwrap_or(candidate);

        let list: Vec<Vec<u8>> = conn.lrange(Self::list_key(channel), 0, -1).await?;
        let len = list.len() as u64;
        // Each element's absolute offset is its distance from the tail (newest ==
        // top_offset). `saturating_sub` guards the pathological case where the
        // meta and list desynced (e.g. independent key eviction) and top_offset <
        // len — better a clamped offset than an integer underflow panic.
        let pubs = list
            .into_iter()
            .enumerate()
            .map(|(i, raw)| {
                let pd: PubData = serde_json::from_slice(&raw).unwrap_or_default();
                let offset = top_offset.saturating_sub(len - 1 - i as u64);
                pd.into_publication(offset)
            })
            .collect();
        Ok((pubs, StreamPosition { offset: top_offset, epoch }))
    }
}

#[async_trait]
impl Engine for RedisEngine {
    async fn publish(
        &self,
        channel: &str,
        data: &[u8],
        info: Option<ClientInfo>,
        opts: PublishOptions,
    ) -> anyhow::Result<()> {
        if opts.history_enabled() {
            let entry = serde_json::to_vec(&PubData {
                data: data.to_vec(),
                info: info.clone().map(Into::into),
            })?;
            let ttl_ms = (opts.history_lifetime as i64) * 1000;
            let mut conn = self.mgr.read().await.clone();
            let _: redis::Value = redis::Script::new(HIST_ADD)
                .key(Self::meta_key(channel))
                .key(Self::list_key(channel))
                .arg(entry)
                .arg(opts.history_size)
                .arg(new_epoch())
                .arg(ttl_ms)
                .invoke_async(&mut conn)
                .await?;
        }
        // Live publication: raw protobuf Publication (offset 0, position carried via
        // history) — centrifuge-compatible so a Go node receives it.
        let pub_pb = pb::Publication {
            data: data.to_vec(),
            info: info.as_ref().map(to_pb_info),
            ..Default::default()
        };
        self.publish_frame(channel, pub_pb.encode_to_vec()).await
    }

    async fn publish_control(&self, msg: ControlMessage) -> anyhow::Result<()> {
        // Rust-native control channel; every Rust node (incl. this one, via its own
        // subscriber) applies it.
        let payload = serde_json::to_vec(&msg)?;
        let mut conn = self.mgr.read().await.clone();
        let _: () = conn.publish(format!("{PREFIX}.control.rust"), payload).await?;
        Ok(())
    }

    async fn publish_join(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()> {
        // centrifuge join frame: `__j__` + protobuf ClientInfo.
        let mut frame = b"__j__".to_vec();
        frame.extend_from_slice(&to_pb_info(&info).encode_to_vec());
        self.publish_frame(channel, frame).await
    }

    async fn publish_leave(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()> {
        // centrifuge leave frame: `__l__` + protobuf ClientInfo.
        let mut frame = b"__l__".to_vec();
        frame.extend_from_slice(&to_pb_info(&info).encode_to_vec());
        self.publish_frame(channel, frame).await
    }

    async fn subscribe(&self, _channel: &str) -> anyhow::Result<()> {
        // Pattern subscription covers all channels; the local hub filters.
        Ok(())
    }

    async fn unsubscribe(&self, _channel: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn history(&self, channel: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)> {
        self.read_history(channel).await
    }

    async fn history_since(
        &self,
        channel: &str,
        offset: u64,
        epoch: &str,
    ) -> anyhow::Result<(Vec<Publication>, StreamPosition)> {
        let (all, top) = self.read_history(channel).await?;
        if top.offset == offset && top.epoch == epoch {
            return Ok((Vec::new(), top));
        }
        let pubs = all.into_iter().filter(|p| p.offset > offset).collect();
        Ok((pubs, top))
    }

    async fn remove_history(&self, channel: &str) -> anyhow::Result<()> {
        let mut conn = self.mgr.read().await.clone();
        let _: () = conn
            .del(&[Self::meta_key(channel), Self::list_key(channel)])
            .await?;
        Ok(())
    }

    async fn add_presence(
        &self,
        channel: &str,
        client_id: &str,
        info: ClientInfo,
        ttl_ms: u64,
    ) -> anyhow::Result<()> {
        // Go passes the configured presence TTL straight through (centrifugo's
        // own default of 60s is applied at the config layer, not here).
        let ttl = ttl_ms;
        let payload = serde_json::to_vec(&WireInfo::from(info))?;
        let expire_at = now_ms() + ttl;
        let mut conn = self.mgr.read().await.clone();
        let _: () = redis::Script::new(PRESENCE_ADD)
            .key(Self::presence_data_key(channel))
            .key(Self::presence_exp_key(channel))
            .arg(client_id)
            .arg(payload)
            .arg(expire_at)
            .arg(ttl)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn remove_presence(&self, channel: &str, client_id: &str) -> anyhow::Result<()> {
        let mut conn = self.mgr.read().await.clone();
        let _: () = conn
            .hdel(Self::presence_data_key(channel), client_id)
            .await?;
        let _: () = conn
            .zrem(Self::presence_exp_key(channel), client_id)
            .await?;
        Ok(())
    }

    async fn presence(&self, channel: &str) -> anyhow::Result<HashMap<String, ClientInfo>> {
        let mut conn = self.mgr.read().await.clone();
        // Prune expired entries (by the exp zset) then read the survivors,
        // atomically. Returns a flat [field, value, field, value, ...] array.
        let flat: Vec<Vec<u8>> = redis::Script::new(PRESENCE_READ)
            .key(Self::presence_exp_key(channel))
            .key(Self::presence_data_key(channel))
            .arg(now_ms())
            .invoke_async(&mut conn)
            .await?;
        let mut out = HashMap::new();
        let mut it = flat.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if let (Ok(client), Ok(w)) =
                (String::from_utf8(k), serde_json::from_slice::<WireInfo>(&v))
            {
                out.insert(client, w.into());
            }
        }
        Ok(out)
    }

    async fn presence_stats(&self, channel: &str) -> anyhow::Result<(u32, u32)> {
        let presence = self.presence(channel).await?;
        let num_clients = presence.len() as u32;
        let users: std::collections::HashSet<&str> =
            presence.values().map(|ci| ci.user.as_str()).collect();
        Ok((num_clients, users.len() as u32))
    }
}

/// Current unix time in milliseconds (presence expiry scores).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Opaque per-stream epoch token (stability + change-on-recreate matter).
fn new_epoch() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..10].to_string()
}
