//! The `Node` ties the `Hub` and an `Engine` together and owns the local
//! publication fan-out. A publication is encoded **once** per protocol and the
//! resulting frame bytes are cloned + `try_send`'d to each subscriber's bounded
//! queue; a full (slow) or closed queue causes that client to be dropped, never
//! blocking the broadcaster. The engine (memory or Redis) decides where a
//! publication comes from; it calls back through the installed [`RouteFn`] to
//! reach this node's local subscribers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use centrifugo_auth::TokenVerifier;
use centrifugo_protocol::codec::{self, ProtocolType, WireType};
use centrifugo_protocol::messages::{ClientInfo, Join, Leave, Publication};
use centrifugo_protocol::{Disconnect, Push, PushType};
use serde::Serialize;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

use crate::client::Client;
use crate::engine::{ControlMessage, Engine, NodeInfoData, NodeMessage, PublishOptions, RouteFn};
use crate::hub::{Hub, Out, Signal};
use crate::memory::MemoryEngine;
use crate::proxy::Proxies;

/// Channel options. Resolved per-channel from the namespace registry.
#[derive(Debug, Clone, Default)]
pub struct ChannelOptions {
    pub presence: bool,
    pub join_leave: bool,
    pub presence_disable_for_client: bool,
    /// Max publications kept in a channel's history (0 = history disabled).
    pub history_size: usize,
    /// History retention in seconds (0 = history disabled).
    pub history_lifetime: u64,
    /// Whether (re)subscribe recovery is offered on channels.
    pub history_recover: bool,
    /// Allow anonymous (empty-user) clients to subscribe.
    pub anonymous: bool,
    /// Server-side-only channel: clients may not subscribe directly.
    pub server_side: bool,
    /// Proxy SUBSCRIBE on this channel to the subscribe-proxy endpoint.
    pub proxy_subscribe: bool,
    /// Proxy PUBLISH on this channel to the publish-proxy endpoint.
    pub proxy_publish: bool,
    /// Allow clients to publish to this channel (`publish` option). Without it
    /// (and not insecure) a client PUBLISH is PermissionDenied.
    pub publish: bool,
    /// Require the publisher to be subscribed to the channel (`subscribe_to_publish`).
    pub subscribe_to_publish: bool,
}

impl ChannelOptions {
    /// History is active only when both size and lifetime are positive (matches Go).
    pub fn history_enabled(&self) -> bool {
        self.history_size > 0 && self.history_lifetime > 0
    }

    /// Per-publish history directives for this channel.
    pub fn publish_options(&self) -> PublishOptions {
        if self.history_enabled() {
            PublishOptions {
                history_size: self.history_size,
                history_lifetime: self.history_lifetime,
            }
        } else {
            PublishOptions::default()
        }
    }
}

/// Namespace registry: default (top-level) options plus named namespaces.
/// A channel `ns:rest` (after stripping the private prefix) resolves to namespace
/// `ns`; a channel without the boundary resolves to the default options.
#[derive(Debug, Clone)]
pub struct Namespaces {
    pub default: ChannelOptions,
    pub namespaces: HashMap<String, ChannelOptions>,
    pub namespace_boundary: String,
    pub private_prefix: String,
    /// Auto-subscribe non-anonymous clients to their personal channel on connect.
    pub user_subscribe_to_personal: bool,
    /// Namespace for the personal channel (empty = top-level `#<user>`).
    pub user_personal_channel_namespace: String,
}

impl Default for Namespaces {
    fn default() -> Self {
        Namespaces {
            default: ChannelOptions::default(),
            namespaces: HashMap::new(),
            namespace_boundary: ":".into(),
            private_prefix: "$".into(),
            user_subscribe_to_personal: false,
            user_personal_channel_namespace: String::new(),
        }
    }
}

impl Namespaces {
    /// Resolve channel options for `channel`. `None` means the channel names a
    /// namespace that does not exist (→ UnknownChannel).
    pub fn channel_options(&self, channel: &str) -> Option<&ChannelOptions> {
        let trimmed = channel
            .strip_prefix(&self.private_prefix)
            .unwrap_or(channel);
        if !self.namespace_boundary.is_empty() {
            if let Some((ns, _)) = trimmed.split_once(&self.namespace_boundary) {
                return self.namespaces.get(ns);
            }
        }
        Some(&self.default)
    }

    /// Whether `channel` is a private (token-protected) channel.
    pub fn is_private(&self, channel: &str) -> bool {
        !self.private_prefix.is_empty() && channel.starts_with(&self.private_prefix)
    }

    /// The personal channel for `user` (Go `PersonalChannel`): `#<user>`, or
    /// `<namespace>:#<user>` when a personal-channel namespace is configured.
    /// `None` when personal subscriptions are disabled or `user` is empty.
    pub fn personal_channel(&self, user: &str) -> Option<String> {
        if !self.user_subscribe_to_personal || user.is_empty() {
            return None;
        }
        // ChannelUserBoundary is "#".
        if self.user_personal_channel_namespace.is_empty() {
            Some(format!("#{user}"))
        } else {
            Some(format!(
                "{}{}#{user}",
                self.user_personal_channel_namespace, self.namespace_boundary
            ))
        }
    }
}

/// Current top of a channel's history stream.
#[derive(Debug, Clone, Default)]
pub struct StreamPosition {
    pub offset: u64,
    pub epoch: String,
}

/// Opaque per-stream token; only stability + change-on-recreate matter.
pub(crate) fn new_epoch() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..10].to_string()
}

pub(crate) fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Registry of known cluster nodes — this node plus remotes (Go or Rust) learned
/// from periodic NODE-info control pings. Keyed by node uid with a last-seen
/// timestamp so stale nodes are pruned (mirrors centrifuge's `nodeRegistry`).
pub struct NodeRegistry {
    self_uid: String,
    nodes: parking_lot::Mutex<HashMap<String, (NodeInfoData, i64)>>,
}

impl NodeRegistry {
    pub fn new(self_uid: String) -> Self {
        NodeRegistry {
            self_uid,
            nodes: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// This node's own uid.
    pub fn self_uid(&self) -> &str {
        &self.self_uid
    }

    /// Record (or refresh) a node's info from a NODE ping.
    pub fn add(&self, info: NodeInfoData) {
        let now = now_ms();
        self.nodes.lock().insert(info.uid.clone(), (info, now));
    }

    /// All currently-known nodes.
    pub fn list(&self) -> Vec<NodeInfoData> {
        self.nodes.lock().values().map(|(i, _)| i.clone()).collect()
    }

    /// Drop remote nodes not seen within `max_age_ms` (this node is kept always).
    pub fn clean(&self, max_age_ms: i64) {
        let now = now_ms();
        let self_uid = &self.self_uid;
        self.nodes
            .lock()
            .retain(|uid, (_, seen)| uid == self_uid || now - *seen <= max_age_ms);
    }
}

pub struct Node {
    /// Stable per-process node id (Go node UID); reported by the Info API.
    id: String,
    /// Reported in this node's NODE pings (Go `config.Version`).
    version: String,
    /// Unix seconds at node creation; Info `uptime` is derived from it.
    started_unix: i64,
    /// Known cluster nodes (this node + remotes via NODE pings); backs the Info API.
    registry: Arc<NodeRegistry>,
    /// Prometheus-style counters (commands, messages sent, connects).
    metrics: Arc<crate::metrics::Metrics>,
    hub: Arc<Hub>,
    engine: Arc<dyn Engine>,
    verifier: Arc<TokenVerifier>,
    client_insecure: bool,
    /// Global connect-time anonymous access (Go `client_anonymous`): allow a
    /// tokenless connection with an empty user id (distinct from the per-channel
    /// `anonymous` subscribe option).
    client_anonymous: bool,
    namespaces: Namespaces,
    /// Configured event proxies (connect/refresh/subscribe/publish/rpc).
    proxies: Proxies,
    /// How often a connection re-asserts its presence (Go
    /// `client_presence_ping_interval`).
    presence_ping_interval: Duration,
    /// Presence entry TTL passed to the engine (Go
    /// `client_presence_expire_interval`); the memory engine ignores it.
    presence_expire_ms: u64,
    /// Use seq/gen instead of offset on the wire (centrifugo v2.8.6 default:
    /// config `v3_use_offset=false`). Real SDKs of this era expect seq/gen.
    use_seq_gen: bool,
}

/// centrifugo v2.8.6 default for the `UseSeqGen` compatibility flag (`v3_use_offset`
/// defaults to false). Single source of truth for both the [`Node`] and the route
/// callback ([`make_route`]); we do not expose `v3_use_offset` config.
pub const DEFAULT_USE_SEQ_GEN: bool = true;

impl Node {
    /// Build a node from a pre-constructed hub + engine (used when the engine is
    /// built asynchronously, e.g. the Redis engine). Pair with [`make_route`].
    /// `connect_proxy` enables proxy-based connect authentication when `Some`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_engine(
        hub: Arc<Hub>,
        engine: Arc<dyn Engine>,
        verifier: Arc<TokenVerifier>,
        client_insecure: bool,
        client_anonymous: bool,
        namespaces: Namespaces,
        proxies: Proxies,
        presence_ping_secs: u64,
        presence_expire_secs: u64,
        registry: Arc<NodeRegistry>,
        version: String,
    ) -> Arc<Self> {
        let id = registry.self_uid().to_string();
        // Seed our own registry entry so the Info API always lists this node, even
        // before the first NODE ping (counts are refreshed by the ping + at query).
        registry.add(NodeInfoData {
            uid: id.clone(),
            name: id.clone(),
            version: version.clone(),
            ..Default::default()
        });
        Arc::new(Node {
            id,
            version,
            started_unix: now_unix(),
            registry,
            metrics: Arc::new(crate::metrics::Metrics::new()),
            hub,
            engine,
            verifier,
            client_insecure,
            client_anonymous,
            namespaces,
            proxies,
            presence_ping_interval: Duration::from_secs(presence_ping_secs),
            presence_expire_ms: presence_expire_secs * 1000,
            use_seq_gen: DEFAULT_USE_SEQ_GEN,
        })
    }

    /// Build a single-node memory node with the given verifier, insecure flag,
    /// and namespaces (Go default presence intervals).
    pub fn new_with(
        verifier: Arc<TokenVerifier>,
        client_insecure: bool,
        namespaces: Namespaces,
    ) -> Arc<Self> {
        let hub = Arc::new(Hub::new());
        let registry = Arc::new(NodeRegistry::new(uuid::Uuid::new_v4().to_string()));
        let engine: Arc<dyn Engine> =
            Arc::new(MemoryEngine::new(make_route(&hub, &registry, DEFAULT_USE_SEQ_GEN)));
        Self::new_with_engine(
            hub,
            engine,
            verifier,
            client_insecure,
            false,
            namespaces,
            Proxies::default(),
            25,
            60,
            registry,
            "2.8.6".into(),
        )
    }

    /// Build an insecure single-node memory node (no token, no presence). Used
    /// by tests and the `--client_insecure` server mode default.
    pub fn new() -> Arc<Self> {
        Self::new_with(
            Arc::new(TokenVerifier::default()),
            true,
            Namespaces::default(),
        )
    }

    /// Stable per-process node id (Go node UID).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Seconds since this node started (Info `uptime`).
    pub fn uptime(&self) -> u32 {
        (now_unix() - self.started_unix).max(0) as u32
    }

    /// The Prometheus metrics registry.
    pub fn metrics(&self) -> &Arc<crate::metrics::Metrics> {
        &self.metrics
    }

    /// All known cluster nodes (this node + remotes) for the Info API. This node's
    /// entry is refreshed with live counts at query time; remotes are as last pinged.
    pub fn info_nodes(&self) -> Vec<NodeInfoData> {
        let mut nodes = self.registry.list();
        if let Some(me) = nodes.iter_mut().find(|n| n.uid == self.id) {
            me.num_clients = self.hub.num_clients() as u32;
            me.num_users = self.hub.num_users() as u32;
            me.num_channels = self.hub.num_channels() as u32;
            me.uptime = self.uptime();
        }
        nodes
    }

    /// This node's current info for a NODE ping.
    fn self_node_info(&self) -> NodeInfoData {
        NodeInfoData {
            uid: self.id.clone(),
            name: self.id.clone(),
            version: self.version.clone(),
            num_clients: self.hub.num_clients() as u32,
            num_users: self.hub.num_users() as u32,
            num_channels: self.hub.num_channels() as u32,
            uptime: self.uptime(),
        }
    }

    /// Spawn the cluster-membership tasks: publish a NODE info ping every 3s
    /// (Go `nodeInfoPublishInterval`) so other nodes register this one, and prune
    /// remotes not seen within 7s (`nodeInfoMaxDelay`) every 9s. Server-only.
    pub fn spawn_node_pings(self: &Arc<Self>) {
        let node = self.clone();
        tokio::spawn(async move {
            let mut ping = tokio::time::interval(Duration::from_secs(3));
            let mut clean = tokio::time::interval(Duration::from_secs(9));
            loop {
                tokio::select! {
                    _ = ping.tick() => {
                        let info = node.self_node_info();
                        let _ = node.engine.publish_control(ControlMessage::Node(info)).await;
                    }
                    _ = clean.tick() => node.registry.clean(7000),
                }
            }
        });
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    pub fn verifier(&self) -> &TokenVerifier {
        &self.verifier
    }

    pub fn client_insecure(&self) -> bool {
        self.client_insecure
    }

    pub fn client_anonymous(&self) -> bool {
        self.client_anonymous
    }

    /// The configured event proxies.
    pub fn proxies(&self) -> &Proxies {
        &self.proxies
    }

    /// Channel options for `channel`, or `None` if it names an unknown namespace.
    pub fn channel_options(&self, channel: &str) -> Option<&ChannelOptions> {
        self.namespaces.channel_options(channel)
    }

    /// Whether `channel` is private (token-protected, `$`-prefixed).
    pub fn is_private(&self, channel: &str) -> bool {
        self.namespaces.is_private(channel)
    }

    /// The personal channel to auto-subscribe `user` to on connect, if enabled.
    pub fn personal_channel(&self, user: &str) -> Option<String> {
        self.namespaces.personal_channel(user)
    }

    pub fn use_seq_gen(&self) -> bool {
        self.use_seq_gen
    }

    /// How often a connection should re-assert its presence.
    pub fn presence_ping_interval(&self) -> Duration {
        self.presence_ping_interval
    }

    // ---- presence ----

    pub async fn add_presence(&self, channel: &str, client_id: &str, info: ClientInfo) {
        if let Err(e) = self
            .engine
            .add_presence(channel, client_id, info, self.presence_expire_ms)
            .await
        {
            tracing::warn!("add_presence({channel}): {e}");
        }
    }

    pub async fn remove_presence(&self, channel: &str, client_id: &str) {
        if let Err(e) = self.engine.remove_presence(channel, client_id).await {
            tracing::warn!("remove_presence({channel}): {e}");
        }
    }

    pub async fn presence(&self, channel: &str) -> HashMap<String, ClientInfo> {
        match self.engine.presence(channel).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("presence({channel}): {e}");
                HashMap::new()
            }
        }
    }

    /// (num_clients, num_users): total entries and distinct user ids.
    pub async fn presence_stats(&self, channel: &str) -> (u32, u32) {
        match self.engine.presence_stats(channel).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("presence_stats({channel}): {e}");
                (0, 0)
            }
        }
    }

    // ---- join / leave ----

    pub async fn publish_join(&self, channel: &str, info: ClientInfo) {
        self.metrics.inc_message_sent(1);
        if let Err(e) = self.engine.publish_join(channel, info).await {
            tracing::warn!("publish_join({channel}): {e}");
        }
    }

    pub async fn publish_leave(&self, channel: &str, info: ClientInfo) {
        self.metrics.inc_message_sent(2);
        if let Err(e) = self.engine.publish_leave(channel, info).await {
            tracing::warn!("publish_leave({channel}): {e}");
        }
    }

    // ---- publish + history ----

    /// Publish to a channel: the engine appends to history (when enabled for the
    /// channel) assigning an offset, then fans out the live publication.
    pub async fn publish(&self, channel: &str, data: &[u8], info: Option<ClientInfo>) {
        self.metrics.inc_message_sent(0);
        let opts = self
            .namespaces
            .channel_options(channel)
            .map(|o| o.publish_options())
            .unwrap_or_default();
        if let Err(e) = self.engine.publish(channel, data, info, opts).await {
            tracing::warn!("publish({channel}): {e}");
        }
    }

    /// Full history (all retained publications) + current top position.
    pub async fn history(&self, channel: &str) -> (Vec<Publication>, StreamPosition) {
        match self.engine.history(channel).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("history({channel}): {e}");
                (Vec::new(), StreamPosition::default())
            }
        }
    }

    /// Publications after `since_offset` + current top position (recovery).
    pub async fn history_since(
        &self,
        channel: &str,
        since_offset: u64,
        since_epoch: &str,
    ) -> (Vec<Publication>, StreamPosition) {
        match self
            .engine
            .history_since(channel, since_offset, since_epoch)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("history_since({channel}): {e}");
                (Vec::new(), StreamPosition::default())
            }
        }
    }

    pub async fn remove_history(&self, channel: &str) {
        if let Err(e) = self.engine.remove_history(channel).await {
            tracing::warn!("remove_history({channel}): {e}");
        }
    }

    /// Create a per-connection client bound to this node, writing to `tx`.
    pub fn new_client(self: &Arc<Self>, tx: Sender<Out>, proto: ProtocolType) -> Client {
        Client::new(self.clone(), tx, proto)
    }

    /// Remove a connection (on socket close).
    pub fn remove(&self, id: &str) {
        self.hub.remove(id);
    }

    // ---- server-side client management (API unsubscribe / disconnect) ----

    /// Unsubscribe all of `user`'s connections from `channel` (empty channel = all
    /// channels), cluster-wide. Each affected client gets an Unsubscribe push.
    pub async fn unsubscribe_user(&self, user: &str, channel: &str) {
        if let Err(e) = self
            .engine
            .publish_control(ControlMessage::Unsubscribe {
                user: user.to_string(),
                channel: channel.to_string(),
            })
            .await
        {
            tracing::warn!("unsubscribe_user({user}): {e}");
        }
    }

    /// Disconnect all of `user`'s connections (cluster-wide) with `code`/`reason`.
    pub async fn disconnect_user(&self, user: &str, code: u32, reason: &str) {
        if let Err(e) = self
            .engine
            .publish_control(ControlMessage::Disconnect {
                user: user.to_string(),
                code,
                reason: reason.to_string(),
                // The HTTP/gRPC disconnect API issues DisconnectForceNoReconnect
                // (3012) and exposes no whitelist; cross-node reconnect/whitelist
                // arrive via the decode path (a Go node may set them).
                reconnect: false,
                whitelist: Vec::new(),
            })
            .await
        {
            tracing::warn!("disconnect_user({user}): {e}");
        }
    }
}

/// Apply a cross-node control command to this node's local connections by
/// signalling each affected connection's reader task.
fn apply_control(hub: &Hub, registry: &NodeRegistry, cmd: ControlMessage) {
    match cmd {
        // NODE ping → record the (possibly remote, possibly Go) node in the registry.
        ControlMessage::Node(info) => registry.add(info),
        ControlMessage::Unsubscribe { user, channel } => {
            for h in hub.user_clients(&user) {
                if let Some(ctrl) = &h.ctrl {
                    let _ = ctrl.try_send(Signal::Unsubscribe(channel.clone()));
                }
            }
        }
        ControlMessage::Disconnect {
            user,
            code,
            reason,
            reconnect,
            whitelist,
        } => {
            for h in hub.user_clients(&user) {
                // Spare whitelisted connections (Go hub.go: stringInSlice(c.ID(),
                // whitelist) → continue). `h.id` is the centrifuge wire client-ID,
                // the same value a Go node places in the whitelist.
                if whitelist.contains(&h.id) {
                    continue;
                }
                if let Some(ctrl) = &h.ctrl {
                    let _ = ctrl.try_send(Signal::Disconnect(Disconnect::new(
                        code,
                        reason.clone(),
                        reconnect,
                    )));
                }
            }
        }
    }
}

/// Build the route callback that delivers engine [`NodeMessage`]s to this node's
/// local subscribers (and applies control commands). Both the memory and Redis
/// engines are constructed with it; the `registry` receives NODE pings.
///
/// `use_seq_gen` mirrors Go's `UseSeqGen` compatibility flag (the v2.8.6 default):
/// when set, a recoverable-channel live publication ships seq/gen unpacked from
/// its offset, with the offset zeroed — see [`route_message`].
pub fn make_route(hub: &Arc<Hub>, registry: &Arc<NodeRegistry>, use_seq_gen: bool) -> RouteFn {
    let hub = hub.clone();
    let registry = registry.clone();
    Arc::new(move |msg| route_message(&hub, &registry, use_seq_gen, msg))
}

/// Turn a [`NodeMessage`] into the matching push and fan it out locally.
fn route_message(hub: &Hub, registry: &NodeRegistry, use_seq_gen: bool, msg: NodeMessage) {
    match msg {
        NodeMessage::Publication {
            channel,
            mut publication,
        } => {
            // Go hub.go (broadcastPublication): with UseSeqGen — the v2.8.6 default —
            // a live publication on a recoverable channel carries seq/gen unpacked
            // from its offset, and the offset is zeroed before encoding. Non-recoverable
            // channels have offset 0, so there is nothing to convert.
            if use_seq_gen && publication.offset != 0 {
                let (seq, gen) = crate::client::unpack_offset(publication.offset);
                publication.seq = seq;
                publication.gen = gen;
                publication.offset = 0;
            }
            deliver_push(hub, &channel, PushType::Publication, &publication)
        }
        NodeMessage::Join { channel, info } => {
            deliver_push(hub, &channel, PushType::Join, &Join { info })
        }
        NodeMessage::Leave { channel, info } => {
            deliver_push(hub, &channel, PushType::Leave, &Leave { info })
        }
        NodeMessage::Control(cmd) => apply_control(hub, registry, cmd),
    }
}

/// Encode `inner` (a Publication/Join/Leave) into a push frame once per protocol
/// and fan it out to every subscriber of `channel`, sending each the frame
/// matching its protocol. A slow/full or gone consumer is dropped, never
/// blocking the broadcaster.
fn deliver_push<T: Serialize + WireType>(hub: &Hub, channel: &str, push_type: PushType, inner: &T) {
    let json_frame = make_push_frame(ProtocolType::Json, channel, push_type, inner);
    let pb_frame = make_push_frame(ProtocolType::Protobuf, channel, push_type, inner);

    for handle in hub.subscribers(channel) {
        let frame = match handle.proto {
            ProtocolType::Json => &json_frame,
            ProtocolType::Protobuf => &pb_frame,
        };
        let Some(bytes) = frame else { continue };
        match handle.tx.try_send(Out::Frame(bytes.clone())) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                // Slow (or gone) consumer: drop it from the hub so it stops
                // contending, and — if it was still present — tell it to
                // reconnect with DisconnectSlow (3008), matching Go. The Close is
                // delivered in a detached task that awaits a queue slot, so the
                // broadcaster is never blocked; if the socket is wedged the task
                // resolves (Err) when the connection finally drops. The writer
                // tasks already turn Out::Close into a 3008 close frame.
                if hub.remove(&handle.id) {
                    let tx = handle.tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(Out::Close(Disconnect::slow())).await;
                    });
                }
            }
        }
    }
}

/// Build the full push frame (Reply with id==0 carrying the encoded Push) for one
/// protocol.
fn make_push_frame<T: Serialize + WireType>(
    proto: ProtocolType,
    channel: &str,
    push_type: PushType,
    inner: &T,
) -> Option<Vec<u8>> {
    let data = codec::encode_result(proto, inner).ok()?;
    let push = Push::new(push_type, channel, Some(data));
    codec::encode_push_frame(proto, &push).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use centrifugo_protocol::{Command, MethodType, ProtocolType, Raw};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn node_registry_add_list_and_prune() {
        let r = NodeRegistry::new("self".into());
        r.add(NodeInfoData {
            uid: "self".into(),
            ..Default::default()
        });
        r.add(NodeInfoData {
            uid: "remote".into(),
            ..Default::default()
        });
        assert_eq!(r.list().len(), 2);
        // A negative max-age prunes everything not seen "in the future" — i.e. all
        // remotes — but this node is always kept.
        r.clean(-1);
        let list = r.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].uid, "self");
    }

    fn raw(s: String) -> Raw {
        Raw::from_bytes(s.into_bytes())
    }
    fn connect_cmd(id: u32) -> Command {
        Command {
            id,
            method: MethodType::Connect,
            params: Some(raw("{}".into())),
        }
    }
    fn subscribe_cmd(id: u32, ch: &str) -> Command {
        Command {
            id,
            method: MethodType::Subscribe,
            params: Some(raw(format!(r#"{{"channel":"{ch}"}}"#))),
        }
    }
    fn publish_cmd(id: u32, ch: &str, data: &str) -> Command {
        Command {
            id,
            method: MethodType::Publish,
            params: Some(raw(format!(r#"{{"channel":"{ch}","data":{data}}}"#))),
        }
    }

    #[tokio::test]
    async fn publish_fans_out_to_local_subscriber() {
        let node = Node::new();

        let (tx_b, mut rx_b) = mpsc::channel::<Out>(16);
        let mut sub = node.new_client(tx_b, ProtocolType::Json);
        sub.handle_command(&connect_cmd(1)).await;
        sub.handle_command(&subscribe_cmd(2, "news")).await;

        let (tx_a, _rx_a) = mpsc::channel::<Out>(16);
        let mut pubr = node.new_client(tx_a, ProtocolType::Json);
        pubr.handle_command(&connect_cmd(1)).await;
        let pub_replies = pubr
            .handle_command(&publish_cmd(2, "news", r#"{"msg":"hi"}"#))
            .await;
        assert!(pub_replies.replies[0].error.is_none());

        let out = tokio::time::timeout(Duration::from_secs(1), rx_b.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        let Out::Frame(frame) = out else {
            panic!("expected a frame")
        };
        let s = String::from_utf8(frame).unwrap();
        assert!(s.contains(r#""channel":"news""#), "frame: {s}");
        assert!(s.contains(r#""msg":"hi""#), "frame: {s}");

        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        assert!(v.get("id").is_none(), "push must have no id: {s}");
        assert_eq!(v["result"]["channel"], "news");
        assert_eq!(v["result"]["data"]["data"]["msg"], "hi");
    }

    #[tokio::test]
    async fn live_pub_on_recoverable_channel_carries_seq_gen_not_offset() {
        // Recoverable channel (history on). Go's UseSeqGen default (v2.8.6) ships a
        // live publication with seq/gen unpacked from its offset and the offset
        // zeroed — so a seq/gen-era client can track stream position from live
        // messages. A non-recoverable channel would carry none of these.
        let mut ns = Namespaces::default();
        ns.default.history_size = 10;
        ns.default.history_lifetime = 60;
        ns.default.publish = true;
        let node = Node::new_with(Arc::new(TokenVerifier::default()), true, ns);

        let (tx_b, mut rx_b) = mpsc::channel::<Out>(16);
        let mut sub = node.new_client(tx_b, ProtocolType::Json);
        sub.handle_command(&connect_cmd(1)).await;
        sub.handle_command(&subscribe_cmd(2, "news")).await;

        let (tx_a, _rx_a) = mpsc::channel::<Out>(16);
        let mut pubr = node.new_client(tx_a, ProtocolType::Json);
        pubr.handle_command(&connect_cmd(1)).await;
        let r = pubr
            .handle_command(&publish_cmd(2, "news", r#"{"msg":"hi"}"#))
            .await;
        assert!(r.replies[0].error.is_none(), "publish errored");

        let out = tokio::time::timeout(Duration::from_secs(1), rx_b.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        let Out::Frame(frame) = out else {
            panic!("expected a frame")
        };
        let s = String::from_utf8(frame).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        let publication = &v["result"]["data"];
        // First publish -> offset 1 -> seq 1, gen 0 (omitted), offset omitted.
        assert_eq!(publication["data"]["msg"], "hi", "frame: {s}");
        assert_eq!(publication["seq"], 1, "live pub must carry seq: {s}");
        assert!(
            publication.get("offset").is_none(),
            "live pub must not carry offset on the wire: {s}"
        );
        assert!(
            publication.get("gen").is_none(),
            "gen 0 must be omitted: {s}"
        );
    }

    #[tokio::test]
    async fn second_connect_is_rejected() {
        let node = Node::new();
        let (tx, _rx) = mpsc::channel::<Out>(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        let r1 = c.handle_command(&connect_cmd(1)).await;
        assert!(r1.replies[0].error.is_none());
        let r2 = c.handle_command(&connect_cmd(2)).await;
        assert_eq!(r2.replies[0].error.as_ref().unwrap().code, 107); // bad request
    }

    #[tokio::test]
    async fn send_has_no_reply_and_unimplemented_methods_are_not_available() {
        let node = Node::new();
        let (tx, _rx) = mpsc::channel::<Out>(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.handle_command(&connect_cmd(1)).await;

        let send = Command {
            id: 0,
            method: MethodType::Send,
            params: Some(raw(r#"{"data":{}}"#.into())),
        };
        assert!(
            c.handle_command(&send).await.replies.is_empty(),
            "SEND must produce no reply"
        );

        let presence = Command {
            id: 5,
            method: MethodType::Presence,
            params: Some(raw(r#"{"channel":"x"}"#.into())),
        };
        let r = c.handle_command(&presence).await;
        assert_eq!(r.replies[0].error.as_ref().unwrap().code, 108); // not available
    }
}
