//! Per-connection session state machine. Dispatches the client commands needed
//! for the M1 vertical slice (CONNECT/SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PING) in
//! insecure mode. Full method coverage, CONNECT-first disconnect semantics, and
//! auth arrive in M2/M3.

use std::collections::HashMap;
use std::sync::Arc;

use centrifugo_auth::VerifyError;
use centrifugo_protocol::codec::{self, WireType};
use centrifugo_protocol::messages::{
    ClientInfo, ConnectRequest, ConnectResult, HistoryRequest, HistoryResult, PingResult,
    PresenceRequest, PresenceResult, PresenceStatsRequest, PresenceStatsResult, PublishRequest,
    PublishResult, RefreshRequest, RefreshResult, RpcRequest, RpcResult, SubRefreshRequest,
    SubRefreshResult, SubscribeRequest, SubscribeResult, UnsubscribeRequest, UnsubscribeResult,
};
use centrifugo_protocol::{
    Command, Disconnect, Error, MethodType, ProtocolType, Push, PushType, Raw, Reply,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::hub::{ClientHandle, ClientId, Out, Signal};
use crate::node::Node;
use crate::proxy::{ProxyConnectOutcome, ProxyConnectRequest, ProxyOutcome, ProxyRequest};

/// Result of dispatching one command: replies to send, plus an optional
/// disconnect that closes the connection (with a specific code + reason).
#[derive(Default)]
pub struct CommandOutcome {
    pub replies: Vec<Reply>,
    pub disconnect: Option<Disconnect>,
}

impl CommandOutcome {
    fn replies(replies: Vec<Reply>) -> Self {
        CommandOutcome {
            replies,
            disconnect: None,
        }
    }
    fn disconnect(d: Disconnect) -> Self {
        CommandOutcome {
            replies: Vec::new(),
            disconnect: Some(d),
        }
    }
}

/// Per-subscription state, tracked for each channel the client is subscribed to.
/// `presence`/`join_leave` are captured at subscribe time so leave/presence
/// cleanup uses the options in force then; `expire_at`/`chan_info` are populated
/// from subscription tokens (SUB_REFRESH and private/`$` channels).
#[derive(Default)]
// `expire_at`/`chan_info`/`server_side`/`recoverable` are consumed by later
// phases (SUB_REFRESH, server-side channels, presence-refresh); kept here so the
// subscription state has its final shape.
#[allow(dead_code)]
pub(crate) struct SubState {
    /// Subscription token expiry (unix seconds); 0 = no expiry.
    pub expire_at: i64,
    /// Channel offers recovery.
    pub recoverable: bool,
    pub presence: bool,
    pub join_leave: bool,
    /// Per-channel info from a subscription token (becomes ClientInfo.chan_info).
    pub chan_info: Option<Vec<u8>>,
    /// Subscribed server-side (on connect), not via a client SUBSCRIBE.
    pub server_side: bool,
}

/// Resolved connect credentials: (user, conn_info, expire_at, server-side channels).
type Creds = (String, Option<Vec<u8>>, i64, Vec<String>);

pub struct Client {
    pub id: ClientId,
    pub user: String,
    proto: ProtocolType,
    /// Transport name reported to proxies ("websocket" or "sockjs").
    transport: &'static str,
    authenticated: bool,
    /// Connection info bytes from the token (becomes ClientInfo.conn_info).
    conn_info: Option<Vec<u8>>,
    /// Token expiry (unix seconds); 0 means no expiry.
    expire_at: i64,
    /// Channels this client is subscribed to → per-subscription state.
    subscriptions: HashMap<String, SubState>,
    /// Server-side-subscription Joins to publish AFTER the connect reply is
    /// flushed (Go flushes the reply first, then publishes Join in goroutines).
    pending_joins: Vec<(String, ClientInfo)>,
    node: Arc<Node>,
    tx: Sender<Out>,
    /// Reader-task control channel registered in the hub (server-side
    /// unsubscribe/disconnect); set by the transport via [`Client::set_ctrl`].
    ctrl_tx: Option<Sender<Signal>>,
}

impl Client {
    pub fn new(node: Arc<Node>, tx: Sender<Out>, proto: ProtocolType) -> Self {
        Client {
            id: String::new(),
            user: String::new(),
            proto,
            transport: "websocket",
            authenticated: false,
            conn_info: None,
            expire_at: 0,
            subscriptions: HashMap::new(),
            pending_joins: Vec::new(),
            node,
            tx,
            ctrl_tx: None,
        }
    }

    /// Publish any Joins deferred from server-side subscriptions. The transport
    /// calls this after flushing a command batch's replies, so a server-side
    /// channel's Join push never precedes the Connect reply (matches Go ordering).
    pub async fn flush_pending_joins(&mut self) {
        for (channel, info) in std::mem::take(&mut self.pending_joins) {
            self.node.publish_join(&channel, info).await;
        }
    }

    /// Override the transport name reported to proxies (SockJS sessions call this;
    /// WebSocket keeps the default).
    pub fn set_transport(&mut self, transport: &'static str) {
        self.transport = transport;
    }

    /// Register the reader-task control channel (server-side unsubscribe/disconnect).
    /// Must be called before CONNECT so the handle lands in the hub.
    pub fn set_ctrl(&mut self, ctrl_tx: Sender<Signal>) {
        self.ctrl_tx = Some(ctrl_tx);
    }

    /// Channels this connection is currently subscribed to.
    pub fn subscribed_channels(&self) -> Vec<String> {
        self.subscriptions.keys().cloned().collect()
    }

    /// Server-initiated unsubscribe: drop the subscription (Leave + presence
    /// cleanup) and notify the client with an Unsubscribe push (PushType::Unsub).
    pub async fn server_unsubscribe(&mut self, channel: &str) {
        if !self.is_subscribed(channel) {
            return;
        }
        self.unsubscribe_channel(channel).await;
        if let Ok(body) = codec::encode_result(self.proto, &UnsubscribeResult {}) {
            let push = Push::new(PushType::Unsub, channel, Some(body));
            if let Ok(frame) = codec::encode_push_frame(self.proto, &push) {
                let _ = self.tx.send(Out::Frame(frame)).await;
            }
        }
    }

    fn is_subscribed(&self, channel: &str) -> bool {
        self.subscriptions.contains_key(channel)
    }

    /// Base proxy request carrying this connection's identity + transport.
    fn proxy_request(&self) -> ProxyRequest {
        ProxyRequest {
            client: self.id.clone(),
            user: self.user.clone(),
            transport: self.transport.into(),
            protocol: match self.proto {
                ProtocolType::Json => "json".into(),
                ProtocolType::Protobuf => "protobuf".into(),
            },
            ..Default::default()
        }
    }

    /// Build the ClientInfo for this connection (publisher/presence/join-leave).
    fn client_info(&self) -> ClientInfo {
        self.client_info_with(None)
    }

    /// Like [`client_info`], with per-channel `chan_info` attached (from a
    /// subscription token or the subscribe proxy).
    fn client_info_with(&self, chan_info: Option<Vec<u8>>) -> ClientInfo {
        ClientInfo {
            user: self.user.clone(),
            client: self.id.clone(),
            conn_info: self.conn_info.as_ref().map(|b| Raw::from_bytes(b.clone())),
            chan_info: chan_info.map(Raw::from_bytes),
        }
    }

    /// Dispatch one command. Returns replies and/or a disconnect.
    pub async fn handle_command(&mut self, cmd: &Command) -> CommandOutcome {
        self.node.metrics().inc_command(cmd.method as usize);
        // CONNECT must be the first command; otherwise close the connection
        // (Go centrifuge sends DisconnectBadRequest).
        if !self.authenticated && cmd.method != MethodType::Connect {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        match cmd.method {
            // CONNECT may itself decide to disconnect (invalid token), so it
            // returns its own outcome.
            MethodType::Connect => self.on_connect(cmd).await,
            MethodType::Subscribe => self.on_subscribe(cmd).await,
            MethodType::Publish => self.on_publish(cmd).await,
            MethodType::Unsubscribe => self.on_unsubscribe(cmd).await,
            MethodType::Ping => {
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &PingResult {})])
            }
            MethodType::Refresh => self.on_refresh(cmd).await,
            // SEND is fire-and-forget: no reply.
            MethodType::Send => CommandOutcome::replies(vec![]),
            MethodType::Rpc => self.on_rpc(cmd).await,
            MethodType::Presence => CommandOutcome::replies(self.on_presence(cmd).await),
            MethodType::PresenceStats => CommandOutcome::replies(self.on_presence_stats(cmd).await),
            MethodType::History => CommandOutcome::replies(self.on_history(cmd).await),
            MethodType::SubRefresh => self.on_sub_refresh(cmd),
        }
    }

    async fn on_connect(&mut self, cmd: &Command) -> CommandOutcome {
        if self.authenticated {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::bad_request())]);
        }
        let req: ConnectRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };

        // The client id is assigned before auth so a connect-proxy can receive it.
        self.id = Uuid::new_v4().to_string();

        // Identity resolution mirrors centrifugo v2.8.6 OnClientConnecting:
        //   1. A present token is verified FIRST (insecure only zeroes its
        //      expiry, it does not skip verification).
        //   2. Else, a configured connect-proxy authenticates.
        //   3. Else, anonymous/insecure yields an empty-user connection.
        //   4. Else, reject with DisconnectBadRequest (3003).
        // creds = (user, info, expire_at, server-side channels from the token).
        let creds: Option<Creds> = if !req.token.is_empty() {
            match self.node.verifier().verify_connect_token(&req.token) {
                Ok(ct) => {
                    // Insecure mode keeps the token's user/info but forces no expiry.
                    let expire_at = if self.node.client_insecure() {
                        0
                    } else {
                        ct.expire_at
                    };
                    Some((ct.user, ct.info, expire_at, ct.channels))
                }
                // Expired connect token -> error reply (109).
                Err(VerifyError::Expired) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::token_expired(),
                    )]);
                }
                // Any other token failure -> close (3002).
                Err(VerifyError::Invalid) => {
                    return CommandOutcome::disconnect(Disconnect::invalid_token());
                }
            }
        } else if let Some(proxy) = self.node.proxies().connect.clone() {
            let preq = ProxyConnectRequest {
                client: self.id.clone(),
                transport: self.transport.into(),
                protocol: match self.proto {
                    ProtocolType::Json => "json".into(),
                    ProtocolType::Protobuf => "protobuf".into(),
                },
                data: None,
            };
            match proxy.connect(preq).await {
                // Go builds ConnectReply.Subscriptions from credentials.Channels;
                // surface them so the server-side-subscribe + 102-validation loop runs.
                Ok(ProxyConnectOutcome::Credentials(r)) => {
                    Some((r.user, r.info, r.expire_at, r.channels))
                }
                // Explicit proxy error -> relay that code/message as an error reply.
                Ok(ProxyConnectOutcome::Error { code, message }) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::new(code, message),
                    )]);
                }
                // Explicit proxy disconnect -> close with that code/reason.
                Ok(ProxyConnectOutcome::Disconnect { code, reason }) => {
                    return CommandOutcome::disconnect(Disconnect::new(code, reason, false));
                }
                // No credentials -> fall through to anonymous/insecure handling.
                Ok(ProxyConnectOutcome::NoCredentials) => None,
                // Transport failure -> ErrorInternal (100) reply, matching Go.
                Err(_) => {
                    return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::internal())])
                }
            }
        } else {
            None
        };

        // Anonymous/insecure fallback to an empty-user connection when no
        // identity was established; otherwise reject.
        let (user, info, expire_at, mut channels) = match creds {
            Some(c) => c,
            None if self.node.client_anonymous() || self.node.client_insecure() => {
                (String::new(), None, 0, Vec::new())
            }
            None => return CommandOutcome::disconnect(Disconnect::bad_request()),
        };

        // Personal channel auto-subscription (Go user_subscribe_to_personal).
        if let Some(pc) = self.node.personal_channel(&user) {
            if !channels.contains(&pc) {
                channels.push(pc);
            }
        }

        // Pre-validate server-side channels (from the token) before registering:
        // an unknown namespace fails the connect with UnknownChannel(102), matching
        // Go OnClientConnecting.
        for ch in &channels {
            if self.node.channel_options(ch).is_none() {
                return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::unknown_channel())]);
            }
        }

        self.user = user;
        self.conn_info = info;
        self.expire_at = expire_at;
        self.authenticated = true;
        self.node.metrics().inc_connect(self.transport);
        self.node.hub().add(ClientHandle {
            id: self.id.clone(),
            user: self.user.clone(),
            proto: self.proto,
            tx: self.tx.clone(),
            ctrl: self.ctrl_tx.clone(),
        });

        // Establish server-side subscriptions (JWT `channels`) and report them in
        // the connect reply's `subs` map.
        let mut subs = std::collections::HashMap::new();
        for ch in channels {
            let sub = self.server_side_subscribe(&ch).await;
            subs.insert(ch, sub);
        }

        let (expires, ttl) = if expire_at > 0 {
            (true, (expire_at - now_unix()).max(0) as u32)
        } else {
            (false, 0)
        };
        let result = ConnectResult {
            client: self.id.clone(),
            version: String::new(),
            expires,
            ttl,
            subs,
            ..Default::default()
        };
        CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
    }

    /// Establish a server-side subscription (from the connect token's `channels`)
    /// and build its SubscribeResult for the connect reply. Channel options are
    /// known-present (pre-validated by the caller).
    async fn server_side_subscribe(&mut self, channel: &str) -> SubscribeResult {
        let (presence, join_leave, recoverable) = self
            .node
            .channel_options(channel)
            .map(|o| (o.presence, o.join_leave, o.history_recover))
            .unwrap_or((false, false, false));

        self.node.hub().subscribe(&self.id, channel);
        let _ = self.node.engine().subscribe(channel).await;
        if presence {
            self.node
                .add_presence(channel, &self.id, self.client_info())
                .await;
        }
        self.subscriptions.insert(
            channel.to_string(),
            SubState {
                presence,
                join_leave,
                recoverable,
                server_side: true,
                ..Default::default()
            },
        );

        // Report the current stream top so the client has a recovery baseline.
        let mut result = SubscribeResult::default();
        if recoverable {
            result.recoverable = true;
            let (_pubs, top) = self.node.history(channel).await;
            result.epoch = top.epoch;
            encode_position(&mut result, top.offset, self.node.use_seq_gen());
        }
        // Defer Join until after the connect reply is flushed (see flush_pending_joins).
        if join_leave {
            self.pending_joins
                .push((channel.to_string(), self.client_info()));
        }
        result
    }

    async fn on_subscribe(&mut self, cmd: &Command) -> CommandOutcome {
        let req: SubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        // Go validateSubscribeRequest: empty channel -> DisconnectBadRequest (3003).
        if req.channel.is_empty() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        // Already subscribed -> ErrorAlreadySubscribed (105), checked before the
        // namespace/permission logic (matches Go's validateSubscribeRequest order).
        if self.is_subscribed(&req.channel) {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::already_subscribed())]);
        }
        let (presence, join_leave, history_recover, anonymous, server_side, proxy_subscribe) =
            match self.node.channel_options(&req.channel) {
                Some(o) => (
                    o.presence,
                    o.join_leave,
                    o.history_recover,
                    o.anonymous,
                    o.server_side,
                    o.proxy_subscribe,
                ),
                None => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::unknown_channel(),
                    )])
                }
            };
        // Server-side channels cannot be subscribed to directly by clients.
        if server_side {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::permission_denied())]);
        }
        // Anonymous (empty-user) clients need the channel's `anonymous` option,
        // unless the server runs in insecure mode.
        if !anonymous && self.user.is_empty() && !self.node.client_insecure() {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::permission_denied())]);
        }
        // User-limited channels (`name#u1,u2`): only listed users may subscribe.
        if !user_allowed(&req.channel, &self.user) {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::permission_denied())]);
        }
        // Per-channel info (becomes ClientInfo.chan_info) + subscription expiry,
        // established by the subscription token ($) or the subscribe proxy.
        let mut chan_info: Option<Vec<u8>> = None;
        let mut sub_expire_at: i64 = 0;
        // Private ($-prefixed) channels require a valid subscription token whose
        // client + channel match this connection.
        if self.node.is_private(&req.channel) {
            if req.token.is_empty() {
                return CommandOutcome::replies(vec![Reply::err(
                    cmd.id,
                    Error::permission_denied(),
                )]);
            }
            match self.node.verifier().verify_subscribe_token(&req.token) {
                Ok(t) => {
                    if t.client != self.id || t.channel != req.channel {
                        return CommandOutcome::replies(vec![Reply::err(
                            cmd.id,
                            Error::permission_denied(),
                        )]);
                    }
                    chan_info = t.info;
                    sub_expire_at = if t.expire_token_only { 0 } else { t.expire_at };
                }
                Err(VerifyError::Expired) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::token_expired(),
                    )])
                }
                Err(VerifyError::Invalid) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::permission_denied(),
                    )])
                }
            }
        } else if proxy_subscribe && !is_user_limited(&req.channel) {
            // Subscribe proxy authorizes (and may attach info to) the subscription.
            let Some(proxy) = self.node.proxies().subscribe.clone() else {
                return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::not_available())]);
            };
            let mut preq = self.proxy_request();
            preq.channel = req.channel.clone();
            preq.token = req.token.clone();
            match proxy.subscribe(preq).await {
                Ok(ProxyOutcome::Result(c)) => chan_info = c.info,
                Ok(ProxyOutcome::Error { code, message }) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::new(code, message),
                    )])
                }
                // A subscribe-proxy disconnect closes the connection (Go calls
                // c.close(disconnect)) with the proxy's code/reason.
                Ok(ProxyOutcome::Disconnect { code, reason }) => {
                    return CommandOutcome::disconnect(Disconnect::new(code, reason, false))
                }
                Err(_) => {
                    return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::internal())])
                }
            }
        }
        self.node.hub().subscribe(&self.id, &req.channel);
        let _ = self.node.engine().subscribe(&req.channel).await;

        let sub_info = self.client_info_with(chan_info.clone());
        if presence {
            self.node
                .add_presence(&req.channel, &self.id, sub_info.clone())
                .await;
        }
        self.subscriptions.insert(
            req.channel.clone(),
            SubState {
                presence,
                join_leave,
                recoverable: history_recover,
                expire_at: sub_expire_at,
                chan_info,
                ..Default::default()
            },
        );

        // Recovery (when the channel offers it via history_recover).
        let mut result = SubscribeResult::default();
        if history_recover {
            result.recoverable = true;
            let use_seq_gen = self.node.use_seq_gen();
            if req.recover {
                // The client may send seq/gen (centrifugo v2.8.6 default) or offset.
                let cmd_offset = if req.seq > 0 || req.gen > 0 {
                    pack_offset(req.seq, req.gen)
                } else {
                    req.offset
                };
                let (mut pubs, top) = self
                    .node
                    .history_since(&req.channel, cmd_offset, &req.epoch)
                    .await;
                let next = cmd_offset + 1;
                result.recovered = match pubs.first() {
                    Some(first) => first.offset == next && top.epoch == req.epoch,
                    None => top.offset == cmd_offset && top.epoch == req.epoch,
                };
                result.epoch = top.epoch;
                encode_position(&mut result, top.offset, use_seq_gen);
                if use_seq_gen {
                    for p in &mut pubs {
                        let (s, g) = unpack_offset(p.offset);
                        p.seq = s;
                        p.gen = g;
                        p.offset = 0;
                    }
                    // centrifuge returns recovered publications newest-first
                    // (descending) under the seq/gen compatibility mode.
                    pubs.reverse();
                }
                result.publications = pubs;
            } else {
                let (_pubs, top) = self.node.history(&req.channel).await;
                result.epoch = top.epoch;
                encode_position(&mut result, top.offset, use_seq_gen);
            }
        }

        let reply = ok_reply(self.proto, cmd.id, &result);
        // Join is published after the subscribe reply; the joiner is now a
        // subscriber (matches centrifuge ordering).
        if join_leave {
            self.node.publish_join(&req.channel, sub_info).await;
        }
        CommandOutcome::replies(vec![reply])
    }

    async fn on_history(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: HistoryRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        let history_enabled = match self.node.channel_options(&req.channel) {
            Some(o) => o.history_enabled(),
            None => return vec![Reply::err(cmd.id, Error::unknown_channel())],
        };
        if !history_enabled {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let (mut publications, _top) = self.node.history(&req.channel).await;
        if self.node.use_seq_gen() {
            for p in &mut publications {
                let (s, g) = unpack_offset(p.offset);
                p.seq = s;
                p.gen = g;
            }
        }
        let result = HistoryResult { publications };
        vec![ok_reply(self.proto, cmd.id, &result)]
    }

    async fn on_publish(&mut self, cmd: &Command) -> CommandOutcome {
        let req: PublishRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        // Go (centrifuge handlePublish): empty channel or empty data is rejected
        // with DisconnectBadRequest (3003).
        let mut data: Vec<u8> = match req.data.as_ref() {
            Some(r) if !r.as_bytes().is_empty() => r.as_bytes().to_vec(),
            _ => return CommandOutcome::disconnect(Disconnect::bad_request()),
        };
        if req.channel.is_empty() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }

        // Channel options + publish permission (Go OnPublish): unknown namespace
        // -> UnknownChannel(102); !publish && !insecure -> PermissionDenied(103);
        // subscribe_to_publish requires an active subscription.
        let (can_publish, subscribe_to_publish, proxy_publish) = match self
            .node
            .channel_options(&req.channel)
        {
            Some(o) => (o.publish, o.subscribe_to_publish, o.proxy_publish),
            None => {
                return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::unknown_channel())])
            }
        };
        if !can_publish && !self.node.client_insecure() {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::permission_denied())]);
        }
        if subscribe_to_publish && !self.is_subscribed(&req.channel) {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::permission_denied())]);
        }

        // Publish proxy (per-channel `proxy_publish`): the endpoint may deny
        // (error/disconnect) or transform the payload before it is published.
        // proxy_publish with no proxy configured -> NotAvailable(108), like Go.
        if proxy_publish {
            let Some(proxy) = self.node.proxies().publish.clone() else {
                return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::not_available())]);
            };
            let mut preq = self.proxy_request();
            preq.channel = req.channel.clone();
            preq.data = Some(data.clone());
            match proxy.publish(preq).await {
                Ok(ProxyOutcome::Result(d)) => {
                    if let Some(nd) = d.data {
                        data = nd;
                    }
                }
                Ok(ProxyOutcome::Error { code, message }) => {
                    return CommandOutcome::replies(vec![Reply::err(
                        cmd.id,
                        Error::new(code, message),
                    )]);
                }
                Ok(ProxyOutcome::Disconnect { code, reason }) => {
                    return CommandOutcome::disconnect(Disconnect::new(code, reason, false));
                }
                Err(_) => {
                    return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::internal())]);
                }
            }
        }

        // A client-initiated publication carries the publisher's ClientInfo,
        // matching Go centrifuge behavior.
        let info = self.client_info();
        self.node.publish(&req.channel, &data, Some(info)).await;
        CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &PublishResult {})])
    }

    async fn on_unsubscribe(&mut self, cmd: &Command) -> CommandOutcome {
        let req: UnsubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        // Go handleUnsubscribe: an empty channel is DisconnectBadRequest (3003).
        if req.channel.is_empty() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        self.unsubscribe_channel(&req.channel).await;
        CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &UnsubscribeResult {})])
    }

    async fn unsubscribe_channel(&mut self, channel: &str) {
        // Use the options captured at subscribe time (a no-op if not subscribed).
        let Some(state) = self.subscriptions.remove(channel) else {
            return;
        };
        // Go order: remove presence + publish Leave (carrying the per-channel
        // chan_info captured at subscribe time) FIRST, then remove the hub/engine
        // subscription — so the leaving client still receives its own Leave.
        let info = self.client_info_with(state.chan_info.clone());
        if state.presence {
            self.node.remove_presence(channel, &self.id).await;
        }
        if state.join_leave {
            self.node.publish_leave(channel, info).await;
        }
        self.node.hub().unsubscribe(&self.id, channel);
        let _ = self.node.engine().unsubscribe(channel).await;
    }

    async fn on_presence(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PresenceRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        let (presence, disable_for_client) = match self.node.channel_options(&req.channel) {
            Some(o) => (o.presence, o.presence_disable_for_client),
            None => return vec![Reply::err(cmd.id, Error::unknown_channel())],
        };
        if !presence || disable_for_client {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let result = PresenceResult {
            presence: self.node.presence(&req.channel).await,
        };
        vec![ok_reply(self.proto, cmd.id, &result)]
    }

    async fn on_presence_stats(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PresenceStatsRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        let (presence, disable_for_client) = match self.node.channel_options(&req.channel) {
            Some(o) => (o.presence, o.presence_disable_for_client),
            None => return vec![Reply::err(cmd.id, Error::unknown_channel())],
        };
        if !presence || disable_for_client {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let (num_clients, num_users) = self.node.presence_stats(&req.channel).await;
        let result = PresenceStatsResult {
            num_clients,
            num_users,
        };
        vec![ok_reply(self.proto, cmd.id, &result)]
    }

    /// Re-assert presence for every presence-enabled subscription. Driven by the
    /// transport's presence-ping timer so Redis presence entries (which carry a
    /// TTL) do not expire while the client stays connected.
    pub async fn refresh_presence(&self) {
        if !self.authenticated {
            return;
        }
        // Re-attach each subscription's chan_info (Go updateChannelPresence uses
        // chCtx.Info); using the bare client_info would erase it on every ping.
        for (channel, state) in &self.subscriptions {
            if state.presence {
                let info = self.client_info_with(state.chan_info.clone());
                self.node.add_presence(channel, &self.id, info).await;
            }
        }
    }

    /// Periodic expiry check (driven by the transport's timer). In client-side-
    /// refresh mode, a connection whose token expired (and was not refreshed in
    /// time) is closed with DisconnectExpired (3005); any subscription whose
    /// token expired is closed with DisconnectSubExpired (3006). Both honour a
    /// grace window (Go ClientExpired{,Sub}CloseDelay = 25s) so a client has time
    /// to send REFRESH / SUB_REFRESH. Returns the disconnect to apply, if any.
    pub fn check_expired(&self) -> Option<Disconnect> {
        if !self.authenticated {
            return None;
        }
        const CLOSE_DELAY: i64 = 25;
        let now = now_unix();
        if self.expire_at > 0 && now > self.expire_at + CLOSE_DELAY {
            return Some(Disconnect::expired());
        }
        for state in self.subscriptions.values() {
            if state.expire_at > 0 && now > state.expire_at + CLOSE_DELAY {
                return Some(Disconnect::sub_expired());
            }
        }
        None
    }

    /// Called when the connection closes: publish Leave + clear presence for all
    /// subscribed channels, then unregister from the hub.
    pub async fn on_disconnect(&mut self) {
        let channels = std::mem::take(&mut self.subscriptions);
        for (ch, state) in &channels {
            if state.presence {
                self.node.remove_presence(ch, &self.id).await;
            }
            if state.join_leave {
                let info = self.client_info_with(state.chan_info.clone());
                self.node.publish_leave(ch, info).await;
            }
        }
        self.node.remove(&self.id);
    }

    async fn on_refresh(&mut self, cmd: &Command) -> CommandOutcome {
        let req: RefreshRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        // Go handleRefresh: an empty refresh token is DisconnectBadRequest (3003)
        // before any handler runs — including the refresh proxy.
        if req.token.is_empty() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        // With a refresh proxy configured the connection is server-side-refresh
        // (Go ClientSideRefresh = !refreshProxyEnabled): the server drives the
        // proxy proactively (see `proactive_refresh`), so a client REFRESH command
        // is a protocol violation → DisconnectBadRequest (3003), matching Go
        // handleRefresh ("client not supposed to send refresh command in case of
        // server-side refresh mechanism").
        if self.node.proxies().refresh.is_some() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        match self.node.verifier().verify_connect_token(&req.token) {
            Ok(ct) => {
                if !ct.user.is_empty() {
                    self.user = ct.user;
                }
                self.conn_info = ct.info;
                self.expire_at = ct.expire_at;
                // Go: a *valid* token whose new expiry is already in the past
                // (expire_at > 0 && ttl <= 0) yields ErrorExpired (110); a token
                // with no expiry just refreshes successfully.
                if ct.expire_at > 0 {
                    let ttl = ct.expire_at - now_unix();
                    if ttl <= 0 {
                        return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::expired())]);
                    }
                    let result = RefreshResult {
                        client: self.id.clone(),
                        version: String::new(),
                        expires: true,
                        ttl: ttl as u32,
                    };
                    return CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)]);
                }
                let result = RefreshResult {
                    client: self.id.clone(),
                    version: String::new(),
                    expires: false,
                    ttl: 0,
                };
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
            }
            // Expired refresh token -> DisconnectExpired (3005), matching Go's
            // RefreshReply{Expired:true} path (NOT a 110 error reply).
            Err(VerifyError::Expired) => CommandOutcome::disconnect(Disconnect::expired()),
            // Invalid refresh token -> close (3002).
            Err(VerifyError::Invalid) => CommandOutcome::disconnect(Disconnect::invalid_token()),
        }
    }

    /// Server-side proactive refresh (Go `expire()` with `!ClientSideRefresh`).
    /// When a refresh proxy is configured and the connection's token is within
    /// `lookahead_secs` of expiry, call the proxy to renew it. Returns `Some` to
    /// close the connection (expired / proxy error / proxy disconnect), or `None`
    /// when nothing was due or the connection was renewed. Produces no reply —
    /// there is no client command driving it.
    pub async fn proactive_refresh(&mut self, lookahead_secs: i64) -> Option<Disconnect> {
        // Client-side refresh (no proxy): the client renews via its own REFRESH.
        let proxy = self.node.proxies().refresh.clone()?;
        if !self.authenticated || self.expire_at <= 0 {
            return None; // no expiry to renew
        }
        if self.expire_at - now_unix() > lookahead_secs {
            return None; // not due yet
        }
        match proxy.refresh(self.proxy_request()).await {
            Ok(ProxyOutcome::Result(c)) => {
                if c.expired {
                    return Some(Disconnect::expired()); // 3005
                }
                if c.info.is_some() {
                    self.conn_info = c.info;
                }
                if c.expire_at > 0 {
                    self.expire_at = c.expire_at;
                    // Refreshed into an already-past expiry → expired (Go checkExpired).
                    if c.expire_at - now_unix() <= 0 {
                        return Some(Disconnect::expired());
                    }
                    None
                } else {
                    // No new expiry and not marked expired: nothing to extend → expired.
                    Some(Disconnect::expired())
                }
            }
            // Go's expire() callback: an explicit *Disconnect closes with it; any
            // other proxy/transport error → DisconnectServerError (3004).
            Ok(ProxyOutcome::Disconnect { code, reason }) => {
                Some(Disconnect::new(code, reason, false))
            }
            Ok(ProxyOutcome::Error { .. }) | Err(_) => Some(Disconnect::server_error()),
        }
    }

    /// RPC: with an RPC proxy configured, forward the call and relay its result/
    /// error/disconnect; without one, RPC is method-not-found (matching Go's
    /// behavior when no rpc handler is registered).
    async fn on_rpc(&mut self, cmd: &Command) -> CommandOutcome {
        let Some(proxy) = self.node.proxies().rpc.clone() else {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::method_not_found())]);
        };
        let req: RpcRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        let mut preq = self.proxy_request();
        preq.method = req.method;
        preq.data = req.data.map(|r| r.as_bytes().to_vec());
        match proxy.rpc(preq).await {
            Ok(ProxyOutcome::Result(d)) => {
                // Omit `data` when the proxy returned none (ack-only RPC); an empty
                // Raw breaks JSON encoding (RawValue::from_string("")).
                let result = RpcResult {
                    data: d.data.map(Raw::from_bytes),
                };
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
            }
            Ok(ProxyOutcome::Error { code, message }) => {
                CommandOutcome::replies(vec![Reply::err(cmd.id, Error::new(code, message))])
            }
            Ok(ProxyOutcome::Disconnect { code, reason }) => {
                CommandOutcome::disconnect(Disconnect::new(code, reason, false))
            }
            Err(_) => CommandOutcome::replies(vec![Reply::err(cmd.id, Error::internal())]),
        }
    }

    /// SUB_REFRESH: refresh a subscription's expiry with a new subscription token.
    /// Mirrors centrifuge handleSubRefresh + centrifugo OnSubRefresh: empty
    /// channel / params decode error -> DisconnectBadRequest (3003); not
    /// subscribed -> PermissionDenied (103); server-side subscription -> 3003;
    /// invalid token or client/channel mismatch -> DisconnectInvalidToken (3002);
    /// expired token -> a success reply with no expiry (Go's SubRefreshReply
    /// ignores `Expired`); valid token -> SubRefreshResult{expires, ttl} and the
    /// subscription's expiry/info are updated.
    fn on_sub_refresh(&mut self, cmd: &Command) -> CommandOutcome {
        let req: SubRefreshRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(_) => return CommandOutcome::disconnect(Disconnect::bad_request()),
        };
        if req.channel.is_empty() {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        match self.subscriptions.get(&req.channel) {
            // Server-side subscriptions can't be refreshed by a client command.
            Some(s) if s.server_side => {
                return CommandOutcome::disconnect(Disconnect::bad_request())
            }
            Some(_) => {}
            // Must be subscribed to refresh.
            None => {
                return CommandOutcome::replies(vec![Reply::err(
                    cmd.id,
                    Error::permission_denied(),
                )])
            }
        }
        // Go handleSubRefresh: an empty token is ErrorBadRequest (107) — an in-band
        // error reply, NOT a disconnect.
        if req.token.is_empty() {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::bad_request())]);
        }
        match self.node.verifier().verify_subscribe_token(&req.token) {
            Ok(t) => {
                if t.client != self.id || t.channel != req.channel {
                    return CommandOutcome::disconnect(Disconnect::invalid_token());
                }
                let expire_at = t.expire_at;
                // Go's sub-refresh boundary is strict `<`: expire_at == now is NOT
                // expired (TTL 0, success), unlike connection refresh (`ttl <= 0`).
                let (expires, ttl) = if expire_at > 0 {
                    if expire_at < now_unix() {
                        return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::expired())]);
                    }
                    (true, (expire_at - now_unix()).max(0) as u32)
                } else {
                    (false, 0)
                };
                if let Some(s) = self.subscriptions.get_mut(&req.channel) {
                    s.expire_at = expire_at;
                    s.chan_info = t.info;
                }
                let result = SubRefreshResult { expires, ttl };
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
            }
            // Go quirk: an expired sub-refresh token yields a success reply with
            // no expiry (centrifuge ignores SubRefreshReply.Expired); clear the
            // subscription's expiry to match.
            Err(VerifyError::Expired) => {
                if let Some(s) = self.subscriptions.get_mut(&req.channel) {
                    s.expire_at = 0;
                }
                let result = SubRefreshResult {
                    expires: false,
                    ttl: 0,
                };
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
            }
            Err(VerifyError::Invalid) => CommandOutcome::disconnect(Disconnect::invalid_token()),
        }
    }
}

/// Whether `user` may subscribe to a user-limited channel (Go `UserAllowed`).
/// A channel `name#u1,u2` restricts subscription to the comma-separated user
/// list after the last `#`; channels without `#` are open to everyone.
fn user_allowed(channel: &str, user: &str) -> bool {
    match channel.rsplit_once('#') {
        None => true,
        Some((_, allowed)) => allowed.split(',').any(|u| u == user),
    }
}

/// Whether `channel` is user-limited (contains the `#` boundary). The subscribe
/// proxy is skipped for these (they are allow-list based), matching Go.
fn is_user_limited(channel: &str) -> bool {
    channel.contains('#')
}

/// Parse a typed request from optional params, decoding in the connection's
/// protocol. Missing params → default.
fn parse_params<T: DeserializeOwned + Default + WireType>(
    proto: ProtocolType,
    params: &Option<Raw>,
) -> Result<T, Error> {
    codec::decode_params(proto, params).map_err(|_| Error::bad_request())
}

/// Build an ok reply, encoding the result in the connection's protocol (JSON or
/// protobuf). Falls back to an internal error if encoding fails (it should not).
fn ok_reply<T: Serialize + WireType>(proto: ProtocolType, id: u32, value: &T) -> Reply {
    match codec::encode_result(proto, value) {
        Ok(raw) => Reply::ok(id, raw),
        Err(_) => Reply::err(id, Error::internal()),
    }
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// offset = gen*MaxUint32 + seq (centrifuge recovery.PackUint64). Note this is
/// intentionally NOT the inverse of `unpack_offset` (>>32) — centrifuge v0.14.2's
/// pack/unpack are asymmetric, and we replicate that quirk verbatim for wire
/// compatibility. For gen==0 (normal operation) both reduce to `seq`.
fn pack_offset(seq: u32, gen: u32) -> u64 {
    (gen as u64) * (u32::MAX as u64) + (seq as u64)
}

/// (seq, gen) from offset (centrifuge recovery.UnpackUint64). Shared with the
/// live-broadcast path in [`crate::node`], which converts a recoverable-channel
/// publication's offset into seq/gen on the wire.
pub(crate) fn unpack_offset(v: u64) -> (u32, u32) {
    (v as u32, (v >> 32) as u32)
}

/// Encode a stream-top position into a SubscribeResult as seq/gen or offset.
fn encode_position(result: &mut SubscribeResult, offset: u64, use_seq_gen: bool) {
    if use_seq_gen {
        let (s, g) = unpack_offset(offset);
        result.seq = s;
        result.gen = g;
    } else {
        result.offset = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{Proxies, RefreshCreds, RefreshProxy};
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    /// A refresh proxy that always returns the configured credentials.
    struct StubRefresh {
        expire_at: i64,
        expired: bool,
    }
    #[async_trait]
    impl RefreshProxy for StubRefresh {
        async fn refresh(&self, _req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RefreshCreds>> {
            Ok(ProxyOutcome::Result(RefreshCreds {
                expired: self.expired,
                expire_at: self.expire_at,
                info: None,
            }))
        }
    }
    /// A refresh proxy whose transport always fails.
    struct ErrRefresh;
    #[async_trait]
    impl RefreshProxy for ErrRefresh {
        async fn refresh(&self, _req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RefreshCreds>> {
            Err(anyhow::anyhow!("backend down"))
        }
    }

    fn node_with_refresh(proxy: Arc<dyn RefreshProxy>) -> Arc<Node> {
        use crate::engine::Engine;
        use crate::hub::Hub;
        use crate::memory::MemoryEngine;
        use crate::node::{make_route, Namespaces, NodeRegistry};
        let hub = Arc::new(Hub::new());
        let registry = Arc::new(NodeRegistry::new("test".into()));
        let engine: Arc<dyn Engine> = Arc::new(MemoryEngine::new(make_route(&hub, &registry, true)));
        let proxies = Proxies {
            refresh: Some(proxy),
            ..Default::default()
        };
        Node::new_with_engine(
            hub,
            engine,
            Arc::new(centrifugo_auth::TokenVerifier::default()),
            true,
            false,
            Namespaces::default(),
            proxies,
            25,
            60,
            registry,
            "2.8.6".into(),
            "node".into(),
        )
    }

    #[tokio::test]
    async fn proactive_refresh_extends_expires_and_errors() {
        // Client-side (no proxy): proactive refresh is a no-op.
        let node = Node::new();
        let (tx, _rx) = mpsc::channel(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.authenticated = true;
        c.expire_at = now_unix() + 5;
        assert!(c.proactive_refresh(25).await.is_none(), "no proxy → no-op");

        // Proxy extends: a due connection is renewed (expire_at pushed out, no close).
        let node = node_with_refresh(Arc::new(StubRefresh {
            expire_at: now_unix() + 3600,
            expired: false,
        }));
        let (tx, _rx) = mpsc::channel(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.authenticated = true;
        c.expire_at = now_unix() + 5; // within the 25s lookahead → due
        assert!(c.proactive_refresh(25).await.is_none(), "extend → no close");
        assert!(c.expire_at > now_unix() + 3000, "expire_at must be extended");

        // Not due yet: a far-future expiry must not call the proxy or change state.
        c.expire_at = now_unix() + 10_000;
        let before = c.expire_at;
        assert!(c.proactive_refresh(25).await.is_none());
        assert_eq!(c.expire_at, before, "not-due refresh must not change expiry");

        // Proxy says expired → DisconnectExpired (3005).
        let node = node_with_refresh(Arc::new(StubRefresh {
            expire_at: 0,
            expired: true,
        }));
        let (tx, _rx) = mpsc::channel(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.authenticated = true;
        c.expire_at = now_unix() + 5;
        assert_eq!(c.proactive_refresh(25).await.unwrap().code, 3005);

        // Transport failure → DisconnectServerError (3004).
        let node = node_with_refresh(Arc::new(ErrRefresh));
        let (tx, _rx) = mpsc::channel(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.authenticated = true;
        c.expire_at = now_unix() + 5;
        assert_eq!(c.proactive_refresh(25).await.unwrap().code, 3004);
    }

    #[tokio::test]
    async fn check_expired_honours_deadline_and_grace() {
        let node = Node::new();
        let (tx, _rx) = mpsc::channel(16);
        let mut c = node.new_client(tx, ProtocolType::Json);

        // Unauthenticated / no expiry → never expired.
        assert!(c.check_expired().is_none());
        c.authenticated = true;
        assert!(c.check_expired().is_none());

        // Within the grace window (just expired) → not yet closed.
        c.expire_at = now_unix() - 1;
        assert!(
            c.check_expired().is_none(),
            "must allow the 25s grace window"
        );

        // Past the grace window → DisconnectExpired (3005).
        c.expire_at = now_unix() - 100;
        assert_eq!(c.check_expired().unwrap().code, 3005);

        // A subscription expired past grace → DisconnectSubExpired (3006).
        c.expire_at = 0;
        c.subscriptions.insert(
            "ch".into(),
            SubState {
                expire_at: now_unix() - 100,
                ..Default::default()
            },
        );
        assert_eq!(c.check_expired().unwrap().code, 3006);
    }
}
