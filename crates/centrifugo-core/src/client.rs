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
    PublishResult, RefreshRequest, RefreshResult, SubRefreshRequest, SubRefreshResult,
    SubscribeRequest, SubscribeResult, UnsubscribeRequest, UnsubscribeResult,
};
use centrifugo_protocol::{Command, Disconnect, Error, MethodType, ProtocolType, Raw, Reply};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::hub::{ClientHandle, ClientId, Out};
use crate::node::Node;
use crate::proxy::{ProxyConnectOutcome, ProxyConnectRequest};

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
    authenticated: bool,
    /// Connection info bytes from the token (becomes ClientInfo.conn_info).
    conn_info: Option<Vec<u8>>,
    /// Token expiry (unix seconds); 0 means no expiry.
    expire_at: i64,
    /// Channels this client is subscribed to → per-subscription state.
    subscriptions: HashMap<String, SubState>,
    node: Arc<Node>,
    tx: Sender<Out>,
}

impl Client {
    pub fn new(node: Arc<Node>, tx: Sender<Out>, proto: ProtocolType) -> Self {
        Client {
            id: String::new(),
            user: String::new(),
            proto,
            authenticated: false,
            conn_info: None,
            expire_at: 0,
            subscriptions: HashMap::new(),
            node,
            tx,
        }
    }

    fn is_subscribed(&self, channel: &str) -> bool {
        self.subscriptions.contains_key(channel)
    }

    /// Build the ClientInfo for this connection (publisher/presence/join-leave).
    fn client_info(&self) -> ClientInfo {
        ClientInfo {
            user: self.user.clone(),
            client: self.id.clone(),
            conn_info: self.conn_info.as_ref().map(|b| Raw::from_bytes(b.clone())),
            chan_info: None,
        }
    }

    /// Dispatch one command. Returns replies and/or a disconnect.
    pub async fn handle_command(&mut self, cmd: &Command) -> CommandOutcome {
        // CONNECT must be the first command; otherwise close the connection
        // (Go centrifuge sends DisconnectBadRequest).
        if !self.authenticated && cmd.method != MethodType::Connect {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        match cmd.method {
            // CONNECT may itself decide to disconnect (invalid token), so it
            // returns its own outcome.
            MethodType::Connect => self.on_connect(cmd).await,
            MethodType::Subscribe => CommandOutcome::replies(self.on_subscribe(cmd).await),
            MethodType::Publish => CommandOutcome::replies(self.on_publish(cmd).await),
            MethodType::Unsubscribe => CommandOutcome::replies(self.on_unsubscribe(cmd).await),
            MethodType::Ping => {
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &PingResult {})])
            }
            MethodType::Refresh => self.on_refresh(cmd),
            // SEND is fire-and-forget: no reply.
            MethodType::Send => CommandOutcome::replies(vec![]),
            // RPC with no registered handler -> method not found (matches
            // centrifugo's OnRPC).
            MethodType::Rpc => {
                CommandOutcome::replies(vec![Reply::err(cmd.id, Error::method_not_found())])
            }
            MethodType::Presence => CommandOutcome::replies(self.on_presence(cmd).await),
            MethodType::PresenceStats => {
                CommandOutcome::replies(self.on_presence_stats(cmd).await)
            }
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
        } else if let Some(proxy) = self.node.connect_proxy() {
            let preq = ProxyConnectRequest {
                client: self.id.clone(),
                transport: "websocket".into(),
                protocol: match self.proto {
                    ProtocolType::Json => "json".into(),
                    ProtocolType::Protobuf => "protobuf".into(),
                },
                data: None,
            };
            match proxy.connect(preq).await {
                Ok(ProxyConnectOutcome::Credentials(r)) => {
                    Some((r.user, r.info, r.expire_at, Vec::new()))
                }
                // Explicit proxy error -> relay that code/message as an error reply.
                Ok(ProxyConnectOutcome::Error { code, message }) => {
                    return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::new(code, message))]);
                }
                // Explicit proxy disconnect -> close with that code/reason.
                Ok(ProxyConnectOutcome::Disconnect { code, reason }) => {
                    return CommandOutcome::disconnect(Disconnect::new(code, reason, false));
                }
                // No credentials -> fall through to anonymous/insecure handling.
                Ok(ProxyConnectOutcome::NoCredentials) => None,
                // Transport failure -> ErrorInternal (100) reply, matching Go.
                Err(_) => return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::internal())]),
            }
        } else {
            None
        };

        // Anonymous/insecure fallback to an empty-user connection when no
        // identity was established; otherwise reject.
        let (user, info, expire_at, channels) = match creds {
            Some(c) => c,
            None if self.node.client_anonymous() || self.node.client_insecure() => {
                (String::new(), None, 0, Vec::new())
            }
            None => return CommandOutcome::disconnect(Disconnect::bad_request()),
        };

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
        self.node.hub().add(ClientHandle {
            id: self.id.clone(),
            user: self.user.clone(),
            proto: self.proto,
            tx: self.tx.clone(),
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
        if join_leave {
            self.node.publish_join(channel, self.client_info()).await;
        }
        result
    }

    async fn on_subscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: SubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if req.channel.is_empty() {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        let (presence, join_leave, history_recover, anonymous, server_side) =
            match self.node.channel_options(&req.channel) {
                Some(o) => (
                    o.presence,
                    o.join_leave,
                    o.history_recover,
                    o.anonymous,
                    o.server_side,
                ),
                None => return vec![Reply::err(cmd.id, Error::unknown_channel())],
            };
        // Server-side channels cannot be subscribed to directly by clients.
        if server_side {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        // Anonymous (empty-user) clients need the channel's `anonymous` option,
        // unless the server runs in insecure mode.
        if !anonymous && self.user.is_empty() && !self.node.client_insecure() {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        // User-limited channels (`name#u1,u2`): only listed users may subscribe.
        if !user_allowed(&req.channel, &self.user) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        // Private ($-prefixed) channels require a valid subscription token whose
        // client + channel match this connection.
        if self.node.is_private(&req.channel) {
            if req.token.is_empty() {
                return vec![Reply::err(cmd.id, Error::permission_denied())];
            }
            match self.node.verifier().verify_subscribe_token(&req.token) {
                Ok(t) => {
                    if t.client != self.id || t.channel != req.channel {
                        return vec![Reply::err(cmd.id, Error::permission_denied())];
                    }
                }
                Err(VerifyError::Expired) => {
                    return vec![Reply::err(cmd.id, Error::token_expired())]
                }
                Err(VerifyError::Invalid) => {
                    return vec![Reply::err(cmd.id, Error::permission_denied())]
                }
            }
        }
        self.node.hub().subscribe(&self.id, &req.channel);
        let _ = self.node.engine().subscribe(&req.channel).await;

        if presence {
            self.node
                .add_presence(&req.channel, &self.id, self.client_info())
                .await;
        }
        self.subscriptions.insert(
            req.channel.clone(),
            SubState {
                presence,
                join_leave,
                recoverable: history_recover,
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
            self.node
                .publish_join(&req.channel, self.client_info())
                .await;
        }
        vec![reply]
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

    async fn on_publish(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PublishRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if req.channel.is_empty() {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        let data = req.data.as_ref().map(|r| r.as_bytes()).unwrap_or(b"null");
        // A client-initiated publication carries the publisher's ClientInfo,
        // matching Go centrifuge behavior.
        let info = self.client_info();
        self.node.publish(&req.channel, data, Some(info)).await;
        vec![ok_reply(self.proto, cmd.id, &PublishResult {})]
    }

    async fn on_unsubscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: UnsubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if !req.channel.is_empty() {
            self.unsubscribe_channel(&req.channel).await;
        }
        vec![ok_reply(self.proto, cmd.id, &UnsubscribeResult {})]
    }

    async fn unsubscribe_channel(&mut self, channel: &str) {
        // Use the options captured at subscribe time (a no-op if not subscribed).
        let Some(state) = self.subscriptions.remove(channel) else {
            return;
        };
        self.node.hub().unsubscribe(&self.id, channel);
        let _ = self.node.engine().unsubscribe(channel).await;
        if state.presence {
            self.node.remove_presence(channel, &self.id).await;
        }
        if state.join_leave {
            self.node.publish_leave(channel, self.client_info()).await;
        }
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

    /// Called when the connection closes: publish Leave + clear presence for all
    /// subscribed channels, then unregister from the hub.
    pub async fn on_disconnect(&mut self) {
        let channels = std::mem::take(&mut self.subscriptions);
        for (ch, state) in &channels {
            if state.presence {
                self.node.remove_presence(ch, &self.id).await;
            }
            if state.join_leave {
                self.node.publish_leave(ch, self.client_info()).await;
            }
        }
        self.node.remove(&self.id);
    }

    fn on_refresh(&mut self, cmd: &Command) -> CommandOutcome {
        let req: RefreshRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        // Go (centrifuge handleRefresh): an empty refresh token is rejected with
        // DisconnectBadRequest (3003) before any verification.
        if req.token.is_empty() {
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
            None => return CommandOutcome::replies(vec![Reply::err(
                cmd.id,
                Error::permission_denied(),
            )]),
        }
        match self.node.verifier().verify_subscribe_token(&req.token) {
            Ok(t) => {
                if t.client != self.id || t.channel != req.channel {
                    return CommandOutcome::disconnect(Disconnect::invalid_token());
                }
                let expire_at = t.expire_at;
                let (expires, ttl) = if expire_at > 0 {
                    let ttl = expire_at - now_unix();
                    if ttl <= 0 {
                        return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::expired())]);
                    }
                    (true, ttl as u32)
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

/// (seq, gen) from offset (centrifuge recovery.UnpackUint64).
fn unpack_offset(v: u64) -> (u32, u32) {
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
