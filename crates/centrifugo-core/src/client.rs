//! Per-connection session state machine. Dispatches the client commands needed
//! for the M1 vertical slice (CONNECT/SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PING) in
//! insecure mode. Full method coverage, CONNECT-first disconnect semantics, and
//! auth arrive in M2/M3.

use std::sync::Arc;

use centrifugo_auth::VerifyError;
use centrifugo_protocol::codec::{self, WireType};
use centrifugo_protocol::messages::{
    ClientInfo, ConnectRequest, ConnectResult, HistoryRequest, HistoryResult, PingResult,
    PresenceRequest, PresenceResult, PresenceStatsRequest, PresenceStatsResult, PublishRequest,
    PublishResult, RefreshRequest, RefreshResult, SubscribeRequest, SubscribeResult,
    UnsubscribeRequest, UnsubscribeResult,
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

pub struct Client {
    pub id: ClientId,
    pub user: String,
    proto: ProtocolType,
    authenticated: bool,
    /// Connection info bytes from the token (becomes ClientInfo.conn_info).
    conn_info: Option<Vec<u8>>,
    /// Token expiry (unix seconds); 0 means no expiry.
    expire_at: i64,
    /// Channels this client is subscribed to (for presence/leave on disconnect).
    subscriptions: Vec<String>,
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
            subscriptions: Vec::new(),
            node,
            tx,
        }
    }

    fn is_subscribed(&self, channel: &str) -> bool {
        self.subscriptions.iter().any(|c| c == channel)
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
            // SUB_REFRESH lands with private channels (M6).
            MethodType::SubRefresh => {
                CommandOutcome::replies(vec![Reply::err(cmd.id, Error::not_available())])
            }
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
        let creds: Option<(String, Option<Vec<u8>>, i64)> = if !req.token.is_empty() {
            match self.node.verifier().verify_connect_token(&req.token) {
                Ok(ct) => {
                    // Insecure mode keeps the token's user/info but forces no expiry.
                    let expire_at = if self.node.client_insecure() {
                        0
                    } else {
                        ct.expire_at
                    };
                    Some((ct.user, ct.info, expire_at))
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
                Ok(ProxyConnectOutcome::Credentials(r)) => Some((r.user, r.info, r.expire_at)),
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
        let (user, info, expire_at) = match creds {
            Some(c) => c,
            None if self.node.client_anonymous() || self.node.client_insecure() => {
                (String::new(), None, 0)
            }
            None => return CommandOutcome::disconnect(Disconnect::bad_request()),
        };

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
            ..Default::default()
        };
        CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
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
        if !self.subscriptions.iter().any(|c| c == &req.channel) {
            self.subscriptions.push(req.channel.clone());
        }

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
        self.node.hub().unsubscribe(&self.id, channel);
        let _ = self.node.engine().unsubscribe(channel).await;
        let (presence, join_leave) = match self.node.channel_options(channel) {
            Some(o) => (o.presence, o.join_leave),
            None => (false, false),
        };
        if presence {
            self.node.remove_presence(channel, &self.id).await;
        }
        if join_leave {
            self.node.publish_leave(channel, self.client_info()).await;
        }
        self.subscriptions.retain(|c| c != channel);
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
        for ch in &channels {
            let (presence, join_leave) = match self.node.channel_options(ch) {
                Some(o) => (o.presence, o.join_leave),
                None => continue,
            };
            if presence {
                self.node.remove_presence(ch, &self.id).await;
            }
            if join_leave {
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

/// offset = gen<<32 | seq (centrifuge recovery.PackUint64).
fn pack_offset(seq: u32, gen: u32) -> u64 {
    ((gen as u64) << 32) | (seq as u64)
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
