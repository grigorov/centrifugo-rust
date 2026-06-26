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
    pub fn handle_command(&mut self, cmd: &Command) -> CommandOutcome {
        // CONNECT must be the first command; otherwise close the connection
        // (Go centrifuge sends DisconnectBadRequest).
        if !self.authenticated && cmd.method != MethodType::Connect {
            return CommandOutcome::disconnect(Disconnect::bad_request());
        }
        match cmd.method {
            // CONNECT may itself decide to disconnect (invalid token), so it
            // returns its own outcome.
            MethodType::Connect => self.on_connect(cmd),
            MethodType::Subscribe => CommandOutcome::replies(self.on_subscribe(cmd)),
            MethodType::Publish => CommandOutcome::replies(self.on_publish(cmd)),
            MethodType::Unsubscribe => CommandOutcome::replies(self.on_unsubscribe(cmd)),
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
            MethodType::Presence => CommandOutcome::replies(self.on_presence(cmd)),
            MethodType::PresenceStats => CommandOutcome::replies(self.on_presence_stats(cmd)),
            MethodType::History => CommandOutcome::replies(self.on_history(cmd)),
            // SUB_REFRESH lands with private channels (M6).
            MethodType::SubRefresh => {
                CommandOutcome::replies(vec![Reply::err(cmd.id, Error::not_available())])
            }
        }
    }

    fn on_connect(&mut self, cmd: &Command) -> CommandOutcome {
        if self.authenticated {
            return CommandOutcome::replies(vec![Reply::err(cmd.id, Error::bad_request())]);
        }
        let req: ConnectRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };

        // Determine identity. Insecure mode skips auth; otherwise a token is
        // required and verified.
        let (user, info, expire_at) = if self.node.client_insecure() {
            (String::new(), None, 0)
        } else if !req.token.is_empty() {
            match self.node.verifier().verify_connect_token(&req.token) {
                Ok(ct) => (ct.user, ct.info, ct.expire_at),
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
        } else {
            // No token and not insecure: reject the connection.
            return CommandOutcome::disconnect(Disconnect::invalid_token());
        };

        self.id = Uuid::new_v4().to_string();
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

    fn on_subscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: SubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if req.channel.is_empty() {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        self.node.hub().subscribe(&self.id, &req.channel);
        let _ = self.node.broker().subscribe(&req.channel);

        let (presence, join_leave) = {
            let o = self.node.opts();
            (o.presence, o.join_leave)
        };
        if presence {
            self.node
                .add_presence(&req.channel, &self.id, self.client_info());
        }
        if !self.subscriptions.iter().any(|c| c == &req.channel) {
            self.subscriptions.push(req.channel.clone());
        }

        // Recovery (when the channel offers it via history_recover).
        let mut result = SubscribeResult::default();
        if self.node.opts().history_recover {
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
                    .history_since(&req.channel, cmd_offset, &req.epoch);
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
                let (_pubs, top) = self.node.history(&req.channel);
                result.epoch = top.epoch;
                encode_position(&mut result, top.offset, use_seq_gen);
            }
        }

        let reply = ok_reply(self.proto, cmd.id, &result);
        // Join is published after the subscribe reply; the joiner is now a
        // subscriber (matches centrifuge ordering).
        if join_leave {
            self.node.publish_join(&req.channel, self.client_info());
        }
        vec![reply]
    }

    fn on_history(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: HistoryRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if !self.node.opts().history_enabled() {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let (mut publications, _top) = self.node.history(&req.channel);
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

    fn on_publish(&mut self, cmd: &Command) -> Vec<Reply> {
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
        self.node.publish(&req.channel, data, Some(info));
        vec![ok_reply(self.proto, cmd.id, &PublishResult {})]
    }

    fn on_unsubscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: UnsubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if !req.channel.is_empty() {
            self.unsubscribe_channel(&req.channel);
        }
        vec![ok_reply(self.proto, cmd.id, &UnsubscribeResult {})]
    }

    fn unsubscribe_channel(&mut self, channel: &str) {
        self.node.hub().unsubscribe(&self.id, channel);
        let _ = self.node.broker().unsubscribe(channel);
        let (presence, join_leave) = {
            let o = self.node.opts();
            (o.presence, o.join_leave)
        };
        if presence {
            self.node.remove_presence(channel, &self.id);
        }
        if join_leave {
            self.node.publish_leave(channel, self.client_info());
        }
        self.subscriptions.retain(|c| c != channel);
    }

    fn on_presence(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PresenceRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        let (presence, disable_for_client) = {
            let o = self.node.opts();
            (o.presence, o.presence_disable_for_client)
        };
        if !presence || disable_for_client {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let result = PresenceResult {
            presence: self.node.presence(&req.channel),
        };
        vec![ok_reply(self.proto, cmd.id, &result)]
    }

    fn on_presence_stats(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PresenceStatsRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        let (presence, disable_for_client) = {
            let o = self.node.opts();
            (o.presence, o.presence_disable_for_client)
        };
        if !presence || disable_for_client {
            return vec![Reply::err(cmd.id, Error::not_available())];
        }
        if !self.is_subscribed(&req.channel) {
            return vec![Reply::err(cmd.id, Error::permission_denied())];
        }
        let (num_clients, num_users) = self.node.presence_stats(&req.channel);
        let result = PresenceStatsResult {
            num_clients,
            num_users,
        };
        vec![ok_reply(self.proto, cmd.id, &result)]
    }

    /// Called when the connection closes: publish Leave + clear presence for all
    /// subscribed channels, then unregister from the hub.
    pub fn on_disconnect(&mut self) {
        let channels = std::mem::take(&mut self.subscriptions);
        let (presence, join_leave) = {
            let o = self.node.opts();
            (o.presence, o.join_leave)
        };
        for ch in &channels {
            if presence {
                self.node.remove_presence(ch, &self.id);
            }
            if join_leave {
                self.node.publish_leave(ch, self.client_info());
            }
        }
        self.node.remove(&self.id);
    }

    fn on_refresh(&mut self, cmd: &Command) -> CommandOutcome {
        let req: RefreshRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return CommandOutcome::replies(vec![Reply::err(cmd.id, e)]),
        };
        match self.node.verifier().verify_connect_token(&req.token) {
            Ok(ct) => {
                if !ct.user.is_empty() {
                    self.user = ct.user;
                }
                self.conn_info = ct.info;
                self.expire_at = ct.expire_at;
                let (expires, ttl) = if ct.expire_at > 0 {
                    (true, (ct.expire_at - now_unix()).max(0) as u32)
                } else {
                    (false, 0)
                };
                let result = RefreshResult {
                    client: self.id.clone(),
                    version: String::new(),
                    expires,
                    ttl,
                };
                CommandOutcome::replies(vec![ok_reply(self.proto, cmd.id, &result)])
            }
            // Expired refresh token -> ErrorExpired (110).
            Err(VerifyError::Expired) => {
                CommandOutcome::replies(vec![Reply::err(cmd.id, Error::expired())])
            }
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
