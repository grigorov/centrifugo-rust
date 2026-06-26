//! Per-connection session state machine. Dispatches the client commands needed
//! for the M1 vertical slice (CONNECT/SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PING) in
//! insecure mode. Full method coverage, CONNECT-first disconnect semantics, and
//! auth arrive in M2/M3.

use std::sync::Arc;

use centrifugo_protocol::codec::{self, WireType};
use centrifugo_protocol::messages::{
    ClientInfo, ConnectResult, PingResult, PublishRequest, PublishResult, SubscribeRequest,
    SubscribeResult, UnsubscribeRequest, UnsubscribeResult,
};
use centrifugo_protocol::{Command, Error, MethodType, ProtocolType, Raw, Reply};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::hub::{ClientHandle, ClientId};
use crate::node::Node;

pub struct Client {
    pub id: ClientId,
    pub user: String,
    proto: ProtocolType,
    authenticated: bool,
    node: Arc<Node>,
    tx: Sender<Vec<u8>>,
}

impl Client {
    pub fn new(node: Arc<Node>, tx: Sender<Vec<u8>>, proto: ProtocolType) -> Self {
        Client {
            id: String::new(),
            user: String::new(),
            proto,
            authenticated: false,
            node,
            tx,
        }
    }

    /// Dispatch one command, returning zero or more replies to send back.
    pub fn handle_command(&mut self, cmd: &Command) -> Vec<Reply> {
        // CONNECT must be first. (M2 upgrades this to a hard disconnect.)
        if !self.authenticated && cmd.method != MethodType::Connect {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        match cmd.method {
            MethodType::Connect => self.on_connect(cmd),
            MethodType::Subscribe => self.on_subscribe(cmd),
            MethodType::Publish => self.on_publish(cmd),
            MethodType::Unsubscribe => self.on_unsubscribe(cmd),
            MethodType::Ping => vec![ok_reply(self.proto, cmd.id, &PingResult {})],
            // Remaining methods land in later milestones.
            _ => vec![Reply::err(cmd.id, Error::method_not_found())],
        }
    }

    fn on_connect(&mut self, cmd: &Command) -> Vec<Reply> {
        if self.authenticated {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        // Insecure/anonymous mode for M1: assign a fresh client id, empty user.
        self.id = Uuid::new_v4().to_string();
        self.user = String::new();
        self.authenticated = true;
        self.node.hub().add(ClientHandle {
            id: self.id.clone(),
            user: self.user.clone(),
            proto: self.proto,
            tx: self.tx.clone(),
        });
        let result = ConnectResult {
            client: self.id.clone(),
            version: String::new(),
            ..Default::default()
        };
        vec![ok_reply(self.proto, cmd.id, &result)]
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
        vec![ok_reply(self.proto, cmd.id, &SubscribeResult::default())]
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
        let info = ClientInfo {
            user: self.user.clone(),
            client: self.id.clone(),
            conn_info: None,
            chan_info: None,
        };
        if let Err(_e) = self.node.broker().publish(&req.channel, data, Some(info)) {
            return vec![Reply::err(cmd.id, Error::internal())];
        }
        vec![ok_reply(self.proto, cmd.id, &PublishResult {})]
    }

    fn on_unsubscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: UnsubscribeRequest = match parse_params(self.proto, &cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if !req.channel.is_empty() {
            self.node.hub().unsubscribe(&self.id, &req.channel);
            let _ = self.node.broker().unsubscribe(&req.channel);
        }
        vec![ok_reply(self.proto, cmd.id, &UnsubscribeResult {})]
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
