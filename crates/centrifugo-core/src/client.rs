//! Per-connection session state machine. Dispatches the client commands needed
//! for the M1 vertical slice (CONNECT/SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PING) in
//! insecure mode. Full method coverage, CONNECT-first disconnect semantics, and
//! auth arrive in M2/M3.

use std::sync::Arc;

use centrifugo_protocol::messages::{
    ClientInfo, ConnectResult, PingResult, PublishRequest, PublishResult, SubscribeRequest,
    SubscribeResult, UnsubscribeRequest, UnsubscribeResult,
};
use centrifugo_protocol::{Command, Error, MethodType, Raw, Reply};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::hub::{ClientHandle, ClientId};
use crate::node::Node;

pub struct Client {
    pub id: ClientId,
    pub user: String,
    authenticated: bool,
    node: Arc<Node>,
    tx: Sender<Vec<u8>>,
}

impl Client {
    pub fn new(node: Arc<Node>, tx: Sender<Vec<u8>>) -> Self {
        Client {
            id: String::new(),
            user: String::new(),
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
            MethodType::Ping => vec![ok_reply(cmd.id, &PingResult {})],
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
            tx: self.tx.clone(),
        });
        let result = ConnectResult {
            client: self.id.clone(),
            version: String::new(),
            ..Default::default()
        };
        vec![ok_reply(cmd.id, &result)]
    }

    fn on_subscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: SubscribeRequest = match parse_params(&cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if req.channel.is_empty() {
            return vec![Reply::err(cmd.id, Error::bad_request())];
        }
        self.node.hub().subscribe(&self.id, &req.channel);
        let _ = self.node.broker().subscribe(&req.channel);
        vec![ok_reply(cmd.id, &SubscribeResult::default())]
    }

    fn on_publish(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: PublishRequest = match parse_params(&cmd.params) {
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
        vec![ok_reply(cmd.id, &PublishResult {})]
    }

    fn on_unsubscribe(&mut self, cmd: &Command) -> Vec<Reply> {
        let req: UnsubscribeRequest = match parse_params(&cmd.params) {
            Ok(r) => r,
            Err(e) => return vec![Reply::err(cmd.id, e)],
        };
        if !req.channel.is_empty() {
            self.node.hub().unsubscribe(&self.id, &req.channel);
            let _ = self.node.broker().unsubscribe(&req.channel);
        }
        vec![ok_reply(cmd.id, &UnsubscribeResult {})]
    }
}

/// Parse a typed request from optional inline-raw params. Missing params → default.
fn parse_params<T: DeserializeOwned + Default>(params: &Option<Raw>) -> Result<T, Error> {
    match params {
        None => Ok(T::default()),
        Some(raw) => serde_json::from_slice(raw.as_bytes()).map_err(|_| Error::bad_request()),
    }
}

/// Build an ok reply, falling back to an internal error if serialization fails
/// (which it should not for our result types).
fn ok_reply<T: Serialize>(id: u32, value: &T) -> Reply {
    Reply::ok_value(id, value).unwrap_or_else(|_| Reply::err(id, Error::internal()))
}
