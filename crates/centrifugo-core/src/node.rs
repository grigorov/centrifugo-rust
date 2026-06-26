//! The `Node` ties the `Hub` and a `Broker` together and owns the local
//! publication fan-out. A publication is encoded **once** and the resulting
//! frame bytes are cloned + `try_send`'d to each subscriber's bounded queue;
//! a full (slow) or closed queue causes that client to be dropped, never
//! blocking the broadcaster.

use std::collections::HashMap;
use std::sync::Arc;

use centrifugo_auth::TokenVerifier;
use centrifugo_protocol::codec::{self, ProtocolType, WireType};
use centrifugo_protocol::messages::{ClientInfo, Join, Leave, Publication};
use centrifugo_protocol::{Push, PushType, Raw};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

use crate::client::Client;
use crate::engine::Broker;
use crate::hub::{Hub, Out};
use crate::memory::MemoryBroker;

/// Channel options. In M4 a single default set applies to every channel; full
/// per-namespace options arrive in M6.
#[derive(Debug, Clone, Default)]
pub struct ChannelOptions {
    pub presence: bool,
    pub join_leave: bool,
    pub presence_disable_for_client: bool,
}

pub struct Node {
    hub: Arc<Hub>,
    broker: Arc<dyn Broker>,
    verifier: Arc<TokenVerifier>,
    client_insecure: bool,
    opts: ChannelOptions,
    /// In-memory presence: channel -> (clientID -> ClientInfo). The memory
    /// engine has no TTL (matches centrifuge MemoryEngine).
    presence: Mutex<HashMap<String, HashMap<String, ClientInfo>>>,
}

impl Node {
    /// Build a single-node memory node with the given verifier, insecure flag,
    /// and default channel options.
    pub fn new_with(
        verifier: Arc<TokenVerifier>,
        client_insecure: bool,
        opts: ChannelOptions,
    ) -> Arc<Self> {
        let hub = Arc::new(Hub::new());
        let hub_for_route = hub.clone();
        let broker: Arc<dyn Broker> = Arc::new(MemoryBroker::new(move |channel, data, info| {
            deliver_publication(&hub_for_route, &channel, &data, info);
        }));
        Arc::new(Node {
            hub,
            broker,
            verifier,
            client_insecure,
            opts,
            presence: Mutex::new(HashMap::new()),
        })
    }

    /// Build an insecure single-node memory node (no token, no presence). Used
    /// by tests and the `--client_insecure` server mode default.
    pub fn new() -> Arc<Self> {
        Self::new_with(
            Arc::new(TokenVerifier::default()),
            true,
            ChannelOptions::default(),
        )
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn broker(&self) -> &Arc<dyn Broker> {
        &self.broker
    }

    pub fn verifier(&self) -> &TokenVerifier {
        &self.verifier
    }

    pub fn client_insecure(&self) -> bool {
        self.client_insecure
    }

    pub fn opts(&self) -> &ChannelOptions {
        &self.opts
    }

    // ---- presence ----

    pub fn add_presence(&self, channel: &str, client_id: &str, info: ClientInfo) {
        self.presence
            .lock()
            .entry(channel.to_string())
            .or_default()
            .insert(client_id.to_string(), info);
    }

    pub fn remove_presence(&self, channel: &str, client_id: &str) {
        let mut p = self.presence.lock();
        if let Some(chan) = p.get_mut(channel) {
            chan.remove(client_id);
            if chan.is_empty() {
                p.remove(channel);
            }
        }
    }

    pub fn presence(&self, channel: &str) -> HashMap<String, ClientInfo> {
        self.presence
            .lock()
            .get(channel)
            .cloned()
            .unwrap_or_default()
    }

    /// (num_clients, num_users): total entries and distinct user ids.
    pub fn presence_stats(&self, channel: &str) -> (u32, u32) {
        let p = self.presence.lock();
        let Some(chan) = p.get(channel) else {
            return (0, 0);
        };
        let num_clients = chan.len() as u32;
        let users: std::collections::HashSet<&str> =
            chan.values().map(|ci| ci.user.as_str()).collect();
        (num_clients, users.len() as u32)
    }

    // ---- join / leave ----

    pub fn publish_join(&self, channel: &str, info: ClientInfo) {
        deliver_push(&self.hub, channel, PushType::Join, &Join { info });
    }

    pub fn publish_leave(&self, channel: &str, info: ClientInfo) {
        deliver_push(&self.hub, channel, PushType::Leave, &Leave { info });
    }

    /// Create a per-connection client bound to this node, writing to `tx`.
    pub fn new_client(self: &Arc<Self>, tx: Sender<Out>, proto: ProtocolType) -> Client {
        Client::new(self.clone(), tx, proto)
    }

    /// Remove a connection (on socket close).
    pub fn remove(&self, id: &str) {
        self.hub.remove(id);
    }
}

/// Route a client/API publication into local fan-out as a Publication push.
fn deliver_publication(hub: &Hub, channel: &str, data: &[u8], info: Option<ClientInfo>) {
    let publication = Publication {
        data: Some(Raw::from_bytes(data)),
        info,
        ..Default::default()
    };
    deliver_push(hub, channel, PushType::Publication, &publication);
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
                hub.remove(&handle.id);
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
        sub.handle_command(&connect_cmd(1));
        sub.handle_command(&subscribe_cmd(2, "news"));

        let (tx_a, _rx_a) = mpsc::channel::<Out>(16);
        let mut pubr = node.new_client(tx_a, ProtocolType::Json);
        pubr.handle_command(&connect_cmd(1));
        let pub_replies = pubr.handle_command(&publish_cmd(2, "news", r#"{"msg":"hi"}"#));
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
    async fn second_connect_is_rejected() {
        let node = Node::new();
        let (tx, _rx) = mpsc::channel::<Out>(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        let r1 = c.handle_command(&connect_cmd(1));
        assert!(r1.replies[0].error.is_none());
        let r2 = c.handle_command(&connect_cmd(2));
        assert_eq!(r2.replies[0].error.as_ref().unwrap().code, 107); // bad request
    }

    #[tokio::test]
    async fn send_has_no_reply_and_unimplemented_methods_are_not_available() {
        let node = Node::new();
        let (tx, _rx) = mpsc::channel::<Out>(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        c.handle_command(&connect_cmd(1));

        let send = Command {
            id: 0,
            method: MethodType::Send,
            params: Some(raw(r#"{"data":{}}"#.into())),
        };
        assert!(
            c.handle_command(&send).replies.is_empty(),
            "SEND must produce no reply"
        );

        let presence = Command {
            id: 5,
            method: MethodType::Presence,
            params: Some(raw(r#"{"channel":"x"}"#.into())),
        };
        let r = c.handle_command(&presence);
        assert_eq!(r.replies[0].error.as_ref().unwrap().code, 108); // not available
    }
}
