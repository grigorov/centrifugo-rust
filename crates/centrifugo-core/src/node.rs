//! The `Node` ties the `Hub` and a `Broker` together and owns the local
//! publication fan-out. A publication is encoded **once** and the resulting
//! frame bytes are cloned + `try_send`'d to each subscriber's bounded queue;
//! a full (slow) or closed queue causes that client to be dropped, never
//! blocking the broadcaster.

use std::sync::Arc;

use centrifugo_protocol::codec::{self, ProtocolType};
use centrifugo_protocol::messages::{ClientInfo, Publication};
use centrifugo_protocol::{Push, PushType, Raw};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

use crate::client::Client;
use crate::engine::Broker;
use crate::hub::Hub;
use crate::memory::MemoryBroker;

pub struct Node {
    hub: Arc<Hub>,
    broker: Arc<dyn Broker>,
}

impl Node {
    /// Build a single-node memory node. The broker's route callback performs
    /// local fan-out against the hub.
    pub fn new() -> Arc<Self> {
        let hub = Arc::new(Hub::new());
        let hub_for_route = hub.clone();
        let broker: Arc<dyn Broker> = Arc::new(MemoryBroker::new(move |channel, data, info| {
            deliver_publication(&hub_for_route, &channel, &data, info);
        }));
        Arc::new(Node { hub, broker })
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn broker(&self) -> &Arc<dyn Broker> {
        &self.broker
    }

    /// Create a per-connection client bound to this node, writing frames to `tx`.
    pub fn new_client(self: &Arc<Self>, tx: Sender<Vec<u8>>, proto: ProtocolType) -> Client {
        Client::new(self.clone(), tx, proto)
    }

    /// Remove a connection (on socket close).
    pub fn remove(&self, id: &str) {
        self.hub.remove(id);
    }
}

/// Encode a publication push once per protocol and fan it out to all subscribers
/// of `channel`, sending each subscriber the frame matching its protocol.
fn deliver_publication(hub: &Hub, channel: &str, data: &[u8], info: Option<ClientInfo>) {
    let publication = Publication {
        data: Some(Raw::from_bytes(data)),
        info,
        ..Default::default()
    };
    // Encode once per protocol (lazily simple: build both; either may be unused).
    let json_frame = make_push_frame(ProtocolType::Json, channel, &publication);
    let pb_frame = make_push_frame(ProtocolType::Protobuf, channel, &publication);

    for handle in hub.subscribers(channel) {
        let frame = match handle.proto {
            ProtocolType::Json => &json_frame,
            ProtocolType::Protobuf => &pb_frame,
        };
        let Some(bytes) = frame else { continue };
        match handle.tx.try_send(bytes.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                // Slow or gone consumer: drop it so it cannot block the broadcaster.
                // (Full DisconnectSlow close-frame semantics arrive in M2.5/M2.6.)
                hub.remove(&handle.id);
            }
        }
    }
}

/// Build the full push frame (Reply with id==0 carrying the encoded Publication
/// Push) for one protocol.
fn make_push_frame(
    proto: ProtocolType,
    channel: &str,
    publication: &Publication,
) -> Option<Vec<u8>> {
    let data = codec::encode_result(proto, publication).ok()?;
    let push = Push::new(PushType::Publication, channel, Some(data));
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

        let (tx_b, mut rx_b) = mpsc::channel::<Vec<u8>>(16);
        let mut sub = node.new_client(tx_b, ProtocolType::Json);
        sub.handle_command(&connect_cmd(1));
        sub.handle_command(&subscribe_cmd(2, "news"));

        let (tx_a, _rx_a) = mpsc::channel::<Vec<u8>>(16);
        let mut pubr = node.new_client(tx_a, ProtocolType::Json);
        pubr.handle_command(&connect_cmd(1));
        let pub_replies = pubr.handle_command(&publish_cmd(2, "news", r#"{"msg":"hi"}"#));
        assert!(pub_replies[0].error.is_none());

        let frame = tokio::time::timeout(Duration::from_secs(1), rx_b.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
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
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(16);
        let mut c = node.new_client(tx, ProtocolType::Json);
        let r1 = c.handle_command(&connect_cmd(1));
        assert!(r1[0].error.is_none());
        let r2 = c.handle_command(&connect_cmd(2));
        assert_eq!(r2[0].error.as_ref().unwrap().code, 107); // bad request
    }
}
