//! The `Node` ties the `Hub` and a `Broker` together and owns the local
//! publication fan-out. A publication is encoded **once** and the resulting
//! frame bytes are cloned + `try_send`'d to each subscriber's bounded queue;
//! a full (slow) or closed queue causes that client to be dropped, never
//! blocking the broadcaster.

use std::sync::Arc;

use centrifugo_protocol::command::encode_raw;
use centrifugo_protocol::json::encode_reply;
use centrifugo_protocol::messages::{ClientInfo, Publication};
use centrifugo_protocol::{Push, PushType, Raw, Reply};
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
    pub fn new_client(self: &Arc<Self>, tx: Sender<Vec<u8>>) -> Client {
        Client::new(self.clone(), tx)
    }

    /// Remove a connection (on socket close).
    pub fn remove(&self, id: &str) {
        self.hub.remove(id);
    }
}

/// Encode a publication push once and fan it out to all subscribers of `channel`.
fn deliver_publication(hub: &Hub, channel: &str, data: &[u8], info: Option<ClientInfo>) {
    let publication = Publication {
        data: Some(Raw::from_bytes(data)),
        info,
        ..Default::default()
    };
    let encoded_pub = match encode_raw(&publication) {
        Ok(r) => r,
        Err(_) => return,
    };
    let push = Push::new(PushType::Publication, channel, Some(encoded_pub));
    let reply = match Reply::push(&push) {
        Ok(r) => r,
        Err(_) => return,
    };
    let frame = match encode_reply(&reply) {
        Ok(b) => b,
        Err(_) => return,
    };

    for handle in hub.subscribers(channel) {
        match handle.tx.try_send(frame.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                // Slow or gone consumer: drop it so it cannot block the broadcaster.
                // (Full DisconnectSlow close-frame semantics arrive in M2.)
                hub.remove(&handle.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use centrifugo_protocol::{Command, MethodType, Raw};
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
        let mut sub = node.new_client(tx_b);
        sub.handle_command(&connect_cmd(1));
        sub.handle_command(&subscribe_cmd(2, "news"));

        let (tx_a, _rx_a) = mpsc::channel::<Vec<u8>>(16);
        let mut pubr = node.new_client(tx_a);
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
        let mut c = node.new_client(tx);
        let r1 = c.handle_command(&connect_cmd(1));
        assert!(r1[0].error.is_none());
        let r2 = c.handle_command(&connect_cmd(2));
        assert_eq!(r2[0].error.as_ref().unwrap().code, 107); // bad request
    }
}
