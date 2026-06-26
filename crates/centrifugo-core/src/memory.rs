//! Single-node in-memory broker. `publish` invokes a route callback supplied by
//! the `Node`; that callback performs the local fan-out (encode once, `try_send`
//! to each subscriber). `subscribe`/`unsubscribe` are no-ops because the hub
//! already tracks local subscriptions in the single-node case.

use std::sync::Arc;

use centrifugo_protocol::messages::ClientInfo;

use crate::engine::Broker;

type RouteFn = Arc<dyn Fn(String, Vec<u8>, Option<ClientInfo>) + Send + Sync>;

pub struct MemoryBroker {
    route: RouteFn,
}

impl MemoryBroker {
    pub fn new(
        route: impl Fn(String, Vec<u8>, Option<ClientInfo>) + Send + Sync + 'static,
    ) -> Self {
        MemoryBroker {
            route: Arc::new(route),
        }
    }
}

impl Broker for MemoryBroker {
    fn publish(&self, channel: &str, data: &[u8], info: Option<ClientInfo>) -> anyhow::Result<()> {
        (self.route)(channel.to_string(), data.to_vec(), info);
        Ok(())
    }

    fn subscribe(&self, _channel: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe(&self, _channel: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn publish_routes_to_callback() {
        let routed = Arc::new(Mutex::new(Vec::<(String, Vec<u8>)>::new()));
        let r2 = routed.clone();
        let broker = MemoryBroker::new(move |ch, data, _info| r2.lock().unwrap().push((ch, data)));
        broker.publish("news", br#"{"x":1}"#, None).unwrap();
        let got = routed.lock().unwrap();
        assert_eq!(got[0].0, "news");
        assert_eq!(got[0].1, br#"{"x":1}"#);
    }
}
