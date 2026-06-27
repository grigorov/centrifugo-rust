//! Single-node in-memory engine. Owns the per-channel presence maps and history
//! streams (no TTL on presence, lazy TTL on history — matches centrifuge's
//! MemoryEngine). `publish`/`publish_join`/`publish_leave` invoke the `Node`'s
//! route callback for local fan-out; `subscribe`/`unsubscribe` are no-ops
//! because the hub already tracks local subscriptions in the single-node case.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use centrifugo_protocol::messages::{ClientInfo, Publication};
use centrifugo_protocol::Raw;
use parking_lot::Mutex;

use crate::engine::{Engine, NodeMessage, PublishOptions, RouteFn};
use crate::node::{new_epoch, now_unix, StreamPosition};

/// In-memory per-channel history stream: a monotonic offset, a random epoch
/// (detects stream loss), and a size-bounded ring of publications.
struct Stream {
    offset: u64,
    epoch: String,
    pubs: VecDeque<Publication>,
    /// Unix seconds when this stream's history expires (lazy TTL).
    expire_at: i64,
}

impl Stream {
    fn new() -> Self {
        Stream {
            offset: 0,
            epoch: new_epoch(),
            pubs: VecDeque::new(),
            expire_at: i64::MAX,
        }
    }
    fn position(&self) -> StreamPosition {
        StreamPosition {
            offset: self.offset,
            epoch: self.epoch.clone(),
        }
    }
}

pub struct MemoryEngine {
    route: RouteFn,
    /// channel -> (clientID -> ClientInfo). No TTL (matches centrifuge memory).
    presence: Mutex<HashMap<String, HashMap<String, ClientInfo>>>,
    /// channel -> Stream.
    history: Mutex<HashMap<String, Stream>>,
}

impl MemoryEngine {
    pub fn new(route: RouteFn) -> Self {
        MemoryEngine {
            route,
            presence: Mutex::new(HashMap::new()),
            history: Mutex::new(HashMap::new()),
        }
    }

    /// Append to history and return the offset assigned to this publication.
    fn add_to_history(
        &self,
        channel: &str,
        data: &[u8],
        info: Option<ClientInfo>,
        opts: PublishOptions,
    ) -> u64 {
        let mut hist = self.history.lock();
        let stream = hist.entry(channel.to_string()).or_insert_with(Stream::new);
        stream.offset += 1;
        let offset = stream.offset;
        let publication = Publication {
            data: Some(Raw::from_bytes(data)),
            info,
            offset,
            ..Default::default()
        };
        stream.pubs.push_back(publication);
        while stream.pubs.len() > opts.history_size {
            stream.pubs.pop_front();
        }
        stream.expire_at = now_unix() + opts.history_lifetime as i64;
        offset
    }

    /// On history-lifetime expiry, drop only the buffered publications but keep
    /// the stream's `offset` and `epoch` (matches centrifuge memstream `Clear()`:
    /// the meta — top offset + epoch — persists, since `memory_history_meta_ttl`
    /// defaults to 0 so streams are never removed). A caught-up client recovering
    /// after the window therefore still gets `recovered=true` with its last
    /// seq/gen, instead of a reset epoch/offset.
    fn evict_if_expired(hist: &mut HashMap<String, Stream>, channel: &str) {
        if let Some(s) = hist.get_mut(channel) {
            if now_unix() > s.expire_at {
                s.pubs.clear();
                s.expire_at = i64::MAX;
            }
        }
    }
}

#[async_trait]
impl Engine for MemoryEngine {
    async fn publish(
        &self,
        channel: &str,
        data: &[u8],
        info: Option<ClientInfo>,
        opts: PublishOptions,
    ) -> anyhow::Result<()> {
        // On a recoverable channel the live publication carries its history offset;
        // the route layer converts offset -> seq/gen (zeroing offset) per Go's
        // UseSeqGen default. Non-recoverable channels keep offset 0.
        let offset = if opts.history_enabled() {
            self.add_to_history(channel, data, info.clone(), opts)
        } else {
            0
        };
        let publication = Publication {
            data: Some(Raw::from_bytes(data)),
            info,
            offset,
            ..Default::default()
        };
        (self.route)(NodeMessage::Publication {
            channel: channel.to_string(),
            publication,
        });
        Ok(())
    }

    async fn publish_control(&self, _msg: crate::engine::ControlMessage) -> anyhow::Result<()> {
        // Single node, no bus: the Node applies server-side unsubscribe/disconnect to
        // the local hub directly (Node::unsubscribe_user/disconnect_user), and a NODE
        // ping to ourselves is redundant (self is seeded + live-refreshed in the
        // registry). Nothing to propagate.
        Ok(())
    }

    async fn publish_join(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()> {
        (self.route)(NodeMessage::Join {
            channel: channel.to_string(),
            info,
        });
        Ok(())
    }

    async fn publish_leave(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()> {
        (self.route)(NodeMessage::Leave {
            channel: channel.to_string(),
            info,
        });
        Ok(())
    }

    async fn subscribe(&self, _channel: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn unsubscribe(&self, _channel: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn history(&self, channel: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)> {
        let mut hist = self.history.lock();
        Self::evict_if_expired(&mut hist, channel);
        let stream = hist.entry(channel.to_string()).or_insert_with(Stream::new);
        Ok((stream.pubs.iter().cloned().collect(), stream.position()))
    }

    async fn history_since(
        &self,
        channel: &str,
        offset: u64,
        epoch: &str,
    ) -> anyhow::Result<(Vec<Publication>, StreamPosition)> {
        let mut hist = self.history.lock();
        Self::evict_if_expired(&mut hist, channel);
        let stream = hist.entry(channel.to_string()).or_insert_with(Stream::new);
        let top = stream.position();
        if top.offset == offset && top.epoch == epoch {
            return Ok((Vec::new(), top));
        }
        let pubs = stream
            .pubs
            .iter()
            .filter(|p| p.offset > offset)
            .cloned()
            .collect();
        Ok((pubs, top))
    }

    async fn remove_history(&self, channel: &str) -> anyhow::Result<()> {
        self.history.lock().remove(channel);
        Ok(())
    }

    async fn add_presence(
        &self,
        channel: &str,
        client_id: &str,
        info: ClientInfo,
        _ttl_ms: u64,
    ) -> anyhow::Result<()> {
        // Memory presence has no TTL (matches centrifuge MemoryEngine, which
        // ignores the expire duration); entries persist until explicit removal.
        self.presence
            .lock()
            .entry(channel.to_string())
            .or_default()
            .insert(client_id.to_string(), info);
        Ok(())
    }

    async fn remove_presence(&self, channel: &str, client_id: &str) -> anyhow::Result<()> {
        let mut p = self.presence.lock();
        if let Some(chan) = p.get_mut(channel) {
            chan.remove(client_id);
            if chan.is_empty() {
                p.remove(channel);
            }
        }
        Ok(())
    }

    async fn presence(&self, channel: &str) -> anyhow::Result<HashMap<String, ClientInfo>> {
        Ok(self
            .presence
            .lock()
            .get(channel)
            .cloned()
            .unwrap_or_default())
    }

    async fn presence_stats(&self, channel: &str) -> anyhow::Result<(u32, u32)> {
        let p = self.presence.lock();
        let Some(chan) = p.get(channel) else {
            return Ok((0, 0));
        };
        let num_clients = chan.len() as u32;
        let users: std::collections::HashSet<&str> =
            chan.values().map(|ci| ci.user.as_str()).collect();
        Ok((num_clients, users.len() as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn publish_routes_to_callback() {
        let routed = Arc::new(StdMutex::new(Vec::<(String, Vec<u8>)>::new()));
        let r2 = routed.clone();
        let engine = MemoryEngine::new(Arc::new(move |msg: NodeMessage| {
            if let NodeMessage::Publication {
                channel,
                publication,
            } = msg
            {
                let data = publication
                    .data
                    .map(|d| d.as_bytes().to_vec())
                    .unwrap_or_default();
                r2.lock().unwrap().push((channel, data));
            }
        }));
        engine
            .publish("news", br#"{"x":1}"#, None, PublishOptions::default())
            .await
            .unwrap();
        let got = routed.lock().unwrap();
        assert_eq!(got[0].0, "news");
        assert_eq!(got[0].1, br#"{"x":1}"#);
    }

    #[tokio::test]
    async fn history_append_and_since() {
        let engine = MemoryEngine::new(Arc::new(|_| {}));
        let opts = PublishOptions {
            history_size: 10,
            history_lifetime: 60,
        };
        for i in 1..=3 {
            engine
                .publish("h", format!("{{\"n\":{i}}}").as_bytes(), None, opts)
                .await
                .unwrap();
        }
        let (all, top) = engine.history("h").await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(top.offset, 3);
        let (since, _) = engine.history_since("h", 1, &top.epoch).await.unwrap();
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].offset, 2);
    }

    #[tokio::test]
    async fn presence_add_read_stats() {
        let engine = MemoryEngine::new(Arc::new(|_| {}));
        engine
            .add_presence(
                "room",
                "c1",
                ClientInfo {
                    user: "u1".into(),
                    client: "c1".into(),
                    ..Default::default()
                },
                0,
            )
            .await
            .unwrap();
        engine
            .add_presence(
                "room",
                "c2",
                ClientInfo {
                    user: "u1".into(),
                    client: "c2".into(),
                    ..Default::default()
                },
                0,
            )
            .await
            .unwrap();
        assert_eq!(engine.presence("room").await.unwrap().len(), 2);
        assert_eq!(engine.presence_stats("room").await.unwrap(), (2, 1));
    }
}
