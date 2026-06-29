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
    /// Unix seconds when the buffered pubs expire (history_lifetime).
    expire_at: i64,
    /// Unix seconds when the whole stream (its meta: offset + epoch) is dropped
    /// after inactivity (`memory_history_meta_ttl`). `i64::MAX` = never (the
    /// default, meta_ttl 0).
    meta_expire_at: i64,
}

impl Stream {
    fn new() -> Self {
        Stream {
            offset: 0,
            epoch: new_epoch(),
            pubs: VecDeque::new(),
            expire_at: i64::MAX,
            meta_expire_at: i64::MAX,
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
    /// `memory_history_meta_ttl` in seconds; 0 = streams are never removed
    /// (centrifuge default). When > 0, an idle stream is dropped after this many
    /// seconds, so its next publish restarts at offset 1 with a fresh epoch.
    meta_ttl: i64,
}

impl MemoryEngine {
    pub fn new(route: RouteFn) -> Self {
        MemoryEngine {
            route,
            presence: Mutex::new(HashMap::new()),
            history: Mutex::new(HashMap::new()),
            meta_ttl: 0,
        }
    }

    /// Set `memory_history_meta_ttl` (seconds; 0 keeps streams forever).
    pub fn with_history_meta_ttl(mut self, secs: u64) -> Self {
        self.meta_ttl = secs as i64;
        self
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
        // Drop a meta-expired stream first, so a publish after `meta_ttl` of
        // inactivity restarts at offset 1 with a fresh epoch (centrifuge
        // removeStreams), rather than continuing the stale offset.
        Self::evict_if_expired(&mut hist, channel);
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
        stream.meta_expire_at = if self.meta_ttl > 0 {
            now_unix() + self.meta_ttl
        } else {
            i64::MAX
        };
        offset
    }

    /// Lazy TTL eviction. If `memory_history_meta_ttl` elapsed since the last
    /// publish, drop the whole stream (offset + epoch) so the next publish/read
    /// rebuilds it fresh (centrifuge removeStreams). Otherwise, on history-lifetime
    /// expiry drop only the buffered publications but keep the meta — matching
    /// centrifuge memstream `Clear()`, so a caught-up client still recovers with
    /// its last seq/gen when meta_ttl is 0 (the default; streams never removed).
    fn evict_if_expired(hist: &mut HashMap<String, Stream>, channel: &str) {
        if let Some(s) = hist.get(channel) {
            if now_unix() > s.meta_expire_at {
                hist.remove(channel);
                return;
            }
        }
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
    async fn meta_ttl_resets_stream_after_inactivity() {
        // L3: with memory_history_meta_ttl > 0, an idle stream is dropped, so the
        // next publish restarts at offset 1 with a fresh epoch.
        let engine = MemoryEngine::new(Arc::new(|_| {})).with_history_meta_ttl(3);
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
        let (_, top1) = engine.history("h").await.unwrap();
        assert_eq!(top1.offset, 3);
        // Force the meta TTL to have elapsed.
        engine.history.lock().get_mut("h").unwrap().meta_expire_at = now_unix() - 1;
        engine
            .publish("h", br#"{"n":99}"#, None, opts)
            .await
            .unwrap();
        let (_, top2) = engine.history("h").await.unwrap();
        assert_eq!(top2.offset, 1, "stream offset must reset after meta TTL");
        assert_ne!(
            top2.epoch, top1.epoch,
            "epoch must flip after meta TTL reset"
        );
    }

    #[tokio::test]
    async fn default_meta_ttl_keeps_offset_after_lifetime() {
        // Default (meta_ttl 0): pubs window expiry clears publications but keeps
        // the meta (offset + epoch), so recovery still works for caught-up clients.
        let engine = MemoryEngine::new(Arc::new(|_| {}));
        let opts = PublishOptions {
            history_size: 10,
            history_lifetime: 1,
        };
        engine
            .publish("h", br#"{"n":1}"#, None, opts)
            .await
            .unwrap();
        let epoch1 = engine.history("h").await.unwrap().1.epoch;
        engine.history.lock().get_mut("h").unwrap().expire_at = now_unix() - 1;
        let (pubs, top) = engine.history("h").await.unwrap();
        assert!(pubs.is_empty(), "expired pubs must be cleared");
        assert_eq!(top.offset, 1, "offset must persist when meta_ttl=0");
        assert_eq!(top.epoch, epoch1, "epoch must persist when meta_ttl=0");
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
