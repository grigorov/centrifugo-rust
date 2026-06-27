//! The engine abstraction. In centrifuge an `Engine` is a `Broker` (pub/sub +
//! history) plus a `PresenceManager` (presence); here a single async `Engine`
//! trait covers all three so one `Arc<dyn Engine>` backs the `Node`.
//!
//! Both the single-node [`crate::memory::MemoryEngine`] and the multi-node
//! `RedisEngine` (the `centrifugo-redis` crate) implement it. Methods are async
//! because the Redis engine performs network I/O; the memory engine completes
//! immediately. Delivery into local subscribers happens through a [`RouteFn`]
//! the `Node` installs — each engine calls it with a [`NodeMessage`] (the memory
//! engine inline on publish, the Redis engine from its PUB/SUB subscriber task).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use centrifugo_protocol::messages::{ClientInfo, Publication};

use crate::node::StreamPosition;

/// Per-publish history directives. `0/0` means history is disabled for the
/// channel (no append).
#[derive(Clone, Copy, Default)]
pub struct PublishOptions {
    pub history_size: usize,
    pub history_lifetime: u64,
}

impl PublishOptions {
    pub fn history_enabled(&self) -> bool {
        self.history_size > 0 && self.history_lifetime > 0
    }
}

/// A message an engine hands back for local fan-out. The `Node`'s [`RouteFn`]
/// turns it into the matching push (Publication/Join/Leave) on each local
/// subscriber. The Redis engine reconstructs these from its PUB/SUB envelope.
pub enum NodeMessage {
    Publication {
        channel: String,
        publication: Publication,
    },
    Join {
        channel: String,
        info: ClientInfo,
    },
    Leave {
        channel: String,
        info: ClientInfo,
    },
}

/// Installed by the `Node`; an engine calls it to deliver a [`NodeMessage`] to
/// this node's local subscribers.
pub type RouteFn = Arc<dyn Fn(NodeMessage) + Send + Sync>;

/// Pub/sub + history + presence. One instance backs a `Node`.
#[async_trait]
pub trait Engine: Send + Sync {
    /// Publish `data` (raw JSON bytes) to `channel`. When `opts` enables history
    /// the publication is appended (assigning an offset) before fan-out. `info`
    /// is the publisher's `ClientInfo` (set for client publishes; `None` for the
    /// server API).
    async fn publish(
        &self,
        channel: &str,
        data: &[u8],
        info: Option<ClientInfo>,
        opts: PublishOptions,
    ) -> anyhow::Result<()>;

    /// Fan out a Join push (presence join), across all nodes.
    async fn publish_join(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()>;
    /// Fan out a Leave push (presence leave), across all nodes.
    async fn publish_leave(&self, channel: &str, info: ClientInfo) -> anyhow::Result<()>;

    /// Note interest in a channel (Redis: SUBSCRIBE the bus topic; memory: no-op).
    async fn subscribe(&self, channel: &str) -> anyhow::Result<()>;
    /// Drop interest in a channel.
    async fn unsubscribe(&self, channel: &str) -> anyhow::Result<()>;

    /// All retained publications + current top position (creates an empty stream
    /// so the epoch is stable).
    async fn history(&self, channel: &str) -> anyhow::Result<(Vec<Publication>, StreamPosition)>;
    /// Publications after `offset` (recovery) + current top position.
    async fn history_since(
        &self,
        channel: &str,
        offset: u64,
        epoch: &str,
    ) -> anyhow::Result<(Vec<Publication>, StreamPosition)>;
    /// Drop a channel's history.
    async fn remove_history(&self, channel: &str) -> anyhow::Result<()>;

    /// Record presence for `client_id` on `channel`. `ttl_ms` is the entry's
    /// time-to-live (the memory engine ignores it, like centrifuge's
    /// MemoryEngine; the Redis engine expires the entry after it, refreshed by
    /// the per-connection presence timer).
    async fn add_presence(
        &self,
        channel: &str,
        client_id: &str,
        info: ClientInfo,
        ttl_ms: u64,
    ) -> anyhow::Result<()>;
    async fn remove_presence(&self, channel: &str, client_id: &str) -> anyhow::Result<()>;
    async fn presence(&self, channel: &str) -> anyhow::Result<HashMap<String, ClientInfo>>;
    /// (num_clients, num_users): total presence entries and distinct user ids.
    async fn presence_stats(&self, channel: &str) -> anyhow::Result<(u32, u32)>;
}
