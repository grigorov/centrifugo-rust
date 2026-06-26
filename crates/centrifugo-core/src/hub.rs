//! The hub: a registry of connections (by client id), users (by user id), and
//! channel subscriptions. Subscriptions are sharded by channel hash so fan-out
//! to different channels never contends on one lock. Lookups clone the cheap
//! `ClientHandle` (an mpsc sender) so the caller can `try_send` *outside* the
//! lock — a slow/full client never blocks the broadcaster.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use centrifugo_protocol::ProtocolType;
use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;

pub type ClientId = String;

/// A cheap, clonable handle to a connection's writer queue. `proto` selects which
/// pre-encoded push frame (JSON or protobuf) the broadcaster delivers.
#[derive(Clone)]
pub struct ClientHandle {
    pub id: ClientId,
    pub user: String,
    pub proto: ProtocolType,
    pub tx: Sender<Vec<u8>>,
}

const SHARDS: usize = 16;

#[derive(Default)]
struct Shard {
    /// channel -> set of subscribed client ids
    subs: HashMap<String, HashSet<ClientId>>,
}

pub struct Hub {
    conns: RwLock<HashMap<ClientId, ClientHandle>>,
    users: RwLock<HashMap<String, HashSet<ClientId>>>,
    shards: Vec<RwLock<Shard>>,
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

impl Hub {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            shards.push(RwLock::new(Shard::default()));
        }
        Hub {
            conns: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            shards,
        }
    }

    fn shard_for(&self, channel: &str) -> &RwLock<Shard> {
        let mut h = DefaultHasher::new();
        channel.hash(&mut h);
        &self.shards[(h.finish() as usize) % SHARDS]
    }

    /// Register a connection.
    pub fn add(&self, handle: ClientHandle) {
        let id = handle.id.clone();
        let user = handle.user.clone();
        self.conns.write().insert(id.clone(), handle);
        self.users.write().entry(user).or_default().insert(id);
    }

    /// Remove a connection and all of its subscriptions.
    pub fn remove(&self, id: &str) {
        let handle = self.conns.write().remove(id);
        if let Some(h) = handle {
            let mut users = self.users.write();
            if let Some(set) = users.get_mut(&h.user) {
                set.remove(id);
                if set.is_empty() {
                    users.remove(&h.user);
                }
            }
        }
        for shard in &self.shards {
            let mut s = shard.write();
            s.subs.retain(|_, set| {
                set.remove(id);
                !set.is_empty()
            });
        }
    }

    pub fn get(&self, id: &str) -> Option<ClientHandle> {
        self.conns.read().get(id).cloned()
    }

    pub fn subscribe(&self, id: &str, channel: &str) {
        let mut s = self.shard_for(channel).write();
        s.subs
            .entry(channel.to_string())
            .or_default()
            .insert(id.to_string());
    }

    pub fn unsubscribe(&self, id: &str, channel: &str) {
        let mut s = self.shard_for(channel).write();
        if let Some(set) = s.subs.get_mut(channel) {
            set.remove(id);
            if set.is_empty() {
                s.subs.remove(channel);
            }
        }
    }

    /// Snapshot the handles subscribed to a channel. Clones happen under the
    /// read lock; the caller delivers *after* releasing it.
    pub fn subscribers(&self, channel: &str) -> Vec<ClientHandle> {
        let s = self.shard_for(channel).read();
        let Some(ids) = s.subs.get(channel) else {
            return Vec::new();
        };
        let conns = self.conns.read();
        ids.iter().filter_map(|id| conns.get(id).cloned()).collect()
    }

    pub fn num_clients(&self) -> usize {
        self.conns.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn handle(id: &str, user: &str) -> (ClientHandle, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::channel(16);
        (
            ClientHandle {
                id: id.into(),
                user: user.into(),
                proto: ProtocolType::Json,
                tx,
            },
            rx,
        )
    }

    #[test]
    fn add_remove_subscriber_and_lookup() {
        let hub = Hub::new();
        let (h, _rx) = handle("c1", "u1");
        hub.add(h);
        hub.subscribe("c1", "news");
        assert_eq!(hub.subscribers("news").len(), 1);
        hub.unsubscribe("c1", "news");
        assert_eq!(hub.subscribers("news").len(), 0);
        hub.remove("c1");
        assert!(hub.get("c1").is_none());
    }

    #[test]
    fn remove_clears_subscriptions() {
        let hub = Hub::new();
        let (h, _rx) = handle("c1", "u1");
        hub.add(h);
        hub.subscribe("c1", "a");
        hub.subscribe("c1", "b");
        hub.remove("c1");
        assert_eq!(hub.subscribers("a").len(), 0);
        assert_eq!(hub.subscribers("b").len(), 0);
        assert_eq!(hub.num_clients(), 0);
    }

    #[test]
    fn multiple_subscribers_same_channel() {
        let hub = Hub::new();
        let (h1, _r1) = handle("c1", "u1");
        let (h2, _r2) = handle("c2", "u2");
        hub.add(h1);
        hub.add(h2);
        hub.subscribe("c1", "news");
        hub.subscribe("c2", "news");
        assert_eq!(hub.subscribers("news").len(), 2);
    }
}
