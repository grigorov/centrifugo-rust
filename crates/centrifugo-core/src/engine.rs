//! The engine abstraction. In centrifuge an `Engine` is a `Broker` (pub/sub +
//! history) plus a `PresenceManager`. For M1 only the pub/sub part of `Broker`
//! exists; history/presence land in later milestones, and the Redis engine
//! (M8) implements the same trait for multi-node fan-out.
//!
//! Methods are synchronous for the single-node memory case (delivery is a
//! non-blocking `try_send`). The trait will gain async variants when the Redis
//! engine arrives.

use centrifugo_protocol::messages::ClientInfo;

/// Pub/sub side of an engine.
pub trait Broker: Send + Sync {
    /// Publish `data` (raw JSON bytes) to `channel`, optionally carrying the
    /// publisher's `ClientInfo` (set for client-initiated publishes; the server
    /// API publish leaves it `None`). The single-node memory broker routes
    /// straight back into the node for local fan-out; a network broker would
    /// publish to its bus.
    fn publish(&self, channel: &str, data: &[u8], info: Option<ClientInfo>) -> anyhow::Result<()>;

    /// Note interest in a channel (memory single-node: a no-op, the hub tracks
    /// local subscriptions; a network broker subscribes to the bus topic).
    fn subscribe(&self, channel: &str) -> anyhow::Result<()>;

    /// Drop interest in a channel.
    fn unsubscribe(&self, channel: &str) -> anyhow::Result<()>;
}
