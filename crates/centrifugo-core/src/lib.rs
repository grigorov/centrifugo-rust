//! Core engine: the sharded `Hub`, the per-connection `Client` session state
//! machine, the `Node` that ties them to a `Broker`, and the single-node
//! in-memory broker.

pub mod client;
pub mod engine;
pub mod hub;
pub mod memory;
pub mod node;

pub use client::{Client, CommandOutcome};
pub use engine::Broker;
pub use hub::{ClientHandle, ClientId, Hub, Out};
pub use memory::MemoryBroker;
pub use node::{ChannelOptions, Namespaces, Node};
