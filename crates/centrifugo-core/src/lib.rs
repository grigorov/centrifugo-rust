//! Core engine: the sharded `Hub`, the per-connection `Client` session state
//! machine, the `Node` that ties them to an `Engine`, and the single-node
//! in-memory engine.

pub mod client;
pub mod engine;
pub mod hub;
pub mod memory;
pub mod node;

pub use client::{Client, CommandOutcome};
pub use engine::{Engine, NodeMessage, PublishOptions, RouteFn};
pub use hub::{ClientHandle, ClientId, Hub, Out};
pub use memory::MemoryEngine;
pub use node::{make_route, ChannelOptions, Namespaces, Node, StreamPosition};
