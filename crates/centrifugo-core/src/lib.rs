//! Core engine: the sharded `Hub`, the per-connection `Client` session state
//! machine, the `Node` that ties them to an `Engine`, and the single-node
//! in-memory engine.

pub mod client;
pub mod engine;
pub mod hub;
pub mod memory;
pub mod metrics;
pub mod node;
pub mod proxy;

pub use client::{Client, CommandOutcome};
pub use engine::{ControlMessage, Engine, NodeInfoData, NodeMessage, PublishOptions, RouteFn};
pub use hub::{ClientHandle, ClientId, Hub, Out, Signal};
pub use memory::MemoryEngine;
pub use metrics::Metrics;
pub use node::{
    make_route, ChannelOptions, Limits, Namespaces, Node, NodeRegistry, StreamPosition,
    DEFAULT_USE_SEQ_GEN,
};
pub use proxy::{
    ConnectProxy, Proxies, ProxyConnectOutcome, ProxyConnectReply, ProxyConnectRequest,
    ProxyOutcome, ProxyRequest, PublishData, PublishProxy, RefreshCreds, RefreshProxy, RpcData,
    RpcProxy, SubscribeCreds, SubscribeProxy,
};
