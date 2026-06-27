//! Proxy abstractions. When configured, client events (connect, refresh,
//! subscribe, publish, rpc) are proxied to external HTTP endpoints. The
//! transport (HTTP) lives in the server crate; core only defines the traits +
//! request/reply types so it stays dependency-free.

use std::sync::Arc;

use async_trait::async_trait;

/// What the server sends to the connect-proxy endpoint.
pub struct ProxyConnectRequest {
    pub client: String,
    /// "websocket" or "sockjs".
    pub transport: String,
    /// "json" or "protobuf".
    pub protocol: String,
    /// The client's connect `data` (opaque bytes), if any.
    pub data: Option<Vec<u8>>,
}

/// The identity the proxy grants. Mirrors the fields of a connect token.
#[derive(Default)]
pub struct ProxyConnectReply {
    pub user: String,
    pub info: Option<Vec<u8>>,
    /// Unix seconds; 0 = no expiry.
    pub expire_at: i64,
    /// Server-side channels the proxy granted (Go credentials.Channels →
    /// ConnectReply.Subscriptions); each is validated + auto-subscribed on connect.
    pub channels: Vec<String>,
}

/// What a connect-proxy decided, mirroring centrifugo's proxy connect_handler:
/// the endpoint may grant credentials, relay an explicit error code, force a
/// disconnect, or return no credentials (fall through to anonymous/insecure).
pub enum ProxyConnectOutcome {
    /// `result` present: accept with this identity.
    Credentials(ProxyConnectReply),
    /// `error` present: reply with this error code/message.
    Error { code: u32, message: String },
    /// `disconnect` present: close with this code/reason.
    Disconnect { code: u32, reason: String },
    /// No `result`/`error`/`disconnect`: no credentials established (the
    /// connection then falls through to anonymous/insecure handling).
    NoCredentials,
}

/// Authenticate a CONNECT via an external service. `Ok(outcome)` carries the
/// proxy's decision; `Err` is a transport failure (mapped to ErrorInternal 100,
/// matching Go's proxy connect_handler).
#[async_trait]
pub trait ConnectProxy: Send + Sync {
    async fn connect(&self, req: ProxyConnectRequest) -> anyhow::Result<ProxyConnectOutcome>;
}

/// Common fields sent to refresh/subscribe/publish/rpc proxy endpoints. Only the
/// relevant fields are serialized per endpoint by the HTTP impl.
#[derive(Default)]
pub struct ProxyRequest {
    pub client: String,
    pub user: String,
    pub transport: String,
    pub protocol: String,
    /// Subscribe/publish channel.
    pub channel: String,
    /// RPC method.
    pub method: String,
    /// Publish/RPC data.
    pub data: Option<Vec<u8>>,
    /// Subscribe token (for token-protected channels).
    pub token: String,
}

/// A proxy decision: grant a typed result, relay an error code, or force a
/// disconnect (mirrors the `{result|error|disconnect}` proxy reply shape).
pub enum ProxyOutcome<T> {
    Result(T),
    Error { code: u32, message: String },
    Disconnect { code: u32, reason: String },
}

/// Refresh-proxy credentials.
#[derive(Default)]
pub struct RefreshCreds {
    pub expired: bool,
    pub expire_at: i64,
    pub info: Option<Vec<u8>>,
}

/// Subscribe-proxy grant (per-subscription info).
#[derive(Default)]
pub struct SubscribeCreds {
    pub info: Option<Vec<u8>>,
}

/// Publish-proxy result; `data == None` means publish the original payload.
#[derive(Default)]
pub struct PublishData {
    pub data: Option<Vec<u8>>,
}

/// RPC-proxy result data. `data == None` means the proxy returned no data (an
/// ack-only RPC); the RpcResult then omits `data` (Go nil-vs-present), instead of
/// emitting an empty payload that breaks JSON encoding.
#[derive(Default)]
pub struct RpcData {
    pub data: Option<Vec<u8>>,
}

#[async_trait]
pub trait RefreshProxy: Send + Sync {
    async fn refresh(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RefreshCreds>>;
}

#[async_trait]
pub trait SubscribeProxy: Send + Sync {
    async fn subscribe(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<SubscribeCreds>>;
}

#[async_trait]
pub trait PublishProxy: Send + Sync {
    async fn publish(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<PublishData>>;
}

#[async_trait]
pub trait RpcProxy: Send + Sync {
    async fn rpc(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RpcData>>;
}

/// All configured proxies, held by the `Node`. Default = none.
#[derive(Default, Clone)]
pub struct Proxies {
    pub connect: Option<Arc<dyn ConnectProxy>>,
    pub refresh: Option<Arc<dyn RefreshProxy>>,
    pub subscribe: Option<Arc<dyn SubscribeProxy>>,
    pub publish: Option<Arc<dyn PublishProxy>>,
    pub rpc: Option<Arc<dyn RpcProxy>>,
}
