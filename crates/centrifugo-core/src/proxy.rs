//! Connect-proxy abstraction. When configured, a client CONNECT is authenticated
//! by calling out to an external HTTP endpoint instead of (or in addition to) a
//! JWT. The transport (HTTP) lives in the server crate; core only defines the
//! trait + request/reply types so it stays dependency-free.

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
}

/// Authenticate a CONNECT via an external service. `Ok` accepts the connection
/// with the returned identity; `Err` rejects it (the client is disconnected).
#[async_trait]
pub trait ConnectProxy: Send + Sync {
    async fn connect(&self, req: ProxyConnectRequest) -> anyhow::Result<ProxyConnectReply>;
}
