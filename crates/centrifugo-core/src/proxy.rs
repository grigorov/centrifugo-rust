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
