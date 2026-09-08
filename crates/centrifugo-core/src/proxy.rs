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
    /// Connect data forwarded to the client in the connect reply.
    pub data: Option<Vec<u8>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_connect_outcome_variants() {
        let reply = ProxyConnectReply {
            user: "user123".to_string(),
            info: Some(vec![1, 2, 3]),
            data: None,
            expire_at: 1234567890,
            channels: vec!["channel1".to_string()],
        };
        let outcome = ProxyConnectOutcome::Credentials(reply);
        assert!(matches!(outcome, ProxyConnectOutcome::Credentials(_)));

        let outcome = ProxyConnectOutcome::Error {
            code: 100,
            message: "error".to_string(),
        };
        assert!(matches!(outcome, ProxyConnectOutcome::Error { .. }));

        let outcome = ProxyConnectOutcome::Disconnect {
            code: 3000,
            reason: "reason".to_string(),
        };
        assert!(matches!(outcome, ProxyConnectOutcome::Disconnect { .. }));

        let outcome = ProxyConnectOutcome::NoCredentials;
        assert!(matches!(outcome, ProxyConnectOutcome::NoCredentials));
    }

    #[test]
    fn proxy_outcome_variants() {
        let outcome = ProxyOutcome::<PublishData>::Result(PublishData::default());
        assert!(matches!(outcome, ProxyOutcome::Result(_)));

        let outcome = ProxyOutcome::<PublishData>::Error {
            code: 100,
            message: "error".to_string(),
        };
        assert!(matches!(outcome, ProxyOutcome::Error { .. }));

        let outcome = ProxyOutcome::<PublishData>::Disconnect {
            code: 3000,
            reason: "reason".to_string(),
        };
        assert!(matches!(outcome, ProxyOutcome::Disconnect { .. }));
    }

    #[test]
    fn refresh_creds_default() {
        let creds = RefreshCreds::default();
        assert!(!creds.expired);
        assert_eq!(creds.expire_at, 0);
        assert!(creds.info.is_none());
    }

    #[test]
    fn subscribe_creds_default() {
        let creds = SubscribeCreds::default();
        assert!(creds.info.is_none());
    }

    #[test]
    fn publish_data_default() {
        let data = PublishData::default();
        assert!(data.data.is_none());
    }

    #[test]
    fn rpc_data_default() {
        let data = RpcData::default();
        assert!(data.data.is_none());
    }

    #[test]
    fn proxy_request_default() {
        let req = ProxyRequest::default();
        assert_eq!(req.client, "");
        assert_eq!(req.user, "");
        assert_eq!(req.transport, "");
        assert_eq!(req.protocol, "");
        assert_eq!(req.channel, "");
        assert_eq!(req.method, "");
        assert!(req.data.is_none());
        assert_eq!(req.token, "");
    }

    #[test]
    fn proxies_default_is_none() {
        let proxies = Proxies::default();
        assert!(proxies.connect.is_none());
        assert!(proxies.refresh.is_none());
        assert!(proxies.subscribe.is_none());
        assert!(proxies.publish.is_none());
        assert!(proxies.rpc.is_none());
    }

    #[test]
    fn proxies_clone() {
        let proxies = Proxies::default();
        let cloned = proxies.clone();
        assert!(cloned.connect.is_none());
    }
}
