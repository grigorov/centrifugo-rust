//! HTTP implementations of the event proxies (connect/refresh/subscribe/publish/
//! rpc), mirroring centrifugo v2.8.6's HTTP proxy contract. Each POSTs a JSON
//! request and reads a `{result|error|disconnect}` reply; an `error`/`disconnect`
//! relays that outcome, a non-2xx status or transport failure is `Err` (treated
//! as ErrorInternal by the caller).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use centrifugo_core::{
    ConnectProxy, Proxies, ProxyConnectOutcome, ProxyConnectReply, ProxyConnectRequest,
    ProxyOutcome, ProxyRequest, PublishData, PublishProxy, RefreshCreds, RefreshProxy, RpcData,
    RpcProxy, SubscribeCreds, SubscribeProxy,
};
use serde::Deserialize;
use serde_json::{value::RawValue, Map, Value};

use crate::config::Settings;

const TIMEOUT: Duration = Duration::from_secs(1);

/// Build all configured proxies from settings (a proxy is enabled iff its
/// endpoint is non-empty).
pub fn build_proxies(s: &Settings) -> Proxies {
    let p = |ep: &str| (!ep.is_empty()).then(|| ep.to_string());
    Proxies {
        connect: p(&s.proxy_connect_endpoint)
            .map(|e| Arc::new(HttpConnectProxy::new(e)) as Arc<dyn ConnectProxy>),
        refresh: p(&s.proxy_refresh_endpoint)
            .map(|e| Arc::new(HttpRefreshProxy::new(e)) as Arc<dyn RefreshProxy>),
        subscribe: p(&s.proxy_subscribe_endpoint)
            .map(|e| Arc::new(HttpSubscribeProxy::new(e)) as Arc<dyn SubscribeProxy>),
        publish: p(&s.proxy_publish_endpoint)
            .map(|e| Arc::new(HttpPublishProxy::new(e)) as Arc<dyn PublishProxy>),
        rpc: p(&s.proxy_rpc_endpoint)
            .map(|e| Arc::new(HttpRpcProxy::new(e)) as Arc<dyn RpcProxy>),
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(TIMEOUT)
        .build()
        .unwrap_or_default()
}

fn encoding(protocol: &str) -> &'static str {
    if protocol == "protobuf" {
        "binary"
    } else {
        "json"
    }
}

/// info/data bytes per Go's encoding-gated rule (connect/refresh/subscribe/rpc):
/// a JSON client uses ONLY the raw `field`; a binary client uses ONLY the base64
/// `b64field` (malformed base64 → Err, which the caller maps to ErrorInternal).
fn decode_gated(
    raw: Option<Box<RawValue>>,
    b64: Option<String>,
    protocol: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    if protocol == "protobuf" {
        match b64 {
            Some(b) if !b.is_empty() => {
                Ok(Some(base64::engine::general_purpose::STANDARD.decode(b)?))
            }
            _ => Ok(None),
        }
    } else {
        Ok(raw.map(|r| r.get().as_bytes().to_vec()))
    }
}

/// Publish data per Go's publish_handler: raw `data` preferred, base64 `b64data`
/// fallback, encoding-independent (malformed base64 → Err → ErrorInternal). `None`
/// means the proxy returned no data, so the original payload is kept.
fn decode_publish_data(
    raw: Option<Box<RawValue>>,
    b64: Option<String>,
) -> anyhow::Result<Option<Vec<u8>>> {
    if let Some(r) = raw {
        return Ok(Some(r.get().as_bytes().to_vec()));
    }
    match b64 {
        Some(b) if !b.is_empty() => Ok(Some(base64::engine::general_purpose::STANDARD.decode(b)?)),
        _ => Ok(None),
    }
}

/// Insert a publish/rpc `data` payload into the request body: raw JSON for JSON
/// clients, base64 `b64data` for protobuf (or non-JSON bytes).
fn put_data(map: &mut Map<String, Value>, data: &Option<Vec<u8>>, protocol: &str) {
    let Some(d) = data else { return };
    if protocol != "protobuf" {
        if let Ok(v) = serde_json::from_slice::<Value>(d) {
            map.insert("data".into(), v);
            return;
        }
    }
    map.insert(
        "b64data".into(),
        Value::String(base64::engine::general_purpose::STANDARD.encode(d)),
    );
}

#[derive(Deserialize, Default)]
struct ProxyError {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize, Default)]
struct ProxyDisconnect {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
}

// ---- Connect ----

pub struct HttpConnectProxy {
    endpoint: String,
    client: reqwest::Client,
}
impl HttpConnectProxy {
    pub fn new(endpoint: String) -> Self {
        HttpConnectProxy {
            endpoint,
            client: http_client(),
        }
    }
}

#[derive(Deserialize)]
struct ConnectResponse {
    #[serde(default)]
    result: Option<ConnectResult>,
    #[serde(default)]
    error: Option<ProxyError>,
    #[serde(default)]
    disconnect: Option<ProxyDisconnect>,
}
#[derive(Deserialize, Default)]
struct ConnectResult {
    #[serde(default)]
    user: String,
    #[serde(default)]
    expire_at: i64,
    #[serde(default)]
    info: Option<Box<RawValue>>,
    #[serde(default)]
    b64info: Option<String>,
    #[serde(default)]
    channels: Vec<String>,
}

#[async_trait]
impl ConnectProxy for HttpConnectProxy {
    async fn connect(&self, req: ProxyConnectRequest) -> anyhow::Result<ProxyConnectOutcome> {
        let body = serde_json::json!({
            "client": req.client,
            "transport": req.transport,
            "protocol": req.protocol,
            "encoding": encoding(&req.protocol),
        });
        let resp: ConnectResponse = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(d) = resp.disconnect {
            return Ok(ProxyConnectOutcome::Disconnect {
                code: d.code,
                reason: d.reason,
            });
        }
        if let Some(e) = resp.error {
            return Ok(ProxyConnectOutcome::Error {
                code: e.code,
                message: e.message,
            });
        }
        let Some(result) = resp.result else {
            return Ok(ProxyConnectOutcome::NoCredentials);
        };
        Ok(ProxyConnectOutcome::Credentials(ProxyConnectReply {
            user: result.user,
            info: decode_gated(result.info, result.b64info, &req.protocol)?,
            expire_at: result.expire_at,
            channels: result.channels,
        }))
    }
}

// ---- Refresh ----

pub struct HttpRefreshProxy {
    endpoint: String,
    client: reqwest::Client,
}
impl HttpRefreshProxy {
    pub fn new(endpoint: String) -> Self {
        HttpRefreshProxy {
            endpoint,
            client: http_client(),
        }
    }
}
#[derive(Deserialize)]
struct RefreshResponse {
    #[serde(default)]
    result: Option<RefreshResult>,
    #[serde(default)]
    error: Option<ProxyError>,
    #[serde(default)]
    disconnect: Option<ProxyDisconnect>,
}
#[derive(Deserialize, Default)]
struct RefreshResult {
    #[serde(default)]
    expired: bool,
    #[serde(default)]
    expire_at: i64,
    #[serde(default)]
    info: Option<Box<RawValue>>,
    #[serde(default)]
    b64info: Option<String>,
}

#[async_trait]
impl RefreshProxy for HttpRefreshProxy {
    async fn refresh(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RefreshCreds>> {
        let body = serde_json::json!({
            "client": req.client,
            "user": req.user,
            "transport": req.transport,
            "protocol": req.protocol,
            "encoding": encoding(&req.protocol),
        });
        let resp: RefreshResponse = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(d) = resp.disconnect {
            return Ok(ProxyOutcome::Disconnect {
                code: d.code,
                reason: d.reason,
            });
        }
        if let Some(e) = resp.error {
            return Ok(ProxyOutcome::Error {
                code: e.code,
                message: e.message,
            });
        }
        // Go: a missing `result` means no refresh credentials → RefreshReply{Expired:true}
        // → DisconnectExpired (3005). Distinguish absent from present-but-empty.
        let Some(r) = resp.result else {
            return Ok(ProxyOutcome::Result(RefreshCreds {
                expired: true,
                ..Default::default()
            }));
        };
        Ok(ProxyOutcome::Result(RefreshCreds {
            expired: r.expired,
            expire_at: r.expire_at,
            info: decode_gated(r.info, r.b64info, &req.protocol)?,
        }))
    }
}

// ---- Subscribe ----

pub struct HttpSubscribeProxy {
    endpoint: String,
    client: reqwest::Client,
}
impl HttpSubscribeProxy {
    pub fn new(endpoint: String) -> Self {
        HttpSubscribeProxy {
            endpoint,
            client: http_client(),
        }
    }
}
#[derive(Deserialize)]
struct SubscribeResponse {
    #[serde(default)]
    result: Option<SubscribeResult>,
    #[serde(default)]
    error: Option<ProxyError>,
    #[serde(default)]
    disconnect: Option<ProxyDisconnect>,
}
#[derive(Deserialize, Default)]
struct SubscribeResult {
    #[serde(default)]
    info: Option<Box<RawValue>>,
    #[serde(default)]
    b64info: Option<String>,
}

#[async_trait]
impl SubscribeProxy for HttpSubscribeProxy {
    async fn subscribe(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<SubscribeCreds>> {
        let body = serde_json::json!({
            "client": req.client,
            "user": req.user,
            "channel": req.channel,
            "token": req.token,
            "transport": req.transport,
            "protocol": req.protocol,
            "encoding": encoding(&req.protocol),
        });
        let resp: SubscribeResponse = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(d) = resp.disconnect {
            return Ok(ProxyOutcome::Disconnect {
                code: d.code,
                reason: d.reason,
            });
        }
        if let Some(e) = resp.error {
            return Ok(ProxyOutcome::Error {
                code: e.code,
                message: e.message,
            });
        }
        let r = resp.result.unwrap_or_default();
        Ok(ProxyOutcome::Result(SubscribeCreds {
            info: decode_gated(r.info, r.b64info, &req.protocol)?,
        }))
    }
}

// ---- Publish ----

pub struct HttpPublishProxy {
    endpoint: String,
    client: reqwest::Client,
}
impl HttpPublishProxy {
    pub fn new(endpoint: String) -> Self {
        HttpPublishProxy {
            endpoint,
            client: http_client(),
        }
    }
}
#[derive(Deserialize)]
struct PublishResponse {
    #[serde(default)]
    result: Option<DataResult>,
    #[serde(default)]
    error: Option<ProxyError>,
    #[serde(default)]
    disconnect: Option<ProxyDisconnect>,
}
#[derive(Deserialize, Default)]
struct DataResult {
    #[serde(default)]
    data: Option<Box<RawValue>>,
    #[serde(default)]
    b64data: Option<String>,
}

#[async_trait]
impl PublishProxy for HttpPublishProxy {
    async fn publish(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<PublishData>> {
        let mut map = Map::new();
        map.insert("client".into(), req.client.clone().into());
        map.insert("user".into(), req.user.clone().into());
        map.insert("channel".into(), req.channel.clone().into());
        map.insert("transport".into(), req.transport.clone().into());
        map.insert("protocol".into(), req.protocol.clone().into());
        map.insert("encoding".into(), encoding(&req.protocol).into());
        put_data(&mut map, &req.data, &req.protocol);
        let resp: PublishResponse = self
            .client
            .post(&self.endpoint)
            .json(&Value::Object(map))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(d) = resp.disconnect {
            return Ok(ProxyOutcome::Disconnect {
                code: d.code,
                reason: d.reason,
            });
        }
        if let Some(e) = resp.error {
            return Ok(ProxyOutcome::Error {
                code: e.code,
                message: e.message,
            });
        }
        let r = resp.result.unwrap_or_default();
        Ok(ProxyOutcome::Result(PublishData {
            data: decode_publish_data(r.data, r.b64data)?,
        }))
    }
}

// ---- RPC ----

pub struct HttpRpcProxy {
    endpoint: String,
    client: reqwest::Client,
}
impl HttpRpcProxy {
    pub fn new(endpoint: String) -> Self {
        HttpRpcProxy {
            endpoint,
            client: http_client(),
        }
    }
}

#[async_trait]
impl RpcProxy for HttpRpcProxy {
    async fn rpc(&self, req: ProxyRequest) -> anyhow::Result<ProxyOutcome<RpcData>> {
        let mut map = Map::new();
        map.insert("client".into(), req.client.clone().into());
        map.insert("user".into(), req.user.clone().into());
        map.insert("method".into(), req.method.clone().into());
        map.insert("transport".into(), req.transport.clone().into());
        map.insert("protocol".into(), req.protocol.clone().into());
        map.insert("encoding".into(), encoding(&req.protocol).into());
        put_data(&mut map, &req.data, &req.protocol);
        let resp: PublishResponse = self
            .client
            .post(&self.endpoint)
            .json(&Value::Object(map))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(d) = resp.disconnect {
            return Ok(ProxyOutcome::Disconnect {
                code: d.code,
                reason: d.reason,
            });
        }
        if let Some(e) = resp.error {
            return Ok(ProxyOutcome::Error {
                code: e.code,
                message: e.message,
            });
        }
        let r = resp.result.unwrap_or_default();
        Ok(ProxyOutcome::Result(RpcData {
            data: decode_gated(r.data, r.b64data, &req.protocol)?,
        }))
    }
}
