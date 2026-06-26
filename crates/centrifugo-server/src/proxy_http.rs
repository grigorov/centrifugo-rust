//! HTTP implementation of the connect-proxy. POSTs a JSON request to the
//! configured endpoint and reads `{result:{user,expire_at,info|b64info}}`,
//! mirroring centrifugo v2.8.6's HTTP proxy contract. An `error`/`disconnect`
//! response, a non-2xx status, or a transport failure rejects the connection.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use centrifugo_core::{ConnectProxy, ProxyConnectReply, ProxyConnectRequest};
use serde::Deserialize;
use serde_json::value::RawValue;

const TIMEOUT: Duration = Duration::from_secs(1);

pub struct HttpConnectProxy {
    endpoint: String,
    client: reqwest::Client,
}

impl HttpConnectProxy {
    pub fn new(endpoint: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .build()
            .unwrap_or_default();
        HttpConnectProxy { endpoint, client }
    }
}

#[derive(Deserialize)]
struct ProxyResponse {
    #[serde(default)]
    result: Option<ConnectResult>,
    #[serde(default)]
    error: Option<serde_json::Value>,
    #[serde(default)]
    disconnect: Option<serde_json::Value>,
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
}

#[async_trait]
impl ConnectProxy for HttpConnectProxy {
    async fn connect(&self, req: ProxyConnectRequest) -> anyhow::Result<ProxyConnectReply> {
        let body = serde_json::json!({
            "client": req.client,
            "transport": req.transport,
            "protocol": req.protocol,
            "encoding": if req.protocol == "protobuf" { "binary" } else { "json" },
        });
        let resp: ProxyResponse = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if resp.error.is_some() || resp.disconnect.is_some() {
            anyhow::bail!("connect proxy denied the connection");
        }
        let result = resp
            .result
            .ok_or_else(|| anyhow::anyhow!("connect proxy returned no result"))?;
        // b64info (base64) overrides inline info, matching the token semantics.
        let info = match result.b64info {
            Some(ref b) if !b.is_empty() => {
                Some(base64::engine::general_purpose::STANDARD.decode(b)?)
            }
            _ => result.info.map(|r| r.get().as_bytes().to_vec()),
        };
        Ok(ProxyConnectReply {
            user: result.user,
            info,
            expire_at: result.expire_at,
        })
    }
}
