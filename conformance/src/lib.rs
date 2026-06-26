//! Black-box conformance harness: spawn the `centrifugo` binary, wait until it
//! is healthy, and drive it over the real wire (WebSocket + JSON).
//!
//! `Server::start` rebuilds the binary first (see `ensure_binary_built`), so a
//! plain `cargo test --workspace` always exercises current code even though the
//! binary is spawned by path rather than via a cargo dependency.

use std::process::{Child, Command};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub mod oracle;

/// A spawned Rust centrifugo server, killed on drop.
pub struct Server {
    child: Child,
    pub port: u16,
    pub http: String,
}

/// Ensure the `centrifugo` binary is freshly built before any test spawns it.
/// The conformance crate spawns the binary by path, so there is no cargo
/// dependency edge that would rebuild it; without this, `cargo test` can run
/// against a stale binary. Runs once per test process; a no-op when current.
fn ensure_binary_built() {
    use std::sync::Once;
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "centrifugo-server"])
            .status()
            .expect("run `cargo build -p centrifugo-server`");
        assert!(status.success(), "failed to build centrifugo-server");
    });
}

impl Server {
    // The child is owned by `Server`, whose `Drop` calls kill()+wait(); on the
    // health-timeout panic path it is dropped during unwind, so it is never a
    // true zombie.
    /// Spawn in insecure mode (no token required).
    pub async fn start() -> Server {
        Server::start_with(&["--client_insecure"]).await
    }

    /// Spawn with explicit extra `serve` args (e.g. `--token_hmac_secret_key secret`).
    pub async fn start_with(extra_args: &[&str]) -> Server {
        ensure_binary_built();
        let port = pick_port();
        let mut cmd = Command::new(bin_path());
        cmd.args(["serve", "--port", &port.to_string()]);
        cmd.args(extra_args);
        let child = cmd
            .spawn()
            .expect("spawn centrifugo binary (run `cargo build -p centrifugo-server` first)");
        // Own the child immediately so the panic path drops `Server` (kill+wait)
        // rather than leaking the process.
        let server = Server {
            child,
            port,
            http: format!("http://127.0.0.1:{port}"),
        };
        let client = reqwest::Client::new();
        for _ in 0..200 {
            if let Ok(resp) = client.get(format!("{}/health", server.http)).send().await {
                if resp.status().is_success() {
                    return server;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("server did not become healthy on port {port}");
    }

    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/connection/websocket", self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn bin_path() -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // repo root
    p.push("target");
    p.push("debug");
    p.push("centrifugo");
    p
}

pub(crate) fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// POST a body to `<http_base>/api` with an apikey header; return the first
/// JSON reply line.
pub async fn api_post(http_base: &str, api_key: &str, body: &str) -> serde_json::Value {
    let resp = reqwest::Client::new()
        .post(format!("{http_base}/api"))
        .header("Authorization", format!("apikey {api_key}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let text = resp.text().await.unwrap();
    serde_json::from_str(text.lines().next().unwrap_or("{}")).expect("valid api json reply")
}

/// POST to `/api` and return the HTTP status code.
pub async fn api_status(http_base: &str, api_key: &str, body: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("{http_base}/api"))
        .header("Authorization", format!("apikey {api_key}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// Reduce a JSON value to its structural "shape": object keys (sorted) and value
/// *types*, discarding leaf values. Lets differential tests compare structure
/// against the Go oracle while ignoring volatile values (client ids, epochs).
pub fn key_shape(v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(m) => {
            let mut out = serde_json::Map::new();
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for k in keys {
                out.insert(k.clone(), key_shape(&m[k]));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(key_shape).collect()),
        Value::String(_) => Value::String("<str>".into()),
        Value::Number(_) => Value::String("<num>".into()),
        Value::Bool(_) => Value::String("<bool>".into()),
        Value::Null => Value::String("<null>".into()),
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A minimal JSON WebSocket client speaking the v0.3.4 protocol.
pub struct WsJsonClient {
    ws: WsStream,
}

impl WsJsonClient {
    pub async fn connect(url: &str) -> Self {
        let (ws, _resp) = connect_async(url).await.expect("ws connect");
        WsJsonClient { ws }
    }

    pub async fn send_raw(&mut self, json: &str) {
        self.ws.send(Message::Text(json.to_string())).await.unwrap();
    }

    /// Send a CONNECT command (insecure, empty params); return the assigned client id.
    pub async fn connect_command(&mut self) -> String {
        self.send_raw(r#"{"id":1,"params":{}}"#).await;
        let v = self.next_json().await;
        v["result"]["client"]
            .as_str()
            .expect("connect result client id")
            .to_string()
    }

    /// Send a CONNECT command and return the full reply value.
    pub async fn connect_reply(&mut self) -> serde_json::Value {
        self.send_raw(r#"{"id":1,"params":{}}"#).await;
        self.next_json().await
    }

    /// Send a CONNECT command carrying a JWT; return the full reply value.
    pub async fn connect_with_token(&mut self, token: &str) -> serde_json::Value {
        self.send_raw(&format!(r#"{{"id":1,"params":{{"token":"{token}"}}}}"#))
            .await;
        self.next_json().await
    }

    /// Send a REFRESH command (method 10) with a new token; return the reply.
    pub async fn refresh(&mut self, id: u32, token: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":10,"params":{{"token":"{token}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    pub async fn subscribe(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":1,"params":{{"channel":"{channel}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    pub async fn publish(&mut self, id: u32, channel: &str, data: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":3,"params":{{"channel":"{channel}","data":{data}}}}}"#
        ))
        .await;
        self.next_json().await
    }

    pub async fn unsubscribe(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":2,"params":{{"channel":"{channel}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    /// Subscribe with the recover flag + last seen offset/epoch.
    pub async fn subscribe_recover(
        &mut self,
        id: u32,
        channel: &str,
        offset: u64,
        epoch: &str,
    ) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":1,"params":{{"channel":"{channel}","recover":true,"offset":{offset},"epoch":"{epoch}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    /// HISTORY command (method 6).
    pub async fn history(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":6,"params":{{"channel":"{channel}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    pub async fn presence(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":4,"params":{{"channel":"{channel}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    pub async fn presence_stats(&mut self, id: u32, channel: &str) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":5,"params":{{"channel":"{channel}"}}}}"#
        ))
        .await;
        self.next_json().await
    }

    /// Read pushes until one whose `result.data.info.client` equals `client_id`
    /// and `result.type` equals `push_type` (1=Join, 2=Leave). Panics on timeout.
    pub async fn next_join_leave_for(
        &mut self,
        push_type: u64,
        client_id: &str,
    ) -> serde_json::Value {
        for _ in 0..20 {
            let v = self.next_json().await;
            let r = &v["result"];
            let t = r.get("type").and_then(|x| x.as_u64()).unwrap_or(0);
            if t == push_type && r["data"]["info"]["client"] == client_id {
                return v;
            }
        }
        panic!("did not observe push type {push_type} for client {client_id}");
    }

    /// Read the next text frame's first JSON line, ignoring ping/pong/binary.
    pub async fn next_json(&mut self) -> serde_json::Value {
        loop {
            match tokio::time::timeout(Duration::from_secs(3), self.ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let line = t.lines().next().unwrap_or("{}");
                    return serde_json::from_str(line).expect("valid json frame");
                }
                Ok(Some(Ok(_))) => continue,
                other => panic!("ws closed/timeout waiting for json: {other:?}"),
            }
        }
    }

    /// Wait for the connection to close; return (close code, reason text).
    /// Returns code 0 if the socket ends without a Close frame.
    pub async fn next_close(&mut self) -> (u16, String) {
        loop {
            match tokio::time::timeout(Duration::from_secs(3), self.ws.next()).await {
                Ok(Some(Ok(Message::Close(frame)))) => {
                    return match frame {
                        Some(cf) => (u16::from(cf.code), cf.reason.to_string()),
                        None => (0, String::new()),
                    };
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(_))) | Ok(None) => return (0, String::new()),
                Err(_) => panic!("timeout waiting for close frame"),
            }
        }
    }
}

// ---- Protobuf WS client ----

use centrifugo_protocol::codec::{
    decode_params, decode_replies, encode_commands, encode_result, ProtocolType,
};
use centrifugo_protocol::messages::{
    ConnectRequest, ConnectResult, Publication, PublishRequest, SubscribeRequest,
};
use centrifugo_protocol::{pb, Command as ProtoCommand, MethodType, Raw, Reply};
use prost::Message as _;

/// A protobuf WebSocket client (uvarint-framed, Binary frames). Connect via a
/// URL carrying `?format=protobuf`.
pub struct PbWsClient {
    ws: WsStream,
}

impl PbWsClient {
    pub async fn connect(url: &str) -> Self {
        let (ws, _resp) = connect_async(url).await.expect("pb ws connect");
        PbWsClient { ws }
    }

    async fn send(&mut self, cmd: ProtoCommand) {
        let frame = encode_commands(ProtocolType::Protobuf, &[cmd]).unwrap();
        self.ws.send(Message::Binary(frame)).await.unwrap();
    }

    async fn next_reply(&mut self) -> Reply {
        loop {
            match tokio::time::timeout(Duration::from_secs(3), self.ws.next()).await {
                Ok(Some(Ok(Message::Binary(b)))) => {
                    if let Some(r) = decode_replies(ProtocolType::Protobuf, &b)
                        .unwrap()
                        .into_iter()
                        .next()
                    {
                        return r;
                    }
                }
                Ok(Some(Ok(_))) => continue,
                other => panic!("pb ws closed/timeout: {other:?}"),
            }
        }
    }

    pub async fn connect_command(&mut self) -> ConnectResult {
        let params = encode_result(ProtocolType::Protobuf, &ConnectRequest::default()).unwrap();
        self.send(ProtoCommand {
            id: 1,
            method: MethodType::Connect,
            params: Some(params),
        })
        .await;
        let r = self.next_reply().await;
        decode_params::<ConnectResult>(ProtocolType::Protobuf, &r.result).unwrap()
    }

    pub async fn subscribe(&mut self, id: u32, channel: &str) -> Reply {
        let req = SubscribeRequest {
            channel: channel.into(),
            ..Default::default()
        };
        let params = encode_result(ProtocolType::Protobuf, &req).unwrap();
        self.send(ProtoCommand {
            id,
            method: MethodType::Subscribe,
            params: Some(params),
        })
        .await;
        self.next_reply().await
    }

    pub async fn publish(&mut self, id: u32, channel: &str, data: &[u8]) -> Reply {
        let req = PublishRequest {
            channel: channel.into(),
            data: Some(Raw::from_bytes(data)),
        };
        let params = encode_result(ProtocolType::Protobuf, &req).unwrap();
        self.send(ProtoCommand {
            id,
            method: MethodType::Publish,
            params: Some(params),
        })
        .await;
        self.next_reply().await
    }

    /// Read the next push frame and decode its Publication.
    pub async fn next_publication(&mut self) -> Publication {
        let r = self.next_reply().await;
        assert_eq!(r.id, 0, "push must have id==0");
        let result = r.result.expect("push carries a result");
        let push = pb::Push::decode(result.as_bytes()).expect("decode pb Push");
        let pubn = pb::Publication::decode(&push.data[..]).expect("decode pb Publication");
        pubn.into()
    }
}
