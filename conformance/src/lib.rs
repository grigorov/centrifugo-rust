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
    pub async fn start() -> Server {
        ensure_binary_built();
        let port = pick_port();
        let child = Command::new(bin_path())
            .args(["serve", "--port", &port.to_string(), "--client-insecure"])
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
        for _ in 0..100 {
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
}
