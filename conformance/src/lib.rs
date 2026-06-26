//! Black-box conformance harness: spawn the `centrifugo` binary, wait until it
//! is healthy, and drive it over the real wire (WebSocket + JSON).
//!
//! Tests depend on the binary being built first (`cargo build -p
//! centrifugo-server`); `cargo test --workspace` builds all bins before tests.

use std::process::{Child, Command};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// A spawned Rust centrifugo server, killed on drop.
pub struct Server {
    child: Child,
    pub port: u16,
    pub http: String,
}

impl Server {
    pub async fn start() -> Server {
        let port = pick_port();
        let child = Command::new(bin_path())
            .args(["serve", "--port", &port.to_string(), "--client-insecure"])
            .spawn()
            .expect("spawn centrifugo binary (run `cargo build -p centrifugo-server` first)");
        let http = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();
        for _ in 0..100 {
            if let Ok(resp) = client.get(format!("{http}/health")).send().await {
                if resp.status().is_success() {
                    return Server { child, port, http };
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

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
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
