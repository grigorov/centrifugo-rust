//! Black-box conformance harness: spawn the `centrifugo` binary, wait until it
//! is healthy, and drive it over the real wire (WebSocket + JSON).
//!
//! `Server::start` rebuilds the binary first (see `ensure_binary_built`), so a
//! plain `cargo test --workspace` always exercises current code even though the
//! binary is spawned by path rather than via a cargo dependency.

use std::process::{Child, Command, Stdio};
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
    /// gRPC API port when started via [`Server::start_grpc`].
    pub grpc_port: Option<u16>,
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

    /// Spawn with a JSON config file (`-c`), the way namespaces are configured.
    /// The same config can be passed to `Oracle::start_with_config` for goldens.
    pub async fn start_with_config(config_json: &str) -> Server {
        let port = pick_port();
        let cfg_path = std::env::temp_dir().join(format!("centrifugo-rust-{port}.json"));
        std::fs::write(&cfg_path, config_json).expect("write rust config");
        Server::start_spawn(port, &["-c", cfg_path.to_str().unwrap()]).await
    }

    /// Spawn with explicit extra `serve` args (e.g. `--token_hmac_secret_key secret`).
    pub async fn start_with(extra_args: &[&str]) -> Server {
        Server::start_spawn(pick_port(), extra_args).await
    }

    /// Spawn with `CENTRIFUGO_*` environment variables set (exercises the env
    /// config overlay) plus extra `serve` args.
    pub async fn start_env(env: &[(&str, &str)], extra_args: &[&str]) -> Server {
        Server::start_spawn_env(pick_port(), extra_args, env).await
    }

    /// Spawn with the gRPC API enabled. Injects `grpc_api`/`grpc_api_port`/
    /// `grpc_api_key` into the given JSON config so the chosen (free) gRPC port
    /// is known; `grpc_port` is then populated. The same `config_json` + key can
    /// be passed to `Oracle::start_grpc` for a golden.
    pub async fn start_grpc(config_json: &str, grpc_key: &str) -> Server {
        let port = pick_port();
        let grpc_port = pick_port();
        let cfg = inject_grpc(config_json, grpc_port, grpc_key);
        let cfg_path = std::env::temp_dir().join(format!("centrifugo-rust-grpc-{port}.json"));
        std::fs::write(&cfg_path, cfg).expect("write rust grpc config");
        let mut s = Server::start_spawn(port, &["-c", cfg_path.to_str().unwrap()]).await;
        s.grpc_port = Some(grpc_port);
        s
    }

    /// gRPC endpoint URL (requires a server started via [`Server::start_grpc`]).
    pub fn grpc_addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.grpc_port.expect("grpc enabled"))
    }

    /// Spawn a node backed by the Redis engine at `redis_addr`. Injects
    /// `engine`/`redis_address` into the JSON config so several nodes can share
    /// one Redis instance to form a cluster.
    pub async fn start_redis(redis_addr: &str, config_json: &str) -> Server {
        let port = pick_port();
        let mut cfg: serde_json::Value =
            serde_json::from_str(config_json).unwrap_or_else(|_| serde_json::json!({}));
        let obj = cfg.as_object_mut().expect("config must be a JSON object");
        obj.insert("engine".into(), serde_json::Value::String("redis".into()));
        obj.insert(
            "redis_address".into(),
            serde_json::Value::String(redis_addr.into()),
        );
        let cfg_path = std::env::temp_dir().join(format!("centrifugo-rust-redis-{port}.json"));
        std::fs::write(&cfg_path, cfg.to_string()).expect("write rust redis config");
        Server::start_spawn(port, &["-c", cfg_path.to_str().unwrap()]).await
    }

    /// Spawn a node that reaches Redis via Sentinel (engine=redis +
    /// redis_master_name + redis_sentinels injected into the config).
    pub async fn start_redis_sentinel(
        master_name: &str,
        sentinels: &str,
        config_json: &str,
    ) -> Server {
        let port = pick_port();
        let mut cfg: serde_json::Value =
            serde_json::from_str(config_json).unwrap_or_else(|_| serde_json::json!({}));
        let obj = cfg.as_object_mut().expect("config must be a JSON object");
        obj.insert("engine".into(), serde_json::Value::String("redis".into()));
        obj.insert(
            "redis_master_name".into(),
            serde_json::Value::String(master_name.into()),
        );
        obj.insert(
            "redis_sentinels".into(),
            serde_json::Value::String(sentinels.into()),
        );
        let cfg_path = std::env::temp_dir().join(format!("centrifugo-rust-sentinel-{port}.json"));
        std::fs::write(&cfg_path, cfg.to_string()).expect("write rust sentinel config");
        Server::start_spawn(port, &["-c", cfg_path.to_str().unwrap()]).await
    }

    async fn start_spawn(port: u16, extra_args: &[&str]) -> Server {
        Server::start_spawn_env(port, extra_args, &[]).await
    }

    async fn start_spawn_env(port: u16, extra_args: &[&str], env: &[(&str, &str)]) -> Server {
        ensure_binary_built();
        let mut cmd = Command::new(bin_path());
        cmd.args(["serve", "--port", &port.to_string()]);
        cmd.args(extra_args);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd
            .spawn()
            .expect("spawn centrifugo binary (run `cargo build -p centrifugo-server` first)");
        // Own the child immediately so the panic path drops `Server` (kill+wait)
        // rather than leaking the process.
        let server = Server {
            child,
            port,
            http: format!("http://127.0.0.1:{port}"),
            grpc_port: None,
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

/// A throwaway `redis-server` for multi-node tests, killed on drop. Returns
/// `None` (so the test skips, like the Go oracle) when `redis-server` is absent.
/// Persistence is disabled so it leaves nothing behind.
pub struct Redis {
    child: Child,
    pub addr: String,
}

impl Redis {
    pub async fn start() -> Option<Redis> {
        let port = pick_port();
        let addr = format!("127.0.0.1:{port}");
        let child = Command::new("redis-server")
            .args(["--port", &port.to_string(), "--save", "", "--appendonly", "no"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let child = match child {
            Ok(c) => c,
            Err(_) => {
                eprintln!("redis-server absent; skipping Redis differential test");
                return None;
            }
        };
        let redis = Redis { child, addr };
        for _ in 0..100 {
            if std::net::TcpStream::connect(&redis.addr).is_ok() {
                return Some(redis);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        eprintln!("redis did not become reachable; skipping");
        None
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A throwaway Redis master + a `redis-sentinel` monitoring it, for the Sentinel
/// integration test. Both killed on drop. `None` if either binary is absent.
pub struct RedisSentinel {
    master: Child,
    sentinel: Child,
    pub master_name: String,
    pub sentinel_addr: String,
}

impl RedisSentinel {
    pub async fn start() -> Option<RedisSentinel> {
        let master_port = pick_port();
        let sentinel_port = pick_port();
        let master_name = "mymaster".to_string();

        let master = Command::new("redis-server")
            .args(["--port", &master_port.to_string(), "--save", "", "--appendonly", "no"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let master = match master {
            Ok(c) => c,
            Err(_) => {
                eprintln!("redis-server absent; skipping Sentinel test");
                return None;
            }
        };
        // Wait for the master.
        let mut master = master;
        let master_addr = format!("127.0.0.1:{master_port}");
        let mut ready = false;
        for _ in 0..100 {
            if std::net::TcpStream::connect(&master_addr).is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !ready {
            let _ = master.kill();
            return None;
        }

        let conf = format!(
            "port {sentinel_port}\nsentinel monitor {master_name} 127.0.0.1 {master_port} 1\nsentinel down-after-milliseconds {master_name} 5000\n"
        );
        let conf_path = std::env::temp_dir().join(format!("centrifugo-sentinel-{sentinel_port}.conf"));
        if std::fs::write(&conf_path, conf).is_err() {
            let _ = master.kill();
            return None;
        }
        let sentinel = Command::new("redis-sentinel")
            .arg(&conf_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let sentinel = match sentinel {
            Ok(c) => c,
            Err(_) => {
                eprintln!("redis-sentinel absent; skipping Sentinel test");
                let _ = master.kill();
                return None;
            }
        };
        let sentinel_addr = format!("127.0.0.1:{sentinel_port}");
        for _ in 0..100 {
            if std::net::TcpStream::connect(&sentinel_addr).is_ok() {
                // Give Sentinel a moment to learn the master.
                tokio::time::sleep(Duration::from_millis(300)).await;
                return Some(RedisSentinel {
                    master,
                    sentinel,
                    master_name,
                    sentinel_addr,
                });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = master.kill();
        let mut sentinel = sentinel;
        let _ = sentinel.kill();
        None
    }
}

impl Drop for RedisSentinel {
    fn drop(&mut self) {
        let _ = self.sentinel.kill();
        let _ = self.sentinel.wait();
        let _ = self.master.kill();
        let _ = self.master.wait();
    }
}

pub(crate) fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Run a `centrifugo` subcommand (e.g. `gentoken`/`checkconfig`); return its exit
/// code and captured stdout. Builds the binary first if needed.
pub fn run_cli(args: &[&str]) -> (i32, String) {
    ensure_binary_built();
    let out = Command::new(bin_path())
        .args(args)
        .output()
        .expect("run centrifugo subcommand");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

/// Run the real `centrifuge-go` v0.6.2 SDK probe (`conformance/go-client`)
/// against `ws_url`: connect → subscribe → publish → receive. Returns the
/// process exit code + stdout, or `None` when `go` is unavailable (the test
/// skips, like the Go-oracle differential tests). The probe prints `OK` and
/// exits 0 on success.
pub fn run_go_client(ws_url: &str) -> Option<(i32, String)> {
    run_go_client_token(ws_url, "")
}

/// Like [`run_go_client`], passing a connection JWT to the SDK (exercises the
/// token-auth path). An empty `token` is omitted.
pub fn run_go_client_token(ws_url: &str, token: &str) -> Option<(i32, String)> {
    if Command::new("go")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| !s.success())
        .unwrap_or(true)
    {
        eprintln!("`go` unavailable; skipping centrifuge-go live SDK test");
        return None;
    }
    let dir = {
        let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("go-client");
        p
    };
    let mut args = vec!["run".to_string(), ".".to_string(), ws_url.to_string()];
    if !token.is_empty() {
        args.push(token.to_string());
    }
    let out = Command::new("go")
        .args(&args)
        // -mod=mod so `go run` resolves the pinned deps without a committed vendor dir.
        .env("GOFLAGS", "-mod=mod")
        .current_dir(&dir)
        .output()
        .expect("run centrifuge-go client");
    Some((
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned()
            + &String::from_utf8_lossy(&out.stderr),
    ))
}

/// Merge `grpc_api`/`grpc_api_port`/`grpc_api_key` into a JSON config object so
/// both harnesses configure the gRPC API the same way (Go and Rust read the same
/// keys). Returns the serialized config.
pub(crate) fn inject_grpc(config_json: &str, grpc_port: u16, grpc_key: &str) -> String {
    let mut cfg: serde_json::Value =
        serde_json::from_str(config_json).unwrap_or_else(|_| serde_json::json!({}));
    let obj = cfg.as_object_mut().expect("config must be a JSON object");
    obj.insert("grpc_api".into(), serde_json::Value::Bool(true));
    obj.insert("grpc_api_port".into(), grpc_port.into());
    obj.insert(
        "grpc_api_key".into(),
        serde_json::Value::String(grpc_key.into()),
    );
    cfg.to_string()
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

    /// Subscribe to a (private) channel with a subscription token.
    pub async fn subscribe_token(
        &mut self,
        id: u32,
        channel: &str,
        token: &str,
    ) -> serde_json::Value {
        self.send_raw(&format!(
            r#"{{"id":{id},"method":1,"params":{{"channel":"{channel}","token":"{token}"}}}}"#
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
