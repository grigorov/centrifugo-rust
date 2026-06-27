//! Phase 3: granular HTTP proxies (refresh / subscribe / publish / rpc). Each is
//! driven by a mock endpoint returning `{result|error}`; we assert the proxy's
//! decision flows through to the client.

use conformance::{Server, WsJsonClient};

/// Spawn a minimal HTTP server returning `body` (JSON) for any request.
async fn spawn(body: String) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let body = body.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    format!("http://127.0.0.1:{}/proxy", addr.port())
}

// ---- RPC proxy ----

#[tokio::test]
async fn rpc_proxy_returns_result() {
    let url = spawn(r#"{"result":{"data":{"answer":42}}}"#.into()).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_rpc_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":9,"params":{"method":"sum","data":{"a":1}}}"#).await;
    let r = c.next_json().await;
    assert!(r["error"].is_null(), "rpc error: {r}");
    assert_eq!(r["result"]["data"]["answer"], 42, "rpc result: {r}");
}

#[tokio::test]
async fn rpc_proxy_ack_only_is_success_without_data() {
    // A proxy that returns no `data` (ack-only RPC) must yield a success reply, not
    // an ErrorInternal — for JSON clients an empty Raw would break encoding.
    let url = spawn(r#"{"result":{}}"#.into()).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_rpc_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":9,"params":{"method":"ack"}}"#).await;
    let r = c.next_json().await;
    assert!(r["error"].is_null(), "ack-only rpc must succeed: {r}");
    // Go emits `{}` (data omitempty), not `{"data":null}`.
    assert!(
        r["result"].get("data").is_none(),
        "ack-only rpc must omit data: {r}"
    );
}

#[tokio::test]
async fn rpc_without_proxy_is_method_not_found() {
    let s = Server::start().await; // insecure, no rpc proxy
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":9,"params":{"method":"x"}}"#).await;
    let r = c.next_json().await;
    assert_eq!(r["error"]["code"], 104, "expected method not found: {r}");
}

// ---- Connect proxy (server-side channels) ----

#[tokio::test]
async fn connect_proxy_grants_server_side_channels() {
    // Go builds ConnectReply.Subscriptions from credentials.Channels; the granted
    // channels must appear in the connect reply's `subs` map.
    let url = spawn(r#"{"result":{"user":"u42","channels":["news"]}}"#.into()).await;
    let cfg = format!(r#"{{"proxy_connect_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let v = c.connect_reply().await;
    assert!(v["error"].is_null(), "connect: {v}");
    assert!(
        v["result"]["subs"]["news"].is_object(),
        "proxy-granted server-side sub missing: {v}"
    );
}

// ---- Publish proxy ----

#[tokio::test]
async fn publish_proxy_transforms_data() {
    let url = spawn(r#"{"result":{"data":{"x":2}}}"#.into()).await;
    let cfg =
        format!(r#"{{"client_insecure":true,"proxy_publish":true,"proxy_publish_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;

    let mut sub = WsJsonClient::connect(&s.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "room").await;

    let mut pubr = WsJsonClient::connect(&s.ws_url()).await;
    pubr.connect_command().await;
    pubr.publish(2, "room", r#"{"x":1}"#).await;

    // The subscriber receives the proxy-transformed payload, not the original.
    let push = sub.next_json().await;
    assert_eq!(push["result"]["data"]["data"]["x"], 2, "push: {push}");
}

#[tokio::test]
async fn publish_proxy_denies() {
    let url = spawn(r#"{"error":{"code":1000,"message":"nope"}}"#.into()).await;
    let cfg =
        format!(r#"{{"client_insecure":true,"proxy_publish":true,"proxy_publish_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    let r = c.publish(2, "room", r#"{"x":1}"#).await;
    assert_eq!(r["error"]["code"], 1000, "expected proxy error: {r}");
}

// ---- Subscribe proxy ----

#[tokio::test]
async fn subscribe_proxy_allows_and_denies() {
    let ok_url = spawn(r#"{"result":{}}"#.into()).await;
    let cfg = format!(
        r#"{{"client_insecure":true,"proxy_subscribe":true,"proxy_subscribe_endpoint":"{ok_url}"}}"#
    );
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    let r = c.subscribe(2, "room").await;
    assert!(r["error"].is_null(), "subscribe should be allowed: {r}");

    let deny_url = spawn(r#"{"error":{"code":1001,"message":"no"}}"#.into()).await;
    let cfg = format!(
        r#"{{"client_insecure":true,"proxy_subscribe":true,"proxy_subscribe_endpoint":"{deny_url}"}}"#
    );
    let s2 = Server::start_with_config(&cfg).await;
    let mut c2 = WsJsonClient::connect(&s2.ws_url()).await;
    c2.connect_command().await;
    let r = c2.subscribe(2, "room").await;
    assert_eq!(r["error"]["code"], 1001, "subscribe should be denied: {r}");
}

#[tokio::test]
async fn subscribe_proxy_disconnect_closes_connection() {
    // A subscribe-proxy `disconnect` closes the connection (Go c.close), not a 103
    // reply on an open socket.
    let url = spawn(r#"{"disconnect":{"code":4001,"reason":"go away"}}"#.into()).await;
    let cfg = format!(
        r#"{{"client_insecure":true,"proxy_subscribe":true,"proxy_subscribe_endpoint":"{url}"}}"#
    );
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":1,"params":{"channel":"room"}}"#).await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 4001, "subscribe-proxy disconnect must close with its code");
}

// ---- Refresh proxy ----

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn refresh_proxy_extends_and_expires() {
    let url = spawn(format!(r#"{{"result":{{"expire_at":{}}}}}"#, now() + 3600)).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_refresh_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":10,"params":{"token":"t"}}"#).await;
    let r = c.next_json().await;
    assert!(r["error"].is_null(), "refresh error: {r}");
    assert_eq!(r["result"]["expires"], true, "refresh result: {r}");

    // Expired -> DisconnectExpired (3005).
    let exp_url = spawn(r#"{"result":{"expired":true}}"#.into()).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_refresh_endpoint":"{exp_url}"}}"#);
    let s2 = Server::start_with_config(&cfg).await;
    let mut c2 = WsJsonClient::connect(&s2.ws_url()).await;
    c2.connect_command().await;
    c2.send_raw(r#"{"id":2,"method":10,"params":{"token":"t"}}"#).await;
    let (code, _) = c2.next_close().await;
    assert_eq!(code, 3005, "expired refresh proxy must disconnect 3005");
}

#[tokio::test]
async fn refresh_proxy_missing_result_disconnects_expired() {
    // No `result` (no credentials) → Go RefreshReply{Expired:true} → 3005, not a
    // success reply that would keep the connection alive forever.
    let url = spawn(r#"{}"#.into()).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_refresh_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":10,"params":{"token":"t"}}"#).await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3005, "missing refresh result must disconnect 3005");
}

#[tokio::test]
async fn refresh_empty_token_disconnects_even_with_proxy() {
    // Go handleRefresh rejects an empty token (3003) before any handler/proxy.
    let url = spawn(format!(r#"{{"result":{{"expire_at":{}}}}}"#, now() + 3600)).await;
    let cfg = format!(r#"{{"client_insecure":true,"proxy_refresh_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":10,"params":{}}"#).await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3003, "empty refresh token must disconnect 3003");
}
