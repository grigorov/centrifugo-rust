//! M10 (connect proxy): when `proxy_connect_endpoint` is set, a tokenless
//! CONNECT is authenticated by an HTTP callback. A mock endpoint grants a user;
//! we confirm the connection takes that identity (visible via presence) and that
//! a proxy denial closes the connection.

use conformance::{api_post, Server, WsJsonClient};

/// Spawn a minimal HTTP server returning `body` (JSON) for any request. Returns
/// the endpoint URL; the task is detached (reaped when the test process exits).
async fn spawn_http_json(body: String) -> String {
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
                let _ = sock.read(&mut buf).await; // drain request (headers + small body)
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
    format!("http://127.0.0.1:{}/connect", addr.port())
}

#[tokio::test]
async fn connect_proxy_grants_identity() {
    let url = spawn_http_json(r#"{"result":{"user":"proxied-user"}}"#.into()).await;
    let cfg = format!(r#"{{"proxy_connect_endpoint":"{url}","presence":true,"api_key":"k"}}"#);
    let s = Server::start_with_config(&cfg).await;

    // No token — the proxy is the authenticator.
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_reply().await;
    assert!(reply["error"].is_null(), "connect error: {reply}");
    assert!(
        reply["result"]["client"].as_str().is_some(),
        "expected client id: {reply}"
    );

    // The proxied identity shows up in presence.
    c.subscribe(2, "room").await;
    let p = api_post(&s.http, "k", r#"{"method":"presence","params":{"channel":"room"}}"#).await;
    let presence = p["result"]["presence"].as_object().expect("presence map");
    let entry = presence.values().next().expect("one presence entry");
    assert_eq!(entry["user"], "proxied-user", "presence: {p}");
}

#[tokio::test]
async fn connect_proxy_denial_disconnects() {
    let url = spawn_http_json(r#"{"error":{"code":1000,"message":"denied"}}"#.into()).await;
    let cfg = format!(r#"{{"proxy_connect_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;

    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(r#"{"id":1,"params":{}}"#).await;
    let (code, _reason) = c.next_close().await;
    assert!(code >= 3000, "expected a disconnect close code, got {code}");
}
