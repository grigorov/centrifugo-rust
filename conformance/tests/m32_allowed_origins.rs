//! F3 (audit, security): `allowed_origins` is enforced on the WS upgrade and the
//! SockJS xhr transport. A disallowed `Origin` is rejected with HTTP 403 (Go
//! installs CheckOrigin on both transports, main.go:1226-1289). An allowed origin
//! connects; an absent Origin is always permitted (non-browser clients).

use conformance::{Server, WsJsonClient};

const CFG: &str = r#"{"client_insecure":true,"allowed_origins":["https://good.example"]}"#;

#[tokio::test]
async fn ws_disallowed_origin_is_forbidden() {
    let s = Server::start_with_config(CFG).await;
    match WsJsonClient::connect_with_origin(&s.ws_url(), "https://evil.example").await {
        Err(code) => assert_eq!(code, 403, "disallowed origin must be rejected 403"),
        Ok(_) => panic!("disallowed origin must be rejected, but the upgrade succeeded"),
    }
}

#[tokio::test]
async fn ws_allowed_origin_upgrades() {
    let s = Server::start_with_config(CFG).await;
    assert!(
        WsJsonClient::connect_with_origin(&s.ws_url(), "https://good.example")
            .await
            .is_ok(),
        "an allowed origin must upgrade"
    );
    // Origin matching is case-insensitive (Go lowercases the Origin).
    assert!(
        WsJsonClient::connect_with_origin(&s.ws_url(), "https://GOOD.example")
            .await
            .is_ok(),
        "origin match must be case-insensitive"
    );
}

#[tokio::test]
async fn ws_absent_origin_allowed_even_when_configured() {
    let s = Server::start_with_config(CFG).await;
    // A client that sends no Origin header (non-browser) is allowed.
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_reply().await;
    assert!(
        reply["error"].is_null(),
        "no-Origin connect must succeed: {reply}"
    );
}

#[tokio::test]
async fn sockjs_disallowed_origin_is_forbidden() {
    let s = Server::start_with_config(CFG).await;
    let url = format!("{}/connection/sockjs/0/sid_evil/xhr", s.http);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        403,
        "sockjs xhr with a disallowed origin must be 403"
    );
}

#[tokio::test]
async fn sockjs_allowed_origin_opens() {
    let s = Server::start_with_config(CFG).await;
    let url = format!("{}/connection/sockjs/0/sid_good/xhr", s.http);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Origin", "https://good.example")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "sockjs xhr with an allowed origin must open the session"
    );
}
