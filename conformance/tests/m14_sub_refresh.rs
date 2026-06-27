//! Phase 1c: SUB_REFRESH (method 11). Refresh a subscription's expiry with a new
//! subscription token. Mirrors Go: not-subscribed -> 103, empty channel -> 3003,
//! invalid token / client-channel mismatch -> 3002, valid token -> SubRefreshResult.

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const CFG: &str = r#"{"client_insecure":true,"token_hmac_secret_key":"secret"}"#;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sub_token(client: &str, channel: &str, exp: Option<i64>, secret: &[u8]) -> String {
    let mut claims = serde_json::json!({ "client": client, "channel": channel });
    if let Some(e) = exp {
        claims["exp"] = e.into();
    }
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap()
}

fn sub_refresh_cmd(id: u32, channel: &str, token: &str) -> String {
    format!(r#"{{"id":{id},"method":11,"params":{{"channel":"{channel}","token":"{token}"}}}}"#)
}

#[tokio::test]
async fn sub_refresh_valid_token_succeeds() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    c.subscribe(2, "room").await;

    let token = sub_token(&id, "room", Some(now() + 3600), b"secret");
    c.send_raw(&sub_refresh_cmd(3, "room", &token)).await;
    let r = c.next_json().await;
    assert!(r["error"].is_null(), "sub_refresh error: {r}");
    assert_eq!(r["result"]["expires"], true, "expected expires: {r}");
    assert!(r["result"]["ttl"].as_u64().unwrap_or(0) > 0, "expected ttl: {r}");
}

#[tokio::test]
async fn sub_refresh_not_subscribed_permission_denied() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    // Never subscribed to "room".
    let token = sub_token(&id, "room", Some(now() + 3600), b"secret");
    c.send_raw(&sub_refresh_cmd(2, "room", &token)).await;
    let r = c.next_json().await;
    assert_eq!(r["error"]["code"], 103, "expected permission denied: {r}");
}

#[tokio::test]
async fn sub_refresh_empty_channel_disconnects_3003() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":11,"params":{"channel":"","token":"x"}}"#).await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3003, "empty channel must disconnect 3003");
}

#[tokio::test]
async fn sub_refresh_bad_token_disconnects_3002() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    c.subscribe(2, "room").await;
    // Token signed with the wrong secret -> invalid -> 3002.
    let token = sub_token(&id, "room", Some(now() + 3600), b"wrong-secret");
    c.send_raw(&sub_refresh_cmd(3, "room", &token)).await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3002, "bad sub_refresh token must disconnect 3002");
}

#[tokio::test]
async fn sub_refresh_outcome_matches_go() {
    let Some(go) = Oracle::start_with_config(CFG).await else {
        return;
    };
    let rust = Server::start_with_config(CFG).await;

    // Valid refresh: both succeed with expires=true.
    for url in [go.ws_url(), rust.ws_url()] {
        let mut c = WsJsonClient::connect(&url).await;
        let id = c.connect_command().await;
        c.subscribe(2, "room").await;
        let token = sub_token(&id, "room", Some(now() + 3600), b"secret");
        c.send_raw(&sub_refresh_cmd(3, "room", &token)).await;
        let r = c.next_json().await;
        assert!(r["error"].is_null(), "{url}: {r}");
        assert_eq!(r["result"]["expires"], true, "{url}: {r}");
    }
}
