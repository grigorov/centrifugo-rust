//! M3.6: JWT connection auth over the real wire against the Rust binary
//! (HS256 secret = "secret", matching the Go tests), plus a golden diff of the
//! connect reply vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{key_shape, Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const SECRET: &str = "secret";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sign(claims: serde_json::Value) -> String {
    sign_with(claims, SECRET)
}

fn sign_with(claims: serde_json::Value, secret: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn valid_hs256_connect_authenticates_and_user_propagates() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "user42"}));

    // Subscriber connects with token.
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    let reply = a.connect_with_token(&token).await;
    assert!(reply.get("error").is_none(), "connect error: {reply}");
    assert!(!reply["result"]["client"].as_str().unwrap().is_empty());
    let sub = a.subscribe(2, "news").await;
    assert!(sub.get("error").is_none(), "subscribe error: {sub}");

    // Publisher connects with the same token and publishes.
    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    b.connect_with_token(&token).await;
    let pubr = b.publish(2, "news", r#"{"m":1}"#).await;
    assert!(pubr.get("error").is_none(), "publish error: {pubr}");

    // The publication's info carries the authenticated user id.
    let push = a.next_json().await;
    assert_eq!(push["result"]["data"]["info"]["user"], "user42");
}

#[tokio::test]
async fn expired_token_returns_error_109() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "u", "exp": now() - 100}));
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token).await;
    assert_eq!(reply["error"]["code"], 109, "reply: {reply}");
}

#[tokio::test]
async fn bad_signature_closes_invalid_token_3002() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign_with(serde_json::json!({"sub": "u"}), "wrong-secret");
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(&format!(r#"{{"id":1,"params":{{"token":"{token}"}}}}"#))
        .await;
    let (code, reason) = c.next_close().await;
    assert_eq!(code, 3002);
    assert_eq!(reason, r#"{"reason":"invalid token","reconnect":false}"#);
}

#[tokio::test]
async fn no_token_closes_invalid_token_3002() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(r#"{"id":1,"params":{}}"#).await;
    let (code, _reason) = c.next_close().await;
    assert_eq!(code, 3002);
}

#[tokio::test]
async fn refresh_with_valid_token_succeeds() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "u", "exp": now() + 60}));
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token).await;
    assert!(reply.get("error").is_none());

    let new_token = sign(serde_json::json!({"sub": "u", "exp": now() + 3600}));
    let refresh = c.refresh(2, &new_token).await;
    assert!(refresh.get("error").is_none(), "refresh error: {refresh}");
    assert_eq!(refresh["result"]["expires"], true);
}

#[tokio::test]
async fn connect_reply_shape_matches_go() {
    let Some(go) = Oracle::start_with(&["--token_hmac_secret_key", SECRET]).await else {
        return;
    };
    let rust = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "user42"}));

    let go_reply = {
        let mut c = WsJsonClient::connect(&go.ws_url()).await;
        c.connect_with_token(&token).await
    };
    let rust_reply = {
        let mut c = WsJsonClient::connect(&rust.ws_url()).await;
        c.connect_with_token(&token).await
    };
    assert_eq!(
        key_shape(&go_reply),
        key_shape(&rust_reply),
        "\nGO:   {go_reply}\nRUST: {rust_reply}"
    );
}
