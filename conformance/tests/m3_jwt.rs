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
    // `--publish` so the token-mode (non-insecure) client may publish.
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET, "--publish"]).await;
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
async fn no_token_closes_bad_request_3003() {
    // Go (OnClientConnecting): no token + not insecure/anonymous -> credentials
    // nil -> centrifuge DisconnectBadRequest (3003), NOT 3002 (which is reserved
    // for an actually-invalid token).
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(r#"{"id":1,"params":{}}"#).await;
    let (code, _reason) = c.next_close().await;
    assert_eq!(code, 3003);
}

#[tokio::test]
async fn no_token_close_code_matches_go() {
    // Differential: the missing-token close code must match the Go oracle exactly.
    let Some(go) = Oracle::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await else {
        return;
    };
    let rust = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let go_code = {
        let mut c = WsJsonClient::connect(&go.ws_url()).await;
        c.send_raw(r#"{"id":1,"params":{}}"#).await;
        c.next_close().await.0
    };
    let rust_code = {
        let mut c = WsJsonClient::connect(&rust.ws_url()).await;
        c.send_raw(r#"{"id":1,"params":{}}"#).await;
        c.next_close().await.0
    };
    assert_eq!(
        go_code, rust_code,
        "missing-token close code: go={go_code} rust={rust_code}"
    );
}

#[tokio::test]
async fn client_anonymous_allows_tokenless_connect() {
    // With client_anonymous=true and no token, Go accepts an empty-user connection.
    let s =
        Server::start_with_config(r#"{"token_hmac_secret_key":"secret","client_anonymous":true}"#)
            .await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_reply().await;
    assert!(reply["error"].is_null(), "anon connect error: {reply}");
    assert!(
        reply["result"]["client"].as_str().is_some(),
        "no client id: {reply}"
    );
}

#[tokio::test]
async fn insecure_still_uses_token_identity() {
    // Go: in insecure mode a present token is still verified; user/info are kept,
    // only expiry is zeroed. Verify the token's user propagates to presence.
    let s = Server::start_with_config(
        r#"{"client_insecure":true,"token_hmac_secret_key":"secret","presence":true}"#,
    )
    .await;
    let token = sign(serde_json::json!({"sub": "token-user"}));
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    assert!(c.connect_with_token(&token).await["error"].is_null());
    c.subscribe(2, "room").await;
    let p = c.presence(3, "room").await;
    let presence = p["result"]["presence"].as_object().expect("presence map");
    let entry = presence.values().next().expect("one entry");
    assert_eq!(
        entry["user"], "token-user",
        "insecure must keep token user: {p}"
    );
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
async fn refresh_with_expired_token_disconnects_3005() {
    // Go (centrifuge handleRefresh): an expired refresh token -> RefreshReply{Expired}
    // -> DisconnectExpired (3005), NOT a 110 error reply.
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "u", "exp": now() + 60}));
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    assert!(c.connect_with_token(&token).await.get("error").is_none());

    let expired = sign(serde_json::json!({"sub": "u", "exp": now() - 10}));
    c.send_raw(&format!(
        r#"{{"id":2,"method":10,"params":{{"token":"{expired}"}}}}"#
    ))
    .await;
    let (code, _reason) = c.next_close().await;
    assert_eq!(code, 3005, "expired refresh must close with 3005");
}

#[tokio::test]
async fn refresh_with_empty_token_disconnects_3003() {
    // Go: empty refresh token -> DisconnectBadRequest (3003) before verification.
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let token = sign(serde_json::json!({"sub": "u", "exp": now() + 60}));
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    assert!(c.connect_with_token(&token).await.get("error").is_none());

    c.send_raw(r#"{"id":2,"method":10,"params":{"token":""}}"#)
        .await;
    let (code, _reason) = c.next_close().await;
    assert_eq!(code, 3003, "empty refresh token must close with 3003");
}

#[tokio::test]
async fn connect_reply_shape_matches_go() {
    // Go takes the HMAC secret via config file, not a CLI flag.
    let Some(go) = Oracle::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await else {
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
