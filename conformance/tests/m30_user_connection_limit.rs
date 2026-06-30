//! F4 (audit): `client_user_connection_limit` caps concurrent connections per
//! authenticated user. The Nth+1 connection for a user is closed with
//! DisconnectConnectionLimit (3013, reconnect=false), matching centrifuge
//! client.go:1664 (`userConnectionLimit > 0 && user != "" && count >= limit`).
//! The limit is per-user and never applies to the empty (anonymous) user.

use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const SECRET: &str = "secret";

fn token(user: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": user }),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn second_connection_for_user_hits_connection_limit() {
    let s = Server::start_with_config(
        r#"{"token_hmac_secret_key":"secret","client_user_connection_limit":1}"#,
    )
    .await;
    // First connection for u1 is accepted (and stays open).
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    let ra = a.connect_with_token(&token("u1")).await;
    assert!(ra["error"].is_null(), "first connection must succeed: {ra}");

    // Second connection for the same user is closed with 3013 before any reply.
    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    b.send_raw(&format!(
        r#"{{"id":1,"params":{{"token":"{}"}}}}"#,
        token("u1")
    ))
    .await;
    let (code, _) = b.next_close().await;
    assert_eq!(code, 3013, "second connection for the user must close 3013");
}

#[tokio::test]
async fn limit_is_per_user_not_global() {
    let s = Server::start_with_config(
        r#"{"token_hmac_secret_key":"secret","client_user_connection_limit":1}"#,
    )
    .await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    assert!(a.connect_with_token(&token("alice")).await["error"].is_null());
    // A different user is unaffected by alice's connection count.
    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    let rb = b.connect_with_token(&token("bob")).await;
    assert!(rb["error"].is_null(), "a different user must connect: {rb}");
}

#[tokio::test]
async fn connection_limit_is_not_env_overridable() {
    // Parity: Go does NOT bind client_user_connection_limit via viper.BindEnv
    // (absent from main.go's bindEnvs), so a CENTRIFUGO_* env must be IGNORED —
    // config-file/default only. The env limit of 1 must NOT close a 2nd connection.
    let s = Server::start_env(
        &[
            ("CENTRIFUGO_CLIENT_USER_CONNECTION_LIMIT", "1"),
            ("CENTRIFUGO_TOKEN_HMAC_SECRET_KEY", SECRET),
        ],
        &[],
    )
    .await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    assert!(a.connect_with_token(&token("u1")).await["error"].is_null());
    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    let rb = b.connect_with_token(&token("u1")).await;
    assert!(
        rb["error"].is_null(),
        "client_user_connection_limit must NOT be env-overridable (Go parity): {rb}"
    );
}
