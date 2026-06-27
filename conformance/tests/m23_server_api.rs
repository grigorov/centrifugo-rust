//! A1: server-side `unsubscribe` / `disconnect` via the HTTP API. The server can
//! force a user off a channel (client receives an Unsubscribe push) or close a
//! user's connections. Validation mirrors Go (user required → 107, unknown
//! namespace → 102).

use conformance::{api_post, Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const SECRET: &str = "secret";
const KEY: &str = "apikey-test";

fn token(user: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": user }),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn api_unsubscribe_pushes_unsub_to_client() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET, "--api_key", KEY]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token("u1")).await;
    assert!(reply["error"].is_null(), "connect: {reply}");
    let sub = c.subscribe(2, "room").await;
    assert!(sub["error"].is_null(), "subscribe: {sub}");

    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"unsubscribe","params":{"user":"u1","channel":"room"}}"#,
    )
    .await;
    assert!(r["error"].is_null(), "api unsubscribe: {r}");

    // The client receives an Unsubscribe push (PushType::Unsub = 3) for "room".
    let push = c.next_json().await;
    assert!(push.get("id").is_none(), "push has no id: {push}");
    assert_eq!(push["result"]["type"], 3, "unsubscribe push type: {push}");
    assert_eq!(push["result"]["channel"], "room", "unsub channel: {push}");
}

#[tokio::test]
async fn api_disconnect_closes_user_connection() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET, "--api_key", KEY]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token("u2")).await;
    assert!(reply["error"].is_null(), "connect: {reply}");

    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"disconnect","params":{"user":"u2"}}"#,
    )
    .await;
    assert!(r["error"].is_null(), "api disconnect: {r}");

    let (code, _) = c.next_close().await;
    assert_eq!(
        code, 3012,
        "force disconnect code (DisconnectForceNoReconnect)"
    );
}

#[tokio::test]
async fn api_unsubscribe_disconnect_validation() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    // user required → 107.
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"unsubscribe","params":{"channel":"x"}}"#,
    )
    .await;
    assert_eq!(r["error"]["code"], 107, "unsubscribe empty user: {r}");

    let r = api_post(&s.http, KEY, r#"{"method":"disconnect","params":{}}"#).await;
    assert_eq!(r["error"]["code"], 107, "disconnect empty user: {r}");

    // unknown namespace → 102.
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"unsubscribe","params":{"user":"u","channel":"nope:x"}}"#,
    )
    .await;
    assert_eq!(
        r["error"]["code"], 102,
        "unsubscribe unknown namespace: {r}"
    );
}
