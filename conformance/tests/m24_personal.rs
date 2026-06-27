//! C1: personal channels (`user_subscribe_to_personal`). A non-anonymous client
//! is auto-subscribed on connect to its personal channel (`#<user>`), reported in
//! the connect reply's `subs` map; a publish to it reaches the client.

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
async fn personal_channel_auto_subscribed_on_connect() {
    let s = Server::start_with(&[
        "--token_hmac_secret_key",
        SECRET,
        "--user_subscribe_to_personal",
        "--api_key",
        KEY,
    ])
    .await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token("alice")).await;
    assert!(reply["error"].is_null(), "connect: {reply}");
    assert!(
        reply["result"]["subs"]["#alice"].is_object(),
        "personal channel #alice must be in connect subs: {reply}"
    );

    // A publish to the personal channel reaches the auto-subscribed client.
    let r = api_post(
        &s.http,
        KEY,
        r##"{"method":"publish","params":{"channel":"#alice","data":{"hi":1}}}"##,
    )
    .await;
    assert!(r["error"].is_null(), "api publish: {r}");
    let push = c.next_json().await;
    assert_eq!(push["result"]["channel"], "#alice", "push channel: {push}");
    assert_eq!(push["result"]["data"]["data"]["hi"], 1, "push data: {push}");
}

#[tokio::test]
async fn no_personal_channel_for_empty_user() {
    // Personal enabled but an insecure (empty-user) connection gets no personal sub.
    let s = Server::start_with(&["--client_insecure", "--user_subscribe_to_personal"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_reply().await;
    assert!(reply["error"].is_null(), "connect: {reply}");
    let subs = &reply["result"]["subs"];
    assert!(
        subs.is_null() || subs.as_object().map(|m| m.is_empty()).unwrap_or(true),
        "empty user must have no personal sub: {reply}"
    );
}
