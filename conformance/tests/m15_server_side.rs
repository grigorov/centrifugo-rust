//! Phase 1b: server-side channels. A connection JWT with a `channels` claim is
//! auto-subscribed on connect; the connect reply carries a `subs` map, and the
//! client receives publications on those channels without sending SUBSCRIBE.

use conformance::oracle::Oracle;
use conformance::{api_post, key_shape, Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

fn token(user: &str, channels: &[&str]) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": user, "channels": channels }),
        &EncodingKey::from_secret(b"secret"),
    )
    .unwrap()
}

#[tokio::test]
async fn connect_reply_carries_subs() {
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token("u", &["news"])).await;
    assert!(reply["error"].is_null(), "connect error: {reply}");
    assert!(
        reply["result"]["subs"]["news"].is_object(),
        "expected subs.news: {reply}"
    );
}

#[tokio::test]
async fn server_side_channel_delivers_without_subscribe() {
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret","api_key":"k"}"#).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    let reply = a.connect_with_token(&token("u", &["news"])).await;
    assert!(reply["result"]["subs"]["news"].is_object(), "subs: {reply}");

    // Publish via the HTTP API; `a` never sent SUBSCRIBE.
    let r = api_post(
        &s.http,
        "k",
        r#"{"method":"publish","params":{"channel":"news","data":{"x":1}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "publish error: {r}");

    let push = a.next_json().await;
    assert_eq!(push["result"]["channel"], "news", "push: {push}");
    assert_eq!(push["result"]["data"]["data"]["x"], 1, "push: {push}");
}

#[tokio::test]
async fn unknown_namespace_in_token_fails_connect() {
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    // "nope:room" names an undefined namespace -> connect error 102.
    let reply = c.connect_with_token(&token("u", &["nope:room"])).await;
    assert_eq!(
        reply["error"]["code"], 102,
        "expected unknown channel: {reply}"
    );
}

#[tokio::test]
async fn connect_subs_shape_matches_go() {
    let cfg = r#"{"token_hmac_secret_key":"secret"}"#;
    let Some(go) = Oracle::start_with_config(cfg).await else {
        return;
    };
    let rust = Server::start_with_config(cfg).await;
    let tok = token("u", &["news"]);
    let go_reply = {
        let mut c = WsJsonClient::connect(&go.ws_url()).await;
        c.connect_with_token(&tok).await
    };
    let rust_reply = {
        let mut c = WsJsonClient::connect(&rust.ws_url()).await;
        c.connect_with_token(&tok).await
    };
    assert_eq!(
        key_shape(&go_reply["result"]["subs"]),
        key_shape(&rust_reply["result"]["subs"]),
        "\nGO:   {go_reply}\nRUST: {rust_reply}"
    );
}
