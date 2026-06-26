//! M1 golden differential: drive the Rust binary and the real Go centrifugo
//! v2.8.6 with identical commands and assert the reply/push *shapes* match
//! (keys + value types; volatile values like client ids are ignored). Skips
//! cleanly if the Go oracle binary is not built.

use conformance::oracle::Oracle;
use conformance::{key_shape, Server, WsJsonClient};

#[tokio::test]
async fn connect_reply_matches_go() {
    let Some(go) = Oracle::start().await else {
        return;
    };
    let rust = Server::start().await;

    let go_reply = {
        let mut c = WsJsonClient::connect(&go.ws_url()).await;
        c.connect_reply().await
    };
    let rust_reply = {
        let mut c = WsJsonClient::connect(&rust.ws_url()).await;
        c.connect_reply().await
    };

    assert_eq!(
        key_shape(&go_reply),
        key_shape(&rust_reply),
        "\nGO:   {go_reply}\nRUST: {rust_reply}"
    );
}

#[tokio::test]
async fn publication_push_matches_go() {
    let Some(go) = Oracle::start().await else {
        return;
    };
    let rust = Server::start().await;

    let go_push = capture_push(&go.ws_url()).await;
    let rust_push = capture_push(&rust.ws_url()).await;

    assert_eq!(
        key_shape(&go_push),
        key_shape(&rust_push),
        "\nGO push:   {go_push}\nRUST push: {rust_push}"
    );
}

/// Connect a subscriber + a publisher, publish once, and return the publication
/// push the subscriber receives.
async fn capture_push(ws_url: &str) -> serde_json::Value {
    let mut a = WsJsonClient::connect(ws_url).await;
    a.connect_command().await;
    let sub = a.subscribe(2, "news").await;
    assert!(sub.get("error").is_none(), "subscribe error: {sub}");

    let mut b = WsJsonClient::connect(ws_url).await;
    b.connect_command().await;
    let pubr = b.publish(2, "news", r#"{"msg":"hello"}"#).await;
    assert!(pubr.get("error").is_none(), "publish error: {pubr}");

    a.next_json().await
}
