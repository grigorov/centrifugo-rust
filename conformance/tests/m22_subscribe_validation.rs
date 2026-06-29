//! Audit fixes: client SUBSCRIBE/UNSUBSCRIBE/SUB_REFRESH validation parity with
//! centrifuge v0.14.2 — already-subscribed (105), empty channel disconnects
//! (3003), and empty sub-refresh token (107 in-band).

use conformance::{Server, WsJsonClient};

#[tokio::test]
async fn duplicate_subscribe_is_already_subscribed() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    let first = c.subscribe(2, "room").await;
    assert!(first["error"].is_null(), "first subscribe: {first}");
    let second = c.subscribe(3, "room").await;
    assert_eq!(
        second["error"]["code"], 105,
        "duplicate subscribe: {second}"
    );
}

#[tokio::test]
async fn empty_subscribe_channel_disconnects_bad_request() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":1,"params":{"channel":""}}"#)
        .await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3003, "empty subscribe channel must disconnect 3003");
}

#[tokio::test]
async fn empty_unsubscribe_channel_disconnects_bad_request() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":2,"params":{"channel":""}}"#)
        .await;
    let (code, _) = c.next_close().await;
    assert_eq!(code, 3003, "empty unsubscribe channel must disconnect 3003");
}

#[tokio::test]
async fn subscribe_tolerates_explicit_null_recovery_fields() {
    // L2: a hand-rolled client sending explicit null for seq/gen/epoch must be
    // accepted (Go decodes null as zero), not rejected with 107.
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(
        r#"{"id":2,"method":1,"params":{"channel":"news","epoch":null,"seq":null,"gen":null}}"#,
    )
    .await;
    let r = c.next_json().await;
    assert!(
        r["error"].is_null(),
        "null recovery fields must not error: {r}"
    );
    assert_eq!(r["id"], 2);
}

#[tokio::test]
async fn empty_sub_refresh_token_is_bad_request_reply() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "room").await;
    // Empty token on a subscribed channel: in-band ErrorBadRequest (107), socket
    // stays open (NOT a DisconnectInvalidToken close).
    c.send_raw(r#"{"id":3,"method":11,"params":{"channel":"room","token":""}}"#)
        .await;
    let r = c.next_json().await;
    assert_eq!(r["error"]["code"], 107, "empty sub-refresh token: {r}");
}
