//! F2 (audit): node-wide `channel_max_length` (default 255) and
//! `client_channel_limit` (default 128) are enforced at SUBSCRIBE, returning
//! ErrorLimitExceeded (106), matching centrifuge `validateSubscribeRequest`
//! (client.go:1841-1875). Channel length uses `>` (255 ok, 256 rejected); the
//! per-client channel count uses `>=` (the Nth+1 subscribe is rejected).

use conformance::{Server, WsJsonClient};

#[tokio::test]
async fn channel_longer_than_max_length_is_limit_exceeded() {
    // Default channel_max_length is 255: a 255-char channel is accepted, a
    // 256-char channel is rejected with ErrorLimitExceeded (106).
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;

    let ok_channel = "a".repeat(255);
    let r = c.subscribe(2, &ok_channel).await;
    assert!(
        r["error"].is_null(),
        "255-char channel must be accepted: {r}"
    );

    let too_long = "b".repeat(256);
    let r = c.subscribe(3, &too_long).await;
    assert_eq!(r["error"]["code"], 106, "256-char channel must be 106: {r}");
}

#[tokio::test]
async fn channel_count_over_limit_is_limit_exceeded() {
    // client_channel_limit caps channels per client; with a limit of 2 the third
    // subscribe is rejected (count check is `numChannels >= limit`).
    let s = Server::start_with_config(r#"{"client_insecure":true,"client_channel_limit":2}"#).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;

    assert!(c.subscribe(2, "c1").await["error"].is_null(), "1st sub ok");
    assert!(c.subscribe(3, "c2").await["error"].is_null(), "2nd sub ok");
    let third = c.subscribe(4, "c3").await;
    assert_eq!(
        third["error"]["code"], 106,
        "3rd subscribe over the limit must be 106: {third}"
    );
}
