//! M2.5b: a command sent before CONNECT closes the connection with the
//! DisconnectBadRequest close code (3003) + JSON reason, and this matches the
//! real Go centrifugo v2.8.6.

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};

const EXPECTED_REASON: &str = r#"{"reason":"bad request","reconnect":false}"#;

#[tokio::test]
async fn command_before_connect_closes_bad_request() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    // SUBSCRIBE (method 1) before CONNECT.
    c.send_raw(r#"{"id":1,"method":1,"params":{"channel":"x"}}"#)
        .await;
    let (code, reason) = c.next_close().await;
    assert_eq!(code, 3003, "expected DisconnectBadRequest close code");
    assert_eq!(reason, EXPECTED_REASON);
}

#[tokio::test]
async fn command_before_connect_matches_go() {
    let Some(go) = Oracle::start().await else {
        return;
    };
    let rust = Server::start().await;

    let go_close = capture_close(&go.ws_url()).await;
    let rust_close = capture_close(&rust.ws_url()).await;
    assert_eq!(
        go_close, rust_close,
        "disconnect close (code, reason) differ"
    );
}

async fn capture_close(ws_url: &str) -> (u16, String) {
    let mut c = WsJsonClient::connect(ws_url).await;
    c.send_raw(r#"{"id":1,"method":1,"params":{"channel":"x"}}"#)
        .await;
    c.next_close().await
}
