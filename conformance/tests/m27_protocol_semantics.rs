//! M27: post-audit protocol disconnect/error semantics, matching Go centrifugo
//! v2.8.6 on the abnormal-input paths:
//!   H1 malformed command params  -> close 3003 (not an in-band 107 reply)
//!   H5 unknown method int         -> reply 104, connection stays open
//!   M1 second CONNECT             -> close 3003
//!   M2 id==0 (non-Send) command   -> close 3003
//!   M3 RPC with no proxy          -> reply 108 (not 104)

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};

const BAD_REQUEST_REASON: &str = r#"{"reason":"bad request","reconnect":false}"#;

// H1: malformed params (subscribe params is a string, not an object) -> 3003.
#[tokio::test]
async fn malformed_params_close_bad_request() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":1,"params":"oops"}"#).await;
    let (code, reason) = c.next_close().await;
    assert_eq!(code, 3003, "malformed params must close 3003");
    assert_eq!(reason, BAD_REQUEST_REASON);
}

// H5: an unrecognized method int gets a 104 error reply; the connection stays
// open (a following PING still gets a reply).
#[tokio::test]
async fn unknown_method_replies_method_not_found() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":99}"#).await;
    let reply = c.next_json().await;
    assert_eq!(reply["id"], 2);
    assert_eq!(reply["error"]["code"], 104, "unknown method must reply 104");

    // Connection still open: a PING gets a reply.
    c.send_raw(r#"{"id":3,"method":7}"#).await;
    let ping = c.next_json().await;
    assert_eq!(
        ping["id"], 3,
        "connection must stay open after unknown method"
    );
    assert!(ping.get("error").is_none());
}

// M1: a second CONNECT on an already-authenticated connection -> 3003.
#[tokio::test]
async fn second_connect_closes_bad_request() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"params":{}}"#).await; // method 0 = CONNECT again
    let (code, reason) = c.next_close().await;
    assert_eq!(code, 3003, "second CONNECT must close 3003");
    assert_eq!(reason, BAD_REQUEST_REASON);
}

// M2: a reply-expecting command with id==0 -> 3003 ("command ID required").
#[tokio::test]
async fn zero_id_non_send_closes_bad_request() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"method":7}"#).await; // PING with id omitted (0)
    let (code, reason) = c.next_close().await;
    assert_eq!(code, 3003, "id==0 non-Send must close 3003");
    assert_eq!(reason, BAD_REQUEST_REASON);
}

// M3: RPC when no RPC proxy is configured -> ErrorNotAvailable (108), not 104.
#[tokio::test]
async fn rpc_without_proxy_not_available() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":9,"params":{"method":"foo","data":{}}}"#)
        .await;
    let reply = c.next_json().await;
    assert_eq!(
        reply["error"]["code"], 108,
        "RPC with no proxy must reply 108"
    );
}

// Golden parity: the unknown-method 104 reply matches the Go oracle exactly.
#[tokio::test]
async fn unknown_method_matches_go() {
    let Some(go) = Oracle::start_with_config(r#"{"client_insecure":true}"#).await else {
        return;
    };
    let rust = Server::start_with(&["--client_insecure"]).await;

    let go_reply = unknown_method_reply(&go.ws_url()).await;
    let rust_reply = unknown_method_reply(&rust.ws_url()).await;
    assert_eq!(go_reply, rust_reply, "unknown-method reply differs from Go");
}

async fn unknown_method_reply(ws_url: &str) -> serde_json::Value {
    let mut c = WsJsonClient::connect(ws_url).await;
    c.connect_command().await;
    c.send_raw(r#"{"id":2,"method":99}"#).await;
    c.next_json().await
}
