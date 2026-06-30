//! F5 (audit): `history_disable_for_client` makes client-side HISTORY return
//! ErrorNotAvailable (108) even when history IS stored (size+lifetime > 0),
//! matching Go handler.go:527 (`HistorySize<=0 || HistoryLifetime<=0 ||
//! HistoryDisableForClient`). Without the flag, a subscribed client can read
//! history normally.

use conformance::{Server, WsJsonClient};

const CFG: &str = r#"{"client_insecure":true,"namespaces":[
  {"name":"hd","history_size":10,"history_lifetime":60,"history_disable_for_client":true},
  {"name":"hn","history_size":10,"history_lifetime":60}
]}"#;

#[tokio::test]
async fn history_disabled_for_client_returns_not_available() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    // History is stored, but disabled for clients on this namespace -> 108. The
    // not-available gate precedes the subscription check, so no subscribe needed.
    let r = c.history(2, "hd:1").await;
    assert_eq!(
        r["error"]["code"], 108,
        "history_disable_for_client must yield 108: {r}"
    );
}

#[tokio::test]
async fn history_enabled_without_flag_is_available() {
    // Control: same history window, flag not set -> a subscribed client reads it.
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    assert!(c.subscribe(2, "hn:1").await["error"].is_null());
    let r = c.history(3, "hn:1").await;
    assert!(
        r["error"].is_null(),
        "history must be available without the flag: {r}"
    );
}
