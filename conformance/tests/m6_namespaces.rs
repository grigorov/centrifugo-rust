//! M6.2: namespaces. A channel `ns:rest` resolves to namespace `ns`'s options;
//! an undefined namespace -> UnknownChannel(102). Configured via a JSON config
//! file (same content for Rust and the Go oracle).

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};

const CFG: &str = r#"{"client_insecure":true,"namespaces":[{"name":"news","presence":true}]}"#;

#[tokio::test]
async fn namespace_presence_enabled() {
    let s = Server::start_with_config(CFG).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    let sub = a.subscribe(2, "news:room").await;
    assert!(sub.get("error").is_none(), "subscribe error: {sub}");
    // presence is on in the `news` namespace.
    let pres = a.presence(3, "news:room").await;
    assert!(pres.get("error").is_none(), "presence error: {pres}");
    assert_eq!(pres["result"]["presence"].as_object().unwrap().len(), 1);
}

#[tokio::test]
async fn default_namespace_presence_disabled() {
    let s = Server::start_with_config(CFG).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    a.subscribe(2, "plain").await;
    // default namespace has presence off -> not available.
    let pres = a.presence(3, "plain").await;
    assert_eq!(pres["error"]["code"], 108);
}

#[tokio::test]
async fn unknown_namespace_returns_unknown_channel() {
    let s = Server::start_with_config(CFG).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    let sub = a.subscribe(2, "other:room").await;
    assert_eq!(sub["error"]["code"], 102, "sub: {sub}");
}

#[tokio::test]
async fn unknown_namespace_matches_go() {
    let Some(go) = Oracle::start_with_config(CFG).await else {
        return;
    };
    let rust = Server::start_with_config(CFG).await;
    assert_eq!(
        subscribe_err(&go.ws_url()).await,
        subscribe_err(&rust.ws_url()).await,
        "unknown-namespace subscribe error differs"
    );
}

async fn subscribe_err(url: &str) -> i64 {
    let mut a = WsJsonClient::connect(url).await;
    a.connect_command().await;
    let sub = a.subscribe(2, "other:room").await;
    sub["error"]["code"].as_i64().unwrap_or(0)
}
