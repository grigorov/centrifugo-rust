//! M6.1: server HTTP API (POST /api, apikey auth) over the real wire, with a
//! golden check of the history result shape vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{api_post, api_status, key_shape, Server, WsJsonClient};

const KEY: &str = "testkey";
const HIST: &[&str] = &[
    "--client_insecure",
    "--api_key",
    KEY,
    "--history_size",
    "10",
    "--history_lifetime",
    "60",
];

#[tokio::test]
async fn publish_via_api_returns_void_reply() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"publish","params":{"channel":"x","data":{"a":1}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "publish error: {r}");
    assert!(r.get("result").is_none(), "publish must omit result: {r}");
}

#[tokio::test]
async fn api_echoes_request_content_type() {
    // Go's API handler sets the response Content-Type to the request's (audit #24).
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api", s.http))
        .header("Authorization", format!("apikey {KEY}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .body(r#"{"method":"info","params":{}}"#)
        .send()
        .await
        .unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert_eq!(ct, "application/json; charset=utf-8", "echoed content-type");
}

#[tokio::test]
async fn bad_apikey_returns_401() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let code = api_status(&s.http, "wrong", r#"{"method":"info","params":{}}"#).await;
    assert_eq!(code, 401);
}

#[tokio::test]
async fn channels_lists_subscribed() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "room").await;
    let r = api_post(&s.http, KEY, r#"{"method":"channels","params":{}}"#).await;
    let channels = r["result"]["channels"].as_array().unwrap();
    assert!(channels.iter().any(|c| c == "room"), "channels: {r}");
}

#[tokio::test]
async fn info_returns_nodes() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let r = api_post(&s.http, KEY, r#"{"method":"info","params":{}}"#).await;
    assert!(r["result"]["nodes"].as_array().is_some(), "info: {r}");
    assert_eq!(r["result"]["nodes"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn history_via_api_returns_publications() {
    let s = Server::start_with(HIST).await;
    for i in 1..=3u32 {
        let r = api_post(
            &s.http,
            KEY,
            &format!(r#"{{"method":"publish","params":{{"channel":"h","data":{{"n":{i}}}}}}}"#),
        )
        .await;
        assert!(r.get("error").is_none(), "publish error: {r}");
    }
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"history","params":{"channel":"h"}}"#,
    )
    .await;
    let pubs = r["result"]["publications"].as_array().unwrap();
    assert_eq!(pubs.len(), 3, "history: {r}");
    assert_eq!(pubs[2]["data"]["n"], 3);
    // API publications carry no offset/seq.
    assert!(pubs[0].get("offset").is_none());
    assert!(pubs[0].get("seq").is_none());
}

#[tokio::test]
async fn presence_stats_via_api() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY, "--presence"]).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"presence_stats","params":{"channel":"room"}}"#,
    )
    .await;
    assert_eq!(r["result"]["num_clients"], 1, "stats: {r}");
    assert_eq!(r["result"]["num_users"], 1);
}

#[tokio::test]
async fn api_validation_error_codes() {
    // Go executor parity: empty channel/data -> 107, unknown namespace -> 102,
    // capability disabled -> 108.
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    // publish empty data -> 107.
    let r = api_post(&s.http, KEY, r#"{"method":"publish","params":{"channel":"x"}}"#).await;
    assert_eq!(r["error"]["code"], 107, "empty data: {r}");

    // presence on a channel without presence enabled -> 108.
    let r = api_post(&s.http, KEY, r#"{"method":"presence","params":{"channel":"x"}}"#).await;
    assert_eq!(r["error"]["code"], 108, "presence disabled: {r}");

    // history on a channel without history enabled -> 108.
    let r = api_post(&s.http, KEY, r#"{"method":"history","params":{"channel":"x"}}"#).await;
    assert_eq!(r["error"]["code"], 108, "history disabled: {r}");

    // empty channel -> 107.
    let r = api_post(&s.http, KEY, r#"{"method":"presence","params":{"channel":""}}"#).await;
    assert_eq!(r["error"]["code"], 107, "empty channel: {r}");
}

#[tokio::test]
async fn api_unknown_namespace_matches_go() {
    // A channel naming an undefined namespace -> 102 on both implementations.
    let cfg = r#"{"client_insecure":true,"api_key":"testkey"}"#;
    let Some(go) = Oracle::start_with_config(cfg).await else {
        return;
    };
    let rust = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;
    let body = r#"{"method":"publish","params":{"channel":"nope:room","data":{"a":1}}}"#;
    let go_r = api_post(&go.http, KEY, body).await;
    let rust_r = api_post(&rust.http, KEY, body).await;
    assert_eq!(go_r["error"]["code"], 102, "go: {go_r}");
    assert_eq!(rust_r["error"]["code"], 102, "rust: {rust_r}");
}

#[tokio::test]
async fn history_result_shape_matches_go() {
    let go_cfg =
        r#"{"client_insecure":true,"api_key":"testkey","history_size":10,"history_lifetime":60}"#;
    let Some(go) = Oracle::start_with_config(go_cfg).await else {
        return;
    };
    let rust = Server::start_with(HIST).await;
    let go_hist = capture_api_history(&go.http).await;
    let rust_hist = capture_api_history(&rust.http).await;
    assert_eq!(
        key_shape(&go_hist),
        key_shape(&rust_hist),
        "\nGO:   {go_hist}\nRUST: {rust_hist}"
    );
}

async fn capture_api_history(http: &str) -> serde_json::Value {
    for i in 1..=3u32 {
        api_post(
            http,
            KEY,
            &format!(r#"{{"method":"publish","params":{{"channel":"h","data":{{"n":{i}}}}}}}"#),
        )
        .await;
    }
    api_post(
        http,
        KEY,
        r#"{"method":"history","params":{"channel":"h"}}"#,
    )
    .await
}
