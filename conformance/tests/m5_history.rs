//! M5: history + recovery over the real wire, with golden checks vs the Go
//! oracle. History enabled via --history_size/--history_lifetime, recovery via
//! --history_recover; insecure mode for simplicity.

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};

const HIST: &[&str] = &[
    "--client_insecure",
    "--history_size",
    "10",
    "--history_lifetime",
    "60",
    "--history_recover",
];

/// Go centrifugo exposes history only via config file (not CLI flags like
/// `--presence`), so the oracle gets it through a `-c` config.
const GO_HIST_CFG: &str =
    r#"{"client_insecure":true,"history_size":10,"history_lifetime":60,"history_recover":true}"#;

#[tokio::test]
async fn history_returns_published_with_offsets() {
    let s = Server::start_with(HIST).await;
    let mut p = WsJsonClient::connect(&s.ws_url()).await;
    p.connect_command().await;
    for i in 1..=3u32 {
        let r = p.publish(i, "h", &format!(r#"{{"n":{i}}}"#)).await;
        assert!(r.get("error").is_none(), "publish error: {r}");
    }
    // HISTORY requires an active subscription (Go returns PermissionDenied otherwise).
    let mut reader = WsJsonClient::connect(&s.ws_url()).await;
    reader.connect_command().await;
    reader.subscribe(2, "h").await;
    let hist = reader.history(10, "h").await;
    assert!(hist.get("error").is_none(), "history error: {hist}");
    let pubs = hist["result"]["publications"].as_array().unwrap();
    let offsets: Vec<u64> = pubs.iter().map(|p| p["offset"].as_u64().unwrap()).collect();
    assert_eq!(offsets, vec![1, 2, 3]);
    assert_eq!(pubs[2]["data"]["n"], 3);
}

#[tokio::test]
async fn history_disabled_returns_not_available() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut p = WsJsonClient::connect(&s.ws_url()).await;
    p.connect_command().await;
    let hist = p.history(10, "h").await;
    assert_eq!(hist["error"]["code"], 108);
}

#[tokio::test]
async fn recover_returns_missed_publications() {
    let s = Server::start_with(HIST).await;
    let (recovered, offsets) = recover_flow(&s.ws_url(), 1).await;
    assert!(recovered, "expected recovered=true");
    // Descending (newest first) under the seq/gen compatibility mode.
    assert_eq!(offsets, vec![3, 2]);
}

#[tokio::test]
async fn caught_up_client_recovers_after_history_lifetime_window() {
    // Go: history_lifetime clears the publication list but the stream meta (top
    // offset + epoch) persists (memory_history_meta_ttl defaults to 0). A
    // caught-up client reconnecting after the window must still get
    // recovered=true with no missed publications — NOT a reset epoch/offset.
    let s = Server::start_with_config(
        r#"{"client_insecure":true,"history_size":10,"history_lifetime":1,"history_recover":true}"#,
    )
    .await;
    let mut p = WsJsonClient::connect(&s.ws_url()).await;
    p.connect_command().await;
    for i in 1..=3u32 {
        p.publish(i, "r", &format!(r#"{{"n":{i}}}"#)).await;
    }
    // Learn the top position + epoch (caught-up client's last seen state).
    let mut s1 = WsJsonClient::connect(&s.ws_url()).await;
    s1.connect_command().await;
    let sub = s1.subscribe(2, "r").await;
    let epoch = sub["result"]["epoch"].as_str().unwrap().to_string();
    let top_seq = sub["result"]["seq"].as_u64().unwrap_or(0);
    assert_eq!(top_seq, 3, "top seq before window: {sub}");

    // Let the history window elapse (lifetime=1s) so the publication list is cleared.
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;

    // A caught-up client recovers from the top: meta persists -> recovered=true,
    // no publications.
    let mut s2 = WsJsonClient::connect(&s.ws_url()).await;
    s2.connect_command().await;
    let rec = s2.subscribe_recover(2, "r", top_seq, &epoch).await;
    assert!(
        rec["result"]["recovered"].as_bool().unwrap_or(false),
        "caught-up client must recover after the window: {rec}"
    );
    let n = rec["result"]["publications"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(n, 0, "no missed publications expected: {rec}");
}

#[tokio::test]
async fn recover_with_wrong_epoch_not_recovered() {
    let s = Server::start_with(HIST).await;
    let mut p = WsJsonClient::connect(&s.ws_url()).await;
    p.connect_command().await;
    for i in 1..=3u32 {
        p.publish(i, "r", &format!(r#"{{"n":{i}}}"#)).await;
    }
    let mut s2 = WsJsonClient::connect(&s.ws_url()).await;
    s2.connect_command().await;
    let rec = s2.subscribe_recover(2, "r", 1, "bogus-epoch").await;
    // `recovered` is false here; on the wire `false` is omitted (Go matches).
    assert!(
        !rec["result"]["recovered"].as_bool().unwrap_or(false),
        "expected not recovered: {rec}"
    );
}

#[tokio::test]
async fn history_matches_go() {
    let Some(go) = Oracle::start_with_config(GO_HIST_CFG).await else {
        return;
    };
    let rust = Server::start_with(HIST).await;
    assert_eq!(
        capture_history_offsets(&go.ws_url()).await,
        capture_history_offsets(&rust.ws_url()).await,
        "history offsets differ"
    );
}

#[tokio::test]
async fn recover_matches_go() {
    let Some(go) = Oracle::start_with_config(GO_HIST_CFG).await else {
        return;
    };
    let rust = Server::start_with(HIST).await;
    assert_eq!(
        recover_flow(&go.ws_url(), 1).await,
        recover_flow(&rust.ws_url(), 1).await,
        "recover outcome differs"
    );
}

async fn capture_history_offsets(url: &str) -> Vec<u64> {
    let mut p = WsJsonClient::connect(url).await;
    p.connect_command().await;
    for i in 1..=3u32 {
        p.publish(i, "h", &format!(r#"{{"n":{i}}}"#)).await;
    }
    let mut reader = WsJsonClient::connect(url).await;
    reader.connect_command().await;
    reader.subscribe(2, "h").await;
    let hist = reader.history(10, "h").await;
    hist["result"]["publications"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["offset"].as_u64().unwrap())
        .collect()
}

/// Publish 3 messages, learn the channel epoch, then recover from `since_offset`.
/// Returns (recovered, recovered-publication-offsets).
async fn recover_flow(url: &str, since_offset: u64) -> (bool, Vec<u64>) {
    let mut p = WsJsonClient::connect(url).await;
    p.connect_command().await;
    for i in 1..=3u32 {
        p.publish(i, "r", &format!(r#"{{"n":{i}}}"#)).await;
    }
    // Learn epoch via a non-recovering subscribe.
    let mut s1 = WsJsonClient::connect(url).await;
    s1.connect_command().await;
    let sub = s1.subscribe(2, "r").await;
    let epoch = sub["result"]["epoch"].as_str().unwrap().to_string();

    // New subscriber recovers from since_offset.
    let mut s2 = WsJsonClient::connect(url).await;
    s2.connect_command().await;
    let rec = s2.subscribe_recover(2, "r", since_offset, &epoch).await;
    let recovered = rec["result"]["recovered"].as_bool().unwrap_or(false);
    // Recovered publications carry seq (centrifugo default) or offset.
    let offsets = rec["result"]["publications"]
        .as_array()
        .map(|a| a.iter().map(pub_position).collect())
        .unwrap_or_default();
    (recovered, offsets)
}

/// A publication's stream position from seq (centrifugo default) or offset.
fn pub_position(p: &serde_json::Value) -> u64 {
    p.get("seq")
        .and_then(|x| x.as_u64())
        .or_else(|| p.get("offset").and_then(|x| x.as_u64()))
        .unwrap()
}
