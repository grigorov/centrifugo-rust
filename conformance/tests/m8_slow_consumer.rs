//! Slow-consumer handling (audit #16): a subscriber that stops reading must be
//! force-disconnected with DisconnectSlow (3008) once its bounded write queue
//! fills — and that must NOT block the broadcaster or other subscribers. This is
//! the wire-visible side of the project's hard "10k/100k users don't block each
//! other" requirement.

use std::time::Duration;

use conformance::{Server, WsJsonClient};

const KEY: &str = "slowkey";

#[tokio::test]
async fn slow_consumer_is_disconnected_with_3008() {
    // Hard overall bound: a misbehaving runner fails fast with a clear message
    // instead of hanging the job indefinitely.
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("slow-consumer test exceeded 60s");
}

async fn run() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    // A healthy subscriber (will keep reading) and a slow one (stops reading).
    let mut healthy = WsJsonClient::connect(&s.ws_url()).await;
    healthy.connect_command().await;
    healthy.subscribe(2, "room").await;

    let mut slow = WsJsonClient::connect(&s.ws_url()).await;
    slow.connect_command().await;
    slow.subscribe(2, "room").await;

    // Flood via the HTTP API (no per-publish round-trip). The slow client never
    // reads, so its 256-deep write queue + socket buffers fill and it is dropped.
    //
    // Reuse ONE keep-alive client for the whole flood: a fresh `reqwest::Client`
    // per publish would open 5000 short-lived TCP connections in a burst, which on
    // a constrained CI runner exhausts ephemeral ports / piles up TIME_WAIT sockets
    // and stalls for minutes. A pooled client keeps it to a handful of connections.
    let client = reqwest::Client::new();
    let payload = "x".repeat(200);
    for i in 0..5000u32 {
        let _ = client
            .post(format!("{}/api", s.http))
            .header("Authorization", format!("apikey {KEY}"))
            .body(format!(
                r#"{{"method":"publish","params":{{"channel":"room","data":{{"i":{i},"p":"{payload}"}}}}}}"#
            ))
            .send()
            .await;
    }

    // The broadcaster was never blocked: the healthy subscriber still receives.
    let got = healthy.next_json().await;
    assert_eq!(got["result"]["channel"], "room", "healthy sub got: {got}");

    // The slow subscriber, once it drains, observes a DisconnectSlow (3008) close.
    let (code, _reason) = slow.next_close().await;
    assert_eq!(code, 3008, "slow consumer must be closed with 3008");
}
