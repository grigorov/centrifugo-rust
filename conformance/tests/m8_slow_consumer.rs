//! Slow-consumer handling (audit #16): a subscriber that stops reading must be
//! force-disconnected with DisconnectSlow (3008) once its bounded write queue
//! fills — and that must NOT block the broadcaster or other subscribers. This is
//! the wire-visible side of the project's hard "10k/100k users don't block each
//! other" requirement.

use std::time::Duration;

use conformance::{Server, WsJsonClient};
use futures_util::StreamExt;

const KEY: &str = "slowkey";
// Total flooded volume must overflow the slow client's socket buffers + 256-deep
// write queue on any runner (Linux CI autotunes TCP buffers larger than macOS), so
// use a large payload × enough messages: ~32 MB ≫ any reasonable socket buffer.
// (Once the slow client is dropped the remaining publishes are cheap no-ops, so a
// generous cap costs little.)
const PAYLOAD_BYTES: usize = 8192;
const PUBLISHES: u32 = 4000;
const CONCURRENCY: usize = 32;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_consumer_is_disconnected_with_3008() {
    // Hard overall bound: a misbehaving runner fails fast with a clear message
    // instead of hanging the job indefinitely.
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("slow-consumer test exceeded 60s");
}

async fn run() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    // Healthy subscriber that actively drains in the background — the witness that
    // the broadcaster keeps serving others while the slow consumer backs up.
    let mut healthy = WsJsonClient::connect(&s.ws_url()).await;
    healthy.connect_command().await;
    healthy.subscribe(2, "room").await;
    let healthy_task = tokio::spawn(async move {
        for _ in 0..5 {
            let v = healthy.next_json().await;
            assert_eq!(v["result"]["channel"], "room", "healthy sub got: {v}");
        }
    });

    // Slow subscriber: never reads.
    let mut slow = WsJsonClient::connect(&s.ws_url()).await;
    slow.connect_command().await;
    slow.subscribe(2, "room").await;

    // Flood via the HTTP API, concurrently (a sequential awaited loop is both slow
    // on a loaded runner and lets the slow client's writer keep draining; a burst
    // guarantees the queue overflows). A single pooled keep-alive client keeps this
    // to a handful of connections — a fresh client per call would churn thousands of
    // short-lived TCP connections and exhaust the runner's ephemeral ports.
    let client = reqwest::Client::new();
    let payload = "x".repeat(PAYLOAD_BYTES);
    let api = format!("{}/api", s.http);
    futures_util::stream::iter(0..PUBLISHES)
        .for_each_concurrent(CONCURRENCY, |i| {
            let client = &client;
            let api = &api;
            let body = format!(
                r#"{{"method":"publish","params":{{"channel":"room","data":{{"i":{i},"p":"{payload}"}}}}}}"#
            );
            async move {
                let _ = client
                    .post(api)
                    .header("Authorization", format!("apikey {KEY}"))
                    .body(body)
                    .send()
                    .await;
            }
        })
        .await;

    // The broadcaster was never blocked: the healthy subscriber kept receiving.
    healthy_task.await.expect("healthy subscriber task");

    // The slow subscriber, once it drains, observes a DisconnectSlow (3008) close.
    let (code, _reason) = slow.next_close().await;
    assert_eq!(code, 3008, "slow consumer must be closed with 3008");
}
