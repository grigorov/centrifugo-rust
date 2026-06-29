//! L5: the WebSocket writer coalesces up to 4 queued messages into one WS frame
//! (Go's per-connection writer, defaultMaxMessagesInFrame=4). This is a frame
//! boundary optimization: coalescing must never drop, reorder, or corrupt
//! messages — the SDK splits a frame back into individual NDJSON messages.

use conformance::{api_post, Server, WsJsonClient};

const KEY: &str = "seckey";
const BURST: usize = 40;

#[tokio::test]
async fn writer_coalesces_without_losing_messages() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "bench").await;

    // Fire the whole burst concurrently so the publications pile into the
    // subscriber's write queue faster than the writer drains them one-by-one,
    // exercising the coalescing path.
    let publishes = (0..BURST).map(|i| {
        let http = s.http.clone();
        async move {
            api_post(
                &http,
                KEY,
                &format!(
                    r#"{{"method":"publish","params":{{"channel":"bench","data":{{"n":{i}}}}}}}"#
                ),
            )
            .await
        }
    });
    futures_util::future::join_all(publishes).await;

    // Read raw frames until all BURST publications arrive; count frames + messages.
    let mut frames = 0usize;
    let mut got: Vec<u64> = Vec::new();
    while got.len() < BURST {
        let frame = c.next_text_frame().await;
        frames += 1;
        for line in frame.lines().filter(|l| !l.is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid json");
            // A publication push is {"result":{"data":<Publication>}} where the
            // Publication's own `data` is our payload {"n":N}.
            if let Some(n) = v["result"]["data"]["data"]["n"].as_u64() {
                got.push(n);
            }
        }
    }

    // Integrity: every publication arrives exactly once (concurrent publishes
    // have no defined order, so check the set, not the sequence).
    assert_eq!(
        got.len(),
        BURST,
        "all publications must arrive exactly once"
    );
    got.sort_unstable();
    assert!(
        got.iter().enumerate().all(|(i, n)| *n == i as u64),
        "publications must arrive without loss or duplication: {got:?}"
    );
    // Coalescing observed: the burst was delivered in fewer frames than messages.
    assert!(
        frames < BURST,
        "expected coalescing (frames {frames} < messages {BURST})"
    );
}
