//! Performance comparison: the Rust reimplementation vs the real Go Centrifugo
//! v2.8.6, over identical load. Ignored by default (slow + needs the Go oracle):
//!
//!     cargo test --test perf -- --ignored --nocapture
//!
//! Two metrics, same methodology for both backends so the *ratio* is meaningful:
//!  - **Fan-out throughput**: SUBS subscribers on one channel, PUBS publishes via
//!    the HTTP API → SUBS×PUBS deliveries; rate = deliveries / wall-clock.
//!  - **Broadcast latency**: single subscriber, median/p95 of publish-call→delivery.
//!
//! Both run insecure + memory engine (apples-to-apples single-node fan-out). A raw
//! frame counter (substring match, no JSON parse) keeps the client off the critical
//! path so the *server* is what's measured.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use conformance::oracle::Oracle;
use conformance::Server;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const KEY: &str = "perfkey";
const CFG: &str = r#"{"client_insecure":true,"api_key":"perfkey"}"#;
const CHANNEL: &str = "bench";

const SUBS: usize = 100;
const PUBS: usize = 500;
const PUBLISHERS: usize = 10;
const LAT_SAMPLES: usize = 200;

/// A publication push carries `"channel":"bench"`; command replies do not.
fn is_push(t: &str) -> bool {
    t.contains(r#""channel":"bench""#)
}

async fn connect_sub(
    ws_url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (mut ws, _) = connect_async(ws_url).await.expect("ws connect");
    ws.send(Message::Text(r#"{"id":1,"params":{}}"#.into()))
        .await
        .unwrap();
    ws.send(Message::Text(format!(
        r#"{{"id":2,"method":1,"params":{{"channel":"{CHANNEL}"}}}}"#
    )))
    .await
    .unwrap();
    // Drain the connect + subscribe replies (both carry "id").
    let mut acks = 0;
    while acks < 2 {
        if let Some(Ok(Message::Text(t))) = ws.next().await {
            if t.contains(r#""id":"#) {
                acks += 1;
            }
        }
    }
    ws
}

async fn publish_n(http: &str, n: usize) {
    let client = reqwest::Client::new();
    let per = n / PUBLISHERS;
    let mut handles = Vec::new();
    for p in 0..PUBLISHERS {
        let client = client.clone();
        let http = http.to_string();
        handles.push(tokio::spawn(async move {
            for i in 0..per {
                let body = format!(
                    r#"{{"method":"publish","params":{{"channel":"{CHANNEL}","data":{{"x":{}}}}}}}"#,
                    p * per + i
                );
                let _ = client
                    .post(format!("{http}/api"))
                    .header("Authorization", format!("apikey {KEY}"))
                    .body(body)
                    .send()
                    .await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// SUBS subscribers, PUBS publishes; returns (deliveries/sec, delivered, expected).
///
/// Drop-tolerant: instead of requiring every subscriber to receive every message
/// (which hangs forever if a backend sheds a slow client), we count deliveries
/// into a shared atomic and stop when either the full count arrives or deliveries
/// go quiet — measuring elapsed only up to the last delivery seen.
async fn fanout_throughput(ws_url: &str, http: &str) -> (f64, usize, usize) {
    let target = (PUBS / PUBLISHERS) * PUBLISHERS;
    let expected = SUBS * target;
    let delivered = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(SUBS);
    for _ in 0..SUBS {
        let url = ws_url.to_string();
        let ready_tx = ready_tx.clone();
        let delivered = delivered.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut ws = connect_sub(&url).await;
            let _ = ready_tx.send(()).await;
            while !stop.load(Ordering::Relaxed) {
                match ws.next().await {
                    Some(Ok(Message::Text(t))) if is_push(&t) => {
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Ok(_)) => {}
                    _ => break, // closed/error (e.g. backend dropped a slow client)
                }
            }
        });
    }
    drop(ready_tx);
    for _ in 0..SUBS {
        ready_rx.recv().await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await; // let subscriptions settle

    let t0 = Instant::now();
    publish_n(http, target).await;

    // Poll until complete, or until deliveries go quiet for 2s, or a 30s cap.
    let mut last = 0usize;
    let mut last_progress = Instant::now();
    let mut done_at = t0.elapsed();
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let now = delivered.load(Ordering::Relaxed);
        if now > last {
            last = now;
            last_progress = Instant::now();
            done_at = t0.elapsed();
        }
        if now >= expected
            || last_progress.elapsed() > Duration::from_secs(2)
            || t0.elapsed() > Duration::from_secs(30)
        {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    let count = delivered.load(Ordering::Relaxed);
    (count as f64 / done_at.as_secs_f64(), count, expected)
}

/// Single subscriber; returns (median_ms, p95_ms) of publish-call→delivery.
async fn broadcast_latency(ws_url: &str, http: &str) -> (f64, f64) {
    let mut ws = connect_sub(ws_url).await;
    let client = reqwest::Client::new();
    let mut lats = Vec::with_capacity(LAT_SAMPLES);
    for i in 0..LAT_SAMPLES {
        let body = format!(
            r#"{{"method":"publish","params":{{"channel":"{CHANNEL}","data":{{"x":{i}}}}}}}"#
        );
        let t0 = Instant::now();
        let _ = client
            .post(format!("{http}/api"))
            .header("Authorization", format!("apikey {KEY}"))
            .body(body)
            .send()
            .await;
        // await the delivered push (bounded so a dropped frame can't hang us)
        let deadline = Duration::from_secs(2);
        while let Ok(Some(Ok(msg))) = tokio::time::timeout(deadline, ws.next()).await {
            if let Message::Text(t) = msg {
                if is_push(&t) {
                    lats.push(t0.elapsed().as_secs_f64() * 1000.0);
                    break;
                }
            }
        }
    }
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = lats[lats.len() / 2];
    let p95 = lats[(lats.len() as f64 * 0.95) as usize];
    (median, p95)
}

async fn bench(label: &str, ws_url: &str, http: &str) -> (f64, f64, f64) {
    let (tput, delivered, expected) = fanout_throughput(ws_url, http).await;
    let (med, p95) = broadcast_latency(ws_url, http).await;
    let pct = 100.0 * delivered as f64 / expected as f64;
    println!(
        "{label:<6} | fan-out {tput:>12.0} deliveries/s ({delivered}/{expected} = {pct:.1}%) | latency median {med:>6.2} ms  p95 {p95:>6.2} ms"
    );
    (tput, med, p95)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf benchmark — run with --ignored --nocapture"]
async fn compare_rust_vs_go() {
    println!(
        "\n=== perf: {SUBS} subscribers × {PUBS} publishes = {} deliveries ===",
        SUBS * PUBS
    );

    let rust = Server::start_with_config(CFG).await;
    let (r_tput, r_med, _r_p95) = bench("rust", &rust.ws_url(), &rust.http).await;

    let Some(go) = Oracle::start_with_config(CFG).await else {
        println!("(Go oracle not available — skipping the comparison)");
        return;
    };
    let (g_tput, g_med, _g_p95) = bench("go", &go.ws_url(), &go.http).await;

    println!(
        "\nfan-out throughput: rust is {:.2}× Go   |   broadcast latency: rust is {:.2}× Go (lower = faster)",
        r_tput / g_tput,
        r_med / g_med
    );
}
