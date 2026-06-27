//! D10: Go + Rust interop on a shared Redis. A Go centrifugo v2.8.6 node and a
//! Rust node share one redis-server; a live publication on either is delivered to
//! a subscriber on the other (the Rust engine speaks centrifuge's `<prefix>.client.<ch>`
//! channel + protobuf Publication/Join/Leave framing). Skips cleanly if
//! `redis-server` or the Go oracle binary is absent.
//!
//! Scope: live pub/sub fan-out. History/presence/control remain Rust-native
//! (different on-Redis format), so those don't cross-interop with Go nodes.

use conformance::oracle::Oracle;
use conformance::{api_post, Redis, Server, WsJsonClient};

const KEY: &str = "apikey-interop";

#[tokio::test]
async fn go_publish_reaches_rust_subscriber() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","client_insecure":true,"api_key":"{KEY}"}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return; // Go oracle binary not built — skip.
    };
    let rust =
        Server::start_redis(&redis.addr, &format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#))
            .await;

    // Subscriber on the RUST node.
    let mut sub = WsJsonClient::connect(&rust.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "shared").await;

    // Publish via the GO node's HTTP API.
    let r = api_post(
        &go.http,
        KEY,
        r#"{"method":"publish","params":{"channel":"shared","data":{"from":"go"}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "go publish: {r}");

    // The Go-encoded publication crosses Redis and the Rust node decodes + delivers it.
    let push = sub.next_json().await;
    assert_eq!(push["result"]["channel"], "shared", "push: {push}");
    assert_eq!(push["result"]["data"]["data"]["from"], "go", "push: {push}");
}

#[tokio::test]
async fn rust_publish_reaches_go_subscriber() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","client_insecure":true,"api_key":"{KEY}"}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust =
        Server::start_redis(&redis.addr, &format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#))
            .await;

    // Subscriber on the GO node (centrifuge protocol v0.3.4, same as ours).
    let mut sub = WsJsonClient::connect(&go.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "shared2").await;

    // Publish via the RUST node's HTTP API.
    let r = api_post(
        &rust.http,
        KEY,
        r#"{"method":"publish","params":{"channel":"shared2","data":{"from":"rust"}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "rust publish: {r}");

    // The Rust-encoded publication crosses Redis and the Go node decodes + delivers it.
    let push = sub.next_json().await;
    assert_eq!(push["result"]["channel"], "shared2", "push: {push}");
    assert_eq!(push["result"]["data"]["data"]["from"], "rust", "push: {push}");
}
