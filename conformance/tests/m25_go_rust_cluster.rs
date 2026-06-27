//! D10: Go + Rust interop on a shared Redis. A Go centrifugo v2.8.6 node and a
//! Rust node share one redis-server; a live publication on either is delivered to
//! a subscriber on the other (the Rust engine speaks centrifuge's `<prefix>.client.<ch>`
//! channel + protobuf Publication/Join/Leave framing). Skips cleanly if
//! `redis-server` or the Go oracle binary is absent.
//!
//! Covers live pub/sub, history, presence, AND control (server-side
//! unsubscribe/disconnect via centrifuge's `<prefix>.control` protobuf protocol),
//! interoperating in both directions Go⇄Rust on one Redis.

use conformance::oracle::Oracle;
use conformance::{api_post, Redis, Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const KEY: &str = "apikey-interop";
const SECRET: &str = "secret";

fn token(user: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": user }),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

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
    let rust = Server::start_redis(
        &redis.addr,
        &format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#),
    )
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
    let rust = Server::start_redis(
        &redis.addr,
        &format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#),
    )
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
    assert_eq!(
        push["result"]["data"]["data"]["from"], "rust",
        "push: {push}"
    );
}

// ---- History interop (shared centrifuge list format) ----

const HIST: &str =
    r#""history_size":10,"history_lifetime":60,"history_recover":true,"client_insecure":true"#;

fn ns(values: &[u32]) -> std::collections::BTreeSet<u32> {
    values.iter().copied().collect()
}

/// Collect the `n` field of every history publication into a set.
fn history_ns(reply: &serde_json::Value) -> std::collections::BTreeSet<u32> {
    reply["result"]["publications"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|p| p["data"]["n"].as_u64().map(|n| n as u32))
        .collect()
}

#[tokio::test]
async fn go_history_readable_by_rust() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","api_key":"{KEY}",{HIST}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{"api_key":"{KEY}",{HIST}}}"#)).await;

    // Go writes 3 publications into the shared centrifuge history list.
    for n in 1..=3u32 {
        let body =
            format!(r#"{{"method":"publish","params":{{"channel":"h","data":{{"n":{n}}}}}}}"#);
        assert!(api_post(&go.http, KEY, &body).await.get("error").is_none());
    }

    // The Rust node reads that history back (same list/meta keys + protobuf framing).
    let r = api_post(
        &rust.http,
        KEY,
        r#"{"method":"history","params":{"channel":"h"}}"#,
    )
    .await;
    assert_eq!(history_ns(&r), ns(&[1, 2, 3]), "rust reads go history: {r}");
}

#[tokio::test]
async fn rust_history_readable_by_go() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","api_key":"{KEY}",{HIST}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{"api_key":"{KEY}",{HIST}}}"#)).await;

    for n in 1..=3u32 {
        let body =
            format!(r#"{{"method":"publish","params":{{"channel":"h2","data":{{"n":{n}}}}}}}"#);
        assert!(api_post(&rust.http, KEY, &body)
            .await
            .get("error")
            .is_none());
    }

    let r = api_post(
        &go.http,
        KEY,
        r#"{"method":"history","params":{"channel":"h2"}}"#,
    )
    .await;
    assert_eq!(history_ns(&r), ns(&[1, 2, 3]), "go reads rust history: {r}");
}

// ---- Presence interop (shared centrifuge presence hash/zset) ----

const PRES: &str = r#""presence":true,"client_insecure":true"#;

#[tokio::test]
async fn go_presence_visible_to_rust() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","api_key":"{KEY}",{PRES}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{"api_key":"{KEY}",{PRES}}}"#)).await;

    // A client subscribes on the GO node → Go writes presence into shared Redis.
    let mut c = WsJsonClient::connect(&go.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "p").await;

    // The Rust node reads that presence entry (protobuf ClientInfo in the data hash).
    let r = api_post(
        &rust.http,
        KEY,
        r#"{"method":"presence","params":{"channel":"p"}}"#,
    )
    .await;
    assert_eq!(
        r["result"]["presence"]
            .as_object()
            .map(|m| m.len())
            .unwrap_or(0),
        1,
        "rust reads go presence: {r}"
    );
}

#[tokio::test]
async fn rust_presence_visible_to_go() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}","api_key":"{KEY}",{PRES}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{"api_key":"{KEY}",{PRES}}}"#)).await;

    // A client subscribes on the RUST node → Rust writes presence into shared Redis.
    let mut c = WsJsonClient::connect(&rust.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "p2").await;

    let r = api_post(
        &go.http,
        KEY,
        r#"{"method":"presence","params":{"channel":"p2"}}"#,
    )
    .await;
    assert_eq!(
        r["result"]["presence"]
            .as_object()
            .map(|m| m.len())
            .unwrap_or(0),
        1,
        "go reads rust presence: {r}"
    );
}

// ---- Control interop (centrifuge `<prefix>.control` protobuf protocol) ----
//
// Server-side unsubscribe/disconnect on one node propagate to a user's clients on
// the OTHER node, across the Go⇄Rust boundary.

const CTRL: &str = r#""api_key":"apikey-interop","token_hmac_secret_key":"secret""#;

#[tokio::test]
async fn rust_api_unsubscribe_reaches_go_client() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}",{CTRL}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{{CTRL}}}"#)).await;

    // Client on the GO node (user alice), subscribed to "cu".
    let mut c = WsJsonClient::connect(&go.ws_url()).await;
    assert!(c.connect_with_token(&token("alice")).await["error"].is_null());
    assert!(c.subscribe(2, "cu").await["error"].is_null());

    // The RUST node's API unsubscribes alice → control over Redis → Go node.
    let r = api_post(
        &rust.http,
        KEY,
        r#"{"method":"unsubscribe","params":{"user":"alice","channel":"cu"}}"#,
    )
    .await;
    assert!(r["error"].is_null(), "rust api unsubscribe: {r}");

    // The Go-connected client receives an Unsubscribe push (type 3).
    let push = c.next_json().await;
    assert_eq!(push["result"]["type"], 3, "unsubscribe push: {push}");
    assert_eq!(push["result"]["channel"], "cu", "unsub channel: {push}");
}

#[tokio::test]
async fn go_api_unsubscribe_reaches_rust_client() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}",{CTRL}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{{CTRL}}}"#)).await;

    // Client on the RUST node (user bob), subscribed to "cu2".
    let mut c = WsJsonClient::connect(&rust.ws_url()).await;
    assert!(c.connect_with_token(&token("bob")).await["error"].is_null());
    assert!(c.subscribe(2, "cu2").await["error"].is_null());

    // The GO node's API unsubscribes bob → control over Redis → Rust node.
    let r = api_post(
        &go.http,
        KEY,
        r#"{"method":"unsubscribe","params":{"user":"bob","channel":"cu2"}}"#,
    )
    .await;
    assert!(r["error"].is_null(), "go api unsubscribe: {r}");

    let push = c.next_json().await;
    assert_eq!(push["result"]["type"], 3, "unsubscribe push: {push}");
    assert_eq!(push["result"]["channel"], "cu2", "unsub channel: {push}");
}

#[tokio::test]
async fn rust_api_disconnect_reaches_go_client() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let go_cfg = format!(
        r#"{{"engine":"redis","redis_url":"redis://{}",{CTRL}}}"#,
        redis.addr
    );
    let Some(go) = Oracle::start_with_config(&go_cfg).await else {
        return;
    };
    let rust = Server::start_redis(&redis.addr, &format!(r#"{{{CTRL}}}"#)).await;

    // Client on the GO node (user carol).
    let mut c = WsJsonClient::connect(&go.ws_url()).await;
    assert!(c.connect_with_token(&token("carol")).await["error"].is_null());

    // The RUST node's API disconnects carol → control over Redis → Go closes it.
    let r = api_post(
        &rust.http,
        KEY,
        r#"{"method":"disconnect","params":{"user":"carol"}}"#,
    )
    .await;
    assert!(r["error"].is_null(), "rust api disconnect: {r}");

    let (code, _) = c.next_close().await;
    assert_eq!(code, 3012, "cross-node force disconnect (3012)");
}

// ---- Node-info interop (NODE pings → cluster membership in the info API) ----

#[tokio::test]
async fn node_info_lists_both_go_and_rust() {
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
    let rust = Server::start_redis(
        &redis.addr,
        &format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#),
    )
    .await;

    // Wait past one NODE-ping interval (3s) so the nodes register each other.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let node_count = |r: &serde_json::Value| {
        r["result"]["nodes"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };

    // Go's info lists both nodes — the Rust node appeared in Go's registry.
    let g = api_post(&go.http, KEY, r#"{"method":"info","params":{}}"#).await;
    assert!(node_count(&g) >= 2, "go must list both nodes: {g}");

    // Rust's info lists both nodes — the Go node appeared in Rust's registry.
    let r = api_post(&rust.http, KEY, r#"{"method":"info","params":{}}"#).await;
    assert!(node_count(&r) >= 2, "rust must list both nodes: {r}");
}
