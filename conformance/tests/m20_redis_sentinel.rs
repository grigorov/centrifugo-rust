//! Phase 5: Redis Sentinel. A node configured with `redis_master_name` +
//! `redis_sentinels` discovers the master via Sentinel and uses it as its Redis
//! engine. Skips cleanly when `redis-server`/`redis-sentinel` are absent.

use conformance::{api_post, RedisSentinel, Server, WsJsonClient};

const KEY: &str = "sentinelkey";

#[tokio::test]
async fn node_connects_via_sentinel_and_pubsub_works() {
    let Some(sentinel) = RedisSentinel::start().await else {
        return;
    };
    let cfg = format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#);
    let s = Server::start_redis_sentinel(&sentinel.master_name, &sentinel.sentinel_addr, &cfg).await;

    // A subscriber on the sentinel-backed node.
    let mut sub = WsJsonClient::connect(&s.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "room").await;

    // Publish via the HTTP API; it must round-trip through the Sentinel-resolved
    // master's PUB/SUB back to the subscriber.
    let r = api_post(
        &s.http,
        KEY,
        r#"{"method":"publish","params":{"channel":"room","data":{"via":"sentinel"}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "publish error: {r}");

    let push = sub.next_json().await;
    assert_eq!(push["result"]["channel"], "room", "push: {push}");
    assert_eq!(push["result"]["data"]["data"]["via"], "sentinel", "push: {push}");
}

#[tokio::test]
async fn presence_works_via_sentinel() {
    let Some(sentinel) = RedisSentinel::start().await else {
        return;
    };
    let cfg = format!(r#"{{"client_insecure":true,"api_key":"{KEY}","presence":true}}"#);
    let s = Server::start_redis_sentinel(&sentinel.master_name, &sentinel.sentinel_addr, &cfg).await;

    let mut sub = WsJsonClient::connect(&s.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "room").await;

    let stats = api_post(
        &s.http,
        KEY,
        r#"{"method":"presence_stats","params":{"channel":"room"}}"#,
    )
    .await;
    assert_eq!(stats["result"]["num_clients"], 1, "stats: {stats}");
}
