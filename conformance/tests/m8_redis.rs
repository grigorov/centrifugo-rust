//! M8: Redis engine multi-node behavior. Two Rust nodes share one Redis; we
//! verify that a publication, presence entry, and history written on one node
//! are visible/delivered on the other. Skips cleanly when `redis-server` is not
//! installed (like the Go oracle tests).

use conformance::{api_post, Redis, Server, WsJsonClient};

const KEY: &str = "apikey-redis";

#[tokio::test]
async fn cross_node_publish() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let cfg = format!(r#"{{"client_insecure":true,"api_key":"{KEY}"}}"#);
    let node_a = Server::start_redis(&redis.addr, &cfg).await;
    let node_b = Server::start_redis(&redis.addr, &cfg).await;

    // Subscriber lives on node A.
    let mut sub = WsJsonClient::connect(&node_a.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "room").await;

    // Publish on node B via its HTTP API.
    let r = api_post(
        &node_b.http,
        KEY,
        r#"{"method":"publish","params":{"channel":"room","data":{"msg":"hi"}}}"#,
    )
    .await;
    assert!(r.get("error").is_none(), "publish error: {r}");

    // The publication crosses Redis and reaches node A's subscriber.
    let push = sub.next_json().await;
    assert_eq!(push["result"]["channel"], "room", "push: {push}");
    assert_eq!(push["result"]["data"]["data"]["msg"], "hi", "push: {push}");
}

#[tokio::test]
async fn cross_node_presence() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let cfg = format!(r#"{{"client_insecure":true,"api_key":"{KEY}","presence":true}}"#);
    let node_a = Server::start_redis(&redis.addr, &cfg).await;
    let node_b = Server::start_redis(&redis.addr, &cfg).await;

    // A client subscribes on node A (writing presence to shared Redis).
    let mut sub = WsJsonClient::connect(&node_a.ws_url()).await;
    sub.connect_command().await;
    sub.subscribe(2, "room").await;

    // Node B sees the presence entry.
    let stats = api_post(
        &node_b.http,
        KEY,
        r#"{"method":"presence_stats","params":{"channel":"room"}}"#,
    )
    .await;
    assert_eq!(stats["result"]["num_clients"], 1, "stats: {stats}");
    assert_eq!(stats["result"]["num_users"], 1, "stats: {stats}");

    let presence = api_post(
        &node_b.http,
        KEY,
        r#"{"method":"presence","params":{"channel":"room"}}"#,
    )
    .await;
    assert_eq!(
        presence["result"]["presence"].as_object().unwrap().len(),
        1,
        "presence: {presence}"
    );
}

#[tokio::test]
async fn cross_node_history_recovery() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    let cfg = format!(
        r#"{{"client_insecure":true,"api_key":"{KEY}","history_size":10,"history_lifetime":60,"history_recover":true}}"#
    );
    let node_a = Server::start_redis(&redis.addr, &cfg).await;
    let node_b = Server::start_redis(&redis.addr, &cfg).await;

    // Publish 3 messages via node A's API (appended to shared Redis history).
    for i in 1..=3u32 {
        let body =
            format!(r#"{{"method":"publish","params":{{"channel":"room","data":{{"n":{i}}}}}}}"#);
        let r = api_post(&node_a.http, KEY, &body).await;
        assert!(r.get("error").is_none(), "publish {i}: {r}");
    }

    // A fresh client subscribes-with-recover on node B from the start; it should
    // recover all 3 publications, newest-first (seq/gen descending mode).
    let mut c = WsJsonClient::connect(&node_b.ws_url()).await;
    c.connect_command().await;
    let r = c.subscribe_recover(2, "room", 0, "").await;
    let pubs = r["result"]["publications"].as_array().unwrap();
    assert_eq!(pubs.len(), 3, "recover: {r}");
    assert_eq!(pubs[0]["data"]["n"], 3, "newest-first: {r}");
}
