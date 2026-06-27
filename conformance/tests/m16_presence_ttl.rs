//! Phase 2: Redis presence TTL + the per-connection presence-refresh timer.
//! A presence entry expires after `client_presence_expire_interval` unless the
//! connection re-asserts it every `client_presence_ping_interval`. (The memory
//! engine has no TTL, matching Go, so this is exercised on Redis.)

use conformance::{api_post, Redis, Server, WsJsonClient};

const KEY: &str = "ttlkey";

async fn num_clients(http: &str) -> i64 {
    let r = api_post(
        http,
        KEY,
        r#"{"method":"presence_stats","params":{"channel":"room"}}"#,
    )
    .await;
    r["result"]["num_clients"].as_i64().unwrap_or(-1)
}

#[tokio::test]
async fn redis_presence_expires_without_refresh() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    // expire after 1s; ping effectively never (300s) -> no refresh.
    let cfg = format!(
        r#"{{"client_insecure":true,"presence":true,"api_key":"{KEY}","client_presence_ping_interval":300,"client_presence_expire_interval":1}}"#
    );
    let s = Server::start_redis(&redis.addr, &cfg).await;

    let mut ws = WsJsonClient::connect(&s.ws_url()).await;
    ws.connect_command().await;
    ws.subscribe(2, "room").await;
    assert_eq!(
        num_clients(&s.http).await,
        1,
        "present right after subscribe"
    );

    // Past the expire window with no refresh -> pruned on read.
    tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
    assert_eq!(num_clients(&s.http).await, 0, "expired after TTL");
}

#[tokio::test]
async fn redis_presence_refresh_keeps_alive() {
    let Some(redis) = Redis::start().await else {
        return;
    };
    // expire after 2s; ping every 1s -> the timer keeps the entry alive.
    let cfg = format!(
        r#"{{"client_insecure":true,"presence":true,"api_key":"{KEY}","client_presence_ping_interval":1,"client_presence_expire_interval":2}}"#
    );
    let s = Server::start_redis(&redis.addr, &cfg).await;

    let mut ws = WsJsonClient::connect(&s.ws_url()).await;
    ws.connect_command().await;
    ws.subscribe(2, "room").await;

    // Past the expire window, but the refresh timer (1s) has re-asserted presence.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(num_clients(&s.http).await, 1, "refresh kept presence alive");
}
