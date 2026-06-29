//! M4: presence + presence_stats + join/leave over the real wire, with golden
//! checks vs the Go oracle. Presence/join_leave enabled via `--presence` /
//! `--join_leave` (default channel options); insecure mode for simplicity.

use conformance::oracle::Oracle;
use conformance::{key_shape, Server, WsJsonClient};

#[tokio::test]
async fn presence_lists_subscribers_and_stats() {
    let s = Server::start_with(&["--client_insecure", "--presence"]).await;

    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;
    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    b.connect_command().await;
    b.subscribe(2, "room").await;

    let pres = a.presence(3, "room").await;
    assert!(pres.get("error").is_none(), "presence error: {pres}");
    let map = pres["result"]["presence"].as_object().unwrap();
    assert_eq!(map.len(), 2, "expected 2 present clients: {pres}");

    let stats = a.presence_stats(4, "room").await;
    assert_eq!(stats["result"]["num_clients"], 2);
    assert_eq!(stats["result"]["num_users"], 1); // both anonymous (user "")
}

#[tokio::test]
async fn join_and_leave_pushes() {
    let s = Server::start_with(&["--client_insecure", "--presence", "--join_leave"]).await;

    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;

    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    let b_id = b.connect_command().await;
    b.subscribe(2, "room").await;

    // A observes B's Join.
    let join = a.next_join_leave_for(1, &b_id).await;
    assert_eq!(join["result"]["channel"], "room");

    // B disconnects -> A observes B's Leave.
    drop(b);
    let leave = a.next_join_leave_for(2, &b_id).await;
    assert_eq!(leave["result"]["channel"], "room");
}

#[tokio::test]
async fn self_join_follows_subscribe_reply() {
    // H2 regression: on a client SUBSCRIBE to a join_leave channel, the subscribe
    // REPLY must be flushed before the self-JOIN push (Go flushes the reply first,
    // then publishes Join on a detached goroutine). Pre-fix Rust emitted JOIN first.
    let s = Server::start_with(&["--client_insecure", "--presence", "--join_leave"]).await;

    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    let a_id = a.connect_command().await;

    // Send SUBSCRIBE raw so we can inspect frame order ourselves.
    a.send_raw(r#"{"id":2,"method":1,"params":{"channel":"room"}}"#)
        .await;

    // First frame must be the subscribe reply (carries id:2, no push `type`).
    let first = a.next_json().await;
    assert_eq!(
        first["id"], 2,
        "first frame must be the subscribe reply: {first}"
    );
    assert!(
        first["result"].get("type").is_none(),
        "first frame must not be a push: {first}"
    );

    // The self-Join arrives only after the reply.
    let join = a.next_join_leave_for(1, &a_id).await;
    assert_eq!(join["result"]["channel"], "room");
}

#[tokio::test]
async fn presence_disabled_returns_not_available() {
    let s = Server::start_with(&["--client_insecure"]).await;
    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;
    let pres = a.presence(3, "room").await;
    assert_eq!(pres["error"]["code"], 108); // not available
}

#[tokio::test]
async fn presence_stats_matches_go() {
    let Some(go) = Oracle::start_with_config(r#"{"client_insecure":true,"presence":true}"#).await
    else {
        return;
    };
    let rust = Server::start_with(&["--client_insecure", "--presence"]).await;
    assert_eq!(
        capture_stats(&go.ws_url()).await,
        capture_stats(&rust.ws_url()).await,
        "presence_stats (num_clients, num_users) differ"
    );
}

#[tokio::test]
async fn presence_entry_shape_matches_go() {
    let Some(go) = Oracle::start_with_config(r#"{"client_insecure":true,"presence":true}"#).await
    else {
        return;
    };
    let rust = Server::start_with(&["--client_insecure", "--presence"]).await;
    let go_entry = capture_one_entry(&go.ws_url()).await;
    let rust_entry = capture_one_entry(&rust.ws_url()).await;
    assert_eq!(
        key_shape(&go_entry),
        key_shape(&rust_entry),
        "\nGO:   {go_entry}\nRUST: {rust_entry}"
    );
}

async fn capture_stats(url: &str) -> (i64, i64) {
    let mut a = WsJsonClient::connect(url).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;
    let mut b = WsJsonClient::connect(url).await;
    b.connect_command().await;
    b.subscribe(2, "room").await;
    let stats = a.presence_stats(3, "room").await;
    (
        stats["result"]["num_clients"].as_i64().unwrap(),
        stats["result"]["num_users"].as_i64().unwrap(),
    )
}

async fn capture_one_entry(url: &str) -> serde_json::Value {
    let mut a = WsJsonClient::connect(url).await;
    a.connect_command().await;
    a.subscribe(2, "room").await;
    let pres = a.presence(3, "room").await;
    pres["result"]["presence"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone()
}
