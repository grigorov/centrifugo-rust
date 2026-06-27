//! M11 (/metrics): the Prometheus endpoint exposes node gauges that reflect live
//! connections/subscriptions.

use conformance::{Server, WsJsonClient};

fn gauge(body: &str, name: &str) -> Option<i64> {
    body.lines()
        .find(|l| l.starts_with(&format!("{name} ")))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test]
async fn metrics_reflects_connections() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    c.subscribe(2, "room").await;

    let body = reqwest::get(format!("{}/metrics", s.http))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(body.contains("centrifugo_build_info"), "metrics:\n{body}");
    assert_eq!(
        gauge(&body, "centrifugo_node_num_clients"),
        Some(1),
        "metrics:\n{body}"
    );
    assert_eq!(
        gauge(&body, "centrifugo_node_num_channels"),
        Some(1),
        "metrics:\n{body}"
    );
}

#[tokio::test]
async fn metrics_per_command_counters() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await; // 1 connect
    c.connect_command().await;
    c.subscribe(2, "room").await; // 1 subscribe
    c.publish(3, "room", r#"{"a":1}"#).await; // 1 publish -> 1 message sent

    let body = reqwest::get(format!("{}/metrics", s.http))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Per-method command counters present and counted.
    assert!(
        gauge(&body, r#"centrifugo_client_command_count{method="connect"}"#).unwrap_or(0) >= 1,
        "connect command count:\n{body}"
    );
    assert!(
        gauge(&body, r#"centrifugo_client_command_count{method="subscribe"}"#).unwrap_or(0) >= 1,
        "subscribe command count:\n{body}"
    );
    // A publication fan-out was recorded.
    assert!(
        gauge(&body, r#"centrifugo_node_messages_sent_count{type="publication"}"#).unwrap_or(0) >= 1,
        "messages_sent publication:\n{body}"
    );
    // One websocket connection accepted.
    assert!(
        gauge(&body, r#"centrifugo_transport_connect_count{transport="websocket"}"#).unwrap_or(0)
            >= 1,
        "connect_count websocket:\n{body}"
    );
}
