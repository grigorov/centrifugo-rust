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
