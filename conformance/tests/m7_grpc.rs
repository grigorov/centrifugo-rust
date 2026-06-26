//! M7: gRPC server API (`api.Centrifugo`) over the real wire, with apikey
//! metadata auth and a golden check of the history result vs the Go oracle's
//! gRPC API.

use std::time::Duration;

use centrifugo_grpc::pb;
use centrifugo_grpc::pb::centrifugo_client::CentrifugoClient;
use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};
use tonic::transport::Channel;
use tonic::Request;

const KEY: &str = "grpckey";

/// Connect a gRPC client, retrying until the server's gRPC port is listening
/// (it binds in a task spawned alongside the HTTP server).
async fn grpc_client(addr: &str) -> CentrifugoClient<Channel> {
    for _ in 0..50 {
        let endpoint = Channel::from_shared(addr.to_string()).expect("valid grpc endpoint");
        if let Ok(ch) = endpoint.connect().await {
            return CentrifugoClient::new(ch);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not connect to grpc at {addr}");
}

/// Wrap a request with `authorization: apikey <KEY>` metadata.
fn with_key<T>(msg: T, key: &str) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("apikey {key}").parse().expect("ascii metadata"),
    );
    req
}

#[tokio::test]
async fn grpc_publish_history_roundtrip() {
    let s = Server::start_grpc(
        r#"{"client_insecure":true,"history_size":10,"history_lifetime":60}"#,
        KEY,
    )
    .await;
    let mut c = grpc_client(&s.grpc_addr()).await;
    for i in 1..=3u32 {
        c.publish(with_key(
            pb::PublishRequest {
                channel: "h".into(),
                data: format!(r#"{{"n":{i}}}"#).into_bytes(),
            },
            KEY,
        ))
        .await
        .expect("publish");
    }
    let result = c
        .history(with_key(
            pb::HistoryRequest {
                channel: "h".into(),
            },
            KEY,
        ))
        .await
        .expect("history")
        .into_inner()
        .result
        .expect("history result");
    assert_eq!(result.publications.len(), 3);
    assert_eq!(result.publications[2].data, br#"{"n":3}"#);
}

#[tokio::test]
async fn grpc_rejects_bad_apikey() {
    let s = Server::start_grpc(r#"{"client_insecure":true}"#, KEY).await;
    let mut c = grpc_client(&s.grpc_addr()).await;

    let no_key = c.info(Request::new(pb::InfoRequest {})).await.unwrap_err();
    assert_eq!(no_key.code(), tonic::Code::Unauthenticated, "missing key");

    let bad = c
        .info(with_key(pb::InfoRequest {}, "wrong"))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), tonic::Code::Unauthenticated, "wrong key");

    assert!(
        c.info(with_key(pb::InfoRequest {}, KEY)).await.is_ok(),
        "correct key must be accepted"
    );
}

#[tokio::test]
async fn grpc_channels_and_info() {
    let s = Server::start_grpc(r#"{"client_insecure":true}"#, KEY).await;
    let mut ws = WsJsonClient::connect(&s.ws_url()).await;
    ws.connect_command().await;
    ws.subscribe(2, "room").await;

    let mut c = grpc_client(&s.grpc_addr()).await;
    let channels = c
        .channels(with_key(pb::ChannelsRequest {}, KEY))
        .await
        .expect("channels")
        .into_inner()
        .result
        .expect("channels result")
        .channels;
    assert!(channels.iter().any(|x| x == "room"), "channels: {channels:?}");

    let nodes = c
        .info(with_key(pb::InfoRequest {}, KEY))
        .await
        .expect("info")
        .into_inner()
        .result
        .expect("info result")
        .nodes;
    assert_eq!(nodes.len(), 1);
}

#[tokio::test]
async fn grpc_presence_stats() {
    let s = Server::start_grpc(r#"{"client_insecure":true,"presence":true}"#, KEY).await;
    let mut ws = WsJsonClient::connect(&s.ws_url()).await;
    ws.connect_command().await;
    ws.subscribe(2, "room").await;

    let mut c = grpc_client(&s.grpc_addr()).await;
    let stats = c
        .presence_stats(with_key(
            pb::PresenceStatsRequest {
                channel: "room".into(),
            },
            KEY,
        ))
        .await
        .expect("presence_stats")
        .into_inner()
        .result
        .expect("presence_stats result");
    assert_eq!(stats.num_clients, 1, "stats: {stats:?}");
    assert_eq!(stats.num_users, 1);
}

#[tokio::test]
async fn grpc_history_matches_go() {
    let cfg = r#"{"client_insecure":true,"history_size":10,"history_lifetime":60}"#;
    let Some(go) = Oracle::start_grpc(cfg, KEY).await else {
        return;
    };
    let rust = Server::start_grpc(cfg, KEY).await;
    let mut go_data = grpc_capture_history(&go.grpc_addr()).await;
    let mut rust_data = grpc_capture_history(&rust.grpc_addr()).await;
    // Order-agnostic: assert the same set of publication payloads (the API path's
    // ordering is not part of the contract we pin here, but the data set is).
    go_data.sort();
    rust_data.sort();
    assert_eq!(go_data, rust_data, "grpc history payloads differ");
}

async fn grpc_capture_history(addr: &str) -> Vec<Vec<u8>> {
    let mut c = grpc_client(addr).await;
    for i in 1..=3u32 {
        c.publish(with_key(
            pb::PublishRequest {
                channel: "h".into(),
                data: format!(r#"{{"n":{i}}}"#).into_bytes(),
            },
            KEY,
        ))
        .await
        .expect("publish");
    }
    c.history(with_key(
        pb::HistoryRequest {
            channel: "h".into(),
        },
        KEY,
    ))
    .await
    .expect("history")
    .into_inner()
    .result
    .expect("history result")
    .publications
    .into_iter()
    .map(|p| p.data)
    .collect()
}
