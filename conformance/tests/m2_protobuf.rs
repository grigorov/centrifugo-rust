//! M2 protobuf path: end-to-end over `?format=protobuf` against the Rust binary,
//! plus a golden differential of the decoded publication vs the Go oracle.

use centrifugo_protocol::messages::Publication;
use conformance::oracle::Oracle;
use conformance::{PbWsClient, Server};

fn pb_url(base: &str) -> String {
    format!("{base}?format=protobuf")
}

#[tokio::test]
async fn protobuf_connect_subscribe_publish_receive() {
    let s = Server::start().await;
    let url = pb_url(&s.ws_url());

    let mut a = PbWsClient::connect(&url).await;
    let cr = a.connect_command().await;
    assert!(!cr.client.is_empty(), "connect must return a client id");

    let sub = a.subscribe(2, "news").await;
    assert!(sub.error.is_none(), "subscribe error: {:?}", sub.error);

    let mut b = PbWsClient::connect(&url).await;
    b.connect_command().await;
    let pubr = b.publish(2, "news", br#"{"msg":"hi"}"#).await;
    assert!(pubr.error.is_none(), "publish error: {:?}", pubr.error);

    let p = a.next_publication().await;
    assert_eq!(
        p.data.as_ref().map(|r| r.as_bytes().to_vec()),
        Some(br#"{"msg":"hi"}"#.to_vec())
    );
    assert!(p.info.is_some(), "publication carries publisher info");
}

#[tokio::test]
async fn protobuf_publication_matches_go() {
    let Some(go) = Oracle::start().await else {
        return;
    };
    let rust = Server::start().await;

    let go_pub = capture(&pb_url(&go.ws_url())).await;
    let rust_pub = capture(&pb_url(&rust.ws_url())).await;

    // Same opaque data bytes.
    assert_eq!(
        go_pub.data.as_ref().map(|r| r.as_bytes().to_vec()),
        rust_pub.data.as_ref().map(|r| r.as_bytes().to_vec()),
        "publication data bytes differ"
    );
    // Same info presence + user; client id present in both (value differs).
    assert_eq!(go_pub.info.is_some(), rust_pub.info.is_some());
    assert_eq!(
        go_pub.info.as_ref().map(|i| i.user.clone()),
        rust_pub.info.as_ref().map(|i| i.user.clone())
    );
    assert!(go_pub.info.as_ref().is_some_and(|i| !i.client.is_empty()));
    assert!(rust_pub.info.as_ref().is_some_and(|i| !i.client.is_empty()));
}

async fn capture(url: &str) -> Publication {
    let mut a = PbWsClient::connect(url).await;
    a.connect_command().await;
    let sub = a.subscribe(2, "news").await;
    assert!(sub.error.is_none(), "subscribe error: {:?}", sub.error);

    let mut b = PbWsClient::connect(url).await;
    b.connect_command().await;
    let pubr = b.publish(2, "news", br#"{"msg":"hello"}"#).await;
    assert!(pubr.error.is_none(), "publish error: {:?}", pubr.error);

    a.next_publication().await
}
