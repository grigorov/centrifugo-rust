//! M1 vertical slice over the real wire: connect → subscribe → publish → receive.

use conformance::{Server, WsJsonClient};

#[tokio::test]
async fn connect_returns_client_id() {
    let s = Server::start().await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    assert!(!id.is_empty());
}

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    let s = Server::start().await;

    let mut a = WsJsonClient::connect(&s.ws_url()).await;
    a.connect_command().await;
    let sub_reply = a.subscribe(2, "news").await;
    assert!(
        sub_reply.get("error").is_none(),
        "subscribe error: {sub_reply}"
    );

    let mut b = WsJsonClient::connect(&s.ws_url()).await;
    b.connect_command().await;
    let pub_reply = b.publish(2, "news", r#"{"msg":"hello"}"#).await;
    assert!(
        pub_reply.get("error").is_none(),
        "publish error: {pub_reply}"
    );

    // A receives a publication push: a Reply with no id; result is the Push.
    let push = a.next_json().await;
    assert!(push.get("id").is_none(), "push must have no id: {push}");
    let result = &push["result"];
    assert_eq!(result["channel"], "news");
    assert_eq!(result["data"]["data"]["msg"], "hello");
}
