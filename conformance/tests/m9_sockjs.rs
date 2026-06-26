//! M9: SockJS fallback transport (xhr-polling + /info). Verifies a full
//! connect→subscribe→receive-publication round-trip over xhr-polling, and a
//! golden check of the `/info` shape vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{key_shape, Server, WsJsonClient};

/// POST a SockJS endpoint; return (status, body).
async fn post(http: &str, path: &str, body: &str) -> (u16, String) {
    let resp = reqwest::Client::new()
        .post(format!("{http}{path}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// Parse a SockJS message frame `a["...","..."]\n` into the decoded JSON of each
/// wrapped centrifuge reply/push.
fn parse_a_frame(s: &str) -> Vec<serde_json::Value> {
    let s = s.trim_end();
    assert!(s.starts_with('a'), "expected an a-frame, got: {s}");
    let strings: Vec<String> = serde_json::from_str(&s[1..]).expect("a-frame array");
    strings
        .iter()
        .map(|m| serde_json::from_str(m).expect("wrapped json"))
        .collect()
}

/// Wrap centrifuge commands as a SockJS xhr_send body (a JSON array of strings).
fn send_body(commands: &[&str]) -> String {
    serde_json::to_string(commands).unwrap()
}

#[tokio::test]
async fn sockjs_connect_subscribe_receive() {
    let s = Server::start().await; // --client_insecure
    let xhr = "/connection/sockjs/000/sessA/xhr";
    let xhr_send = "/connection/sockjs/000/sessA/xhr_send";

    // Open session.
    let (_, body) = post(&s.http, xhr, "").await;
    assert_eq!(body, "o\n");

    // CONNECT then read the reply off the next poll.
    let (code, _) = post(&s.http, xhr_send, &send_body(&[r#"{"id":1,"params":{}}"#])).await;
    assert_eq!(code, 204);
    let (_, frame) = post(&s.http, xhr, "").await;
    let replies = parse_a_frame(&frame);
    let client_id = replies[0]["result"]["client"]
        .as_str()
        .expect("connect reply client id")
        .to_string();
    assert!(!client_id.is_empty());

    // SUBSCRIBE to "room".
    let (code, _) = post(
        &s.http,
        xhr_send,
        &send_body(&[r#"{"id":2,"method":1,"params":{"channel":"room"}}"#]),
    )
    .await;
    assert_eq!(code, 204);
    let (_, frame) = post(&s.http, xhr, "").await;
    let replies = parse_a_frame(&frame);
    assert!(replies[0]["error"].is_null(), "subscribe error: {frame}");

    // A native WebSocket client publishes to "room".
    let mut pubr = WsJsonClient::connect(&s.ws_url()).await;
    pubr.connect_command().await;
    pubr.publish(2, "room", r#"{"msg":"via-sockjs"}"#).await;

    // The SockJS subscriber receives the publication push on its next poll.
    let (_, frame) = post(&s.http, xhr, "").await;
    let pushes = parse_a_frame(&frame);
    assert_eq!(pushes[0]["result"]["channel"], "room", "frame: {frame}");
    assert_eq!(
        pushes[0]["result"]["data"]["data"]["msg"], "via-sockjs",
        "frame: {frame}"
    );
}

#[tokio::test]
async fn sockjs_info_shape_matches_go() {
    let Some(go) = Oracle::start().await else {
        return;
    };
    let rust = Server::start().await;
    let go_info = get_json(&format!("{}/connection/sockjs/info", go.http)).await;
    let rust_info = get_json(&format!("{}/connection/sockjs/info", rust.http)).await;
    assert_eq!(
        key_shape(&go_info),
        key_shape(&rust_info),
        "\nGO:   {go_info}\nRUST: {rust_info}"
    );
}

async fn get_json(url: &str) -> serde_json::Value {
    let text = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    serde_json::from_str(&text).expect("info json")
}
