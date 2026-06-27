//! Phase 4: protobuf HTTP API. With `Content-Type: application/octet-stream` the
//! `/api` body is a uvarint-length-delimited stream of pb `api.Command`; the
//! reply is the matching pb `api.Reply` stream.

use centrifugo_grpc::pb;
use conformance::Server;
use prost::Message;

const KEY: &str = "pbkey";

fn command(id: u32, method: pb::MethodType, params: Vec<u8>) -> Vec<u8> {
    let cmd = pb::Command {
        id,
        method: method as i32,
        params,
    };
    let mut out = Vec::new();
    cmd.encode_length_delimited(&mut out).unwrap();
    out
}

async fn post_pb(http: &str, body: Vec<u8>) -> Vec<u8> {
    reqwest::Client::new()
        .post(format!("{http}/api"))
        .header("Authorization", format!("apikey {KEY}"))
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn protobuf_api_publish_then_history() {
    let s = Server::start_with(&[
        "--client_insecure",
        "--api_key",
        KEY,
        "--history_size",
        "10",
        "--history_lifetime",
        "60",
    ])
    .await;

    // Publish via the protobuf API.
    let pub_req = pb::PublishRequest {
        channel: "h".into(),
        data: br#"{"n":1}"#.to_vec(),
    }
    .encode_to_vec();
    let resp = post_pb(&s.http, command(1, pb::MethodType::Publish, pub_req)).await;
    let reply = pb::Reply::decode_length_delimited(&resp[..]).unwrap();
    assert!(reply.error.is_none(), "publish error: {:?}", reply.error);

    // History via the protobuf API returns the publication.
    let hist_req = pb::HistoryRequest {
        channel: "h".into(),
    }
    .encode_to_vec();
    let resp = post_pb(&s.http, command(2, pb::MethodType::History, hist_req)).await;
    let reply = pb::Reply::decode_length_delimited(&resp[..]).unwrap();
    assert!(reply.error.is_none(), "history error: {:?}", reply.error);
    let result = pb::HistoryResult::decode(&reply.result[..]).unwrap();
    assert_eq!(result.publications.len(), 1, "expected 1 publication");
    assert_eq!(result.publications[0].data, br#"{"n":1}"#);
}

#[tokio::test]
async fn protobuf_api_validation_and_info() {
    let s = Server::start_with(&["--client_insecure", "--api_key", KEY]).await;

    // Empty publish data -> 107 (in the pb Reply error).
    let bad = pb::PublishRequest {
        channel: "x".into(),
        data: Vec::new(),
    }
    .encode_to_vec();
    let resp = post_pb(&s.http, command(1, pb::MethodType::Publish, bad)).await;
    let reply = pb::Reply::decode_length_delimited(&resp[..]).unwrap();
    assert_eq!(reply.error.unwrap().code, 107, "empty data must be 107");

    // Info returns one node.
    let resp = post_pb(
        &s.http,
        command(2, pb::MethodType::Info, pb::InfoRequest {}.encode_to_vec()),
    )
    .await;
    let reply = pb::Reply::decode_length_delimited(&resp[..]).unwrap();
    assert!(reply.error.is_none());
    let info = pb::InfoResult::decode(&reply.result[..]).unwrap();
    assert_eq!(info.nodes.len(), 1);
}
