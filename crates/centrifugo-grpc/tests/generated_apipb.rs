use std::collections::HashMap;
use std::fmt::Debug;

use centrifugo_grpc::pb as api;
use prost::Message;

fn assert_prost_roundtrip<T>(value: T)
where
    T: Message + Default + PartialEq + Debug + Clone,
{
    let bytes = value.encode_to_vec();
    assert_eq!(
        value.encoded_len(),
        bytes.len(),
        "encoded_len must match actual encoded size for {value:?}"
    );
    let decoded = T::decode(bytes.as_slice()).expect("decode encoded message");
    assert_eq!(decoded, value);
}

fn from_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "hex string length must be even");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex byte"))
        .collect()
}

fn assert_encoded_hex<T: Message>(value: T, expected_hex: &str) {
    assert_eq!(value.encode_to_vec(), from_hex(expected_hex));
}

fn client_info() -> api::ClientInfo {
    api::ClientInfo {
        user: "user-1".into(),
        client: "client-1".into(),
        conn_info: br#"{"conn":true}"#.to_vec(),
        chan_info: br#"{"chan":1}"#.to_vec(),
    }
}

fn publication() -> api::Publication {
    api::Publication {
        uid: "pub-1".into(),
        data: br#"{"message":"hello"}"#.to_vec(),
        info: Some(client_info()),
    }
}

fn error() -> api::Error {
    api::Error {
        code: 102,
        message: "unknown channel".into(),
    }
}

#[test]
fn server_api_generated_messages_roundtrip() {
    assert_prost_roundtrip(client_info());
    assert_prost_roundtrip(publication());
    assert_prost_roundtrip(error());
    assert_prost_roundtrip(api::Command {
        id: 7,
        method: api::MethodType::Publish as i32,
        params: br#"{"channel":"room","data":{"x":1}}"#.to_vec(),
    });
    assert_prost_roundtrip(api::Reply {
        id: 7,
        error: Some(error()),
        result: Vec::new(),
    });

    assert_prost_roundtrip(api::PublishRequest {
        channel: "room".into(),
        data: br#"{"x":1}"#.to_vec(),
    });
    assert_prost_roundtrip(api::PublishResponse {
        error: None,
        result: Some(api::PublishResult {}),
    });
    assert_prost_roundtrip(api::PublishResult {});

    assert_prost_roundtrip(api::BroadcastRequest {
        channels: vec!["a".into(), "b".into()],
        data: br#"{"x":1}"#.to_vec(),
    });
    assert_prost_roundtrip(api::BroadcastResponse {
        error: None,
        result: Some(api::BroadcastResult {}),
    });
    assert_prost_roundtrip(api::BroadcastResult {});

    assert_prost_roundtrip(api::UnsubscribeRequest {
        channel: "room".into(),
        user: "user-1".into(),
    });
    assert_prost_roundtrip(api::UnsubscribeResponse {
        error: None,
        result: Some(api::UnsubscribeResult {}),
    });
    assert_prost_roundtrip(api::UnsubscribeResult {});

    assert_prost_roundtrip(api::DisconnectRequest {
        user: "user-1".into(),
    });
    assert_prost_roundtrip(api::DisconnectResponse {
        error: None,
        result: Some(api::DisconnectResult {}),
    });
    assert_prost_roundtrip(api::DisconnectResult {});

    assert_prost_roundtrip(api::PresenceRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(api::PresenceResponse {
        error: None,
        result: Some(api::PresenceResult {
            presence: HashMap::from([("client-1".into(), client_info())]),
        }),
    });
    assert_prost_roundtrip(api::PresenceResult {
        presence: HashMap::from([("client-1".into(), client_info())]),
    });

    assert_prost_roundtrip(api::PresenceStatsRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(api::PresenceStatsResponse {
        error: None,
        result: Some(api::PresenceStatsResult {
            num_clients: 3,
            num_users: 2,
        }),
    });
    assert_prost_roundtrip(api::PresenceStatsResult {
        num_clients: 3,
        num_users: 2,
    });

    assert_prost_roundtrip(api::HistoryRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(api::HistoryResponse {
        error: None,
        result: Some(api::HistoryResult {
            publications: vec![publication()],
        }),
    });
    assert_prost_roundtrip(api::HistoryResult {
        publications: vec![publication()],
    });

    assert_prost_roundtrip(api::HistoryRemoveRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(api::HistoryRemoveResponse {
        error: None,
        result: Some(api::HistoryRemoveResult {}),
    });
    assert_prost_roundtrip(api::HistoryRemoveResult {});

    assert_prost_roundtrip(api::ChannelsRequest {});
    assert_prost_roundtrip(api::ChannelsResponse {
        error: None,
        result: Some(api::ChannelsResult {
            channels: vec!["a".into(), "b".into()],
        }),
    });
    assert_prost_roundtrip(api::ChannelsResult {
        channels: vec!["a".into(), "b".into()],
    });

    let metrics = api::Metrics {
        interval: 1.5,
        items: HashMap::from([
            ("centrifugo.node.num_clients".into(), 3.0),
            ("centrifugo.node.num_channels".into(), 2.0),
        ]),
    };
    let node = api::NodeResult {
        uid: "node-1".into(),
        name: "rust-node".into(),
        version: "2.8.6".into(),
        num_clients: 3,
        num_users: 2,
        num_channels: 1,
        uptime: 60,
        metrics: Some(metrics.clone()),
    };
    assert_prost_roundtrip(api::InfoRequest {});
    assert_prost_roundtrip(api::InfoResponse {
        error: None,
        result: Some(api::InfoResult {
            nodes: vec![node.clone()],
        }),
    });
    assert_prost_roundtrip(api::InfoResult {
        nodes: vec![node.clone()],
    });
    assert_prost_roundtrip(node);
    assert_prost_roundtrip(metrics);

    assert_prost_roundtrip(api::RpcRequest {
        method: "sum".into(),
        params: br#"{"a":1}"#.to_vec(),
    });
    assert_prost_roundtrip(api::RpcResponse {
        error: None,
        result: Some(api::RpcResult {
            data: br#"{"answer":42}"#.to_vec(),
        }),
    });
    assert_prost_roundtrip(api::RpcResult {
        data: br#"{"answer":42}"#.to_vec(),
    });
}

#[test]
fn server_api_error_response_roundtrips() {
    assert_prost_roundtrip(api::PublishResponse {
        error: Some(error()),
        result: None,
    });
    assert_prost_roundtrip(api::PresenceResponse {
        error: Some(api::Error {
            code: 108,
            message: "not available".into(),
        }),
        result: None,
    });
    assert_prost_roundtrip(api::RpcResponse {
        error: Some(api::Error {
            code: 104,
            message: "method not found".into(),
        }),
        result: None,
    });
}

#[test]
fn server_api_enum_values_stay_wire_compatible() {
    assert_eq!(api::MethodType::Publish as i32, 0);
    assert_eq!(api::MethodType::Broadcast as i32, 1);
    assert_eq!(api::MethodType::HistoryRemove as i32, 7);
    assert_eq!(api::MethodType::Info as i32, 9);
    assert_eq!(api::MethodType::Rpc as i32, 10);
}

#[test]
fn server_api_fixed_golden_bytes() {
    assert_encoded_hex(
        api::Error {
            code: 102,
            message: "unknown channel".into(),
        },
        "0866120f756e6b6e6f776e206368616e6e656c",
    );
    assert_encoded_hex(
        api::PublishRequest {
            channel: "room".into(),
            data: br#"{"x":1}"#.to_vec(),
        },
        "0a04726f6f6d12077b2278223a317d",
    );
    assert_encoded_hex(
        api::PresenceStatsResult {
            num_clients: 3,
            num_users: 2,
        },
        "08031002",
    );
    assert_encoded_hex(
        api::ChannelsResult {
            channels: vec!["a".into(), "b".into()],
        },
        "0a01610a0162",
    );
    assert_encoded_hex(
        api::DisconnectRequest {
            user: "user-1".into(),
        },
        "0a06757365722d31",
    );
    assert_encoded_hex(
        api::RpcRequest {
            method: "sum".into(),
            params: br#"{"a":1}"#.to_vec(),
        },
        "0a0373756d12077b2261223a317d",
    );
}
