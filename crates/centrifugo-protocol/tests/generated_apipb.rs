use std::collections::HashMap;
use std::fmt::Debug;

use centrifugo_protocol::messages as msg;
use centrifugo_protocol::{pb, Command, Error, MethodType, Push, PushType, Raw, Reply};
use prost::Message;
use serde::Serialize;

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

fn assert_domain_roundtrip<D, P>(domain: D)
where
    D: Clone + Serialize + From<P>,
    P: Message + Default + PartialEq + Debug + Clone + From<D>,
{
    let encoded: P = domain.clone().into();
    assert_prost_roundtrip(encoded.clone());
    let decoded: D = encoded.into();
    assert_eq!(json_value(&decoded), json_value(&domain));
}

fn json_value<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("domain value serializes to JSON")
}

fn raw(json: &str) -> Raw {
    Raw::from_bytes(json.as_bytes())
}

fn client_info() -> pb::ClientInfo {
    pb::ClientInfo {
        user: "user-1".into(),
        client: "client-1".into(),
        conn_info: br#"{"conn":true}"#.to_vec(),
        chan_info: br#"{"chan":1}"#.to_vec(),
    }
}

fn publication() -> pb::Publication {
    pb::Publication {
        seq: 11,
        gen: 3,
        uid: "pub-1".into(),
        data: br#"{"message":"hello"}"#.to_vec(),
        info: Some(client_info()),
        offset: 42,
    }
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

#[test]
fn client_protocol_generated_messages_roundtrip() {
    assert_prost_roundtrip(pb::Error {
        code: 102,
        message: "unknown channel".into(),
    });
    assert_prost_roundtrip(pb::Command {
        id: 7,
        method: pb::MethodType::Publish as i32,
        params: br#"{"x":1}"#.to_vec(),
    });
    assert_prost_roundtrip(pb::Reply {
        id: 7,
        error: Some(pb::Error {
            code: 103,
            message: "permission denied".into(),
        }),
        result: Vec::new(),
    });
    assert_prost_roundtrip(pb::Push {
        r#type: pb::PushType::Publication as i32,
        channel: "room".into(),
        data: publication().encode_to_vec(),
    });
    assert_prost_roundtrip(client_info());
    assert_prost_roundtrip(publication());
    assert_prost_roundtrip(pb::ConnectRequest {
        token: "token".into(),
        data: br#"{"connect":1}"#.to_vec(),
        subs: HashMap::from([(
            "room".into(),
            pb::SubscribeRequest {
                channel: "room".into(),
                token: "sub-token".into(),
                recover: true,
                seq: 10,
                gen: 2,
                epoch: "abcd".into(),
                offset: 99,
            },
        )]),
        name: "rust-client".into(),
        version: "1.0.0".into(),
    });
    assert_prost_roundtrip(pb::ConnectResult {
        client: "client-1".into(),
        version: "2.8.6".into(),
        expires: true,
        ttl: 60,
        data: br#"{"server":true}"#.to_vec(),
        subs: HashMap::from([(
            "room".into(),
            pb::SubscribeResult {
                expires: true,
                ttl: 30,
                recoverable: true,
                seq: 12,
                gen: 3,
                epoch: "efgh".into(),
                publications: vec![publication()],
                recovered: true,
                offset: 100,
            },
        )]),
    });
    assert_prost_roundtrip(pb::SubscribeRequest {
        channel: "room".into(),
        token: "sub-token".into(),
        recover: true,
        seq: 10,
        gen: 2,
        epoch: "abcd".into(),
        offset: 99,
    });
    assert_prost_roundtrip(pb::SubscribeResult {
        expires: true,
        ttl: 30,
        recoverable: true,
        seq: 12,
        gen: 3,
        epoch: "efgh".into(),
        publications: vec![publication()],
        recovered: true,
        offset: 100,
    });
    assert_prost_roundtrip(pb::UnsubscribeRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(pb::UnsubscribeResult {});
    assert_prost_roundtrip(pb::PublishRequest {
        channel: "room".into(),
        data: br#"{"x":1}"#.to_vec(),
    });
    assert_prost_roundtrip(pb::PublishResult {});
    assert_prost_roundtrip(pb::PresenceRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(pb::PresenceResult {
        presence: HashMap::from([("client-1".into(), client_info())]),
    });
    assert_prost_roundtrip(pb::PresenceStatsRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(pb::PresenceStatsResult {
        num_clients: 3,
        num_users: 2,
    });
    assert_prost_roundtrip(pb::HistoryRequest {
        channel: "room".into(),
    });
    assert_prost_roundtrip(pb::HistoryResult {
        publications: vec![publication()],
    });
    assert_prost_roundtrip(pb::RefreshRequest {
        token: "refresh-token".into(),
    });
    assert_prost_roundtrip(pb::RefreshResult {
        client: "client-1".into(),
        version: "2.8.6".into(),
        expires: true,
        ttl: 60,
    });
    assert_prost_roundtrip(pb::SubRefreshRequest {
        channel: "room".into(),
        token: "sub-refresh-token".into(),
    });
    assert_prost_roundtrip(pb::SubRefreshResult {
        expires: true,
        ttl: 30,
    });
    assert_prost_roundtrip(pb::RpcRequest {
        method: "sum".into(),
        data: br#"{"a":1}"#.to_vec(),
    });
    assert_prost_roundtrip(pb::RpcResult {
        data: br#"{"answer":42}"#.to_vec(),
    });
    assert_prost_roundtrip(pb::Join {
        info: Some(client_info()),
    });
    assert_prost_roundtrip(pb::Leave {
        info: Some(client_info()),
    });
    assert_prost_roundtrip(pb::PingResult {});
}

#[test]
fn client_protocol_enum_values_stay_wire_compatible() {
    assert_eq!(pb::MethodType::Connect as i32, 0);
    assert_eq!(pb::MethodType::Publish as i32, 3);
    assert_eq!(pb::MethodType::Rpc as i32, 9);
    assert_eq!(pb::MethodType::SubRefresh as i32, 11);
    assert_eq!(pb::PushType::Publication as i32, 0);
    assert_eq!(pb::PushType::Join as i32, 1);
    assert_eq!(pb::PushType::Sub as i32, 5);
}

#[test]
fn client_protocol_fixed_golden_bytes() {
    assert_encoded_hex(
        pb::Error {
            code: 102,
            message: "unknown channel".into(),
        },
        "0866120f756e6b6e6f776e206368616e6e656c",
    );
    assert_encoded_hex(
        pb::Command {
            id: 7,
            method: pb::MethodType::Publish as i32,
            params: br#"{"x":1}"#.to_vec(),
        },
        "080710031a077b2278223a317d",
    );
    assert_encoded_hex(
        pb::PresenceStatsResult {
            num_clients: 3,
            num_users: 2,
        },
        "08031002",
    );
    assert_encoded_hex(
        pb::PublishRequest {
            channel: "room".into(),
            data: br#"{"x":1}"#.to_vec(),
        },
        "0a04726f6f6d12077b2278223a317d",
    );
    assert_encoded_hex(
        pb::UnsubscribeRequest {
            channel: "room".into(),
        },
        "0a04726f6f6d",
    );
    assert_encoded_hex(pb::RefreshRequest { token: "t".into() }, "0a0174");
    assert_encoded_hex(
        pb::RpcRequest {
            method: "sum".into(),
            data: br#"{"a":1}"#.to_vec(),
        },
        "0a077b2261223a317d120373756d",
    );
}

#[test]
fn domain_messages_roundtrip_through_protobuf() {
    let info = msg::ClientInfo {
        user: "user-1".into(),
        client: "client-1".into(),
        conn_info: Some(raw(r#"{"conn":true}"#)),
        chan_info: Some(raw(r#"{"chan":1}"#)),
    };
    let publication = msg::Publication {
        seq: 11,
        gen: 3,
        uid: "pub-1".into(),
        data: Some(raw(r#"{"message":"hello"}"#)),
        info: Some(info.clone()),
        offset: 42,
    };

    assert_domain_roundtrip::<msg::ClientInfo, pb::ClientInfo>(info.clone());
    assert_domain_roundtrip::<msg::Publication, pb::Publication>(publication.clone());
    assert_domain_roundtrip::<msg::ConnectRequest, pb::ConnectRequest>(msg::ConnectRequest {
        token: "token".into(),
        data: Some(raw(r#"{"connect":1}"#)),
        name: "rust-client".into(),
        version: "1.0.0".into(),
        subs: HashMap::from([(
            "room".into(),
            msg::SubscribeRequest {
                channel: "room".into(),
                token: "sub-token".into(),
                recover: true,
                seq: 10,
                gen: 2,
                epoch: "abcd".into(),
                offset: 99,
            },
        )]),
    });
    assert_domain_roundtrip::<msg::ConnectResult, pb::ConnectResult>(msg::ConnectResult {
        client: "client-1".into(),
        version: "2.8.6".into(),
        expires: true,
        ttl: 60,
        data: Some(raw(r#"{"server":true}"#)),
        subs: HashMap::from([(
            "room".into(),
            msg::SubscribeResult {
                expires: true,
                ttl: 30,
                recoverable: true,
                seq: 12,
                gen: 3,
                epoch: "efgh".into(),
                publications: vec![publication.clone()],
                recovered: true,
                offset: 100,
            },
        )]),
    });
    assert_domain_roundtrip::<msg::SubscribeRequest, pb::SubscribeRequest>(msg::SubscribeRequest {
        channel: "room".into(),
        token: "sub-token".into(),
        recover: true,
        seq: 10,
        gen: 2,
        epoch: "abcd".into(),
        offset: 99,
    });
    assert_domain_roundtrip::<msg::SubscribeResult, pb::SubscribeResult>(msg::SubscribeResult {
        expires: true,
        ttl: 30,
        recoverable: true,
        seq: 12,
        gen: 3,
        epoch: "efgh".into(),
        publications: vec![publication.clone()],
        recovered: true,
        offset: 100,
    });
    assert_domain_roundtrip::<msg::UnsubscribeRequest, pb::UnsubscribeRequest>(
        msg::UnsubscribeRequest {
            channel: "room".into(),
        },
    );
    assert_domain_roundtrip::<msg::UnsubscribeResult, pb::UnsubscribeResult>(
        msg::UnsubscribeResult {},
    );
    assert_domain_roundtrip::<msg::PublishRequest, pb::PublishRequest>(msg::PublishRequest {
        channel: "room".into(),
        data: Some(raw(r#"{"x":1}"#)),
    });
    assert_domain_roundtrip::<msg::PublishResult, pb::PublishResult>(msg::PublishResult {});
    assert_domain_roundtrip::<msg::PresenceRequest, pb::PresenceRequest>(msg::PresenceRequest {
        channel: "room".into(),
    });
    assert_domain_roundtrip::<msg::PresenceResult, pb::PresenceResult>(msg::PresenceResult {
        presence: HashMap::from([("client-1".into(), info.clone())]),
    });
    assert_domain_roundtrip::<msg::PresenceStatsRequest, pb::PresenceStatsRequest>(
        msg::PresenceStatsRequest {
            channel: "room".into(),
        },
    );
    assert_domain_roundtrip::<msg::PresenceStatsResult, pb::PresenceStatsResult>(
        msg::PresenceStatsResult {
            num_clients: 3,
            num_users: 2,
        },
    );
    assert_domain_roundtrip::<msg::HistoryRequest, pb::HistoryRequest>(msg::HistoryRequest {
        channel: "room".into(),
    });
    assert_domain_roundtrip::<msg::HistoryResult, pb::HistoryResult>(msg::HistoryResult {
        publications: vec![publication],
    });
    assert_domain_roundtrip::<msg::RefreshRequest, pb::RefreshRequest>(msg::RefreshRequest {
        token: "refresh-token".into(),
    });
    assert_domain_roundtrip::<msg::RefreshResult, pb::RefreshResult>(msg::RefreshResult {
        client: "client-1".into(),
        version: "2.8.6".into(),
        expires: true,
        ttl: 60,
    });
    assert_domain_roundtrip::<msg::SubRefreshRequest, pb::SubRefreshRequest>(
        msg::SubRefreshRequest {
            channel: "room".into(),
            token: "sub-refresh-token".into(),
        },
    );
    assert_domain_roundtrip::<msg::SubRefreshResult, pb::SubRefreshResult>(msg::SubRefreshResult {
        expires: true,
        ttl: 30,
    });
    assert_domain_roundtrip::<msg::RpcRequest, pb::RpcRequest>(msg::RpcRequest {
        method: "sum".into(),
        data: Some(raw(r#"{"a":1}"#)),
    });
    assert_domain_roundtrip::<msg::RpcResult, pb::RpcResult>(msg::RpcResult {
        data: Some(raw(r#"{"answer":42}"#)),
    });
    assert_domain_roundtrip::<msg::Join, pb::Join>(msg::Join { info: info.clone() });
    assert_domain_roundtrip::<msg::Leave, pb::Leave>(msg::Leave { info });
    assert_domain_roundtrip::<msg::PingResult, pb::PingResult>(msg::PingResult {});
}

#[test]
fn envelopes_and_raw_bytes_cross_protobuf_boundary() {
    let command = Command {
        id: 7,
        method: MethodType::Publish,
        params: Some(raw(r#"{"x":1}"#)),
    };
    let pb_command: pb::Command = command.into();
    assert_eq!(pb_command.id, 7);
    assert_eq!(pb_command.method, pb::MethodType::Publish as i32);
    assert_eq!(pb_command.params, br#"{"x":1}"#);

    let reply = Reply {
        id: 7,
        error: Some(Error::permission_denied()),
        result: None,
    };
    let pb_reply: pb::Reply = reply.clone().into();
    assert_prost_roundtrip(pb_reply.clone());
    let decoded_reply: Reply = pb_reply.into();
    assert_eq!(json_value(&decoded_reply), json_value(&reply));

    let push = Push::new(
        PushType::Publication,
        "room",
        Some(Raw::from_bytes(publication().encode_to_vec())),
    );
    let pb_push: pb::Push = push.into();
    assert_eq!(pb_push.r#type, pb::PushType::Publication as i32);
    assert_eq!(pb_push.channel, "room");
    assert!(!pb_push.data.is_empty());

    let pb_empty = pb::PublishRequest {
        channel: "room".into(),
        data: Vec::new(),
    };
    let decoded_empty: msg::PublishRequest = pb_empty.into();
    assert!(
        decoded_empty.data.is_none(),
        "empty protobuf bytes are represented as None in the domain layer"
    );

    let domain = msg::PublishRequest {
        channel: "room".into(),
        data: Some(raw(r#"{"x":1}"#)),
    };
    let pb_non_empty: pb::PublishRequest = domain.into();
    assert_eq!(pb_non_empty.data, br#"{"x":1}"#);
}
