//! Request / Result payloads and inner objects (Publication, ClientInfo, Join,
//! Leave, Sub, Unsub, Message). Field presence (`omitempty`) and JSON key names
//! (snake_case where required) follow `docs/reference/protocol-v0.3.4-wire-format.md`.
//!
//! Only the messages needed up to the current milestone are defined; the rest
//! are added in their milestones.

use serde::{Deserialize, Serialize};

use crate::command::Raw;

fn is_false(b: &bool) -> bool {
    !*b
}
fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}
fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

// ---- Connect ----

#[derive(Debug, Default, Deserialize)]
pub struct ConnectRequest {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub data: Option<Raw>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Default, Serialize)]
pub struct ConnectResult {
    pub client: String,
    pub version: String,
    #[serde(skip_serializing_if = "is_false")]
    pub expires: bool,
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub ttl: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Raw>,
}

// ---- Subscribe ----

#[derive(Debug, Default, Deserialize)]
pub struct SubscribeRequest {
    #[serde(default)]
    pub channel: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub recover: bool,
    #[serde(default)]
    pub epoch: String,
    #[serde(default)]
    pub offset: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct SubscribeResult {
    #[serde(skip_serializing_if = "is_false")]
    pub expires: bool,
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub ttl: u32,
    #[serde(skip_serializing_if = "is_false")]
    pub recoverable: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub epoch: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<Publication>,
    #[serde(skip_serializing_if = "is_false")]
    pub recovered: bool,
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub offset: u64,
}

// ---- Publish ----

#[derive(Debug, Default, Deserialize)]
pub struct PublishRequest {
    #[serde(default)]
    pub channel: String,
    pub data: Option<Raw>,
}

#[derive(Debug, Default, Serialize)]
pub struct PublishResult {}

// ---- Unsubscribe ----

#[derive(Debug, Default, Deserialize)]
pub struct UnsubscribeRequest {
    #[serde(default)]
    pub channel: String,
}

#[derive(Debug, Default, Serialize)]
pub struct UnsubscribeResult {}

// ---- Ping ----

#[derive(Debug, Default, Serialize)]
pub struct PingResult {}

// ---- Inner objects ----

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientInfo {
    pub user: String,
    pub client: String,
    #[serde(rename = "conn_info", default, skip_serializing_if = "Option::is_none")]
    pub conn_info: Option<Raw>,
    #[serde(rename = "chan_info", default, skip_serializing_if = "Option::is_none")]
    pub chan_info: Option<Raw>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Publication {
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub seq: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gen: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
    /// No `omitempty`: serializes as `null` when `None`.
    pub data: Option<Raw>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<ClientInfo>,
    /// Zeroed before pushing to clients in v0.14.2 (so usually absent).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;

    fn raw(s: &str) -> Raw {
        RawValue::from_string(s.to_string()).unwrap()
    }

    #[test]
    fn connect_result_always_emits_client_and_version() {
        let r = ConnectResult {
            client: "abc".into(),
            version: String::new(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"client":"abc","version":""}"#
        );
    }

    #[test]
    fn empty_results_serialize_as_empty_object() {
        assert_eq!(serde_json::to_string(&PublishResult {}).unwrap(), "{}");
        assert_eq!(serde_json::to_string(&UnsubscribeResult {}).unwrap(), "{}");
        assert_eq!(
            serde_json::to_string(&SubscribeResult::default()).unwrap(),
            "{}"
        );
    }

    #[test]
    fn publication_with_only_data() {
        let p = Publication {
            data: Some(raw(r#"{"msg":"hi"}"#)),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&p).unwrap(),
            r#"{"data":{"msg":"hi"}}"#
        );
    }

    #[test]
    fn client_info_uses_snake_case_keys() {
        let ci = ClientInfo {
            user: "u".into(),
            client: "c".into(),
            conn_info: Some(raw(r#"{"a":1}"#)),
            chan_info: None,
        };
        assert_eq!(
            serde_json::to_string(&ci).unwrap(),
            r#"{"user":"u","client":"c","conn_info":{"a":1}}"#
        );
    }
}
