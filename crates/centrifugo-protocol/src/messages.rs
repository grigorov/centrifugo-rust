//! Request / Result payloads and inner objects (Publication, ClientInfo, …).
//! Field presence (`omitempty`) and JSON key names (snake_case where required)
//! follow `docs/reference/protocol-v0.3.4-wire-format.md`.
//!
//! Every type derives both `Serialize` and `Deserialize` (and `Default`) so it
//! round-trips in either codec and can be produced by test clients. Fields carry
//! `#[serde(default)]` so a reply that omits an `omitempty` field still decodes.
//!
//! Only the messages needed up to the current milestone are defined; the rest
//! are added in their milestones.

use serde::{Deserialize, Serialize};

use crate::raw::Raw;

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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Raw>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectResult {
    #[serde(default)]
    pub client: String,
    #[serde(default)]
    pub version: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub expires: bool,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub ttl: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Raw>,
}

// ---- Subscribe ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recover: bool,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub seq: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gen: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub epoch: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub offset: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SubscribeResult {
    #[serde(default, skip_serializing_if = "is_false")]
    pub expires: bool,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub ttl: u32,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recoverable: bool,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub seq: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gen: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub epoch: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<Publication>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recovered: bool,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub offset: u64,
}

// ---- Publish ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PublishRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    #[serde(default)]
    pub data: Option<Raw>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PublishResult {}

// ---- Unsubscribe ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    #[serde(default)]
    pub channel: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UnsubscribeResult {}

// ---- Ping ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PingResult {}

// ---- History ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HistoryRequest {
    #[serde(default)]
    pub channel: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HistoryResult {
    /// No `omitempty`: an empty history serializes as `"publications":[]`.
    #[serde(default)]
    pub publications: Vec<Publication>,
}

// ---- Refresh ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RefreshRequest {
    #[serde(default)]
    pub token: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RefreshResult {
    #[serde(default)]
    pub client: String,
    #[serde(default)]
    pub version: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub expires: bool,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub ttl: u32,
}

// ---- Presence ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PresenceRequest {
    #[serde(default)]
    pub channel: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PresenceResult {
    #[serde(default)]
    pub presence: std::collections::HashMap<String, ClientInfo>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PresenceStatsRequest {
    #[serde(default)]
    pub channel: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PresenceStatsResult {
    #[serde(default, rename = "num_clients")]
    pub num_clients: u32,
    #[serde(default, rename = "num_users")]
    pub num_users: u32,
}

// ---- Join / Leave (push payloads) ----

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Join {
    #[serde(default)]
    pub info: ClientInfo,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Leave {
    #[serde(default)]
    pub info: ClientInfo,
}

// ---- Inner objects ----

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientInfo {
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub client: String,
    #[serde(rename = "conn_info", default, skip_serializing_if = "Option::is_none")]
    pub conn_info: Option<Raw>,
    #[serde(rename = "chan_info", default, skip_serializing_if = "Option::is_none")]
    pub chan_info: Option<Raw>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Publication {
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub seq: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gen: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
    /// No `omitempty`: serializes as `null` when `None`.
    #[serde(default)]
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

    fn raw(s: &str) -> Raw {
        Raw::from_bytes(s.as_bytes())
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

    #[test]
    fn connect_result_decodes_from_partial_json() {
        // a reply that omits omitempty fields still decodes
        let r: ConnectResult = serde_json::from_str(r#"{"client":"x","version":"v"}"#).unwrap();
        assert_eq!(r.client, "x");
        assert!(!r.expires);
    }
}
