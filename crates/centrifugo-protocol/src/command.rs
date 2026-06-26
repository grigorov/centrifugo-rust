//! The three wire envelopes: `Command` (client→server), `Reply` (server→client
//! response), `Push` (server→client async event, delivered as a `Reply` with
//! `id==0` whose `result` is the encoded `Push`).

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::method::{MethodType, PushType};
use crate::raw::Raw;

fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub id: u32,
    #[serde(default, skip_serializing_if = "MethodType::is_default")]
    pub method: MethodType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Raw>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Raw>,
}

impl Reply {
    /// A command reply carrying an already-encoded result object.
    pub fn ok(id: u32, result: Raw) -> Self {
        Reply {
            id,
            error: None,
            result: Some(result),
        }
    }

    /// An error reply.
    pub fn err(id: u32, error: Error) -> Self {
        Reply {
            id,
            error: Some(error),
            result: None,
        }
    }

    /// Serialize any result value, then build a command reply.
    pub fn ok_value<T: Serialize>(id: u32, value: &T) -> Result<Self, serde_json::Error> {
        Ok(Reply::ok(id, encode_raw(value)?))
    }

    /// Frame an async push: a `Reply` with `id==0` whose `result` is the encoded `Push`.
    pub fn push(push: &Push) -> Result<Self, serde_json::Error> {
        Ok(Reply {
            id: 0,
            error: None,
            result: Some(encode_raw(push)?),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Push {
    #[serde(default, skip_serializing_if = "PushType::is_default")]
    pub r#type: PushType,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    /// No `omitempty`: serializes as `null` when `None`.
    pub data: Option<Raw>,
}

impl Push {
    pub fn new(r#type: PushType, channel: impl Into<String>, data: Option<Raw>) -> Self {
        Push {
            r#type,
            channel: channel.into(),
            data,
        }
    }
}

/// Encode a value to JSON bytes wrapped as `Raw`.
pub fn encode_raw<T: Serialize>(value: &T) -> Result<Raw, serde_json::Error> {
    Ok(Raw(serde_json::to_vec(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(s: &str) -> Raw {
        Raw::from_bytes(s.as_bytes())
    }

    #[test]
    fn connect_command_omits_method_and_uses_inline_params() {
        let cmd = Command {
            id: 1,
            method: MethodType::Connect,
            params: Some(raw("{}")),
        };
        assert_eq!(
            serde_json::to_string(&cmd).unwrap(),
            r#"{"id":1,"params":{}}"#
        );
    }

    #[test]
    fn subscribe_command_has_integer_method() {
        let cmd = Command {
            id: 2,
            method: MethodType::Subscribe,
            params: Some(raw(r#"{"channel":"news"}"#)),
        };
        assert_eq!(
            serde_json::to_string(&cmd).unwrap(),
            r#"{"id":2,"method":1,"params":{"channel":"news"}}"#
        );
    }

    #[test]
    fn reply_with_result_no_id_for_push() {
        let push = Push::new(
            PushType::Publication,
            "news",
            Some(raw(r#"{"data":{"x":1}}"#)),
        );
        let reply = Reply::push(&push).unwrap();
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"result":{"channel":"news","data":{"data":{"x":1}}}}"#
        );
    }

    #[test]
    fn command_reply_has_id_and_result() {
        let reply = Reply {
            id: 7,
            error: None,
            result: Some(raw(r#"{"client":"abc","version":""}"#)),
        };
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"id":7,"result":{"client":"abc","version":""}}"#
        );
    }

    #[test]
    fn reply_with_error() {
        let reply = Reply::err(3, Error::unknown_channel());
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"id":3,"error":{"code":102,"message":"unknown channel"}}"#
        );
    }

    #[test]
    fn decode_command_captures_inline_params() {
        let cmd: Command =
            serde_json::from_str(r#"{"id":5,"method":3,"params":{"channel":"x","data":{"a":1}}}"#)
                .unwrap();
        assert_eq!(cmd.id, 5);
        assert_eq!(cmd.method, MethodType::Publish);
        let p = cmd.params.unwrap();
        assert_eq!(&*p.as_str(), r#"{"channel":"x","data":{"a":1}}"#);
    }

    #[test]
    fn decode_connect_command_without_method() {
        let cmd: Command = serde_json::from_str(r#"{"id":1,"params":{}}"#).unwrap();
        assert_eq!(cmd.method, MethodType::Connect);
    }
}
