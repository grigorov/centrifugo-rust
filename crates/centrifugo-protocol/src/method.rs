//! `MethodType` (client command method) and `PushType` (async push kind).
//!
//! Both are encoded in JSON as their **integer** value and omitted when 0
//! (CONNECT / PUBLICATION) because the Go struct tags carry `omitempty`. On
//! decode, `MethodType` accepts either an integer or a quoted name.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum MethodType {
    #[default]
    Connect = 0,
    Subscribe = 1,
    Unsubscribe = 2,
    Publish = 3,
    Presence = 4,
    PresenceStats = 5,
    History = 6,
    Ping = 7,
    Send = 8,
    Rpc = 9,
    Refresh = 10,
    SubRefresh = 11,
}

impl MethodType {
    /// CONNECT is the zero value; the `method` field is omitted when it equals this.
    pub fn is_default(&self) -> bool {
        *self == MethodType::Connect
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(match n {
            0 => Self::Connect,
            1 => Self::Subscribe,
            2 => Self::Unsubscribe,
            3 => Self::Publish,
            4 => Self::Presence,
            5 => Self::PresenceStats,
            6 => Self::History,
            7 => Self::Ping,
            8 => Self::Send,
            9 => Self::Rpc,
            10 => Self::Refresh,
            11 => Self::SubRefresh,
            _ => return None,
        })
    }

    fn from_name(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "CONNECT" => Self::Connect,
            "SUBSCRIBE" => Self::Subscribe,
            "UNSUBSCRIBE" => Self::Unsubscribe,
            "PUBLISH" => Self::Publish,
            "PRESENCE" => Self::Presence,
            "PRESENCE_STATS" => Self::PresenceStats,
            "HISTORY" => Self::History,
            "PING" => Self::Ping,
            "SEND" => Self::Send,
            "RPC" => Self::Rpc,
            "REFRESH" => Self::Refresh,
            "SUB_REFRESH" => Self::SubRefresh,
            _ => return None,
        })
    }
}

impl Serialize for MethodType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for MethodType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MethodType;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("method as integer or name")
            }
            fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<MethodType, E> {
                MethodType::from_u64(n).ok_or_else(|| E::custom(format!("bad method int {n}")))
            }
            fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<MethodType, E> {
                if n < 0 {
                    return Err(E::custom("negative method"));
                }
                MethodType::from_u64(n as u64)
                    .ok_or_else(|| E::custom(format!("bad method int {n}")))
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<MethodType, E> {
                MethodType::from_name(s).ok_or_else(|| E::custom(format!("bad method name {s}")))
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PushType {
    #[default]
    Publication = 0,
    Join = 1,
    Leave = 2,
    Unsub = 3,
    Message = 4,
    Sub = 5,
}

impl PushType {
    /// PUBLICATION is the zero value; the `type` field is omitted when it equals this.
    pub fn is_default(&self) -> bool {
        *self == PushType::Publication
    }
}

impl Serialize for PushType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for PushType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let n = u8::deserialize(d)?;
        Ok(match n {
            0 => Self::Publication,
            1 => Self::Join,
            2 => Self::Leave,
            3 => Self::Unsub,
            4 => Self::Message,
            5 => Self::Sub,
            _ => return Err(serde::de::Error::custom(format!("bad push type {n}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_serializes_as_integer() {
        assert_eq!(serde_json::to_string(&MethodType::Subscribe).unwrap(), "1");
        assert_eq!(
            serde_json::to_string(&MethodType::SubRefresh).unwrap(),
            "11"
        );
    }

    #[test]
    fn method_deserializes_from_int_or_string() {
        assert_eq!(
            serde_json::from_str::<MethodType>("3").unwrap(),
            MethodType::Publish
        );
        assert_eq!(
            serde_json::from_str::<MethodType>("\"publish\"").unwrap(),
            MethodType::Publish
        );
        assert_eq!(
            serde_json::from_str::<MethodType>("\"PUBLISH\"").unwrap(),
            MethodType::Publish
        );
    }

    #[test]
    fn connect_is_default_zero() {
        assert_eq!(MethodType::default(), MethodType::Connect);
        assert_eq!(MethodType::Connect as u8, 0);
        assert!(MethodType::Connect.is_default());
    }

    #[test]
    fn push_type_publication_is_zero() {
        assert_eq!(serde_json::to_string(&PushType::Join).unwrap(), "1");
        assert!(PushType::Publication.is_default());
    }
}
