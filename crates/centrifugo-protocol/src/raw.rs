//! `Raw` — a raw payload byte field (`protocol.Raw` in Go). Holds the canonical
//! bytes of a `params`/`result`/`data`/`*_info` field.
//!
//! - In **JSON** mode the bytes are inline JSON: `Serialize` emits them verbatim
//!   (never base64), `Deserialize` captures the raw JSON slice. (A JSON
//!   connection's payloads are always valid UTF-8 JSON.)
//! - In **Protobuf** mode the codec uses `as_bytes()`/`from_bytes()` directly;
//!   serde is not involved.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

#[derive(Clone, PartialEq, Eq, Default)]
pub struct Raw(pub Vec<u8>);

impl Raw {
    pub fn from_bytes(b: impl Into<Vec<u8>>) -> Self {
        Raw(b.into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
    /// Lossy str view, for tests/debug.
    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl std::fmt::Debug for Raw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raw({})", self.as_str())
    }
}

impl From<Vec<u8>> for Raw {
    fn from(v: Vec<u8>) -> Self {
        Raw(v)
    }
}
impl From<&[u8]> for Raw {
    fn from(v: &[u8]) -> Self {
        Raw(v.to_vec())
    }
}

impl Serialize for Raw {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Emit the bytes verbatim as raw JSON. Requires valid UTF-8 JSON, which
        // holds for JSON connections; the protobuf codec never goes through serde.
        let text = std::str::from_utf8(&self.0).map_err(serde::ser::Error::custom)?;
        let rv = RawValue::from_string(text.to_owned()).map_err(serde::ser::Error::custom)?;
        rv.serialize(s)
    }
}

impl<'de> Deserialize<'de> for Raw {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let rv: Box<RawValue> = Box::<RawValue>::deserialize(d)?;
        Ok(Raw(rv.get().as_bytes().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct Holder {
        data: Option<Raw>,
    }

    #[test]
    fn serialize_inline_not_base64() {
        let h = Holder {
            data: Some(Raw::from_bytes(&b"{\"a\":1}"[..])),
        };
        assert_eq!(serde_json::to_string(&h).unwrap(), r#"{"data":{"a":1}}"#);
    }

    #[test]
    fn none_serializes_as_null() {
        let h = Holder { data: None };
        assert_eq!(serde_json::to_string(&h).unwrap(), r#"{"data":null}"#);
    }

    #[test]
    fn deserialize_captures_raw_bytes() {
        let h: Holder = serde_json::from_str(r#"{"data":{"nested":[1,2,3]}}"#).unwrap();
        assert_eq!(h.data.unwrap().as_bytes(), br#"{"nested":[1,2,3]}"#);
    }

    #[test]
    fn roundtrip_preserves_bytes() {
        let original = br#"{"x":1,"y":"z"}"#;
        let h = Holder {
            data: Some(Raw::from_bytes(&original[..])),
        };
        let s = serde_json::to_string(&h).unwrap();
        let back: Holder = serde_json::from_str(&s).unwrap();
        assert_eq!(back.data.unwrap().as_bytes(), original);
    }
}
