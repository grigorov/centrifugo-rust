//! Protobuf wire types generated from `proto/client.proto` (protocol v0.3.4) via
//! protox + prost-build. The `protocol` proto package becomes this module.
//!
//! These are the binary-codec counterparts of the serde domain types in
//! `command.rs`/`messages.rs`; `bytes` fields are `Vec<u8>` here and `Raw` in
//! the domain types. Conversions live in `convert.rs` (M2.3).

#![allow(clippy::all)]
#![allow(missing_docs)]

include!(concat!(env!("OUT_DIR"), "/protocol.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn connect_result_roundtrip() {
        let r = ConnectResult {
            client: "abc".into(),
            version: "2.8.6".into(),
            ..Default::default()
        };
        let bytes = r.encode_to_vec();
        let back = ConnectResult::decode(&bytes[..]).unwrap();
        assert_eq!(back.client, "abc");
        assert_eq!(back.version, "2.8.6");
    }

    #[test]
    fn command_roundtrip_with_method() {
        let c = Command {
            id: 5,
            method: MethodType::Publish as i32,
            params: br#"{"x":1}"#.to_vec(),
        };
        let bytes = c.encode_to_vec();
        let back = Command::decode(&bytes[..]).unwrap();
        assert_eq!(back.id, 5);
        assert_eq!(back.method, MethodType::Publish as i32);
        assert_eq!(back.params, br#"{"x":1}"#);
    }

    #[test]
    fn method_enum_values_match_proto() {
        assert_eq!(MethodType::Connect as i32, 0);
        assert_eq!(MethodType::SubRefresh as i32, 11);
        assert_eq!(PushType::Publication as i32, 0);
        assert_eq!(PushType::Sub as i32, 5);
    }
}
