//! Per-connection wire codec. A connection is either `Json` (NDJSON) or
//! `Protobuf` (uvarint length-prefixed). The same logical messages flow through
//! both; this module decodes commands / typed params and encodes replies / typed
//! results / push frames in the connection's format.

use prost::Message as _;

use crate::command::encode_raw;
use crate::messages::{
    ConnectRequest, ConnectResult, PingResult, Publication, PublishRequest, PublishResult,
    SubscribeRequest, SubscribeResult, UnsubscribeRequest, UnsubscribeResult,
};
use crate::raw::Raw;
use crate::{json, pb, Command, MethodType, Push, Reply};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    Json,
    Protobuf,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf decode: {0}")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("unknown method {0}")]
    BadMethod(i32),
    #[error("truncated protobuf frame")]
    Truncated,
}

// ---- uvarint (LEB128, matches Go encoding/binary Uvarint) ----

fn encode_uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Returns (value, bytes_consumed) or None if truncated/overflow.
fn decode_uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i >= 10 {
            return None; // more than 10 bytes => overflow for u64
        }
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
}

// ---- envelopes ----

fn command_from_pb(c: pb::Command) -> Result<Command, CodecError> {
    let method = MethodType::from_i32(c.method).ok_or(CodecError::BadMethod(c.method))?;
    Ok(Command {
        id: c.id,
        method,
        params: crate::convert::vec_to_raw(c.params),
    })
}

/// Decode all commands from one transport frame.
pub fn decode_commands(proto: ProtocolType, frame: &[u8]) -> Result<Vec<Command>, CodecError> {
    match proto {
        ProtocolType::Json => Ok(json::decode_commands(frame)?),
        ProtocolType::Protobuf => {
            let mut out = Vec::new();
            let mut rest = frame;
            while !rest.is_empty() {
                let (len, consumed) = decode_uvarint(rest).ok_or(CodecError::Truncated)?;
                let start = consumed;
                let end = start
                    .checked_add(len as usize)
                    .ok_or(CodecError::Truncated)?;
                if end > rest.len() {
                    return Err(CodecError::Truncated);
                }
                let msg = pb::Command::decode(&rest[start..end])?;
                out.push(command_from_pb(msg)?);
                rest = &rest[end..];
            }
            Ok(out)
        }
    }
}

/// Encode commands into one transport frame (mainly for test clients).
pub fn encode_commands(proto: ProtocolType, commands: &[Command]) -> Result<Vec<u8>, CodecError> {
    match proto {
        ProtocolType::Json => {
            let mut buf = Vec::new();
            for c in commands {
                serde_json::to_writer(&mut buf, c)?;
                buf.push(b'\n');
            }
            Ok(buf)
        }
        ProtocolType::Protobuf => {
            let mut buf = Vec::new();
            for c in commands {
                let pbc: pb::Command = c.clone().into();
                let bytes = pbc.encode_to_vec();
                encode_uvarint(bytes.len() as u64, &mut buf);
                buf.extend_from_slice(&bytes);
            }
            Ok(buf)
        }
    }
}

/// Decode replies from one transport frame (mainly for test clients).
pub fn decode_replies(proto: ProtocolType, frame: &[u8]) -> Result<Vec<Reply>, CodecError> {
    match proto {
        ProtocolType::Json => {
            let de = serde_json::Deserializer::from_slice(frame);
            let mut out = Vec::new();
            for r in de.into_iter::<Reply>() {
                out.push(r?);
            }
            Ok(out)
        }
        ProtocolType::Protobuf => {
            let mut out = Vec::new();
            let mut rest = frame;
            while !rest.is_empty() {
                let (len, consumed) = decode_uvarint(rest).ok_or(CodecError::Truncated)?;
                let start = consumed;
                let end = start
                    .checked_add(len as usize)
                    .ok_or(CodecError::Truncated)?;
                if end > rest.len() {
                    return Err(CodecError::Truncated);
                }
                out.push(pb::Reply::decode(&rest[start..end])?.into());
                rest = &rest[end..];
            }
            Ok(out)
        }
    }
}

/// Encode replies into one transport frame.
pub fn encode_replies(proto: ProtocolType, replies: &[Reply]) -> Result<Vec<u8>, CodecError> {
    match proto {
        ProtocolType::Json => Ok(json::encode_replies(replies)?),
        ProtocolType::Protobuf => {
            let mut buf = Vec::new();
            for r in replies {
                let pbr: pb::Reply = r.clone().into();
                let bytes = pbr.encode_to_vec();
                encode_uvarint(bytes.len() as u64, &mut buf);
                buf.extend_from_slice(&bytes);
            }
            Ok(buf)
        }
    }
}

// ---- typed params / results ----

/// A domain type that has a protobuf (`pb`) counterpart for the protobuf codec.
pub trait WireType: Sized {
    fn pb_decode(bytes: &[u8]) -> Result<Self, CodecError>;
    fn pb_encode(&self) -> Vec<u8>;
}

macro_rules! wire {
    ($domain:ty, $pb:ty) => {
        impl WireType for $domain {
            fn pb_decode(bytes: &[u8]) -> Result<Self, CodecError> {
                Ok(<$pb as prost::Message>::decode(bytes)?.into())
            }
            fn pb_encode(&self) -> Vec<u8> {
                let m: $pb = self.clone().into();
                prost::Message::encode_to_vec(&m)
            }
        }
    };
}

wire!(ConnectRequest, pb::ConnectRequest);
wire!(ConnectResult, pb::ConnectResult);
wire!(SubscribeRequest, pb::SubscribeRequest);
wire!(SubscribeResult, pb::SubscribeResult);
wire!(PublishRequest, pb::PublishRequest);
wire!(PublishResult, pb::PublishResult);
wire!(UnsubscribeRequest, pb::UnsubscribeRequest);
wire!(UnsubscribeResult, pb::UnsubscribeResult);
wire!(PingResult, pb::PingResult);
wire!(Publication, pb::Publication);

/// Decode a command's params into a typed request (missing params -> default).
pub fn decode_params<T>(proto: ProtocolType, raw: &Option<Raw>) -> Result<T, CodecError>
where
    T: Default + serde::de::DeserializeOwned + WireType,
{
    match raw {
        None => Ok(T::default()),
        Some(r) => match proto {
            ProtocolType::Json => Ok(serde_json::from_slice(r.as_bytes())?),
            ProtocolType::Protobuf => T::pb_decode(r.as_bytes()),
        },
    }
}

/// Encode a typed result into raw bytes for a reply's `result` field.
pub fn encode_result<T>(proto: ProtocolType, value: &T) -> Result<Raw, CodecError>
where
    T: serde::Serialize + WireType,
{
    match proto {
        ProtocolType::Json => Ok(Raw(serde_json::to_vec(value)?)),
        ProtocolType::Protobuf => Ok(Raw(value.pb_encode())),
    }
}

/// Build a full push frame (a `Reply` with `id==0` carrying the encoded `Push`).
pub fn encode_push_frame(proto: ProtocolType, push: &Push) -> Result<Vec<u8>, CodecError> {
    let result = match proto {
        ProtocolType::Json => encode_raw(push)?,
        ProtocolType::Protobuf => Raw(<pb::Push>::from(push.clone()).encode_to_vec()),
    };
    let reply = Reply {
        id: 0,
        error: None,
        result: Some(result),
    };
    encode_replies(proto, &[reply])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_uvarint(v, &mut buf);
            let (got, n) = decode_uvarint(&buf).unwrap();
            assert_eq!(got, v);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn uvarint_known_values() {
        // Go binary.PutUvarint: 1 -> [0x01], 300 -> [0xAC, 0x02]
        let mut b = Vec::new();
        encode_uvarint(1, &mut b);
        assert_eq!(b, vec![0x01]);
        b.clear();
        encode_uvarint(300, &mut b);
        assert_eq!(b, vec![0xAC, 0x02]);
    }

    #[test]
    fn protobuf_command_frame_roundtrip() {
        // two packed commands
        let cmds = vec![
            Command {
                id: 1,
                method: MethodType::Connect,
                params: Some(Raw::from_bytes(&b"x"[..])),
            },
            Command {
                id: 2,
                method: MethodType::Subscribe,
                params: None,
            },
        ];
        // encode each as pb with uvarint prefix
        let mut frame = Vec::new();
        for c in &cmds {
            let pbc = pb::Command {
                id: c.id,
                method: c.method as i32,
                params: c
                    .params
                    .as_ref()
                    .map(|r| r.as_bytes().to_vec())
                    .unwrap_or_default(),
            };
            let bytes = pbc.encode_to_vec();
            encode_uvarint(bytes.len() as u64, &mut frame);
            frame.extend_from_slice(&bytes);
        }
        let decoded = decode_commands(ProtocolType::Protobuf, &frame).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].id, 1);
        assert_eq!(decoded[0].method, MethodType::Connect);
        assert_eq!(decoded[1].id, 2);
        assert_eq!(decoded[1].method, MethodType::Subscribe);
    }

    #[test]
    fn json_and_protobuf_result_encoding() {
        let r = ConnectResult {
            client: "abc".into(),
            version: "v".into(),
            ..Default::default()
        };
        let json = encode_result(ProtocolType::Json, &r).unwrap();
        assert_eq!(&*json.as_str(), r#"{"client":"abc","version":"v"}"#);
        let pbuf = encode_result(ProtocolType::Protobuf, &r).unwrap();
        let back = pb::ConnectResult::decode(pbuf.as_bytes()).unwrap();
        assert_eq!(back.client, "abc");
        assert_eq!(back.version, "v");
    }
}
