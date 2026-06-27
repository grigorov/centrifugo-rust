//! Centrifugal protocol v0.3.4 wire types and codecs (the protocol-v2 era used by
//! Centrifugo v2.8.6 / centrifuge v0.14.2).
//!
//! Authority for all wire bytes: `docs/reference/protocol-v0.3.4-wire-format.md`.

pub mod codec;
pub mod command;
pub mod convert;
pub mod disconnect;
pub mod error;
pub mod json;
pub mod messages;
pub mod method;
pub mod pb;
pub mod raw;

/// centrifuge inter-node control protocol (`controlpb` package) — generated from
/// `proto/control.proto`. Wire-identical to centrifuge v0.14.2 for Go interop.
pub mod controlpb {
    #![allow(clippy::all)]
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/controlpb.rs"));
}

pub use codec::ProtocolType;
pub use command::{Command, Push, Reply};
pub use disconnect::Disconnect;
pub use error::Error;
pub use method::{MethodType, PushType};
pub use raw::Raw;
