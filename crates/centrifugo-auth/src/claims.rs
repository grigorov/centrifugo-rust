//! JWT claim structs. Field names match the Go `ConnectTokenClaims` /
//! `SubscribeTokenClaims` JSON tags. Standard claims (`sub`/`exp`/`nbf`/`iat`)
//! are flattened in.

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Debug, Default, Deserialize)]
pub struct ConnectTokenClaims {
    #[serde(default)]
    pub sub: Option<String>,
    #[serde(default)]
    pub exp: Option<i64>,
    #[serde(default)]
    pub nbf: Option<i64>,
    #[serde(default)]
    pub iat: Option<i64>,
    #[serde(default)]
    pub info: Option<Box<RawValue>>,
    #[serde(default)]
    pub b64info: Option<String>,
    #[serde(default)]
    pub channels: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SubscribeTokenClaims {
    #[serde(default)]
    pub client: Option<String>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub exp: Option<i64>,
    #[serde(default)]
    pub nbf: Option<i64>,
    #[serde(default)]
    pub info: Option<Box<RawValue>>,
    #[serde(default)]
    pub b64info: Option<String>,
    #[serde(default, rename = "eto")]
    pub expire_token_only: bool,
}
