//! JWT claim structs. Field names match the Go `ConnectTokenClaims` /
//! `SubscribeTokenClaims` JSON tags. Standard claims (`sub`/`exp`/`nbf`/`iat`)
//! are flattened in.

use serde::Deserialize;
use serde_json::value::RawValue;

/// Deserialize a JWT NumericDate (`exp`/`nbf`) the way Go's cristalhq/jwt does:
/// accept an integer, a fractional float, or a numeric string, all as seconds.
/// An explicit JSON `null` or an absent field yields `None`; a non-numeric value
/// is an error (→ invalid token), matching `NumericDate.UnmarshalJSON`.
fn deserialize_numeric_date<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumericDate {
        Num(f64),
        Str(String),
    }
    match Option::<NumericDate>::deserialize(deserializer)? {
        None => Ok(None),
        Some(NumericDate::Num(n)) => Ok(Some(n)),
        Some(NumericDate::Str(s)) => s
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ConnectTokenClaims {
    #[serde(default)]
    pub sub: Option<String>,
    #[serde(default, deserialize_with = "deserialize_numeric_date")]
    pub exp: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_numeric_date")]
    pub nbf: Option<f64>,
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
    #[serde(default, deserialize_with = "deserialize_numeric_date")]
    pub exp: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_numeric_date")]
    pub nbf: Option<f64>,
    #[serde(default)]
    pub info: Option<Box<RawValue>>,
    #[serde(default)]
    pub b64info: Option<String>,
    #[serde(default, rename = "eto")]
    pub expire_token_only: bool,
}
