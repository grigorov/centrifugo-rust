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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_numeric_date_integer() {
        let json = json!({"exp": 1234567890});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(1234567890.0));
    }

    #[test]
    fn deserialize_numeric_date_fractional_float() {
        let json = json!({"exp": 1234567890.5});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(1234567890.5));
    }

    #[test]
    fn deserialize_numeric_date_string_integer() {
        let json = json!({"exp": "1234567890"});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(1234567890.0));
    }

    #[test]
    fn deserialize_numeric_date_string_fractional() {
        let json = json!({"exp": "1234567890.5"});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(1234567890.5));
    }

    #[test]
    fn deserialize_numeric_date_string_with_whitespace() {
        let json = json!({"exp": "  1234567890.5  "});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(1234567890.5));
    }

    #[test]
    fn deserialize_numeric_date_null() {
        let json = json!({"exp": null});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, None);
    }

    #[test]
    fn deserialize_numeric_date_missing() {
        let json = json!({});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, None);
    }

    #[test]
    fn deserialize_numeric_date_empty_string_is_error() {
        let json = json!({"exp": ""});
        let result: Result<ConnectTokenClaims, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_numeric_date_non_numeric_string_is_error() {
        let json = json!({"exp": "not-a-number"});
        let result: Result<ConnectTokenClaims, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_numeric_date_negative_number() {
        let json = json!({"exp": -100.5});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(-100.5));
    }

    #[test]
    fn deserialize_numeric_date_very_large_number() {
        let json = json!({"exp": 9999999999999.0});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(9999999999999.0));
    }

    #[test]
    fn deserialize_numeric_date_zero() {
        let json = json!({"exp": 0});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.exp, Some(0.0));
    }

    #[test]
    fn deserialize_nbf_same_as_exp() {
        let json = json!({"nbf": 1234567890.5});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.nbf, Some(1234567890.5));
    }

    #[test]
    fn connect_token_claims_all_fields() {
        let json = json!({
            "sub": "user123",
            "exp": 1234567890,
            "nbf": 1234567800,
            "iat": 1234567700,
            "info": {"key": "value"},
            "b64info": "dGVzdA==",
            "channels": ["channel1", "channel2"]
        });
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.sub, Some("user123".to_string()));
        assert_eq!(claims.exp, Some(1234567890.0));
        assert_eq!(claims.nbf, Some(1234567800.0));
        assert_eq!(claims.iat, Some(1234567700));
        assert!(claims.info.is_some());
        assert_eq!(claims.b64info, Some("dGVzdA==".to_string()));
        assert_eq!(
            claims.channels,
            Some(vec!["channel1".to_string(), "channel2".to_string()])
        );
    }

    #[test]
    fn subscribe_token_claims_all_fields() {
        let json = json!({
            "client": "client123",
            "channel": "room:123",
            "exp": 1234567890,
            "nbf": 1234567800,
            "info": {"key": "value"},
            "b64info": "dGVzdA==",
            "eto": true
        });
        let claims: SubscribeTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.client, Some("client123".to_string()));
        assert_eq!(claims.channel, Some("room:123".to_string()));
        assert_eq!(claims.exp, Some(1234567890.0));
        assert_eq!(claims.nbf, Some(1234567800.0));
        assert!(claims.info.is_some());
        assert_eq!(claims.b64info, Some("dGVzdA==".to_string()));
        assert!(claims.expire_token_only);
    }

    #[test]
    fn subscribe_token_claims_eto_false_by_default() {
        let json = json!({
            "client": "client123",
            "channel": "room:123"
        });
        let claims: SubscribeTokenClaims = serde_json::from_value(json).unwrap();
        assert!(!claims.expire_token_only);
    }

    #[test]
    fn connect_token_claims_defaults() {
        let json = json!({});
        let claims: ConnectTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.sub, None);
        assert_eq!(claims.exp, None);
        assert_eq!(claims.nbf, None);
        assert_eq!(claims.iat, None);
        assert!(claims.info.is_none());
        assert_eq!(claims.b64info, None);
        assert_eq!(claims.channels, None);
    }

    #[test]
    fn subscribe_token_claims_defaults() {
        let json = json!({});
        let claims: SubscribeTokenClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.client, None);
        assert_eq!(claims.channel, None);
        assert_eq!(claims.exp, None);
        assert_eq!(claims.nbf, None);
        assert!(claims.info.is_none());
        assert_eq!(claims.b64info, None);
        assert!(!claims.expire_token_only);
    }
}
