//! `TokenVerifier`: verifies a JWT using the key matching its header algorithm,
//! then applies Centrifugo's claim semantics.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

use crate::claims::{ConnectTokenClaims, SubscribeTokenClaims};
use crate::error::VerifyError;

/// The verified connection token result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectToken {
    pub user: String,
    pub info: Option<Vec<u8>>,
    pub channels: Vec<String>,
    /// Unix seconds; 0 means no expiry.
    pub expire_at: i64,
}

/// The verified subscription token result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeToken {
    pub client: String,
    pub channel: String,
    pub info: Option<Vec<u8>>,
    pub expire_at: i64,
    /// `eto` — expire the token only (do not also expire the subscription).
    pub expire_token_only: bool,
}

#[derive(Default)]
pub struct TokenVerifier {
    hmac: Option<DecodingKey>,
    rsa: Option<DecodingKey>,
    ecdsa: Option<DecodingKey>,
}

impl TokenVerifier {
    /// Build a verifier. `hmac_secret` empty disables HMAC; PEM args `None`
    /// disable RSA/ECDSA. Invalid PEM yields `VerifyError::Invalid`.
    pub fn new(
        hmac_secret: &str,
        rsa_pem: Option<&[u8]>,
        ecdsa_pem: Option<&[u8]>,
    ) -> Result<Self, VerifyError> {
        let hmac = if hmac_secret.is_empty() {
            None
        } else {
            Some(DecodingKey::from_secret(hmac_secret.as_bytes()))
        };
        let rsa = match rsa_pem {
            Some(p) => Some(DecodingKey::from_rsa_pem(p).map_err(|_| VerifyError::Invalid)?),
            None => None,
        };
        let ecdsa = match ecdsa_pem {
            Some(p) => Some(DecodingKey::from_ec_pem(p).map_err(|_| VerifyError::Invalid)?),
            None => None,
        };
        Ok(TokenVerifier { hmac, rsa, ecdsa })
    }

    /// Convenience: HMAC-only verifier.
    pub fn hmac(secret: &str) -> Self {
        TokenVerifier::new(secret, None, None).expect("hmac-only verifier")
    }

    /// Whether any verification key is configured.
    pub fn is_configured(&self) -> bool {
        self.hmac.is_some() || self.rsa.is_some() || self.ecdsa.is_some()
    }

    fn key_for(&self, alg: Algorithm) -> Option<&DecodingKey> {
        match alg {
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => self.hmac.as_ref(),
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => self.rsa.as_ref(),
            Algorithm::ES256 | Algorithm::ES384 => self.ecdsa.as_ref(),
            // ES512 (P-521) is not supported by jsonwebtoken; PS* not used by centrifugo v2.
            _ => None,
        }
    }

    /// Verify signature (key chosen by header alg) and deserialize claims of
    /// type `T`. Parse/signature/disabled-alg failures → `Invalid`. exp/nbf are
    /// NOT checked here (claim-type specific; done by callers).
    fn verify_and_decode<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
    ) -> Result<T, VerifyError> {
        let header = decode_header(token).map_err(|_| VerifyError::Invalid)?;
        let key = self.key_for(header.alg).ok_or(VerifyError::Invalid)?;
        let mut validation = Validation::new(header.alg);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        Ok(decode::<T>(token, key, &validation)
            .map_err(|_| VerifyError::Invalid)?
            .claims)
    }

    /// Verify a connection token. Signature/parse/disabled-alg failures →
    /// `Invalid`; failed exp/nbf checks → `Expired`.
    pub fn verify_connect_token(&self, token: &str) -> Result<ConnectToken, VerifyError> {
        let claims: ConnectTokenClaims = self.verify_and_decode(token)?;
        check_expiry(claims.exp, claims.nbf)?;
        Ok(ConnectToken {
            user: claims.sub.unwrap_or_default(),
            info: resolve_info(claims.b64info, claims.info)?,
            channels: claims.channels.unwrap_or_default(),
            expire_at: claims.exp.unwrap_or(0),
        })
    }

    /// Verify a subscription token (for private/`$`-prefixed channels).
    pub fn verify_subscribe_token(&self, token: &str) -> Result<SubscribeToken, VerifyError> {
        let claims: SubscribeTokenClaims = self.verify_and_decode(token)?;
        check_expiry(claims.exp, claims.nbf)?;
        Ok(SubscribeToken {
            client: claims.client.unwrap_or_default(),
            channel: claims.channel.unwrap_or_default(),
            info: resolve_info(claims.b64info, claims.info)?,
            expire_at: claims.exp.unwrap_or(0),
            expire_token_only: claims.expire_token_only,
        })
    }
}

/// exp/nbf validity (matches Go's ErrTokenExpired path; absent claims pass).
fn check_expiry(exp: Option<i64>, nbf: Option<i64>) -> Result<(), VerifyError> {
    let now = now_unix();
    if let Some(exp) = exp {
        if now >= exp {
            return Err(VerifyError::Expired);
        }
    }
    if let Some(nbf) = nbf {
        if now < nbf {
            return Err(VerifyError::Expired);
        }
    }
    Ok(())
}

/// `b64info` (base64) overrides `info` (inline JSON) when present.
fn resolve_info(
    b64info: Option<String>,
    info: Option<Box<serde_json::value::RawValue>>,
) -> Result<Option<Vec<u8>>, VerifyError> {
    match b64info {
        Some(ref b) if !b.is_empty() => Ok(Some(
            base64::engine::general_purpose::STANDARD
                .decode(b)
                .map_err(|_| VerifyError::Invalid)?,
        )),
        _ => Ok(info.map(|r| r.get().as_bytes().to_vec())),
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    fn sign(claims: serde_json::Value, secret: &str) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn valid_hs256_yields_user() {
        let token = sign(json!({"sub": "user42"}), "secret");
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.user, "user42");
        assert_eq!(ct.expire_at, 0);
        assert!(ct.info.is_none());
    }

    #[test]
    fn expired_token_is_expired() {
        let token = sign(json!({"sub": "u", "exp": 100}), "secret");
        assert_eq!(
            TokenVerifier::hmac("secret").verify_connect_token(&token),
            Err(VerifyError::Expired)
        );
    }

    #[test]
    fn future_nbf_is_expired() {
        let token = sign(json!({"sub": "u", "nbf": now_unix() + 10_000}), "secret");
        assert_eq!(
            TokenVerifier::hmac("secret").verify_connect_token(&token),
            Err(VerifyError::Expired)
        );
    }

    #[test]
    fn bad_signature_is_invalid() {
        let token = sign(json!({"sub": "u"}), "wrong-secret");
        assert_eq!(
            TokenVerifier::hmac("secret").verify_connect_token(&token),
            Err(VerifyError::Invalid)
        );
    }

    #[test]
    fn disabled_algorithm_is_invalid() {
        // HS256 token but verifier has no HMAC key configured.
        let token = sign(json!({"sub": "u"}), "secret");
        let verifier = TokenVerifier::new("", None, None).unwrap();
        assert_eq!(
            verifier.verify_connect_token(&token),
            Err(VerifyError::Invalid)
        );
    }

    #[test]
    fn b64info_overrides_info_and_is_decoded() {
        let b64 = base64::engine::general_purpose::STANDARD.encode(br#"{"a":1}"#);
        let token = sign(
            json!({"sub": "u", "info": {"ignored": true}, "b64info": b64}),
            "secret",
        );
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.info.unwrap(), br#"{"a":1}"#);
    }

    #[test]
    fn info_passed_through_as_raw_json() {
        let token = sign(json!({"sub": "u", "info": {"a": [1, 2]}}), "secret");
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.info.unwrap(), br#"{"a":[1,2]}"#);
    }

    #[test]
    fn valid_token_with_future_exp_carries_expire_at() {
        let exp = now_unix() + 3600;
        let token = sign(json!({"sub": "u", "exp": exp}), "secret");
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.expire_at, exp);
    }
}
