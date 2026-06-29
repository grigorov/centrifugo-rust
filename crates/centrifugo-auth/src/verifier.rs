//! `TokenVerifier`: verifies a JWT using the key matching its header algorithm,
//! then applies Centrifugo's claim semantics.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, PublicKeyUse};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};

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
    /// JWKS keys by `kid`, refreshable at runtime (background fetch). A token
    /// carrying a `kid` present here is verified with the matching key,
    /// regardless of the static PEM keys.
    jwks: Arc<RwLock<HashMap<String, DecodingKey>>>,
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
        Ok(TokenVerifier {
            hmac,
            rsa,
            ecdsa,
            jwks: Arc::default(),
        })
    }

    /// Convenience: HMAC-only verifier.
    pub fn hmac(secret: &str) -> Self {
        TokenVerifier::new(secret, None, None).expect("hmac-only verifier")
    }

    /// Whether any verification key is configured (static PEM/HMAC or JWKS).
    pub fn is_configured(&self) -> bool {
        self.hmac.is_some()
            || self.rsa.is_some()
            || self.ecdsa.is_some()
            || !self.jwks.read().unwrap().is_empty()
    }

    /// Replace the JWKS key set (called by the server's background refresh task
    /// after fetching `token_jwks_public_endpoint`). Returns the number of keys
    /// loaded. Keys without a `kid` are skipped (they cannot be matched).
    pub fn set_jwks(&self, set: &JwkSet) -> usize {
        let mut map = HashMap::new();
        for jwk in &set.keys {
            // Centrifugo's JWKS path is RSA-only: jwksManager.verify rejects any
            // key whose Kty != "RSA" (token_verifier_jwt.go), and the manager
            // skips keys whose `use` is not "sig". Mirror both — load only RSA
            // keys, and drop keys explicitly tagged for a non-signature use.
            if !matches!(jwk.algorithm, AlgorithmParameters::RSA(_)) {
                continue;
            }
            if matches!(
                jwk.common.public_key_use,
                Some(PublicKeyUse::Encryption) | Some(PublicKeyUse::Other(_))
            ) {
                continue;
            }
            if let Some(kid) = jwk.common.key_id.clone() {
                if let Ok(key) = DecodingKey::from_jwk(jwk) {
                    map.insert(kid, key);
                }
            }
        }
        let n = map.len();
        *self.jwks.write().unwrap() = map;
        n
    }

    /// Parse a JWKS JSON document and install it. Lets the server load
    /// `token_jwks_public_endpoint` without depending on `jsonwebtoken` directly.
    pub fn set_jwks_from_json(&self, json: &str) -> Result<usize, VerifyError> {
        let set: JwkSet = serde_json::from_str(json).map_err(|_| VerifyError::Invalid)?;
        Ok(self.set_jwks(&set))
    }

    /// Pick the verification key: a `kid` matching the JWKS set wins; otherwise
    /// fall back to the static key for the header algorithm.
    fn select_key(&self, header: &Header) -> Option<DecodingKey> {
        if let Some(kid) = &header.kid {
            if let Some(key) = self.jwks.read().unwrap().get(kid) {
                return Some(key.clone());
            }
        }
        self.key_for(header.alg).cloned()
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
        let key = self.select_key(&header).ok_or(VerifyError::Invalid)?;
        let mut validation = Validation::new(header.alg);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        Ok(decode::<T>(token, &key, &validation)
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
            expire_at: expire_at_secs(claims.exp),
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
            expire_at: expire_at_secs(claims.exp),
            expire_token_only: claims.expire_token_only,
        })
    }
}

/// Generate an HS256 connection token for `user` (used by the `gentoken` CLI).
/// `ttl_secs == 0` omits `exp` (a non-expiring token).
pub fn gen_connect_token(
    hmac_secret: &str,
    user: &str,
    ttl_secs: u64,
) -> Result<String, VerifyError> {
    #[derive(serde::Serialize)]
    struct Claims<'a> {
        sub: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        exp: Option<i64>,
    }
    let exp = (ttl_secs > 0).then(|| now_unix() + ttl_secs as i64);
    encode(
        &Header::new(Algorithm::HS256),
        &Claims { sub: user, exp },
        &EncodingKey::from_secret(hmac_secret.as_bytes()),
    )
    .map_err(|_| VerifyError::Invalid)
}

/// Sign an admin session token (HS256 over `admin_secret`), returned by
/// `POST /admin/auth` after a correct password.
pub fn gen_admin_token(admin_secret: &str) -> Result<String, VerifyError> {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"admin": true}),
        &EncodingKey::from_secret(admin_secret.as_bytes()),
    )
    .map_err(|_| VerifyError::Invalid)
}

/// Verify an admin session token against `admin_secret`.
pub fn verify_admin_token(admin_secret: &str, token: &str) -> bool {
    if admin_secret.is_empty() {
        return false;
    }
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    validation.required_spec_claims.clear();
    decode::<serde_json::Value>(
        token,
        &DecodingKey::from_secret(admin_secret.as_bytes()),
        &validation,
    )
    .is_ok()
}

/// exp/nbf validity (matches Go's ErrTokenExpired path; absent claims pass).
/// exp/nbf are floats (NumericDate may be fractional) compared at full precision
/// against the current second.
fn check_expiry(exp: Option<f64>, nbf: Option<f64>) -> Result<(), VerifyError> {
    let now = now_unix() as f64;
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

/// Floor a fractional NumericDate to whole seconds for the reported `expire_at`
/// (matches Go's `NumericDate.Unix()`); absent → 0.
fn expire_at_secs(exp: Option<f64>) -> i64 {
    exp.map(|e| e.floor() as i64).unwrap_or(0)
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
    fn fractional_exp_is_accepted_and_floored() {
        // H3: RFC-7519 allows fractional NumericDate; Go accepts it. expire_at is
        // floored to whole seconds (NumericDate.Unix()).
        let exp = now_unix() + 10_000;
        let token = sign(json!({"sub": "u", "exp": exp as f64 + 0.5}), "secret");
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.expire_at, exp);
    }

    #[test]
    fn expired_fractional_exp_is_expired_not_invalid() {
        // H3: an expired fractional token must classify as Expired (→ error 109 /
        // refresh), never Invalid (→ 3002 no-reconnect close).
        let token = sign(
            json!({"sub": "u", "exp": (now_unix() - 100) as f64 + 0.5}),
            "secret",
        );
        assert_eq!(
            TokenVerifier::hmac("secret").verify_connect_token(&token),
            Err(VerifyError::Expired)
        );
    }

    #[test]
    fn string_exp_is_accepted() {
        // H3: numeric-string NumericDate is accepted (Go parses via json.Number).
        let exp = now_unix() + 10_000;
        let token = sign(json!({"sub": "u", "exp": exp.to_string()}), "secret");
        let ct = TokenVerifier::hmac("secret")
            .verify_connect_token(&token)
            .unwrap();
        assert_eq!(ct.expire_at, exp);
    }

    #[test]
    fn fractional_future_nbf_is_expired() {
        let token = sign(
            json!({"sub": "u", "nbf": (now_unix() + 10_000) as f64 + 0.5}),
            "secret",
        );
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

    // A 2048-bit RSA test keypair: the private key (PKCS#8 PEM) signs tokens, and
    // the JWKS below publishes its public modulus. Centrifugo's JWKS path is
    // RSA-only, so the tests use a real RSA key rather than a symmetric `oct` JWK.
    const RSA_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/nEiIHJm+3VBp
QJu0zv3y7FZ+2gzqo6DVW9njmTpYDVFPQHH7Wa0baiZDlZCYceeFe1KNQ1FkJiBX
GNgb3fFMXUkcqnFRqWlFplYKUxePXZOdi/GlBbqC99dIsnfeOOPYTy0+axCA5s2N
SOSROw1kNsifElPz154txfqADf+ByHpNW4ggOjUXQkfd3J1NW4B0xDOHExZ6gYcx
hVaBucJR0xNzTTDdWQGqnPdFPwWWp6YK77mxwV7pIWUCIukVoLmv2rWJ2KeihMze
HJ0zWJbNJhABfEdy8Rbq/9HUhRq+t1q7O6bBSFrrcZg6dHARiYc9ckQqDWuRYI0r
V/HfTiEJAgMBAAECggEAAfb9EbbWXXWyqVBvtrUzkP1BeIDgpnrM6RP6Kkcg1dr1
tGkp8HMa1YsOlaPH24WGfRDjl7FJWT+TvKtNH846sLoy20CeeeeE13Se6v5Vx/y+
CRZqNDNrwx198QZFRaUO2Nt1/UiW688O3xXwlxJaxXls1pNXHLF4/+/o5ppOJMAb
X6v7kmpbEZU7JcAv0aZx/kpVVoCZV79WWfgBCicBv6mml5NawoPBG/9d8PvAtAst
8GYwFP2JlQorJa6lk1uKLhu2LxsNSDtIw3+jrLYssEpkJFRxu2glhdpMtJE/X0/R
AXssACqCWnqgqJuKX9Co3Iq2vbl1qr/6SiOHv/ARAQKBgQD5hoodKISvWUL1ZRjO
k8R7ClSrFVNQDxbPzkRvQodLdxAFyYjn4z+EDwfmSnHnhLdZbAZhbk+Vyoigsif7
9RD+8p+fVW8C5TFeg6q/8Ij9LjUMBR5lhB7RhHClfa7nLyM0AJxeUuQJ+1Xm3k9b
uvLmWJ4HWbQUiZXtJ6hs+anVAQKBgQDElQvHq2zYcEurbYEu9uWBpSqLOJqWfUpb
KQZoM5bhElwt3vdOydkt240Dg21Omlv1giGDA1Wuzi4vcQTzlRqHawSYsrL1r4oK
VcISTma6SkHPSNX4AgJ+/62nmrWT9pa01sNTnclOE2s6V2l5awW4EFni/juSqaUH
y3OaFuGkCQKBgQCjDzt0QIUsvW0XRcCHRmMwcJjR0DbIa4Phuo5YEqatNxoeXgv8
VTGtj9D+ugljXQQgCIrG4rpZTagpMyMT8JrxsAWFruPDhZjUhcBwe7RZlveNak7p
0gP9sMmYK+C/LLuZgQiuTwa8SyVgoEhFzo5q3uAuN32JqjtyZecXh7NnAQKBgFrd
ng1UQsKk3YVG36CqxRkxFEI4DtSi4zzR8ME3n3U3vF4DowLLMFUPF9ZY6KydkwYf
eYgKgY+EhDqvnh9Ne26+2+gNKcWAt2jhjQxTKw7PBi5fN3Ak1ayIWGeRjn7vS2gZ
oT3EQGmTdkwIXZufCYy0GihfZX/8ZGj+9Ndz3iapAoGAVMhJJZ9NFnQg8FnTWVMz
32lrQ7ySHBZkWb4hLh+x3wLNNy32hHlxgSHwcD/euZGyqsPh7gHbF8JYMkiwAhVx
ueHGIivqCFfLTIiPy+cHUusq4lwDbpNg+ELrDuqQXAOiUyjlBUscR0X+VdSAHuSU
WGE/+8RJi42fdcJbdeguCLA=
-----END PRIVATE KEY-----";
    const RSA_N: &str = "v5xIiByZvt1QaUCbtM798uxWftoM6qOg1VvZ45k6WA1RT0Bx-1mtG2omQ5WQmHHnhXtSjUNRZCYgVxjYG93xTF1JHKpxUalpRaZWClMXj12TnYvxpQW6gvfXSLJ33jjj2E8tPmsQgObNjUjkkTsNZDbInxJT89eeLcX6gA3_gch6TVuIIDo1F0JH3dydTVuAdMQzhxMWeoGHMYVWgbnCUdMTc00w3VkBqpz3RT8FlqemCu-5scFe6SFlAiLpFaC5r9q1idinooTM3hydM1iWzSYQAXxHcvEW6v_R1IUavrdauzumwUha63GYOnRwEYmHPXJEKg1rkWCNK1fx304hCQ";

    fn rsa_sig_jwks(kid: &str) -> JwkSet {
        let doc = format!(
            r#"{{"keys":[{{"kty":"RSA","use":"sig","alg":"RS256","kid":"{kid}","n":"{RSA_N}","e":"AQAB"}}]}}"#
        );
        serde_json::from_str(&doc).unwrap()
    }

    fn rsa_kid_token(kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.into());
        encode(
            &header,
            &json!({"sub": "jwks-user"}),
            &EncodingKey::from_rsa_pem(RSA_PRIV_PEM.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn jwks_verifies_rsa_token_by_kid() {
        let verifier = TokenVerifier::new("", None, None).unwrap();
        assert_eq!(verifier.set_jwks(&rsa_sig_jwks("key1")), 1);
        assert!(verifier.is_configured());
        let ct = verifier
            .verify_connect_token(&rsa_kid_token("key1"))
            .unwrap();
        assert_eq!(ct.user, "jwks-user");
    }

    #[test]
    fn jwks_skips_non_rsa_and_enc_keys() {
        // L4: Centrifugo's JWKS is RSA-only and signature-only. A symmetric `oct`
        // key and an RSA key tagged use:enc are both skipped (Go rejects them).
        let secret = b"jwks-shared-secret";
        let k = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        let oct = format!(r#"{{"keys":[{{"kty":"oct","kid":"o1","k":"{k}","alg":"HS256"}}]}}"#);
        let enc = format!(
            r#"{{"keys":[{{"kty":"RSA","use":"enc","kid":"e1","n":"{RSA_N}","e":"AQAB"}}]}}"#
        );
        let verifier = TokenVerifier::new("", None, None).unwrap();
        assert_eq!(
            verifier.set_jwks(&serde_json::from_str(&oct).unwrap()),
            0,
            "oct (non-RSA) key must be skipped"
        );
        assert_eq!(
            verifier.set_jwks(&serde_json::from_str(&enc).unwrap()),
            0,
            "RSA use:enc key must be skipped"
        );
    }

    #[test]
    fn jwks_unknown_kid_is_invalid() {
        let verifier = TokenVerifier::new("", None, None).unwrap();
        verifier.set_jwks(&rsa_sig_jwks("key1"));
        // A token whose kid is not in the set has no key -> Invalid.
        assert_eq!(
            verifier.verify_connect_token(&rsa_kid_token("other")),
            Err(VerifyError::Invalid)
        );
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
