//! M3.2: RSA (RS256) and ECDSA (ES256) connect-token verification, plus the
//! disabled-algorithm path (a token whose alg has no configured key → Invalid).
//! Keys in `tests/fixtures/` are throwaway test keypairs.

use centrifugo_auth::{TokenVerifier, VerifyError};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;

const RSA_PRIV: &str = include_str!("fixtures/rsa_priv.pem");
const RSA_PUB: &str = include_str!("fixtures/rsa_pub.pem");
const EC_PRIV: &str = include_str!("fixtures/ec_priv.pem");
const EC_PUB: &str = include_str!("fixtures/ec_pub.pem");

fn sign(alg: Algorithm, key: &EncodingKey, claims: serde_json::Value) -> String {
    encode(&Header::new(alg), &claims, key).unwrap()
}

fn rsa_verifier() -> TokenVerifier {
    TokenVerifier::new("", Some(RSA_PUB.as_bytes()), None).unwrap()
}
fn ec_verifier() -> TokenVerifier {
    TokenVerifier::new("", None, Some(EC_PUB.as_bytes())).unwrap()
}

#[test]
fn rs256_valid() {
    let key = EncodingKey::from_rsa_pem(RSA_PRIV.as_bytes()).unwrap();
    let token = sign(Algorithm::RS256, &key, json!({"sub": "rsa-user"}));
    let ct = rsa_verifier().verify_connect_token(&token).unwrap();
    assert_eq!(ct.user, "rsa-user");
}

#[test]
fn es256_valid() {
    let key = EncodingKey::from_ec_pem(EC_PRIV.as_bytes()).unwrap();
    let token = sign(Algorithm::ES256, &key, json!({"sub": "ec-user"}));
    let ct = ec_verifier().verify_connect_token(&token).unwrap();
    assert_eq!(ct.user, "ec-user");
}

#[test]
fn rs256_expired() {
    let key = EncodingKey::from_rsa_pem(RSA_PRIV.as_bytes()).unwrap();
    let token = sign(Algorithm::RS256, &key, json!({"sub": "u", "exp": 100}));
    assert_eq!(
        rsa_verifier().verify_connect_token(&token),
        Err(VerifyError::Expired)
    );
}

#[test]
fn rs256_token_against_ecdsa_only_verifier_is_disabled() {
    // RS256 token, but only an ECDSA key is configured → no RSA key → Invalid.
    let key = EncodingKey::from_rsa_pem(RSA_PRIV.as_bytes()).unwrap();
    let token = sign(Algorithm::RS256, &key, json!({"sub": "u"}));
    assert_eq!(
        ec_verifier().verify_connect_token(&token),
        Err(VerifyError::Invalid)
    );
}

#[test]
fn tampered_rs256_signature_is_invalid() {
    let key = EncodingKey::from_rsa_pem(RSA_PRIV.as_bytes()).unwrap();
    let mut token = sign(Algorithm::RS256, &key, json!({"sub": "u"}));
    // Flip the last base64 char of the signature.
    let last = token.pop().unwrap();
    token.push(if last == 'A' { 'B' } else { 'A' });
    assert_eq!(
        rsa_verifier().verify_connect_token(&token),
        Err(VerifyError::Invalid)
    );
}
