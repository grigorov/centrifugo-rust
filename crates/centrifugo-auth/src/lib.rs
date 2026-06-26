//! JWT token verification for Centrifugo connection (and, later, subscription)
//! tokens. Mirrors `internal/jwtverify/token_verifier_jwt.go` of centrifugo
//! v2.8.6: algorithm selected from the JWT header, HMAC/RSA/ECDSA, manual
//! exp/nbf checks (so expiry maps to a distinct error), `b64info` overriding
//! `info`.

pub mod claims;
pub mod error;
pub mod verifier;

pub use claims::{ConnectTokenClaims, SubscribeTokenClaims};
pub use error::VerifyError;
pub use verifier::{ConnectToken, TokenVerifier};
