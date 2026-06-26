/// Connection/subscription token verification outcome on failure. Matches the
/// two paths Go centrifugo distinguishes in the connect flow: an expired token
/// (→ `ErrorTokenExpired`, 109) vs any other failure (→ `DisconnectInvalidToken`,
/// 3002).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("token expired")]
    Expired,
    #[error("invalid token")]
    Invalid,
}
