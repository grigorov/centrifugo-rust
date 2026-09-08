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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_error_display_expired() {
        let err = VerifyError::Expired;
        assert_eq!(err.to_string(), "token expired");
    }

    #[test]
    fn verify_error_display_invalid() {
        let err = VerifyError::Invalid;
        assert_eq!(err.to_string(), "invalid token");
    }

    #[test]
    fn verify_error_debug_expired() {
        let err = VerifyError::Expired;
        assert_eq!(format!("{:?}", err), "Expired");
    }

    #[test]
    fn verify_error_debug_invalid() {
        let err = VerifyError::Invalid;
        assert_eq!(format!("{:?}", err), "Invalid");
    }

    #[test]
    fn verify_error_clone() {
        let err = VerifyError::Expired;
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn verify_error_eq() {
        assert_eq!(VerifyError::Expired, VerifyError::Expired);
        assert_eq!(VerifyError::Invalid, VerifyError::Invalid);
        assert_ne!(VerifyError::Expired, VerifyError::Invalid);
    }
}
