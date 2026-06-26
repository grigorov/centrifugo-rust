//! Client protocol errors (`Error{code,message}`), codes 100..=111, with the
//! exact messages from centrifuge v0.14.2 `errors.go`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub code: u32,
    pub message: String,
}

impl Error {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Error {
            code,
            message: message.into(),
        }
    }

    pub fn internal() -> Self {
        Error::new(100, "internal server error")
    }
    pub fn unauthorized() -> Self {
        Error::new(101, "unauthorized")
    }
    pub fn unknown_channel() -> Self {
        Error::new(102, "unknown channel")
    }
    pub fn permission_denied() -> Self {
        Error::new(103, "permission denied")
    }
    pub fn method_not_found() -> Self {
        Error::new(104, "method not found")
    }
    pub fn already_subscribed() -> Self {
        Error::new(105, "already subscribed")
    }
    pub fn limit_exceeded() -> Self {
        Error::new(106, "limit exceeded")
    }
    pub fn bad_request() -> Self {
        Error::new(107, "bad request")
    }
    pub fn not_available() -> Self {
        Error::new(108, "not available")
    }
    pub fn token_expired() -> Self {
        Error::new(109, "token expired")
    }
    pub fn expired() -> Self {
        Error::new(110, "expired")
    }
    pub fn too_many_requests() -> Self {
        Error::new(111, "too many requests")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_and_messages() {
        assert_eq!(
            serde_json::to_string(&Error::unknown_channel()).unwrap(),
            r#"{"code":102,"message":"unknown channel"}"#
        );
        assert_eq!(
            serde_json::to_string(&Error::permission_denied()).unwrap(),
            r#"{"code":103,"message":"permission denied"}"#
        );
        assert_eq!(Error::bad_request().code, 107);
        assert_eq!(Error::internal().code, 100);
        assert_eq!(Error::too_many_requests().code, 111);
        assert_eq!(Error::token_expired().message, "token expired");
    }
}
