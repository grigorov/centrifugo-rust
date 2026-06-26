//! Disconnect advice (centrifuge v0.14.2 `disconnect.go`). Not a protocol
//! message — it is delivered in the WebSocket/SockJS close frame: the `code`
//! is the close code, and `close_text()` (`{"reason":..,"reconnect":..}`) is
//! the close reason text (must be < 127 bytes).

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    pub code: u32,
    pub reason: String,
    pub reconnect: bool,
}

#[derive(Serialize)]
struct CloseText<'a> {
    reason: &'a str,
    reconnect: bool,
}

impl Disconnect {
    pub fn new(code: u32, reason: impl Into<String>, reconnect: bool) -> Self {
        Disconnect {
            code,
            reason: reason.into(),
            reconnect,
        }
    }

    /// The JSON close-reason text: `{"reason":"...","reconnect":true|false}`.
    pub fn close_text(&self) -> String {
        let t = CloseText {
            reason: &self.reason,
            reconnect: self.reconnect,
        };
        let s = serde_json::to_string(&t).expect("disconnect close text");
        debug_assert!(s.len() < 127, "disconnect close text must be < 127 bytes");
        s
    }

    pub fn normal() -> Self {
        Disconnect::new(3000, "normal", true)
    }
    pub fn shutdown() -> Self {
        Disconnect::new(3001, "shutdown", true)
    }
    pub fn invalid_token() -> Self {
        Disconnect::new(3002, "invalid token", false)
    }
    pub fn bad_request() -> Self {
        Disconnect::new(3003, "bad request", false)
    }
    pub fn server_error() -> Self {
        Disconnect::new(3004, "internal server error", true)
    }
    pub fn expired() -> Self {
        Disconnect::new(3005, "expired", true)
    }
    pub fn sub_expired() -> Self {
        Disconnect::new(3006, "subscription expired", true)
    }
    pub fn stale() -> Self {
        Disconnect::new(3007, "stale", false)
    }
    pub fn slow() -> Self {
        Disconnect::new(3008, "slow", true)
    }
    pub fn write_error() -> Self {
        Disconnect::new(3009, "write error", true)
    }
    pub fn insufficient_state() -> Self {
        Disconnect::new(3010, "insufficient state", true)
    }
    pub fn force_reconnect() -> Self {
        Disconnect::new(3011, "force reconnect", true)
    }
    pub fn force_no_reconnect() -> Self {
        Disconnect::new(3012, "force disconnect", false)
    }
    pub fn connection_limit() -> Self {
        Disconnect::new(3013, "connection limit", false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect_slow_close_text() {
        let d = Disconnect::slow();
        assert_eq!(d.code, 3008);
        assert!(d.reconnect);
        assert_eq!(d.close_text(), r#"{"reason":"slow","reconnect":true}"#);
    }

    #[test]
    fn disconnect_invalid_token_no_reconnect() {
        let d = Disconnect::invalid_token();
        assert_eq!(d.code, 3002);
        assert!(!d.reconnect);
        assert_eq!(
            d.close_text(),
            r#"{"reason":"invalid token","reconnect":false}"#
        );
    }

    #[test]
    fn all_codes_present() {
        let all = [
            Disconnect::normal(),
            Disconnect::shutdown(),
            Disconnect::invalid_token(),
            Disconnect::bad_request(),
            Disconnect::server_error(),
            Disconnect::expired(),
            Disconnect::sub_expired(),
            Disconnect::stale(),
            Disconnect::slow(),
            Disconnect::write_error(),
            Disconnect::insufficient_state(),
            Disconnect::force_reconnect(),
            Disconnect::force_no_reconnect(),
            Disconnect::connection_limit(),
        ];
        let codes: Vec<u32> = all.iter().map(|d| d.code).collect();
        assert_eq!(codes, (3000..=3013).collect::<Vec<_>>());
    }
}
