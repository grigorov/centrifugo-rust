//! Minimal runtime config for M1. Grows into the full layered config (file +
//! env + flags) in M11.

#[derive(Debug, Clone)]
pub struct Config {
    pub address: String,
    pub port: u16,
    /// Allow anonymous connections (no token); skips JWT verification.
    pub client_insecure: bool,
    /// HMAC secret for HS256/384/512 connect tokens (empty = HMAC disabled).
    pub token_hmac_secret_key: String,
    /// Path to a PEM RSA public key for RS256/384/512 (empty = disabled).
    pub token_rsa_public_key: String,
    /// Path to a PEM ECDSA public key for ES256/384 (empty = disabled).
    pub token_ecdsa_public_key: String,
    /// Enable presence on all channels (default namespace option).
    pub presence: bool,
    /// Enable join/leave pushes on all channels.
    pub join_leave: bool,
    /// Disable the client-side PRESENCE/PRESENCE_STATS commands even if presence
    /// is enabled.
    pub presence_disable_for_client: bool,
}

impl Config {
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.address, self.port)
            .parse()
            .expect("valid socket address")
    }
}
