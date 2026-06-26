//! Minimal runtime config for M1. Grows into the full layered config (file +
//! env + flags) in M11.

#[derive(Debug, Clone)]
pub struct Config {
    pub address: String,
    pub port: u16,
    /// Allow anonymous connections (no token). M1 always behaves as insecure;
    /// this gates the JWT requirement once auth lands in M3.
    #[allow(dead_code)]
    pub client_insecure: bool,
}

impl Config {
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.address, self.port)
            .parse()
            .expect("valid socket address")
    }
}
