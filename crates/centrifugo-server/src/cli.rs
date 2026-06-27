//! Command-line interface. Mirrors the subset of Centrifugo subcommands needed
//! so far (`serve`, `version`); more land in M11.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "centrifugo", disable_version_flag = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
// The CLI command enum is built once at startup; the Serve/Version size gap is
// irrelevant and boxing fights the clap derive.
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Run the server.
    Serve(ServeArgs),
    /// Generate a connection JWT (HS256) for a user.
    Gentoken(GentokenArgs),
    /// Write a fresh config file with generated secrets.
    Genconfig(ConfigPathArgs),
    /// Validate a config file and exit non-zero on error.
    Checkconfig(ConfigPathArgs),
    /// Print version and exit.
    Version,
}

#[derive(clap::Args, Debug)]
pub struct GentokenArgs {
    /// Config file to read `token_hmac_secret_key` from.
    #[arg(short = 'c', long = "config")]
    pub config: Option<String>,
    /// Subject (user id) for the token.
    #[arg(short = 'u', long = "user", default_value = "")]
    pub user: String,
    /// Token TTL in seconds (0 = no expiry).
    #[arg(long = "ttl", default_value_t = 0)]
    pub ttl: u64,
    /// HMAC secret (overrides the config file).
    #[arg(long = "token_hmac_secret_key", default_value = "")]
    pub token_hmac_secret_key: String,
}

#[derive(clap::Args, Debug)]
pub struct ConfigPathArgs {
    /// Config file path.
    #[arg(short = 'c', long = "config", default_value = "config.json")]
    pub config: String,
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Path to a JSON config file (required to define namespaces).
    #[arg(short = 'c', long = "config")]
    pub config: Option<String>,
    #[arg(long, default_value = "127.0.0.1")]
    pub address: String,
    #[arg(long, default_value_t = 8000)]
    pub port: u16,
    /// Allow connections without a token (anonymous), assigning a fresh client id.
    #[arg(long = "client_insecure")]
    pub client_insecure: bool,
    /// Allow tokenless connections with an empty user id (Go `client_anonymous`).
    #[arg(long = "client_anonymous")]
    pub client_anonymous: bool,
    /// HMAC secret for HS256/384/512 connection tokens.
    #[arg(long = "token_hmac_secret_key", default_value = "")]
    pub token_hmac_secret_key: String,
    /// Path to a PEM RSA public key for RS256/384/512 tokens.
    #[arg(long = "token_rsa_public_key", default_value = "")]
    pub token_rsa_public_key: String,
    /// Path to a PEM ECDSA public key for ES256/384 tokens.
    #[arg(long = "token_ecdsa_public_key", default_value = "")]
    pub token_ecdsa_public_key: String,
    /// JWKS endpoint URL; keys are fetched and matched by token `kid`.
    #[arg(long = "token_jwks_public_endpoint", default_value = "")]
    pub token_jwks_public_endpoint: String,
    /// Allow clients to publish to channels (default namespace).
    #[arg(long = "publish")]
    pub publish: bool,
    /// Require publishers to be subscribed (default namespace).
    #[arg(long = "subscribe_to_publish")]
    pub subscribe_to_publish: bool,
    /// Enable presence on all channels.
    #[arg(long = "presence")]
    pub presence: bool,
    /// Enable join/leave pushes on all channels.
    #[arg(long = "join_leave")]
    pub join_leave: bool,
    /// Disable client-side presence commands even when presence is enabled.
    #[arg(long = "presence_disable_for_client")]
    pub presence_disable_for_client: bool,
    /// How often (seconds) a connection re-asserts its presence.
    #[arg(long = "client_presence_ping_interval", default_value_t = 25)]
    pub client_presence_ping_interval: u64,
    /// Presence entry TTL in seconds (Redis engine; memory ignores it).
    #[arg(long = "client_presence_expire_interval", default_value_t = 60)]
    pub client_presence_expire_interval: u64,
    /// Max publications kept in channel history (0 disables history).
    #[arg(long = "history_size", default_value_t = 0)]
    pub history_size: usize,
    /// History retention in seconds (0 disables history).
    #[arg(long = "history_lifetime", default_value_t = 0)]
    pub history_lifetime: u64,
    /// Offer (re)subscribe recovery on channels.
    #[arg(long = "history_recover")]
    pub history_recover: bool,
    /// Server HTTP API key for apikey auth.
    #[arg(long = "api_key", default_value = "")]
    pub api_key: String,
    /// Disable HTTP API auth.
    #[arg(long = "api_insecure")]
    pub api_insecure: bool,
    /// Enable the gRPC server API.
    #[arg(long = "grpc_api")]
    pub grpc_api: bool,
    /// TCP port for the gRPC server API.
    #[arg(long = "grpc_api_port", default_value_t = 10000)]
    pub grpc_api_port: u16,
    /// API key required in `authorization: apikey <KEY>` gRPC metadata (empty = open).
    #[arg(long = "grpc_api_key", default_value = "")]
    pub grpc_api_key: String,
    /// Connect-proxy endpoint URL; when set, CONNECT is authenticated via this
    /// HTTP callback instead of a JWT.
    #[arg(long = "proxy_connect_endpoint", default_value = "")]
    pub proxy_connect_endpoint: String,
    /// Refresh-proxy endpoint URL (connection refresh via HTTP callback).
    #[arg(long = "proxy_refresh_endpoint", default_value = "")]
    pub proxy_refresh_endpoint: String,
    /// Subscribe-proxy endpoint URL (authorize SUBSCRIBE on proxy_subscribe channels).
    #[arg(long = "proxy_subscribe_endpoint", default_value = "")]
    pub proxy_subscribe_endpoint: String,
    /// Publish-proxy endpoint URL (authorize/transform PUBLISH on proxy_publish channels).
    #[arg(long = "proxy_publish_endpoint", default_value = "")]
    pub proxy_publish_endpoint: String,
    /// RPC-proxy endpoint URL (handle client RPC via HTTP callback).
    #[arg(long = "proxy_rpc_endpoint", default_value = "")]
    pub proxy_rpc_endpoint: String,
    /// Enable the admin endpoints.
    #[arg(long = "admin")]
    pub admin: bool,
    /// Skip admin auth entirely (Go `admin_insecure`): `/admin/auth` returns the
    /// `insecure` token and `/admin/api` needs no token.
    #[arg(long = "admin_insecure")]
    pub admin_insecure: bool,
    /// Admin password (for `POST /admin/auth`).
    #[arg(long = "admin_password", default_value = "")]
    pub admin_password: String,
    /// Admin session token secret.
    #[arg(long = "admin_secret", default_value = "")]
    pub admin_secret: String,
    /// Serve the admin web UI from this directory (empty = embedded bundle).
    #[arg(long = "admin_web_path", default_value = "")]
    pub admin_web_path: String,
    /// Engine backing pub/sub, history and presence: `memory` or `redis`.
    #[arg(long = "engine", default_value = "memory")]
    pub engine: String,
    /// Redis address (`host:port` or a full `redis://` URL) when `engine=redis`.
    #[arg(long = "redis_address", default_value = "127.0.0.1:6379")]
    pub redis_address: String,
    /// Redis Sentinel master name; when set (with `redis_sentinels`), the master
    /// is discovered via Sentinel instead of using `redis_address`.
    #[arg(long = "redis_master_name", default_value = "")]
    pub redis_master_name: String,
    /// Comma-separated Redis Sentinel addresses (`host:port`).
    #[arg(long = "redis_sentinels", default_value = "")]
    pub redis_sentinels: String,
}
