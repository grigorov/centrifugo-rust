//! Command-line interface. Mirrors the subset of Centrifugo subcommands needed
//! so far (`serve`, `version`); more land in M11.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "centrifugo",
    disable_version_flag = true,
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// Server flags — the bare root command runs the server (matches Go centrifugo,
    /// whose root command is the server). All have defaults, so they don't
    /// interfere with the maintenance subcommands below.
    #[command(flatten)]
    pub serve: ServeArgs,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
// The CLI command enum is built once at startup; the Serve/Version size gap is
// irrelevant and boxing fights the clap derive.
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Run the server (alias; the bare root command does the same).
    #[command(hide = true)]
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

// Every operator flag also reads its `CENTRIFUGO_<UPPER>` env var (clap `env`),
// matching Go centrifugo's viper convention. Boolean env vars are handled in
// `config::Settings::apply_env` (clap's bool+env handling is awkward).
#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Path to a JSON config file (auto-discovers ./config.json when omitted).
    #[arg(short = 'c', long = "config")]
    pub config: Option<String>,
    #[arg(
        short = 'a',
        long = "address",
        env = "CENTRIFUGO_ADDRESS",
        default_value = "127.0.0.1"
    )]
    pub address: String,
    #[arg(
        short = 'p',
        long = "port",
        env = "CENTRIFUGO_PORT",
        default_value_t = 8000
    )]
    pub port: u16,
    /// Node name for display/Info (Go `name`); empty → `hostname_port`.
    #[arg(
        short = 'n',
        long = "name",
        env = "CENTRIFUGO_NAME",
        default_value = ""
    )]
    pub name: String,
    /// Allow connections without a token (anonymous), assigning a fresh client id.
    #[arg(long = "client_insecure")]
    pub client_insecure: bool,
    /// Allow tokenless connections with an empty user id (Go `client_anonymous`).
    #[arg(long = "client_anonymous")]
    pub client_anonymous: bool,
    /// Auto-subscribe non-anonymous clients to their personal channel on connect.
    #[arg(long = "user_subscribe_to_personal")]
    pub user_subscribe_to_personal: bool,
    /// Namespace for the personal channel (empty = top-level `#<user>`).
    #[arg(
        long = "user_personal_channel_namespace",
        env = "CENTRIFUGO_USER_PERSONAL_CHANNEL_NAMESPACE",
        default_value = ""
    )]
    pub user_personal_channel_namespace: String,
    /// HMAC secret for HS256/384/512 connection tokens.
    #[arg(
        long = "token_hmac_secret_key",
        env = "CENTRIFUGO_TOKEN_HMAC_SECRET_KEY",
        default_value = ""
    )]
    pub token_hmac_secret_key: String,
    /// Path to a PEM RSA public key for RS256/384/512 tokens.
    #[arg(
        long = "token_rsa_public_key",
        env = "CENTRIFUGO_TOKEN_RSA_PUBLIC_KEY",
        default_value = ""
    )]
    pub token_rsa_public_key: String,
    /// Path to a PEM ECDSA public key for ES256/384 tokens.
    #[arg(
        long = "token_ecdsa_public_key",
        env = "CENTRIFUGO_TOKEN_ECDSA_PUBLIC_KEY",
        default_value = ""
    )]
    pub token_ecdsa_public_key: String,
    /// JWKS endpoint URL; keys are fetched and matched by token `kid`.
    #[arg(
        long = "token_jwks_public_endpoint",
        env = "CENTRIFUGO_TOKEN_JWKS_PUBLIC_ENDPOINT",
        default_value = ""
    )]
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
    #[arg(
        long = "client_presence_ping_interval",
        env = "CENTRIFUGO_CLIENT_PRESENCE_PING_INTERVAL",
        default_value_t = 25
    )]
    pub client_presence_ping_interval: u64,
    /// Presence entry TTL in seconds (Redis engine; memory ignores it).
    #[arg(
        long = "client_presence_expire_interval",
        env = "CENTRIFUGO_CLIENT_PRESENCE_EXPIRE_INTERVAL",
        default_value_t = 60
    )]
    pub client_presence_expire_interval: u64,
    /// Max publications kept in channel history (0 disables history).
    #[arg(
        long = "history_size",
        env = "CENTRIFUGO_HISTORY_SIZE",
        default_value_t = 0
    )]
    pub history_size: usize,
    /// History retention in seconds (0 disables history).
    #[arg(
        long = "history_lifetime",
        env = "CENTRIFUGO_HISTORY_LIFETIME",
        default_value_t = 0
    )]
    pub history_lifetime: u64,
    /// Offer (re)subscribe recovery on channels.
    #[arg(long = "history_recover")]
    pub history_recover: bool,
    /// Server HTTP API key for apikey auth.
    #[arg(long = "api_key", env = "CENTRIFUGO_API_KEY", default_value = "")]
    pub api_key: String,
    /// Disable HTTP API auth.
    #[arg(long = "api_insecure")]
    pub api_insecure: bool,
    /// Enable the gRPC server API.
    #[arg(long = "grpc_api")]
    pub grpc_api: bool,
    /// TCP port for the gRPC server API.
    #[arg(
        long = "grpc_api_port",
        env = "CENTRIFUGO_GRPC_API_PORT",
        default_value_t = 10000
    )]
    pub grpc_api_port: u16,
    /// API key required in `authorization: apikey <KEY>` gRPC metadata (empty = open).
    #[arg(
        long = "grpc_api_key",
        env = "CENTRIFUGO_GRPC_API_KEY",
        default_value = ""
    )]
    pub grpc_api_key: String,
    /// Connect-proxy endpoint URL; when set, CONNECT is authenticated via this
    /// HTTP callback instead of a JWT.
    #[arg(
        long = "proxy_connect_endpoint",
        env = "CENTRIFUGO_PROXY_CONNECT_ENDPOINT",
        default_value = ""
    )]
    pub proxy_connect_endpoint: String,
    /// Refresh-proxy endpoint URL (connection refresh via HTTP callback).
    #[arg(
        long = "proxy_refresh_endpoint",
        env = "CENTRIFUGO_PROXY_REFRESH_ENDPOINT",
        default_value = ""
    )]
    pub proxy_refresh_endpoint: String,
    /// Subscribe-proxy endpoint URL (authorize SUBSCRIBE on proxy_subscribe channels).
    #[arg(
        long = "proxy_subscribe_endpoint",
        env = "CENTRIFUGO_PROXY_SUBSCRIBE_ENDPOINT",
        default_value = ""
    )]
    pub proxy_subscribe_endpoint: String,
    /// Publish-proxy endpoint URL (authorize/transform PUBLISH on proxy_publish channels).
    #[arg(
        long = "proxy_publish_endpoint",
        env = "CENTRIFUGO_PROXY_PUBLISH_ENDPOINT",
        default_value = ""
    )]
    pub proxy_publish_endpoint: String,
    /// RPC-proxy endpoint URL (handle client RPC via HTTP callback).
    #[arg(
        long = "proxy_rpc_endpoint",
        env = "CENTRIFUGO_PROXY_RPC_ENDPOINT",
        default_value = ""
    )]
    pub proxy_rpc_endpoint: String,
    /// Enable the admin endpoints.
    #[arg(long = "admin")]
    pub admin: bool,
    /// Skip admin auth entirely (Go `admin_insecure`): `/admin/auth` returns the
    /// `insecure` token and `/admin/api` needs no token.
    #[arg(long = "admin_insecure")]
    pub admin_insecure: bool,
    /// Admin password (for `POST /admin/auth`).
    #[arg(
        long = "admin_password",
        env = "CENTRIFUGO_ADMIN_PASSWORD",
        default_value = ""
    )]
    pub admin_password: String,
    /// Admin session token secret.
    #[arg(
        long = "admin_secret",
        env = "CENTRIFUGO_ADMIN_SECRET",
        default_value = ""
    )]
    pub admin_secret: String,
    /// Serve the admin web UI from this directory (empty = embedded bundle).
    #[arg(
        long = "admin_web_path",
        env = "CENTRIFUGO_ADMIN_WEB_PATH",
        default_value = ""
    )]
    pub admin_web_path: String,
    /// Engine backing pub/sub, history and presence: `memory` or `redis`.
    #[arg(
        short = 'e',
        long = "engine",
        env = "CENTRIFUGO_ENGINE",
        default_value = "memory"
    )]
    pub engine: String,
    /// Redis address (`host:port` or a full `redis://` URL) when `engine=redis`.
    #[arg(
        long = "redis_address",
        env = "CENTRIFUGO_REDIS_ADDRESS",
        default_value = "127.0.0.1:6379"
    )]
    pub redis_address: String,
    /// Redis host (Go-compatible alias; combined with `--redis_port` into the address).
    #[arg(long = "redis_host", env = "CENTRIFUGO_REDIS_HOST", default_value = "")]
    pub redis_host: String,
    /// Redis port (Go-compatible alias; combined with `--redis_host`).
    #[arg(long = "redis_port", env = "CENTRIFUGO_REDIS_PORT", default_value = "")]
    pub redis_port: String,
    /// Redis URL (Go-compatible alias `redis://[:pw@]host:port/db`); wins over host/port/address.
    #[arg(long = "redis_url", env = "CENTRIFUGO_REDIS_URL", default_value = "")]
    pub redis_url: String,
    /// Redis Sentinel master name; when set (with `redis_sentinels`), the master
    /// is discovered via Sentinel instead of using `redis_address`.
    #[arg(
        long = "redis_master_name",
        env = "CENTRIFUGO_REDIS_MASTER_NAME",
        default_value = ""
    )]
    pub redis_master_name: String,
    /// Comma-separated Redis Sentinel addresses (`host:port`).
    #[arg(
        long = "redis_sentinels",
        env = "CENTRIFUGO_REDIS_SENTINELS",
        default_value = ""
    )]
    pub redis_sentinels: String,
    /// Redis password (applied on top of `redis_address`; required for AUTH in
    /// Sentinel mode where it cannot be carried in the address URL).
    #[arg(
        long = "redis_password",
        env = "CENTRIFUGO_REDIS_PASSWORD",
        default_value = ""
    )]
    pub redis_password: String,
    /// Redis database number (`SELECT`); defaults to 0.
    #[arg(long = "redis_db", env = "CENTRIFUGO_REDIS_DB", default_value_t = 0)]
    pub redis_db: i64,
    /// Redis key/channel namespace (must match peer Go nodes for interop).
    #[arg(
        long = "redis_prefix",
        env = "CENTRIFUGO_REDIS_PREFIX",
        default_value = "centrifugo"
    )]
    pub redis_prefix: String,
    /// History meta-hash TTL in seconds (0 = never expire), matching Go.
    #[arg(
        long = "redis_history_meta_ttl",
        env = "CENTRIFUGO_REDIS_HISTORY_META_TTL",
        default_value_t = 0
    )]
    pub redis_history_meta_ttl: u64,
    /// Log level: `debug` | `info` | `error` | `fatal` | `none` (Go-compatible).
    #[arg(
        long = "log_level",
        env = "CENTRIFUGO_LOG_LEVEL",
        default_value = "info"
    )]
    pub log_level: String,
    /// Write the process id to this file at startup.
    #[arg(long = "pid_file", env = "CENTRIFUGO_PID_FILE", default_value = "")]
    pub pid_file: String,

    // ---- Accepted for Go drop-in compatibility but NOT implemented in this build.
    // Passing any of these logs a warning at startup instead of aborting, so an
    // existing official command line still starts. See docs/COMPATIBILITY_v2.8.6.md.
    // (`--prometheus`/`--health` are no-ops only because `/metrics` and `/health`
    // are already served unconditionally.)
    #[arg(long = "log_file", default_value = "", hide = true)]
    pub log_file: String,
    #[arg(long = "tls", hide = true)]
    pub tls: bool,
    #[arg(long = "tls_cert", default_value = "", hide = true)]
    pub tls_cert: String,
    #[arg(long = "tls_key", default_value = "", hide = true)]
    pub tls_key: String,
    #[arg(long = "tls_external", hide = true)]
    pub tls_external: bool,
    #[arg(long = "internal_address", default_value = "", hide = true)]
    pub internal_address: String,
    #[arg(long = "internal_port", default_value = "", hide = true)]
    pub internal_port: String,
    #[arg(long = "admin_external", hide = true)]
    pub admin_external: bool,
    #[arg(long = "broker", default_value = "", hide = true)]
    pub broker: String,
    #[arg(long = "nats_url", default_value = "", hide = true)]
    pub nats_url: String,
    #[arg(long = "redis_tls", hide = true)]
    pub redis_tls: bool,
    #[arg(long = "redis_tls_skip_verify", hide = true)]
    pub redis_tls_skip_verify: bool,
    #[arg(long = "redis_sentinel_password", default_value = "", hide = true)]
    pub redis_sentinel_password: String,
    #[arg(long = "grpc_api_tls", hide = true)]
    pub grpc_api_tls: bool,
    #[arg(long = "grpc_api_tls_cert", default_value = "", hide = true)]
    pub grpc_api_tls_cert: String,
    #[arg(long = "grpc_api_tls_key", default_value = "", hide = true)]
    pub grpc_api_tls_key: String,
    #[arg(long = "grpc_api_tls_disable", hide = true)]
    pub grpc_api_tls_disable: bool,
    #[arg(long = "prometheus", hide = true)]
    pub prometheus: bool,
    #[arg(long = "health", hide = true)]
    pub health: bool,
    #[arg(long = "debug", hide = true)]
    pub debug: bool,
}
