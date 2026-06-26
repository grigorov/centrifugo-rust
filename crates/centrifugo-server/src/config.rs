//! Effective server settings, built from CLI flags or a `-c` JSON config file.
//! The config file is required to define `namespaces` (Go has no CLI flags for
//! them); flags cover the single-namespace case. Full layered config (env/TOML/
//! YAML, flag>file>env precedence) is M11; here `-c` is authoritative when given.

use std::collections::HashMap;
use std::net::SocketAddr;

use centrifugo_core::{ChannelOptions, Namespaces};
use serde::Deserialize;

use crate::cli::ServeArgs;

pub struct Settings {
    pub address: String,
    pub port: u16,
    pub client_insecure: bool,
    pub token_hmac_secret_key: String,
    pub token_rsa_public_key: String,
    pub token_ecdsa_public_key: String,
    pub token_jwks_public_endpoint: String,
    pub api_key: String,
    pub api_insecure: bool,
    pub grpc_api: bool,
    pub grpc_api_port: u16,
    pub grpc_api_key: String,
    pub engine: String,
    pub redis_address: String,
    pub proxy_connect_endpoint: String,
    pub admin: bool,
    pub admin_password: String,
    pub admin_secret: String,
    pub namespaces: Namespaces,
}

impl Settings {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.address, self.port)
            .parse()
            .expect("valid socket address")
    }

    /// Overlay `CENTRIFUGO_*` environment variables as a fallback **below** flags
    /// and the config file (the spec's flags > file > env precedence): a value
    /// already set by a flag/file is kept; an unset one is filled from env.
    pub fn apply_env(&mut self) {
        fn env(key: &str) -> Option<String> {
            std::env::var(format!("CENTRIFUGO_{key}"))
                .ok()
                .filter(|s| !s.is_empty())
        }
        fn fill(field: &mut String, key: &str) {
            if field.is_empty() {
                if let Some(v) = env(key) {
                    *field = v;
                }
            }
        }
        fill(&mut self.token_hmac_secret_key, "TOKEN_HMAC_SECRET_KEY");
        fill(&mut self.token_rsa_public_key, "TOKEN_RSA_PUBLIC_KEY");
        fill(&mut self.token_ecdsa_public_key, "TOKEN_ECDSA_PUBLIC_KEY");
        fill(&mut self.token_jwks_public_endpoint, "TOKEN_JWKS_PUBLIC_ENDPOINT");
        fill(&mut self.api_key, "API_KEY");
        fill(&mut self.proxy_connect_endpoint, "PROXY_CONNECT_ENDPOINT");
        if !self.client_insecure && env("CLIENT_INSECURE").as_deref() == Some("true") {
            self.client_insecure = true;
        }
        if !self.api_insecure && env("API_INSECURE").as_deref() == Some("true") {
            self.api_insecure = true;
        }
        // Engine/redis address: overlay only when still at the built-in default.
        if self.engine == "memory" {
            if let Some(v) = env("ENGINE") {
                self.engine = v;
            }
        }
        if self.redis_address == "127.0.0.1:6379" {
            if let Some(v) = env("REDIS_ADDRESS") {
                self.redis_address = v;
            }
        }
    }

    /// gRPC API bind address — same host as the HTTP listener, `grpc_api_port`.
    pub fn grpc_socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.address, self.grpc_api_port)
            .parse()
            .expect("valid grpc socket address")
    }

    /// Build settings from CLI flags (single default namespace, no named ones).
    pub fn from_args(a: &ServeArgs) -> Self {
        Settings {
            address: a.address.clone(),
            port: a.port,
            client_insecure: a.client_insecure,
            token_hmac_secret_key: a.token_hmac_secret_key.clone(),
            token_rsa_public_key: a.token_rsa_public_key.clone(),
            token_ecdsa_public_key: a.token_ecdsa_public_key.clone(),
            token_jwks_public_endpoint: a.token_jwks_public_endpoint.clone(),
            api_key: a.api_key.clone(),
            api_insecure: a.api_insecure,
            grpc_api: a.grpc_api,
            grpc_api_port: a.grpc_api_port,
            grpc_api_key: a.grpc_api_key.clone(),
            engine: a.engine.clone(),
            redis_address: a.redis_address.clone(),
            proxy_connect_endpoint: a.proxy_connect_endpoint.clone(),
            admin: a.admin,
            admin_password: a.admin_password.clone(),
            admin_secret: a.admin_secret.clone(),
            namespaces: Namespaces {
                default: ChannelOptions {
                    presence: a.presence,
                    join_leave: a.join_leave,
                    presence_disable_for_client: a.presence_disable_for_client,
                    history_size: a.history_size,
                    history_lifetime: a.history_lifetime,
                    history_recover: a.history_recover,
                    anonymous: false,
                    server_side: false,
                },
                namespaces: HashMap::new(),
                namespace_boundary: ":".into(),
                private_prefix: "$".into(),
            },
        }
    }

    /// Build settings from a JSON config file (authoritative for everything but
    /// the listen address/port, which come from flags).
    pub fn from_file_and_args(json: &str, a: &ServeArgs) -> anyhow::Result<Self> {
        let fc: FileConfig = serde_json::from_str(json)?;
        let namespaces = fc
            .namespaces
            .into_iter()
            .map(|n| (n.name, n.options.into()))
            .collect();
        Ok(Settings {
            address: a.address.clone(),
            port: a.port,
            client_insecure: fc.client_insecure,
            token_hmac_secret_key: fc.token_hmac_secret_key,
            token_rsa_public_key: fc.token_rsa_public_key,
            token_ecdsa_public_key: fc.token_ecdsa_public_key,
            token_jwks_public_endpoint: fc.token_jwks_public_endpoint,
            api_key: fc.api_key,
            api_insecure: fc.api_insecure,
            grpc_api: fc.grpc_api,
            grpc_api_port: fc.grpc_api_port,
            grpc_api_key: fc.grpc_api_key,
            engine: fc.engine,
            redis_address: fc.redis_address,
            proxy_connect_endpoint: fc.proxy_connect_endpoint,
            admin: fc.admin,
            admin_password: fc.admin_password,
            admin_secret: fc.admin_secret,
            namespaces: Namespaces {
                default: fc.options.into(),
                namespaces,
                namespace_boundary: fc.channel_namespace_boundary,
                private_prefix: fc.channel_private_prefix,
            },
        })
    }
}

/// Validate a config file body (parse + reject unknown structure). Used by the
/// `checkconfig` subcommand.
pub fn check_config(json: &str) -> anyhow::Result<()> {
    let _fc: FileConfig = serde_json::from_str(json)?;
    Ok(())
}

#[derive(Deserialize, Default)]
struct ChannelOptionsCfg {
    #[serde(default)]
    presence: bool,
    #[serde(default)]
    join_leave: bool,
    #[serde(default)]
    presence_disable_for_client: bool,
    #[serde(default)]
    history_size: usize,
    #[serde(default)]
    history_lifetime: u64,
    #[serde(default)]
    history_recover: bool,
    #[serde(default)]
    anonymous: bool,
    #[serde(default)]
    server_side: bool,
}

impl From<ChannelOptionsCfg> for ChannelOptions {
    fn from(c: ChannelOptionsCfg) -> Self {
        ChannelOptions {
            presence: c.presence,
            join_leave: c.join_leave,
            presence_disable_for_client: c.presence_disable_for_client,
            history_size: c.history_size,
            history_lifetime: c.history_lifetime,
            history_recover: c.history_recover,
            anonymous: c.anonymous,
            server_side: c.server_side,
        }
    }
}

#[derive(Deserialize)]
struct NamespaceCfg {
    name: String,
    #[serde(flatten)]
    options: ChannelOptionsCfg,
}

fn default_ns_boundary() -> String {
    ":".into()
}
fn default_private_prefix() -> String {
    "$".into()
}
fn default_grpc_port() -> u16 {
    10000
}
fn default_engine() -> String {
    "memory".into()
}
fn default_redis_address() -> String {
    "127.0.0.1:6379".into()
}

#[derive(Deserialize, Default)]
struct FileConfig {
    #[serde(default)]
    client_insecure: bool,
    #[serde(default)]
    token_hmac_secret_key: String,
    #[serde(default)]
    token_rsa_public_key: String,
    #[serde(default)]
    token_ecdsa_public_key: String,
    #[serde(default)]
    token_jwks_public_endpoint: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    api_insecure: bool,
    #[serde(default)]
    grpc_api: bool,
    #[serde(default = "default_grpc_port")]
    grpc_api_port: u16,
    #[serde(default)]
    grpc_api_key: String,
    #[serde(default = "default_engine")]
    engine: String,
    #[serde(default = "default_redis_address")]
    redis_address: String,
    #[serde(default)]
    proxy_connect_endpoint: String,
    #[serde(default)]
    admin: bool,
    #[serde(default)]
    admin_password: String,
    #[serde(default)]
    admin_secret: String,
    #[serde(flatten)]
    options: ChannelOptionsCfg,
    #[serde(default = "default_ns_boundary")]
    channel_namespace_boundary: String,
    #[serde(default = "default_private_prefix")]
    channel_private_prefix: String,
    #[serde(default)]
    namespaces: Vec<NamespaceCfg>,
}
