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
    pub api_key: String,
    pub api_insecure: bool,
    pub namespaces: Namespaces,
}

impl Settings {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.address, self.port)
            .parse()
            .expect("valid socket address")
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
            api_key: a.api_key.clone(),
            api_insecure: a.api_insecure,
            namespaces: Namespaces {
                default: ChannelOptions {
                    presence: a.presence,
                    join_leave: a.join_leave,
                    presence_disable_for_client: a.presence_disable_for_client,
                    history_size: a.history_size,
                    history_lifetime: a.history_lifetime,
                    history_recover: a.history_recover,
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
            api_key: fc.api_key,
            api_insecure: fc.api_insecure,
            namespaces: Namespaces {
                default: fc.options.into(),
                namespaces,
                namespace_boundary: fc.channel_namespace_boundary,
                private_prefix: fc.channel_private_prefix,
            },
        })
    }
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
    api_key: String,
    #[serde(default)]
    api_insecure: bool,
    #[serde(flatten)]
    options: ChannelOptionsCfg,
    #[serde(default = "default_ns_boundary")]
    channel_namespace_boundary: String,
    #[serde(default = "default_private_prefix")]
    channel_private_prefix: String,
    #[serde(default)]
    namespaces: Vec<NamespaceCfg>,
}
