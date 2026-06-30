//! Effective server settings, built from CLI flags or a `-c` JSON config file.
//! The config file is required to define `namespaces` (Go has no CLI flags for
//! them); flags cover the single-namespace case. Per-key precedence follows
//! Go/viper: `flag > env > file > default` (see [`Settings::from_file_and_args`]
//! and [`explicit_serve_args`]).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use centrifugo_core::{ChannelOptions, Namespaces};
use serde::Deserialize;

use crate::cli::ServeArgs;

/// Serve-arg ids (clap long names) that were set explicitly on the CLI or via a
/// `CENTRIFUGO_*` env var. These take precedence over a config file, matching
/// Go/viper's `flag > env > file > default` precedence.
pub type ExplicitArgs = HashSet<String>;

/// Serve-arg ids that map 1:1 to a config-file key and so participate in the
/// flag/env-vs-file precedence. Channel options and bools are sourced from the
/// file (bools also get an env "turn-on" overlay in [`Settings::apply_env`]).
const PRECEDENCE_IDS: &[&str] = &[
    "address",
    "port",
    "name",
    "user_personal_channel_namespace",
    "token_hmac_secret_key",
    "token_rsa_public_key",
    "token_ecdsa_public_key",
    "token_jwks_public_endpoint",
    "client_presence_ping_interval",
    "client_presence_expire_interval",
    "api_key",
    "grpc_api_port",
    "grpc_api_key",
    "proxy_connect_endpoint",
    "proxy_refresh_endpoint",
    "proxy_subscribe_endpoint",
    "proxy_publish_endpoint",
    "proxy_rpc_endpoint",
    "engine",
    "redis_address",
    "redis_host",
    "redis_port",
    "redis_url",
    "redis_master_name",
    "redis_sentinels",
    "redis_password",
    "redis_db",
    "redis_prefix",
    "redis_history_meta_ttl",
    "memory_history_meta_ttl",
    "admin_password",
    "admin_secret",
    "admin_web_path",
    // Bool flags Go binds via viper.BindPFlag: an explicit CLI flag beats the
    // config file (these have no clap `env`, so env turn-on stays in apply_env).
    "client_insecure",
    "api_insecure",
    "grpc_api",
    "admin",
    "admin_insecure",
];

/// Build the set of serve args clap resolved from the CLI or a `CENTRIFUGO_*`
/// env var (not a default). Only valid for the root/server command, where the
/// flattened `ServeArgs` live in the top-level matches.
pub fn explicit_serve_args(m: &clap::ArgMatches) -> ExplicitArgs {
    use clap::parser::ValueSource;
    PRECEDENCE_IDS
        .iter()
        .filter(|id| {
            matches!(
                m.value_source(id),
                Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
            )
        })
        .map(|s| s.to_string())
        .collect()
}

/// Resolve the effective Redis address from the Go-compatible aliases: `--redis_url`
/// wins, else `--redis_host`/`--redis_port` (defaulting host `127.0.0.1`, port `6379`)
/// are combined, else the single `--redis_address`.
pub(crate) fn effective_redis_address(a: &ServeArgs) -> String {
    if !a.redis_url.is_empty() {
        a.redis_url.clone()
    } else if !a.redis_host.is_empty() || !a.redis_port.is_empty() {
        let host = if a.redis_host.is_empty() {
            "127.0.0.1"
        } else {
            &a.redis_host
        };
        let port = if a.redis_port.is_empty() {
            "6379"
        } else {
            &a.redis_port
        };
        format!("{host}:{port}")
    } else {
        a.redis_address.clone()
    }
}

/// Config-file Redis target: `redis_url` > `redis_host`/`redis_port` > a non-default
/// `redis_address` in the file; otherwise fall back to the flags/env (`a`).
fn effective_redis_address_file(fc: &FileConfig, a: &ServeArgs) -> String {
    if !fc.redis_url.is_empty() {
        fc.redis_url.clone()
    } else if !fc.redis_host.is_empty() || !fc.redis_port.is_empty() {
        let host = if fc.redis_host.is_empty() {
            "127.0.0.1"
        } else {
            &fc.redis_host
        };
        let port = if fc.redis_port.is_empty() {
            "6379"
        } else {
            &fc.redis_port
        };
        format!("{host}:{port}")
    } else if fc.redis_address != default_redis_address() {
        fc.redis_address.clone()
    } else {
        effective_redis_address(a)
    }
}

/// Official config keys this build does not implement — passing them in a config
/// file loads cleanly (serde ignores unknown keys) but has no effect, so we warn.
const INERT_CONFIG_KEYS: &[&str] = &[
    "tls",
    "tls_cert",
    "tls_key",
    "tls_external",
    "log_level",
    "log_file",
    "internal_address",
    "internal_port",
    "admin_external",
    "broker",
    "nats_url",
    "debug",
    "prometheus",
    "health",
    "redis_tls",
    "redis_tls_skip_verify",
    "redis_sentinel_password",
    "sockjs",
];

/// Warn for any [`INERT_CONFIG_KEYS`] present in the config JSON (so an operator
/// isn't surprised that a security/operational knob silently has no effect).
fn warn_inert_config_keys(json: &str) {
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(json) {
        let present: Vec<&str> = INERT_CONFIG_KEYS
            .iter()
            .copied()
            .filter(|k| map.contains_key(*k))
            .collect();
        if !present.is_empty() {
            tracing::warn!(
                "config keys accepted but not implemented in this build (no effect): {}",
                present.join(", ")
            );
        }
    }
}

pub struct Settings {
    pub address: String,
    pub port: u16,
    /// Node name for display (Go `name`; empty → `hostname_port` at startup).
    pub name: String,
    pub client_insecure: bool,
    pub client_anonymous: bool,
    /// Max channel-name byte length (Go `channel_max_length`, default 255).
    pub channel_max_length: usize,
    /// Max channels a single client may subscribe to (Go `client_channel_limit`,
    /// default 128).
    pub client_channel_limit: usize,
    /// Max concurrent connections per authenticated user (Go
    /// `client_user_connection_limit`, default 0 = unlimited).
    pub client_user_connection_limit: usize,
    /// Allowed WS/SockJS request origins (Go `allowed_origins`). `None` = not
    /// configured → all origins allowed; `Some(list)` → only matching origins.
    pub allowed_origins: Option<Vec<String>>,
    pub client_presence_ping_interval: u64,
    pub client_presence_expire_interval: u64,
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
    pub redis_master_name: String,
    pub redis_sentinels: String,
    pub redis_password: String,
    pub redis_db: i64,
    pub redis_prefix: String,
    pub redis_history_meta_ttl: u64,
    pub memory_history_meta_ttl: u64,
    pub proxy_connect_endpoint: String,
    pub proxy_refresh_endpoint: String,
    pub proxy_subscribe_endpoint: String,
    pub proxy_publish_endpoint: String,
    pub proxy_rpc_endpoint: String,
    pub admin: bool,
    pub admin_insecure: bool,
    pub admin_password: String,
    pub admin_secret: String,
    pub admin_web_path: String,
    pub namespaces: Namespaces,
}

impl Settings {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.address, self.port)
            .parse()
            .expect("valid socket address")
    }

    /// Overlay `CENTRIFUGO_*` env vars. Most scalars are already resolved by clap's
    /// `env` attribute (and take precedence over the file via [`from_file_and_args`]);
    /// this fills the remaining gaps and resolves the bool flags (which have no clap
    /// `env`): for the five Go binds via `viper.BindEnv`, a present env var overrides
    /// the config file either way (incl. `false`), unless the flag was set explicitly
    /// on the CLI. `explicit` names those CLI/env-explicit serve args. Per-key
    /// precedence stays flag > env > file > default.
    pub fn apply_env(&mut self, explicit: &ExplicitArgs) {
        fn env(key: &str) -> Option<String> {
            std::env::var(format!("CENTRIFUGO_{key}"))
                .ok()
                .filter(|s| !s.is_empty())
        }
        // Parse a bool env var the way Go's `strconv.ParseBool` (viper) does.
        fn env_bool(key: &str) -> Option<bool> {
            env(key).and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
                "1" | "t" | "true" => Some(true),
                "0" | "f" | "false" => Some(false),
                _ => None,
            })
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
        fill(
            &mut self.token_jwks_public_endpoint,
            "TOKEN_JWKS_PUBLIC_ENDPOINT",
        );
        fill(&mut self.api_key, "API_KEY");
        fill(&mut self.proxy_connect_endpoint, "PROXY_CONNECT_ENDPOINT");
        // The five bools Go binds via viper.BindEnv: a present env overrides the
        // file (either value); an explicit CLI flag still wins (flag > env).
        let env_bool_overlay = |id: &str, key: &str, current: bool| -> bool {
            match (explicit.contains(id), env_bool(key)) {
                (false, Some(v)) => v,
                _ => current,
            }
        };
        self.client_insecure =
            env_bool_overlay("client_insecure", "CLIENT_INSECURE", self.client_insecure);
        self.api_insecure = env_bool_overlay("api_insecure", "API_INSECURE", self.api_insecure);
        self.grpc_api = env_bool_overlay("grpc_api", "GRPC_API", self.grpc_api);
        self.admin = env_bool_overlay("admin", "ADMIN", self.admin);
        self.admin_insecure =
            env_bool_overlay("admin_insecure", "ADMIN_INSECURE", self.admin_insecure);
        // client_anonymous is not viper-bound in Go (no BindEnv); keep one-way turn-on.
        if !self.client_anonymous && env("CLIENT_ANONYMOUS").as_deref() == Some("true") {
            self.client_anonymous = true;
        }
        // Numeric/list keys Go binds via viper.BindEnv (read with GetInt /
        // GetStringSlice): a present env overrides the config file. No CLI flag, so
        // precedence is env > file > default. NOTE: client_user_connection_limit is
        // intentionally absent — Go does NOT BindEnv it (config-file/default only).
        if let Some(v) = env("CHANNEL_MAX_LENGTH").and_then(|s| s.trim().parse::<usize>().ok()) {
            self.channel_max_length = v;
        }
        if let Some(v) = env("CLIENT_CHANNEL_LIMIT").and_then(|s| s.trim().parse::<usize>().ok()) {
            self.client_channel_limit = v;
        }
        if let Some(s) = env("ALLOWED_ORIGINS") {
            // viper GetStringSlice splits an env string on whitespace (cast strings.Fields).
            self.allowed_origins = Some(s.split_whitespace().map(str::to_string).collect());
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
        fill(&mut self.redis_password, "REDIS_PASSWORD");
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
            name: a.name.clone(),
            client_insecure: a.client_insecure,
            client_anonymous: a.client_anonymous,
            // Not CLI flags (config/default only); Go defaults are 255 / 128 / 0.
            channel_max_length: 255,
            client_channel_limit: 128,
            client_user_connection_limit: 0,
            // No config file → allowed_origins not configured → allow all.
            allowed_origins: None,
            client_presence_ping_interval: a.client_presence_ping_interval,
            client_presence_expire_interval: a.client_presence_expire_interval,
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
            redis_address: effective_redis_address(a),
            redis_master_name: a.redis_master_name.clone(),
            redis_sentinels: a.redis_sentinels.clone(),
            redis_password: a.redis_password.clone(),
            redis_db: a.redis_db,
            redis_prefix: a.redis_prefix.clone(),
            redis_history_meta_ttl: a.redis_history_meta_ttl,
            memory_history_meta_ttl: a.memory_history_meta_ttl,
            proxy_connect_endpoint: a.proxy_connect_endpoint.clone(),
            proxy_refresh_endpoint: a.proxy_refresh_endpoint.clone(),
            proxy_subscribe_endpoint: a.proxy_subscribe_endpoint.clone(),
            proxy_publish_endpoint: a.proxy_publish_endpoint.clone(),
            proxy_rpc_endpoint: a.proxy_rpc_endpoint.clone(),
            admin: a.admin,
            admin_insecure: a.admin_insecure,
            admin_password: a.admin_password.clone(),
            admin_secret: a.admin_secret.clone(),
            admin_web_path: a.admin_web_path.clone(),
            namespaces: Namespaces {
                default: ChannelOptions {
                    presence: a.presence,
                    join_leave: a.join_leave,
                    presence_disable_for_client: a.presence_disable_for_client,
                    history_size: a.history_size,
                    history_lifetime: a.history_lifetime,
                    history_recover: a.history_recover,
                    history_disable_for_client: a.history_disable_for_client,
                    anonymous: false,
                    server_side: false,
                    proxy_subscribe: false,
                    proxy_publish: false,
                    publish: a.publish,
                    subscribe_to_publish: a.subscribe_to_publish,
                },
                namespaces: HashMap::new(),
                namespace_boundary: ":".into(),
                private_prefix: "$".into(),
                user_subscribe_to_personal: a.user_subscribe_to_personal,
                user_personal_channel_namespace: a.user_personal_channel_namespace.clone(),
            },
        }
    }

    /// Build settings from a JSON config file. Per-key precedence is Go/viper's
    /// `flag > env > file > default`: a serve arg named in `explicit` (set on the
    /// CLI or via `CENTRIFUGO_*`) wins over the file; otherwise the file value (or
    /// its own default) applies. Channel options + bools come from the file (bools
    /// also get an env turn-on overlay in [`apply_env`]).
    pub fn from_file_and_args(
        json: &str,
        a: &ServeArgs,
        explicit: &ExplicitArgs,
    ) -> anyhow::Result<Self> {
        let fc: FileConfig = serde_json::from_str(json)?;
        warn_inert_config_keys(json);
        // String field: the explicit flag/env arg wins, else the file value.
        let s = |id: &str, arg: &str, file: String| -> String {
            if explicit.contains(id) {
                arg.to_string()
            } else {
                file
            }
        };
        // Bool field: same precedence. Used for the viper.BindPFlag bools, where
        // an explicit CLI flag must beat the file (Go: flag > file).
        let b = |id: &str, arg: bool, file: bool| -> bool {
            if explicit.contains(id) {
                arg
            } else {
                file
            }
        };
        // Redis target: if any redis address arg was set explicitly, the arg
        // combination wins; otherwise resolve from the file (falling back to args).
        let redis_address = if ["redis_url", "redis_host", "redis_port", "redis_address"]
            .iter()
            .any(|k| explicit.contains(*k))
        {
            effective_redis_address(a)
        } else {
            effective_redis_address_file(&fc, a)
        };
        let namespaces = fc
            .namespaces
            .into_iter()
            .map(|n| (n.name, n.options.into()))
            .collect();
        Ok(Settings {
            // Config-file address/port win when present (Go honors them) unless a
            // flag/env set them explicitly; otherwise fall back to the flag default.
            address: if explicit.contains("address") {
                a.address.clone()
            } else {
                fc.address.clone().unwrap_or_else(|| a.address.clone())
            },
            port: if explicit.contains("port") {
                a.port
            } else {
                fc.port.unwrap_or(a.port)
            },
            name: s("name", &a.name, fc.name),
            client_insecure: b("client_insecure", a.client_insecure, fc.client_insecure),
            // client_anonymous is not in Go's viper.BindPFlag set, so it stays
            // file-sourced (a CLI flag does not override it, matching Go).
            client_anonymous: fc.client_anonymous,
            // Config/default only (no CLI flag); serde defaults to 255 / 128 / 0.
            channel_max_length: fc.channel_max_length,
            client_channel_limit: fc.client_channel_limit,
            client_user_connection_limit: fc.client_user_connection_limit,
            allowed_origins: fc.allowed_origins,
            client_presence_ping_interval: if explicit.contains("client_presence_ping_interval") {
                a.client_presence_ping_interval
            } else {
                fc.client_presence_ping_interval
            },
            client_presence_expire_interval: if explicit.contains("client_presence_expire_interval")
            {
                a.client_presence_expire_interval
            } else {
                fc.client_presence_expire_interval
            },
            token_hmac_secret_key: s(
                "token_hmac_secret_key",
                &a.token_hmac_secret_key,
                fc.token_hmac_secret_key,
            ),
            token_rsa_public_key: s(
                "token_rsa_public_key",
                &a.token_rsa_public_key,
                fc.token_rsa_public_key,
            ),
            token_ecdsa_public_key: s(
                "token_ecdsa_public_key",
                &a.token_ecdsa_public_key,
                fc.token_ecdsa_public_key,
            ),
            token_jwks_public_endpoint: s(
                "token_jwks_public_endpoint",
                &a.token_jwks_public_endpoint,
                fc.token_jwks_public_endpoint,
            ),
            api_key: s("api_key", &a.api_key, fc.api_key),
            api_insecure: b("api_insecure", a.api_insecure, fc.api_insecure),
            grpc_api: b("grpc_api", a.grpc_api, fc.grpc_api),
            grpc_api_port: if explicit.contains("grpc_api_port") {
                a.grpc_api_port
            } else {
                fc.grpc_api_port
            },
            grpc_api_key: s("grpc_api_key", &a.grpc_api_key, fc.grpc_api_key),
            engine: s("engine", &a.engine, fc.engine),
            redis_address,
            redis_master_name: s(
                "redis_master_name",
                &a.redis_master_name,
                fc.redis_master_name,
            ),
            redis_sentinels: s("redis_sentinels", &a.redis_sentinels, fc.redis_sentinels),
            redis_password: s("redis_password", &a.redis_password, fc.redis_password),
            redis_db: if explicit.contains("redis_db") {
                a.redis_db
            } else {
                fc.redis_db
            },
            redis_prefix: s("redis_prefix", &a.redis_prefix, fc.redis_prefix),
            redis_history_meta_ttl: if explicit.contains("redis_history_meta_ttl") {
                a.redis_history_meta_ttl
            } else {
                fc.redis_history_meta_ttl
            },
            memory_history_meta_ttl: if explicit.contains("memory_history_meta_ttl") {
                a.memory_history_meta_ttl
            } else {
                fc.memory_history_meta_ttl
            },
            proxy_connect_endpoint: s(
                "proxy_connect_endpoint",
                &a.proxy_connect_endpoint,
                fc.proxy_connect_endpoint,
            ),
            proxy_refresh_endpoint: s(
                "proxy_refresh_endpoint",
                &a.proxy_refresh_endpoint,
                fc.proxy_refresh_endpoint,
            ),
            proxy_subscribe_endpoint: s(
                "proxy_subscribe_endpoint",
                &a.proxy_subscribe_endpoint,
                fc.proxy_subscribe_endpoint,
            ),
            proxy_publish_endpoint: s(
                "proxy_publish_endpoint",
                &a.proxy_publish_endpoint,
                fc.proxy_publish_endpoint,
            ),
            proxy_rpc_endpoint: s(
                "proxy_rpc_endpoint",
                &a.proxy_rpc_endpoint,
                fc.proxy_rpc_endpoint,
            ),
            admin: b("admin", a.admin, fc.admin),
            admin_insecure: b("admin_insecure", a.admin_insecure, fc.admin_insecure),
            admin_password: s("admin_password", &a.admin_password, fc.admin_password),
            admin_secret: s("admin_secret", &a.admin_secret, fc.admin_secret),
            admin_web_path: s("admin_web_path", &a.admin_web_path, fc.admin_web_path),
            namespaces: Namespaces {
                default: fc.options.into(),
                namespaces,
                namespace_boundary: fc.channel_namespace_boundary,
                private_prefix: fc.channel_private_prefix,
                user_subscribe_to_personal: fc.user_subscribe_to_personal,
                user_personal_channel_namespace: fc.user_personal_channel_namespace,
            },
        })
    }
}

/// Validate a config file body: parse it, then apply Go's `rule.Config.Validate`
/// rules. Used by the `checkconfig` subcommand and at server startup; a failure
/// is a non-zero exit (Go logs fatal and exits 1).
pub fn check_config(json: &str) -> anyhow::Result<()> {
    let fc: FileConfig = serde_json::from_str(json)?;
    validate_file_config(&fc)
}

/// Mirror Go's `rule.Config.Validate` (rule.go): history recovery requires a
/// history window, namespace names must match `^[-a-zA-Z0-9_.]{2,}$` and be
/// unique, and `user_personal_channel_namespace` must reference a defined
/// namespace. Each violation is fatal in Go (exit 1).
fn validate_file_config(fc: &FileConfig) -> anyhow::Result<()> {
    fn check_history(o: &ChannelOptionsCfg, scope: &str) -> anyhow::Result<()> {
        if o.history_recover && (o.history_size == 0 || o.history_lifetime == 0) {
            anyhow::bail!(
                "both history size and history lifetime required for history recovery{scope}"
            );
        }
        Ok(())
    }
    // Go's namespaceNameRe = `^[-a-zA-Z0-9_.]{2,}$`.
    fn valid_namespace_name(name: &str) -> bool {
        name.len() >= 2
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    }
    check_history(&fc.options, "")?;
    let mut seen: HashSet<&str> = HashSet::new();
    for ns in &fc.namespaces {
        if !valid_namespace_name(&ns.name) {
            anyhow::bail!("wrong namespace name: {}", ns.name);
        }
        if !seen.insert(&ns.name) {
            anyhow::bail!("namespace name must be unique: {}", ns.name);
        }
        check_history(&ns.options, &format!(" in namespace {}", ns.name))?;
    }
    if !fc.user_personal_channel_namespace.is_empty()
        && !seen.contains(fc.user_personal_channel_namespace.as_str())
    {
        anyhow::bail!(
            "namespace for user personal channel not found: {}",
            fc.user_personal_channel_namespace
        );
    }
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
    history_disable_for_client: bool,
    #[serde(default)]
    anonymous: bool,
    #[serde(default)]
    server_side: bool,
    #[serde(default)]
    proxy_subscribe: bool,
    #[serde(default)]
    proxy_publish: bool,
    #[serde(default)]
    publish: bool,
    #[serde(default)]
    subscribe_to_publish: bool,
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
            history_disable_for_client: c.history_disable_for_client,
            anonymous: c.anonymous,
            server_side: c.server_side,
            proxy_subscribe: c.proxy_subscribe,
            proxy_publish: c.proxy_publish,
            publish: c.publish,
            subscribe_to_publish: c.subscribe_to_publish,
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
fn default_channel_max_length() -> usize {
    255
}
fn default_client_channel_limit() -> usize {
    128
}
fn default_presence_ping() -> u64 {
    25
}
fn default_presence_expire() -> u64 {
    60
}
fn default_engine() -> String {
    "memory".into()
}
fn default_redis_address() -> String {
    "127.0.0.1:6379".into()
}
fn default_redis_prefix() -> String {
    centrifugo_redis::DEFAULT_PREFIX.into()
}

#[derive(Deserialize, Default)]
struct FileConfig {
    #[serde(default)]
    name: String,
    // Listen address/port: honored from the file when present (Go reads them via
    // viper), else they come from the --address/--port flags or their env vars.
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    client_insecure: bool,
    #[serde(default)]
    client_anonymous: bool,
    #[serde(default = "default_channel_max_length")]
    channel_max_length: usize,
    #[serde(default = "default_client_channel_limit")]
    client_channel_limit: usize,
    #[serde(default)]
    client_user_connection_limit: usize,
    // Absent → None (allow all origins); present (incl. []) → Some(list).
    #[serde(default)]
    allowed_origins: Option<Vec<String>>,
    #[serde(default = "default_presence_ping")]
    client_presence_ping_interval: u64,
    #[serde(default = "default_presence_expire")]
    client_presence_expire_interval: u64,
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
    // Go-compatible Redis target aliases (mapped into redis_address, redis_url wins).
    #[serde(default)]
    redis_host: String,
    #[serde(default)]
    redis_port: String,
    #[serde(default)]
    redis_url: String,
    #[serde(default)]
    redis_master_name: String,
    #[serde(default)]
    redis_sentinels: String,
    #[serde(default)]
    redis_password: String,
    #[serde(default)]
    redis_db: i64,
    #[serde(default = "default_redis_prefix")]
    redis_prefix: String,
    #[serde(default)]
    redis_history_meta_ttl: u64,
    #[serde(default)]
    memory_history_meta_ttl: u64,
    #[serde(default)]
    proxy_connect_endpoint: String,
    #[serde(default)]
    proxy_refresh_endpoint: String,
    #[serde(default)]
    proxy_subscribe_endpoint: String,
    #[serde(default)]
    proxy_publish_endpoint: String,
    #[serde(default)]
    proxy_rpc_endpoint: String,
    #[serde(default)]
    admin: bool,
    #[serde(default)]
    admin_insecure: bool,
    #[serde(default)]
    admin_password: String,
    #[serde(default)]
    admin_secret: String,
    #[serde(default)]
    admin_web_path: String,
    #[serde(flatten)]
    options: ChannelOptionsCfg,
    #[serde(default = "default_ns_boundary")]
    channel_namespace_boundary: String,
    #[serde(default = "default_private_prefix")]
    channel_private_prefix: String,
    #[serde(default)]
    user_subscribe_to_personal: bool,
    #[serde(default)]
    user_personal_channel_namespace: String,
    #[serde(default)]
    namespaces: Vec<NamespaceCfg>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn args(argv: &[&str]) -> ServeArgs {
        let mut v = vec!["centrifugo"];
        v.extend_from_slice(argv);
        crate::cli::Cli::try_parse_from(v).unwrap().serve
    }

    #[test]
    fn default_address_is_all_interfaces() {
        // Go-compatible default: bind all interfaces (so a CMD override is reachable).
        assert_eq!(args(&[]).address, "0.0.0.0");
    }

    #[test]
    fn redis_host_port_url_map_to_address() {
        assert_eq!(
            effective_redis_address(&args(&["--redis_host", "h", "--redis_port", "7000"])),
            "h:7000"
        );
        // redis_url wins over host/port.
        assert_eq!(
            effective_redis_address(&args(&[
                "--redis_url",
                "redis://x:1/2",
                "--redis_host",
                "h"
            ])),
            "redis://x:1/2"
        );
        // host alone defaults the port; neither → the single redis_address.
        assert_eq!(
            effective_redis_address(&args(&["--redis_host", "h"])),
            "h:6379"
        );
        assert_eq!(effective_redis_address(&args(&[])), "127.0.0.1:6379");
    }

    #[test]
    fn config_file_address_port_honored_else_flag() {
        let none = ExplicitArgs::new();
        // File wins when it specifies address/port (Go honors them).
        let s =
            Settings::from_file_and_args(r#"{"address":"1.2.3.4","port":9001}"#, &args(&[]), &none)
                .unwrap();
        assert_eq!(s.address, "1.2.3.4");
        assert_eq!(s.port, 9001);
        // Absent in file → fall back to the flag/default.
        let s = Settings::from_file_and_args(r#"{}"#, &args(&["--port", "7777"]), &none).unwrap();
        assert_eq!(s.port, 7777);
    }

    #[test]
    fn config_file_redis_host_port_mapped() {
        let s = Settings::from_file_and_args(
            r#"{"engine":"redis","redis_host":"cfg","redis_port":"6390"}"#,
            &args(&[]),
            &ExplicitArgs::new(),
        )
        .unwrap();
        assert_eq!(s.redis_address, "cfg:6390");
    }

    #[test]
    fn explicit_arg_beats_config_file() {
        // M5: a serve arg set on the CLI/env beats a config-file value
        // (viper precedence flag > env > file). `args(["--api_key", ...])`
        // simulates the explicit set carrying "api_key".
        let explicit: ExplicitArgs = ["api_key".to_string(), "port".to_string()]
            .into_iter()
            .collect();
        let a = args(&["--api_key", "argkey", "--port", "9999"]);
        let s =
            Settings::from_file_and_args(r#"{"api_key":"filekey","port":18400}"#, &a, &explicit)
                .unwrap();
        assert_eq!(s.api_key, "argkey", "explicit arg must beat file api_key");
        assert_eq!(s.port, 9999, "explicit arg must beat file port");
    }

    #[test]
    fn validate_rejects_history_recover_without_window() {
        // M6: history_recover requires history_size>0 && history_lifetime>0.
        assert!(check_config(r#"{"history_recover":true}"#).is_err());
        assert!(check_config(r#"{"history_recover":true,"history_size":10}"#).is_err());
        assert!(check_config(
            r#"{"history_recover":true,"history_size":10,"history_lifetime":60}"#
        )
        .is_ok());
    }

    #[test]
    fn validate_namespace_names() {
        // Bad name (contains '!'), too short, duplicate -> error; clean -> ok.
        assert!(check_config(r#"{"namespaces":[{"name":"ba!d"}]}"#).is_err());
        assert!(check_config(r#"{"namespaces":[{"name":"a"}]}"#).is_err());
        assert!(check_config(r#"{"namespaces":[{"name":"news"},{"name":"news"}]}"#).is_err());
        assert!(check_config(r#"{"namespaces":[{"name":"news"},{"name":"chat.v2"}]}"#).is_ok());
    }

    #[test]
    fn validate_personal_namespace_must_exist() {
        // M6: user_personal_channel_namespace must reference a defined namespace.
        assert!(check_config(r#"{"user_personal_channel_namespace":"nope"}"#).is_err());
        assert!(check_config(
            r#"{"user_personal_channel_namespace":"personal","namespaces":[{"name":"personal"}]}"#
        )
        .is_ok());
    }

    #[test]
    fn config_file_used_when_arg_not_explicit() {
        // Same args present but NOT in the explicit set (i.e. clap saw only the
        // default) → the file value wins over the default.
        let s = Settings::from_file_and_args(
            r#"{"api_key":"filekey","port":18400}"#,
            &args(&[]),
            &ExplicitArgs::new(),
        )
        .unwrap();
        assert_eq!(s.api_key, "filekey");
        assert_eq!(s.port, 18400);
    }

    #[test]
    fn explicit_serve_args_detects_viper_bound_bools() {
        // F1: the bool flags Go binds via viper.BindPFlag (client_insecure,
        // api_insecure, grpc_api, admin, admin_insecure) must be recognized as
        // explicit when set on the CLI — otherwise from_file_and_args can't let
        // them beat a config file.
        use clap::CommandFactory;
        let m = crate::cli::Cli::command()
            .try_get_matches_from(["centrifugo", "--client_insecure", "--grpc_api", "--admin"])
            .unwrap();
        let explicit = explicit_serve_args(&m);
        assert!(explicit.contains("client_insecure"));
        assert!(explicit.contains("grpc_api"));
        assert!(explicit.contains("admin"));
        // A bool flag not passed must not appear in the explicit set.
        assert!(!explicit.contains("api_insecure"));
        assert!(!explicit.contains("admin_insecure"));
    }

    #[test]
    fn explicit_bool_flag_beats_config_file() {
        // F1: an explicitly-set CLI bool flag overrides the config-file value,
        // matching Go viper.BindPFlag precedence (flag > file). The file sets all
        // five viper-bound bools false; the explicit CLI flags must win.
        let explicit: ExplicitArgs = [
            "client_insecure".to_string(),
            "api_insecure".to_string(),
            "grpc_api".to_string(),
            "admin".to_string(),
            "admin_insecure".to_string(),
        ]
        .into_iter()
        .collect();
        let a = args(&[
            "--client_insecure",
            "--api_insecure",
            "--grpc_api",
            "--admin",
            "--admin_insecure",
        ]);
        let json = r#"{"client_insecure":false,"api_insecure":false,"grpc_api":false,"admin":false,"admin_insecure":false}"#;
        let s = Settings::from_file_and_args(json, &a, &explicit).unwrap();
        assert!(
            s.client_insecure,
            "explicit --client_insecure must beat file"
        );
        assert!(s.api_insecure, "explicit --api_insecure must beat file");
        assert!(s.grpc_api, "explicit --grpc_api must beat file");
        assert!(s.admin, "explicit --admin must beat file");
        assert!(s.admin_insecure, "explicit --admin_insecure must beat file");
    }

    #[test]
    fn bool_flag_from_file_when_not_explicit() {
        // No CLI flag set → the config-file bool value applies (regression guard
        // that the explicit overlay does not clobber file-sourced bools).
        let s = Settings::from_file_and_args(
            r#"{"client_insecure":true,"grpc_api":true,"admin":true}"#,
            &args(&[]),
            &ExplicitArgs::new(),
        )
        .unwrap();
        assert!(s.client_insecure);
        assert!(s.grpc_api);
        assert!(s.admin);
    }
}
