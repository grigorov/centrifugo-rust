//! Centrifugo (Rust) binary entrypoint.

mod admin;
mod api;
mod cli;
mod config;
mod grpc;
mod http;
mod proxy_http;
mod sockjs;
mod webui;
mod ws;

use std::sync::Arc;

use centrifugo_auth::{gen_connect_token, TokenVerifier};
use centrifugo_core::{
    make_route, Engine, Hub, MemoryEngine, Node, NodeRegistry, DEFAULT_USE_SEQ_GEN,
};
use centrifugo_redis::RedisEngine;
use clap::{CommandFactory, FromArgMatches};

use crate::cli::{Cli, Command, GentokenArgs};
use crate::config::{check_config, ExplicitArgs, Settings};

const VERSION: &str = "2.8.6";

/// `gentoken`: print an HS256 connection JWT. The secret comes from `--config`'s
/// `token_hmac_secret_key` (or the `--token_hmac_secret_key` flag, which wins).
fn gentoken(args: GentokenArgs) -> anyhow::Result<()> {
    let secret = if !args.token_hmac_secret_key.is_empty() {
        args.token_hmac_secret_key
    } else if let Some(path) = &args.config {
        let json = std::fs::read_to_string(path)?;
        let v: serde_json::Value = serde_json::from_str(&json)?;
        v.get("token_hmac_secret_key")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string()
    } else {
        String::new()
    };
    if secret.is_empty() {
        anyhow::bail!("no HMAC secret: pass --token_hmac_secret_key or -c <config>");
    }
    let token = gen_connect_token(&secret, &args.user, args.ttl)
        .map_err(|e| anyhow::anyhow!("generate token: {e}"))?;
    if args.ttl > 0 {
        println!(
            "HMAC SHA-256 JWT for user \"{}\" with expiration TTL {} seconds:",
            args.user, args.ttl
        );
    } else {
        println!(
            "HMAC SHA-256 JWT for user \"{}\" (no expiration):",
            args.user
        );
    }
    println!("{token}");
    Ok(())
}

/// `checktoken`: verify a connection JWT against the HMAC secret and print its
/// claims (mirrors Go's `checktoken`). Non-zero exit on a missing/invalid token.
fn checktoken(args: cli::ChecktokenArgs) -> anyhow::Result<()> {
    let token = args
        .token
        .ok_or_else(|| anyhow::anyhow!("usage: centrifugo checktoken [TOKEN]"))?;
    let secret = if !args.token_hmac_secret_key.is_empty() {
        args.token_hmac_secret_key
    } else if let Some(path) = &args.config {
        let json = std::fs::read_to_string(path)?;
        let v: serde_json::Value = serde_json::from_str(&json)?;
        v.get("token_hmac_secret_key")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string()
    } else {
        String::new()
    };
    if secret.is_empty() {
        anyhow::bail!("no HMAC secret: pass --token_hmac_secret_key or -c <config>");
    }
    let verifier = TokenVerifier::new(&secret, None, None)
        .map_err(|e| anyhow::anyhow!("build verifier: {e}"))?;
    match verifier.verify_connect_token(&token) {
        Ok(ct) => {
            let exp = if ct.expire_at > 0 {
                ct.expire_at.to_string()
            } else {
                "none".to_string()
            };
            println!("valid token for user \"{}\" (expire_at: {exp})", ct.user);
            Ok(())
        }
        Err(e) => anyhow::bail!("invalid token: {e:?}"),
    }
}

/// `genconfig`: write a fresh config with random secrets.
fn genconfig(path: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{path} already exists");
    }
    // Mirror Go centrifugo's genconfig starter key set.
    let cfg = serde_json::json!({
        "v3_use_offset": false,
        "token_hmac_secret_key": uuid::Uuid::new_v4().to_string(),
        "admin_password": uuid::Uuid::new_v4().to_string(),
        "admin_secret": uuid::Uuid::new_v4().to_string(),
        "api_key": uuid::Uuid::new_v4().to_string(),
        "allowed_origins": [],
    });
    std::fs::write(path, serde_json::to_string_pretty(&cfg)?)?;
    println!("config written to {path}");
    Ok(())
}

/// `checkconfig`: validate a config file; non-zero exit on error.
fn checkconfig(path: &str) -> anyhow::Result<()> {
    let json = std::fs::read_to_string(path)?;
    check_config(&json).map_err(|e| anyhow::anyhow!("invalid config {path}: {e}"))?;
    println!("config {path} is valid");
    Ok(())
}

fn read_pem_opt(path: &str) -> anyhow::Result<Option<Vec<u8>>> {
    if path.is_empty() {
        Ok(None)
    } else {
        Ok(Some(std::fs::read(path)?))
    }
}

/// The machine hostname (Go uses `os.Hostname()` for the default node name).
/// Dep-free: shell out to `hostname`, fall back to `$HOSTNAME`, then `centrifugo`.
fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "centrifugo".to_string())
}

/// Map Go centrifugo's `log_level` (debug|info|warn|error|fatal|none) to a tracing
/// `EnvFilter` directive. Unknown levels fall back to `info`.
fn map_log_level(level: &str) -> String {
    match level.to_ascii_lowercase().as_str() {
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" => "error",
        "fatal" => "error",
        "none" | "off" => "off",
        _ => "info",
    }
    .to_string()
}

/// Warn for official flags that are accepted (so a Go command line starts) but have
/// no effect in this build. `--prometheus`/`--health` are omitted — `/metrics` and
/// `/health` are always served.
fn warn_unsupported_flags(a: &cli::ServeArgs) {
    let mut ignored: Vec<&str> = Vec::new();
    let mut bool_flag = |on: bool, name: &'static str| {
        if on {
            ignored.push(name);
        }
    };
    bool_flag(a.tls, "--tls");
    bool_flag(a.tls_external, "--tls_external");
    bool_flag(a.admin_external, "--admin_external");
    bool_flag(a.redis_tls, "--redis_tls");
    bool_flag(a.redis_tls_skip_verify, "--redis_tls_skip_verify");
    bool_flag(a.grpc_api_tls, "--grpc_api_tls");
    bool_flag(a.grpc_api_tls_disable, "--grpc_api_tls_disable");
    bool_flag(a.debug, "--debug");
    for (val, name) in [
        (&a.tls_cert, "--tls_cert"),
        (&a.tls_key, "--tls_key"),
        (&a.internal_address, "--internal_address"),
        (&a.internal_port, "--internal_port"),
        (&a.broker, "--broker"),
        (&a.nats_url, "--nats_url"),
        (&a.redis_sentinel_password, "--redis_sentinel_password"),
        (&a.grpc_api_tls_cert, "--grpc_api_tls_cert"),
        (&a.grpc_api_tls_key, "--grpc_api_tls_key"),
        (&a.log_file, "--log_file"),
    ] {
        if !val.is_empty() {
            ignored.push(name);
        }
    }
    if !ignored.is_empty() {
        tracing::warn!(
            "ignoring unsupported flag(s) (no effect in this build): {}",
            ignored.join(", ")
        );
    }
}

/// Fetch a JWKS document body from `url`.
async fn fetch_jwks(url: &str) -> anyhow::Result<String> {
    Ok(reqwest::Client::new()
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

/// Periodically refetch the JWKS so rotated keys are picked up without a restart.
fn spawn_jwks_refresh(verifier: Arc<TokenVerifier>, url: String) {
    const REFRESH: std::time::Duration = std::time::Duration::from_secs(3600);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REFRESH).await;
            match fetch_jwks(&url).await {
                Ok(body) => {
                    let n = verifier.set_jwks_from_json(&body).unwrap_or(0);
                    tracing::debug!("refreshed {n} JWKS key(s) from {url}");
                }
                Err(e) => tracing::warn!("JWKS refresh from {url} failed: {e}"),
            }
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse into ArgMatches first so we can ask clap which serve args were set
    // explicitly (CLI or CENTRIFUGO_* env) — they must beat a config file
    // (viper precedence: flag > env > file > default).
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    match cli.command {
        // No subcommand → run the server (Go centrifugo's root command is the
        // server). `serve` is kept as a hidden alias for the same. Compute the
        // explicit-args set from whichever matches carry the serve args.
        None => run_server(cli.serve, config::explicit_serve_args(&matches)).await,
        Some(Command::Serve(args)) => {
            let explicit = matches
                .subcommand_matches("serve")
                .map(config::explicit_serve_args)
                .unwrap_or_default();
            run_server(args, explicit).await
        }
        Some(Command::Version) => {
            println!("Centrifugo v{VERSION}");
            Ok(())
        }
        Some(Command::Gentoken(args)) => gentoken(args),
        Some(Command::Genconfig(args)) => genconfig(&args.config),
        Some(Command::Checkconfig(args)) => checkconfig(&args.config),
        Some(Command::Checktoken(args)) => checktoken(args),
    }
}

/// Run the messaging server (the root command, and the `serve` alias).
/// `explicit` names the serve args set on the CLI/env so they beat a config file.
async fn run_server(args: cli::ServeArgs, explicit: ExplicitArgs) -> anyhow::Result<()> {
    // RUST_LOG (dev override) wins; otherwise map Go's --log_level / CENTRIFUGO_LOG_LEVEL.
    let log_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| map_log_level(&args.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .init();
    // Accepted-but-unimplemented official flags warn here instead of aborting startup.
    warn_unsupported_flags(&args);
    if !args.pid_file.is_empty() {
        if let Err(e) = std::fs::write(&args.pid_file, std::process::id().to_string()) {
            tracing::warn!("could not write pid_file {}: {e}", args.pid_file);
        }
    }
    // Like Go centrifugo, fall back to ./config.json in the working dir when no
    // -c is given (so a mounted /centrifugo/config.json is picked up by a bare run).
    let config_path = args.config.clone().or_else(|| {
        let p = "config.json";
        std::path::Path::new(p).exists().then(|| p.to_string())
    });
    let mut settings = match &config_path {
        Some(path) => {
            let json = std::fs::read_to_string(path)?;
            // Go validates the rule config at startup and exits 1 on failure.
            check_config(&json).map_err(|e| anyhow::anyhow!("invalid config {path}: {e}"))?;
            Settings::from_file_and_args(&json, &args, &explicit)?
        }
        None => Settings::from_args(&args),
    };
    if args.config.is_none() {
        if let Some(p) = &config_path {
            tracing::info!("auto-discovered config file {p}");
        }
    }
    settings.apply_env();
    // Go's verifier is JWKS-exclusive: when a JWKS endpoint is set it
    // verifies tokens ONLY by JWK (kid), never falling back to static
    // keys. Mirror that by building the verifier with no static keys
    // when JWKS is configured.
    let jwks_enabled = !settings.token_jwks_public_endpoint.is_empty();
    let (rsa_pem, ecdsa_pem) = if jwks_enabled {
        (None, None)
    } else {
        (
            read_pem_opt(&settings.token_rsa_public_key)?,
            read_pem_opt(&settings.token_ecdsa_public_key)?,
        )
    };
    let hmac_secret = if jwks_enabled {
        ""
    } else {
        &settings.token_hmac_secret_key
    };
    let verifier = Arc::new(
        TokenVerifier::new(hmac_secret, rsa_pem.as_deref(), ecdsa_pem.as_deref())
            .map_err(|e| anyhow::anyhow!("invalid token public key: {e}"))?,
    );
    // JWKS: fetch once now (so a token's `kid` verifies as soon as the
    // server is healthy), then refresh in the background.
    if jwks_enabled {
        let url = settings.token_jwks_public_endpoint.clone();
        match fetch_jwks(&url).await {
            Ok(body) => {
                let n = verifier.set_jwks_from_json(&body).unwrap_or(0);
                tracing::info!("loaded {n} JWKS key(s) from {url}");
            }
            Err(e) => tracing::warn!("initial JWKS fetch from {url} failed: {e}"),
        }
        spawn_jwks_refresh(Arc::clone(&verifier), url);
    }
    let addr = settings.socket_addr();
    let api_auth = api::ApiAuth {
        key: settings.api_key.clone(),
        insecure: settings.api_insecure,
    };
    let admin_config = admin::AdminConfig {
        enabled: settings.admin,
        insecure: settings.admin_insecure,
        password: settings.admin_password.clone(),
        secret: settings.admin_secret.clone(),
        web_path: settings.admin_web_path.clone(),
    };
    let grpc = settings
        .grpc_api
        .then(|| (settings.grpc_socket_addr(), settings.grpc_api_key.clone()));
    let proxies = proxy_http::build_proxies(&settings);
    let hub = Arc::new(Hub::new());
    // Shared cluster-node registry; its uid tags this node's control + pings.
    let registry = Arc::new(NodeRegistry::new(uuid::Uuid::new_v4().to_string()));
    let node_uid = registry.self_uid().to_string();
    let engine: Arc<dyn Engine> = match settings.engine.as_str() {
        "redis" => {
            let redis_opts = centrifugo_redis::RedisOptions {
                password: Some(settings.redis_password.clone()),
                db: settings.redis_db,
                prefix: settings.redis_prefix.clone(),
                history_meta_ttl: settings.redis_history_meta_ttl,
            };
            let e = if !settings.redis_master_name.is_empty() {
                tracing::info!(
                    "using redis engine via sentinel (master {})",
                    settings.redis_master_name
                );
                RedisEngine::connect_sentinel(
                    &settings.redis_master_name,
                    &settings.redis_sentinels,
                    make_route(&hub, &registry, DEFAULT_USE_SEQ_GEN),
                    node_uid.clone(),
                    redis_opts,
                )
                .await
                .map_err(|e| anyhow::anyhow!("connect redis via sentinel: {e}"))?
            } else {
                tracing::info!("using redis engine at {}", settings.redis_address);
                RedisEngine::connect(
                    &settings.redis_address,
                    make_route(&hub, &registry, DEFAULT_USE_SEQ_GEN),
                    node_uid.clone(),
                    redis_opts,
                )
                .await
                .map_err(|e| anyhow::anyhow!("connect redis at {}: {e}", settings.redis_address))?
            };
            Arc::new(e)
        }
        _ => Arc::new(
            MemoryEngine::new(make_route(&hub, &registry, DEFAULT_USE_SEQ_GEN))
                .with_history_meta_ttl(settings.memory_history_meta_ttl),
        ),
    };
    // Node name (Go config `name`, default `hostname_port`) — display only;
    // routing/dedup use the UID.
    let node_name = if settings.name.is_empty() {
        format!("{}_{}", hostname(), settings.port)
    } else {
        settings.name.clone()
    };
    let node = Node::new_with_engine(
        hub,
        engine,
        verifier,
        settings.client_insecure,
        settings.client_anonymous,
        settings.namespaces,
        proxies,
        settings.client_presence_ping_interval,
        settings.client_presence_expire_interval,
        registry,
        VERSION.to_string(),
        node_name,
    );
    // Broadcast NODE-info pings + prune stale nodes (cluster membership).
    node.spawn_node_pings();
    if let Some((grpc_addr, grpc_key)) = grpc {
        let grpc_node = Arc::clone(&node);
        tokio::spawn(async move {
            if let Err(e) = grpc::serve(grpc_node, grpc_addr, grpc_key).await {
                tracing::error!("grpc server error: {e}");
            }
        });
        tracing::info!("gRPC API listening on {grpc_addr}");
    }
    let app = http::router(Arc::clone(&node), api_auth, admin_config);
    http::serve(addr, app).await
}
