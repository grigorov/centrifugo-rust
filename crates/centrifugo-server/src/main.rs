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
use clap::Parser;

use crate::cli::{Cli, Command, GentokenArgs};
use crate::config::{check_config, Settings};

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
    println!("{token}");
    Ok(())
}

/// `genconfig`: write a fresh config with random secrets.
fn genconfig(path: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{path} already exists");
    }
    let cfg = serde_json::json!({
        "token_hmac_secret_key": uuid::Uuid::new_v4().to_string(),
        "api_key": uuid::Uuid::new_v4().to_string(),
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
    let cli = Cli::parse();
    match cli.command {
        // No subcommand → run the server (Go centrifugo's root command is the
        // server). `serve` is kept as a hidden alias for the same.
        None => run_server(cli.serve).await,
        Some(Command::Serve(args)) => run_server(args).await,
        Some(Command::Version) => {
            println!("Centrifugo v{VERSION}");
            Ok(())
        }
        Some(Command::Gentoken(args)) => gentoken(args),
        Some(Command::Genconfig(args)) => genconfig(&args.config),
        Some(Command::Checkconfig(args)) => checkconfig(&args.config),
    }
}

/// Run the messaging server (the root command, and the `serve` alias).
async fn run_server(args: cli::ServeArgs) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // Like Go centrifugo, fall back to ./config.json in the working dir when no
    // -c is given (so a mounted /centrifugo/config.json is picked up by a bare run).
    let config_path = args.config.clone().or_else(|| {
        let p = "config.json";
        std::path::Path::new(p).exists().then(|| p.to_string())
    });
    let mut settings = match &config_path {
        Some(path) => Settings::from_file_and_args(&std::fs::read_to_string(path)?, &args)?,
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
        _ => Arc::new(MemoryEngine::new(make_route(
            &hub,
            &registry,
            DEFAULT_USE_SEQ_GEN,
        ))),
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
