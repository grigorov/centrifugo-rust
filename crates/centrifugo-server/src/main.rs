//! Centrifugo (Rust) binary entrypoint.

mod admin;
mod api;
mod cli;
mod config;
mod grpc;
mod http;
mod proxy_http;
mod sockjs;
mod ws;

use std::sync::Arc;

use centrifugo_auth::{gen_connect_token, TokenVerifier};
use centrifugo_core::{make_route, ConnectProxy, Engine, Hub, MemoryEngine, Node};
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
        Command::Version => {
            println!("Centrifugo v{VERSION}");
            Ok(())
        }
        Command::Gentoken(args) => gentoken(args),
        Command::Genconfig(args) => genconfig(&args.config),
        Command::Checkconfig(args) => checkconfig(&args.config),
        Command::Serve(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .init();
            let mut settings = match &args.config {
                Some(path) => Settings::from_file_and_args(&std::fs::read_to_string(path)?, &args)?,
                None => Settings::from_args(&args),
            };
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
                password: settings.admin_password.clone(),
                secret: settings.admin_secret.clone(),
            };
            let grpc = settings
                .grpc_api
                .then(|| (settings.grpc_socket_addr(), settings.grpc_api_key.clone()));
            let connect_proxy: Option<Arc<dyn ConnectProxy>> =
                if settings.proxy_connect_endpoint.is_empty() {
                    None
                } else {
                    tracing::info!(
                        "connect proxy enabled: {}",
                        settings.proxy_connect_endpoint
                    );
                    Some(Arc::new(proxy_http::HttpConnectProxy::new(
                        settings.proxy_connect_endpoint.clone(),
                    )))
                };
            let hub = Arc::new(Hub::new());
            let engine: Arc<dyn Engine> = match settings.engine.as_str() {
                "redis" => {
                    let e = RedisEngine::connect(&settings.redis_address, make_route(&hub))
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("connect redis at {}: {e}", settings.redis_address)
                        })?;
                    tracing::info!("using redis engine at {}", settings.redis_address);
                    Arc::new(e)
                }
                _ => Arc::new(MemoryEngine::new(make_route(&hub))),
            };
            let node = Node::new_with_engine(
                hub,
                engine,
                verifier,
                settings.client_insecure,
                settings.client_anonymous,
                settings.namespaces,
                connect_proxy,
            );
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
    }
}
