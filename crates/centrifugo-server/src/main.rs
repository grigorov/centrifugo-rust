//! Centrifugo (Rust) binary entrypoint.

mod cli;
mod config;
mod http;
mod ws;

use std::sync::Arc;

use centrifugo_auth::TokenVerifier;
use centrifugo_core::Node;
use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::Config;

const VERSION: &str = "2.8.6";

fn read_pem_opt(path: &str) -> anyhow::Result<Option<Vec<u8>>> {
    if path.is_empty() {
        Ok(None)
    } else {
        Ok(Some(std::fs::read(path)?))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("Centrifugo v{VERSION}");
            Ok(())
        }
        Command::Serve(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .init();
            let cfg = Config {
                address: args.address,
                port: args.port,
                client_insecure: args.client_insecure,
                token_hmac_secret_key: args.token_hmac_secret_key,
                token_rsa_public_key: args.token_rsa_public_key,
                token_ecdsa_public_key: args.token_ecdsa_public_key,
            };
            let rsa_pem = read_pem_opt(&cfg.token_rsa_public_key)?;
            let ecdsa_pem = read_pem_opt(&cfg.token_ecdsa_public_key)?;
            let verifier = TokenVerifier::new(
                &cfg.token_hmac_secret_key,
                rsa_pem.as_deref(),
                ecdsa_pem.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("invalid token public key: {e}"))?;
            let node = Node::new_with(Arc::new(verifier), cfg.client_insecure);
            let app = http::router(Arc::clone(&node));
            http::serve(cfg.socket_addr(), app).await
        }
    }
}
