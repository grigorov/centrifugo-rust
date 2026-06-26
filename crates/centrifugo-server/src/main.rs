//! Centrifugo (Rust) binary entrypoint.

mod api;
mod cli;
mod config;
mod grpc;
mod http;
mod ws;

use std::sync::Arc;

use centrifugo_auth::TokenVerifier;
use centrifugo_core::Node;
use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::Settings;

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
            let settings = match &args.config {
                Some(path) => Settings::from_file_and_args(&std::fs::read_to_string(path)?, &args)?,
                None => Settings::from_args(&args),
            };
            let rsa_pem = read_pem_opt(&settings.token_rsa_public_key)?;
            let ecdsa_pem = read_pem_opt(&settings.token_ecdsa_public_key)?;
            let verifier = TokenVerifier::new(
                &settings.token_hmac_secret_key,
                rsa_pem.as_deref(),
                ecdsa_pem.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("invalid token public key: {e}"))?;
            let addr = settings.socket_addr();
            let api_auth = api::ApiAuth {
                key: settings.api_key.clone(),
                insecure: settings.api_insecure,
            };
            let grpc = settings
                .grpc_api
                .then(|| (settings.grpc_socket_addr(), settings.grpc_api_key.clone()));
            let node = Node::new_with(
                Arc::new(verifier),
                settings.client_insecure,
                settings.namespaces,
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
            let app = http::router(Arc::clone(&node), api_auth);
            http::serve(addr, app).await
        }
    }
}
