//! Centrifugo (Rust) binary entrypoint.

mod cli;
mod config;
mod http;
mod ws;

use std::sync::Arc;

use centrifugo_core::Node;
use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::Config;

const VERSION: &str = "2.8.6";

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
            };
            let node = Node::new();
            let app = http::router(Arc::clone(&node));
            http::serve(cfg.socket_addr(), app).await
        }
    }
}
