//! Command-line interface. Mirrors the subset of Centrifugo subcommands needed
//! so far (`serve`, `version`); more land in M11.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "centrifugo", disable_version_flag = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the server.
    Serve(ServeArgs),
    /// Print version and exit.
    Version,
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1")]
    pub address: String,
    #[arg(long, default_value_t = 8000)]
    pub port: u16,
    /// Allow connections without a token (anonymous), assigning a fresh client id.
    #[arg(long = "client_insecure")]
    pub client_insecure: bool,
    /// HMAC secret for HS256/384/512 connection tokens.
    #[arg(long = "token_hmac_secret_key", default_value = "")]
    pub token_hmac_secret_key: String,
    /// Path to a PEM RSA public key for RS256/384/512 tokens.
    #[arg(long = "token_rsa_public_key", default_value = "")]
    pub token_rsa_public_key: String,
    /// Path to a PEM ECDSA public key for ES256/384 tokens.
    #[arg(long = "token_ecdsa_public_key", default_value = "")]
    pub token_ecdsa_public_key: String,
    /// Enable presence on all channels.
    #[arg(long = "presence")]
    pub presence: bool,
    /// Enable join/leave pushes on all channels.
    #[arg(long = "join_leave")]
    pub join_leave: bool,
    /// Disable client-side presence commands even when presence is enabled.
    #[arg(long = "presence_disable_for_client")]
    pub presence_disable_for_client: bool,
}
