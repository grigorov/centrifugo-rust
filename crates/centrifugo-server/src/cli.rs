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
    #[arg(long)]
    pub client_insecure: bool,
}
