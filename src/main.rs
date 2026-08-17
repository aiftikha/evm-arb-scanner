mod amm;
mod breadth;
mod rpc;
mod types;
mod universe;
mod v3_math;

use anyhow::{bail, Result};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "evm-arb-scanner")]
#[command(version)]
#[command(about = "Read-only multichain EVM DEX arbitrage scanner; never signs or broadcasts transactions", long_about = None)]
struct Cli {
    /// Scanner configuration. RPC endpoints are read from environment variables named in this file.
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Optional run duration in seconds. Omit to run until Ctrl+C.
    #[arg(long)]
    seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    if matches!(cli.seconds, Some(0)) {
        bail!("--seconds must be greater than zero when provided");
    }

    info!(
        config = %cli.config,
        seconds = ?cli.seconds,
        "starting read-only EVM arbitrage scanner; signing and transaction submission are not compiled into this binary"
    );

    breadth::run(&cli.config, cli.seconds).await
}
