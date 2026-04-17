use clap::{Parser, Subcommand};

mod client;
mod commands;

#[derive(Parser)]
#[command(name = "synchrotron", about = "Synchrotron-CD CLI", version)]
struct Cli {
    /// Server URL
    #[arg(long, env = "SYNCHROTRON_URL", default_value = "http://localhost:8484")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check server health
    Health,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = client::SynchrotronClient::new(&cli.server);

    match cli.command {
        Commands::Health => commands::health::run(&client).await?,
    }

    Ok(())
}
