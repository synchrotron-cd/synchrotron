use clap::{Parser, Subcommand};

mod client;
mod commands;
mod output;

use commands::{app::AppCmd, cluster::ClusterCmd, repo::RepoCmd};
use output::OutputFormat;

#[derive(Parser)]
#[command(name = "synchrotron", about = "Synchrotron-CD CLI", version)]
struct Cli {
    /// Server URL
    #[arg(long, env = "SYNCHROTRON_URL", default_value = "http://localhost:8484")]
    server: String,

    /// Bearer token sent as `Authorization: Bearer <token>`
    #[arg(long, env = "SYNCHROTRON_TOKEN")]
    token: Option<String>,

    /// Output format
    #[arg(long, short, env = "SYNCHROTRON_OUTPUT", value_enum, default_value_t = OutputFormat::Table)]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check server health
    Health,
    /// Manage applications
    App {
        #[command(subcommand)]
        cmd: AppCmd,
    },
    /// Manage cluster registrations
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
    /// Manage repo registrations
    Repo {
        #[command(subcommand)]
        cmd: RepoCmd,
    },
    /// Trigger a manual sync for an app
    Sync { app: String },
    /// Compute desired-vs-live diff for an app
    Diff { app: String },
    /// Roll an app back to a recorded revision
    Rollback {
        app: String,
        #[arg(long)]
        revision: i64,
    },
    /// Tail the live event stream for an app
    Watch {
        app: String,
        /// Resume from a previous event id (Last-Event-ID)
        #[arg(long)]
        since: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = client::SynchrotronClient::new(&cli.server, cli.token);

    match cli.command {
        Commands::Health => commands::health::run(&client, cli.output).await,
        Commands::App { cmd } => commands::app::run(&client, cli.output, cmd).await,
        Commands::Cluster { cmd } => commands::cluster::run(&client, cli.output, cmd).await,
        Commands::Repo { cmd } => commands::repo::run(&client, cli.output, cmd).await,
        Commands::Sync { app } => commands::ops::sync(&client, cli.output, &app).await,
        Commands::Diff { app } => commands::ops::diff(&client, cli.output, &app).await,
        Commands::Rollback { app, revision } => {
            commands::ops::rollback(&client, cli.output, &app, revision).await
        }
        Commands::Watch { app, since } => {
            commands::watch::run(&client, cli.output, &app, since.as_deref()).await
        }
    }
}
