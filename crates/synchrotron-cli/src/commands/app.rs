use crate::client::SynchrotronClient;
use crate::output::{self, Column, OutputFormat};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::json;

#[derive(Subcommand)]
pub enum AppCmd {
    /// List all applications
    List,
    /// Get one application
    Get { name: String },
    /// Register a new application
    Create(CreateArgs),
    /// Update mutable fields on an application
    Update(UpdateArgs),
    /// Delete an application
    Delete { name: String },
    /// Show recent sync history
    History { name: String },
}

#[derive(Args)]
pub struct CreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub namespace: String,
    #[arg(long)]
    pub repo_url: String,
    #[arg(long)]
    pub path: String,
    #[arg(long, default_value = "main")]
    pub target_revision: String,
    #[arg(long)]
    pub dest_cluster: String,
    #[arg(long)]
    pub dest_namespace: String,
}

#[derive(Args)]
pub struct UpdateArgs {
    pub name: String,
    #[arg(long)]
    pub namespace: Option<String>,
    #[arg(long)]
    pub repo_url: Option<String>,
    #[arg(long)]
    pub path: Option<String>,
    #[arg(long)]
    pub target_revision: Option<String>,
    #[arg(long)]
    pub dest_cluster: Option<String>,
    #[arg(long)]
    pub dest_namespace: Option<String>,
}

const APP_COLUMNS: &[Column] = &[
    Column { header: "NAME", path: "name" },
    Column { header: "NAMESPACE", path: "namespace" },
    Column { header: "DEST", path: "dest_cluster" },
    Column { header: "REVISION", path: "target_revision" },
    Column { header: "SYNC", path: "sync_status" },
    Column { header: "HEALTH", path: "health_status" },
];

const HISTORY_COLUMNS: &[Column] = &[
    Column { header: "ID", path: "id" },
    Column { header: "REVISION", path: "revision" },
    Column { header: "STATUS", path: "status" },
    Column { header: "TRIGGER", path: "trigger" },
    Column { header: "STARTED", path: "started_at" },
];

pub async fn run(client: &SynchrotronClient, format: OutputFormat, cmd: AppCmd) -> Result<()> {
    match cmd {
        AppCmd::List => {
            let v = client.list_apps().await?;
            output::print(format, &v, APP_COLUMNS)
        }
        AppCmd::Get { name } => {
            let v = client.get_app(&name).await?;
            output::print(format, &v, APP_COLUMNS)
        }
        AppCmd::Create(a) => {
            let body = json!({
                "name": a.name,
                "namespace": a.namespace,
                "repo_url": a.repo_url,
                "path": a.path,
                "target_revision": a.target_revision,
                "dest_cluster": a.dest_cluster,
                "dest_namespace": a.dest_namespace,
            });
            let v = client.create_app(body).await?;
            output::print(format, &v, APP_COLUMNS)
        }
        AppCmd::Update(a) => {
            let mut body = serde_json::Map::new();
            if let Some(x) = a.namespace { body.insert("namespace".into(), json!(x)); }
            if let Some(x) = a.repo_url { body.insert("repo_url".into(), json!(x)); }
            if let Some(x) = a.path { body.insert("path".into(), json!(x)); }
            if let Some(x) = a.target_revision { body.insert("target_revision".into(), json!(x)); }
            if let Some(x) = a.dest_cluster { body.insert("dest_cluster".into(), json!(x)); }
            if let Some(x) = a.dest_namespace { body.insert("dest_namespace".into(), json!(x)); }
            let v = client.update_app(&a.name, json!(body)).await?;
            output::print(format, &v, APP_COLUMNS)
        }
        AppCmd::Delete { name } => {
            client.delete_app(&name).await?;
            println!("deleted app `{name}`");
            Ok(())
        }
        AppCmd::History { name } => {
            let v = client.history_app(&name).await?;
            output::print(format, &v, HISTORY_COLUMNS)
        }
    }
}
