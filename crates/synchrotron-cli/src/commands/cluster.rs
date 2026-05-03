use crate::client::SynchrotronClient;
use crate::output::{self, Column, OutputFormat};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::json;

#[derive(Subcommand)]
pub enum ClusterCmd {
    List,
    Get { name: String },
    Create(CreateArgs),
    Update(UpdateArgs),
    Delete { name: String },
    /// Run a connectivity check against a registered cluster
    Check { name: String },
}

#[derive(Args)]
pub struct CreateArgs {
    #[arg(long)]
    pub name: String,
    /// One of `kubeconfig`, `in_cluster`, `default`
    #[arg(long, default_value = "kubeconfig")]
    pub auth_source: String,
    #[arg(long)]
    pub kubeconfig_path: Option<String>,
    #[arg(long)]
    pub context: Option<String>,
    /// Bearer token. Persisted server-side; never echoed back.
    #[arg(long, env = "SYNCHROTRON_CLUSTER_BEARER_TOKEN")]
    pub bearer_token: Option<String>,
}

#[derive(Args)]
pub struct UpdateArgs {
    pub name: String,
    #[arg(long)]
    pub auth_source: Option<String>,
    #[arg(long)]
    pub kubeconfig_path: Option<String>,
    #[arg(long)]
    pub context: Option<String>,
    #[arg(long, env = "SYNCHROTRON_CLUSTER_BEARER_TOKEN")]
    pub bearer_token: Option<String>,
}

const CLUSTER_COLUMNS: &[Column] = &[
    Column { header: "NAME", path: "name" },
    Column { header: "AUTH", path: "auth_source" },
    Column { header: "CONTEXT", path: "context" },
    Column { header: "TOKEN", path: "has_bearer_token" },
];

const CHECK_COLUMNS: &[Column] = &[
    Column { header: "CLUSTER", path: "cluster" },
    Column { header: "OK", path: "ok" },
    Column { header: "VERSION", path: "apiserver_version" },
    Column { header: "FAILED_AT", path: "failed_stage" },
    Column { header: "ERROR", path: "error" },
];

pub async fn run(client: &SynchrotronClient, format: OutputFormat, cmd: ClusterCmd) -> Result<()> {
    match cmd {
        ClusterCmd::List => {
            let v = client.list_clusters().await?;
            output::print(format, &v, CLUSTER_COLUMNS)
        }
        ClusterCmd::Get { name } => {
            let v = client.get_cluster(&name).await?;
            output::print(format, &v, CLUSTER_COLUMNS)
        }
        ClusterCmd::Create(a) => {
            let mut body = serde_json::Map::new();
            body.insert("name".into(), json!(a.name));
            body.insert("auth_source".into(), json!(a.auth_source));
            if let Some(x) = a.kubeconfig_path { body.insert("kubeconfig_path".into(), json!(x)); }
            if let Some(x) = a.context { body.insert("context".into(), json!(x)); }
            if let Some(x) = a.bearer_token { body.insert("bearer_token".into(), json!(x)); }
            let v = client.create_cluster(json!(body)).await?;
            output::print(format, &v, CLUSTER_COLUMNS)
        }
        ClusterCmd::Update(a) => {
            let mut body = serde_json::Map::new();
            if let Some(x) = a.auth_source { body.insert("auth_source".into(), json!(x)); }
            if let Some(x) = a.kubeconfig_path { body.insert("kubeconfig_path".into(), json!(x)); }
            if let Some(x) = a.context { body.insert("context".into(), json!(x)); }
            if let Some(x) = a.bearer_token { body.insert("bearer_token".into(), json!(x)); }
            let v = client.update_cluster(&a.name, json!(body)).await?;
            output::print(format, &v, CLUSTER_COLUMNS)
        }
        ClusterCmd::Delete { name } => {
            client.delete_cluster(&name).await?;
            println!("deleted cluster `{name}`");
            Ok(())
        }
        ClusterCmd::Check { name } => {
            let v = client.check_cluster(&name).await?;
            output::print(format, &v, CHECK_COLUMNS)
        }
    }
}
