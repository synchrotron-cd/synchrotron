use crate::client::SynchrotronClient;
use crate::output::{self, Column, OutputFormat};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::json;

#[derive(Subcommand)]
pub enum RepoCmd {
    List,
    Get { name: String },
    Create(CreateArgs),
    Update(UpdateArgs),
    Delete { name: String },
}

#[derive(Args)]
pub struct CreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub branch: Option<String>,
    /// Pointer into an external secret store (e.g. `vault://...`)
    #[arg(long)]
    pub credentials_secret_ref: Option<String>,
    /// Inline password. Persisted server-side; never echoed back.
    #[arg(long, env = "SYNCHROTRON_REPO_PASSWORD")]
    pub password: Option<String>,
}

#[derive(Args)]
pub struct UpdateArgs {
    pub name: String,
    #[arg(long)]
    pub url: Option<String>,
    #[arg(long)]
    pub branch: Option<String>,
    #[arg(long)]
    pub credentials_secret_ref: Option<String>,
    #[arg(long, env = "SYNCHROTRON_REPO_PASSWORD")]
    pub password: Option<String>,
}

const REPO_COLUMNS: &[Column] = &[
    Column {
        header: "NAME",
        path: "name",
    },
    Column {
        header: "URL",
        path: "url",
    },
    Column {
        header: "BRANCH",
        path: "branch",
    },
    Column {
        header: "SECRET",
        path: "credentials_secret_ref",
    },
    Column {
        header: "PASSWORD",
        path: "has_password",
    },
];

pub async fn run(client: &SynchrotronClient, format: OutputFormat, cmd: RepoCmd) -> Result<()> {
    match cmd {
        RepoCmd::List => {
            let v = client.list_repos().await?;
            output::print(format, &v, REPO_COLUMNS)
        }
        RepoCmd::Get { name } => {
            let v = client.get_repo(&name).await?;
            output::print(format, &v, REPO_COLUMNS)
        }
        RepoCmd::Create(a) => {
            let mut body = serde_json::Map::new();
            body.insert("name".into(), json!(a.name));
            body.insert("url".into(), json!(a.url));
            if let Some(x) = a.branch {
                body.insert("branch".into(), json!(x));
            }
            if let Some(x) = a.credentials_secret_ref {
                body.insert("credentials_secret_ref".into(), json!(x));
            }
            if let Some(x) = a.password {
                body.insert("password".into(), json!(x));
            }
            let v = client.create_repo(json!(body)).await?;
            output::print(format, &v, REPO_COLUMNS)
        }
        RepoCmd::Update(a) => {
            let mut body = serde_json::Map::new();
            if let Some(x) = a.url {
                body.insert("url".into(), json!(x));
            }
            if let Some(x) = a.branch {
                body.insert("branch".into(), json!(x));
            }
            if let Some(x) = a.credentials_secret_ref {
                body.insert("credentials_secret_ref".into(), json!(x));
            }
            if let Some(x) = a.password {
                body.insert("password".into(), json!(x));
            }
            let v = client.update_repo(&a.name, json!(body)).await?;
            output::print(format, &v, REPO_COLUMNS)
        }
        RepoCmd::Delete { name } => {
            client.delete_repo(&name).await?;
            println!("deleted repo `{name}`");
            Ok(())
        }
    }
}
