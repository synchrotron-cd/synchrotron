use crate::client::SynchrotronClient;
use crate::output::{self, Column, OutputFormat};
use anyhow::Result;

const SYNC_COLUMNS: &[Column] = &[
    Column { header: "APP", path: "app" },
    Column { header: "STATUS", path: "status" },
];

const ROLLBACK_COLUMNS: &[Column] = &[
    Column { header: "APP", path: "app" },
    Column { header: "REVISION", path: "revision_id" },
    Column { header: "COMMIT", path: "commit_hash" },
    Column { header: "MANIFESTS", path: "manifest_count" },
    Column { header: "STATUS", path: "status" },
];

pub async fn sync(client: &SynchrotronClient, format: OutputFormat, app: &str) -> Result<()> {
    let v = client.sync_app(app).await?;
    output::print(format, &v, SYNC_COLUMNS)
}

pub async fn diff(client: &SynchrotronClient, format: OutputFormat, app: &str) -> Result<()> {
    let v = client.diff_app(app).await?;
    // Diff payload is free-form; defer to JSON-style rendering even
    // in `table` mode by always pretty-printing JSON for now. The
    // server's diff response shape is still being finalized.
    let fmt = match format {
        OutputFormat::Table => OutputFormat::Json,
        other => other,
    };
    output::print(fmt, &v, &[])
}

pub async fn rollback(
    client: &SynchrotronClient,
    format: OutputFormat,
    app: &str,
    revision_id: i64,
) -> Result<()> {
    let v = client.rollback_app(app, revision_id).await?;
    output::print(format, &v, ROLLBACK_COLUMNS)
}
