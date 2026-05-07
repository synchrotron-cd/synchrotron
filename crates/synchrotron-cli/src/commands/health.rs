use crate::client::SynchrotronClient;
use crate::output::{self, Column, OutputFormat};
use anyhow::Result;

pub async fn run(client: &SynchrotronClient, format: OutputFormat) -> Result<()> {
    let value = client.health().await?;
    output::print(
        format,
        &value,
        &[
            Column {
                header: "STATUS",
                path: "status",
            },
            Column {
                header: "VERSION",
                path: "version",
            },
        ],
    )
}
