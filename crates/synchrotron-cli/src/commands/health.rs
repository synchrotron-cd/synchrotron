use crate::client::SynchrotronClient;
use anyhow::Result;

pub async fn run(client: &SynchrotronClient) -> Result<()> {
    let health = client.health().await?;
    println!("{}", serde_json::to_string_pretty(&health)?);
    Ok(())
}
