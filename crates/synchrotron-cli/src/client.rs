use anyhow::{Context, Result};
use reqwest::Client;

pub struct SynchrotronClient {
    base_url: String,
    client: Client,
}

impl SynchrotronClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    pub async fn health(&self) -> Result<serde_json::Value> {
        let resp = self
            .client
            .get(format!("{}/api/v1/health", self.base_url))
            .send()
            .await
            .context("connecting to server")?
            .error_for_status()
            .context("health check failed")?
            .json()
            .await
            .context("parsing response")?;
        Ok(resp)
    }
}
