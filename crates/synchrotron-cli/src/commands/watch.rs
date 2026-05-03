//! Tail the SSE watch stream for an app and print one event per line.
//!
//! Output shape is one line per event:
//!   `<id>  <event_kind>  <data_json>`
//! In `--output json` mode each line is a full JSON object so the
//! stream is machine-readable. YAML emits a YAML document per event
//! delimited by `---` (standard streaming-YAML convention).

use crate::client::SynchrotronClient;
use crate::output::OutputFormat;
use anyhow::Result;
use serde_json::json;

pub async fn run(
    client: &SynchrotronClient,
    format: OutputFormat,
    app: &str,
    last_event_id: Option<&str>,
) -> Result<()> {
    let mut stream = client.watch_app(app, last_event_id).await?;
    while let Some(msg) = stream.next().await? {
        let parsed: serde_json::Value =
            serde_json::from_str(&msg.data).unwrap_or_else(|_| json!(msg.data));
        let row = json!({
            "id": msg.id,
            "event": msg.event,
            "data": parsed,
        });
        match format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string(&row)?);
            }
            OutputFormat::Yaml => {
                print!("---\n{}", serde_yaml_ng::to_string(&row)?);
            }
            OutputFormat::Table => {
                println!(
                    "{}  {}  {}",
                    msg.id.as_deref().unwrap_or("-"),
                    msg.event.as_deref().unwrap_or("-"),
                    msg.data,
                );
            }
        }
    }
    Ok(())
}
