//! Thin REST/SSE client for the Synchrotron-CD server.
//!
//! Every method is a one-shot HTTP call returning [`serde_json::Value`]
//! — no typed DTO mirroring of the server crate, since the CLI is
//! meant to be a thin wrapper that follows the OpenAPI contract.
//!
//! ## Error mapping
//!
//! Non-2xx responses are surfaced as [`anyhow::Error`]s carrying the
//! status code plus the server's JSON `error.message` when present
//! (the public REST API consistently returns `{"error":{...}}`).

use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, Method, Response, StatusCode};
use serde_json::Value;

pub struct SynchrotronClient {
    base_url: String,
    client: Client,
    token: Option<String>,
}

impl SynchrotronClient {
    pub fn new(base_url: &str, token: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
            token,
        }
    }

    fn auth_headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(t) = &self.token {
            if let Ok(v) = HeaderValue::from_str(&format!("Bearer {t}")) {
                h.insert(AUTHORIZATION, v);
            }
        }
        h
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn json_call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut req = self
            .client
            .request(method, self.url(path))
            .headers(self.auth_headers());
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("HTTP request to {path} failed"))?;
        decode_json(resp).await
    }

    pub async fn health(&self) -> Result<Value> {
        self.json_call(Method::GET, "/api/v1/health", None).await
    }

    // --- Apps ---

    pub async fn list_apps(&self) -> Result<Value> {
        self.json_call(Method::GET, "/api/v1/apps", None).await
    }
    pub async fn get_app(&self, name: &str) -> Result<Value> {
        self.json_call(Method::GET, &format!("/api/v1/apps/{name}"), None)
            .await
    }
    pub async fn create_app(&self, body: Value) -> Result<Value> {
        self.json_call(Method::POST, "/api/v1/apps", Some(body)).await
    }
    pub async fn update_app(&self, name: &str, body: Value) -> Result<Value> {
        self.json_call(Method::PUT, &format!("/api/v1/apps/{name}"), Some(body))
            .await
    }
    pub async fn delete_app(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/apps/{name}")))
            .headers(self.auth_headers())
            .send()
            .await?;
        ensure_ok(resp).await?;
        Ok(())
    }
    pub async fn sync_app(&self, name: &str) -> Result<Value> {
        self.json_call(Method::POST, &format!("/api/v1/apps/{name}/sync"), None)
            .await
    }
    pub async fn diff_app(&self, name: &str) -> Result<Value> {
        self.json_call(Method::POST, &format!("/api/v1/apps/{name}/diff"), None)
            .await
    }
    pub async fn rollback_app(&self, name: &str, revision_id: i64) -> Result<Value> {
        self.json_call(
            Method::POST,
            &format!("/api/v1/apps/{name}/rollback"),
            Some(serde_json::json!({"revision_id": revision_id})),
        )
        .await
    }
    pub async fn history_app(&self, name: &str) -> Result<Value> {
        self.json_call(Method::GET, &format!("/api/v1/apps/{name}/history"), None)
            .await
    }

    // --- Clusters ---

    pub async fn list_clusters(&self) -> Result<Value> {
        self.json_call(Method::GET, "/api/v1/clusters", None).await
    }
    pub async fn get_cluster(&self, name: &str) -> Result<Value> {
        self.json_call(Method::GET, &format!("/api/v1/clusters/{name}"), None)
            .await
    }
    pub async fn create_cluster(&self, body: Value) -> Result<Value> {
        self.json_call(Method::POST, "/api/v1/clusters", Some(body))
            .await
    }
    pub async fn update_cluster(&self, name: &str, body: Value) -> Result<Value> {
        self.json_call(
            Method::PUT,
            &format!("/api/v1/clusters/{name}"),
            Some(body),
        )
        .await
    }
    pub async fn delete_cluster(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/clusters/{name}")))
            .headers(self.auth_headers())
            .send()
            .await?;
        ensure_ok(resp).await?;
        Ok(())
    }
    pub async fn check_cluster(&self, name: &str) -> Result<Value> {
        self.json_call(
            Method::POST,
            &format!("/api/v1/clusters/{name}/check"),
            None,
        )
        .await
    }

    // --- Repos ---

    pub async fn list_repos(&self) -> Result<Value> {
        self.json_call(Method::GET, "/api/v1/repos", None).await
    }
    pub async fn get_repo(&self, name: &str) -> Result<Value> {
        self.json_call(Method::GET, &format!("/api/v1/repos/{name}"), None)
            .await
    }
    pub async fn create_repo(&self, body: Value) -> Result<Value> {
        self.json_call(Method::POST, "/api/v1/repos", Some(body))
            .await
    }
    pub async fn update_repo(&self, name: &str, body: Value) -> Result<Value> {
        self.json_call(Method::PUT, &format!("/api/v1/repos/{name}"), Some(body))
            .await
    }
    pub async fn delete_repo(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/repos/{name}")))
            .headers(self.auth_headers())
            .send()
            .await?;
        ensure_ok(resp).await?;
        Ok(())
    }

    // --- Watch (SSE) ---

    /// Open the SSE stream for `app`. The returned [`SseStream`]
    /// yields `(event, id, data)` triples — one per SSE message.
    pub async fn watch_app(&self, app: &str, last_event_id: Option<&str>) -> Result<SseStream> {
        let mut req = self
            .client
            .get(self.url(&format!("/api/v1/apps/{app}/watch")))
            .headers(self.auth_headers())
            .header("accept", "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("last-event-id", id);
        }
        let resp = req.send().await.context("opening watch stream")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let msg = resp.text().await.unwrap_or_default();
            return Err(anyhow!("watch failed: {status} {msg}"));
        }
        Ok(SseStream::new(resp))
    }
}

/// One SSE message as parsed by the CLI.
#[derive(Debug, Clone, Default)]
pub struct SseMessage {
    pub event: Option<String>,
    pub id: Option<String>,
    pub data: String,
}

/// Streaming SSE parser. Builds messages line-by-line and emits one
/// per blank-line delimiter, per the EventSource spec.
pub struct SseStream {
    body: std::pin::Pin<
        Box<dyn futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>,
    >,
    buf: String,
    pending: SseMessage,
}

impl SseStream {
    fn new(resp: Response) -> Self {
        Self {
            body: Box::pin(resp.bytes_stream()),
            buf: String::new(),
            pending: SseMessage::default(),
        }
    }

    /// Pull the next SSE message. Returns `Ok(None)` on stream end.
    pub async fn next(&mut self) -> Result<Option<SseMessage>> {
        loop {
            // Drain any complete lines already in the buffer.
            while let Some(idx) = self.buf.find('\n') {
                let line = self.buf[..idx].trim_end_matches('\r').to_string();
                self.buf.drain(..=idx);
                if line.is_empty() {
                    if !self.pending.data.is_empty()
                        || self.pending.event.is_some()
                        || self.pending.id.is_some()
                    {
                        let msg = std::mem::take(&mut self.pending);
                        return Ok(Some(msg));
                    }
                    continue;
                }
                if let Some(rest) = line.strip_prefix(':') {
                    // Comment / keep-alive line. Some servers emit
                    // `: keepalive` or similar — ignore.
                    let _ = rest;
                    continue;
                }
                let (field, value) = match line.split_once(':') {
                    Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
                    None => (line.as_str(), ""),
                };
                match field {
                    "event" => self.pending.event = Some(value.to_string()),
                    "id" => self.pending.id = Some(value.to_string()),
                    "data" => {
                        if !self.pending.data.is_empty() {
                            self.pending.data.push('\n');
                        }
                        self.pending.data.push_str(value);
                    }
                    _ => {}
                }
            }

            match self.body.next().await {
                Some(Ok(chunk)) => {
                    self.buf.push_str(&String::from_utf8_lossy(&chunk));
                }
                Some(Err(e)) => return Err(anyhow!("SSE stream error: {e}")),
                None => return Ok(None),
            }
        }
    }
}

async fn ensure_ok(resp: Response) -> Result<Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    Err(format_error(status, resp.text().await.unwrap_or_default()))
}

async fn decode_json(resp: Response) -> Result<Value> {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format_error(status, text));
    }
    if text.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("parsing JSON response: {text}"))
}

fn format_error(status: StatusCode, body: String) -> anyhow::Error {
    let parsed: Option<Value> = serde_json::from_str(&body).ok();
    let msg = parsed
        .as_ref()
        .and_then(|v| v.get("error"))
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(String::from)
        .unwrap_or(body);
    anyhow!("server returned {status}: {msg}")
}
