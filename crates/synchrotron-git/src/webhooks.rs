//! Inbound webhook parsing + HMAC signature verification for the
//! three forges Synchrotron supports: GitHub, GitLab, and Bitbucket.
//!
//! This module is transport-agnostic — it takes raw headers (as a
//! `&[(&str, &str)]`) and the raw request body, and returns a
//! [`ParsedWebhook`] that the HTTP layer feeds into
//! [`RepoTriggers::trigger`]. That split keeps HMAC correctness unit
//! testable without pulling axum into the test tree.
//!
//! Signature verification uses `hmac::Mac::verify_slice`, which is
//! constant-time. GitHub's legacy `X-Hub-Signature` (SHA-1) header is
//! not accepted — we require the SHA-256 variant because GitHub
//! itself has deprecated SHA-1 for new webhooks.

use hmac::{digest::KeyInit, Hmac, Mac};
use sha1::Sha1;
use sha2::Sha256;

use synchrotron_types::RepoUrl;

use crate::error::GitError;
use crate::Result;

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookProvider {
    GitHub,
    GitLab,
    Bitbucket,
}

/// Parsed webhook — what the handler needs to route to a trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedWebhook {
    pub provider: WebhookProvider,
    /// Repo URL as reported by the provider. The HTTP layer resolves
    /// this to a `RepoId` via `RepoId::from_url`.
    pub repo_url: RepoUrl,
    /// The event name (`X-GitHub-Event` / `X-Gitlab-Event` /
    /// `X-Event-Key`). Callers use this to ignore non-push events.
    pub event: String,
}

impl ParsedWebhook {
    /// Whether this payload corresponds to a git push — only those
    /// should trigger a fetch.
    pub fn is_push(&self) -> bool {
        match self.provider {
            WebhookProvider::GitHub => self.event == "push",
            WebhookProvider::GitLab => self.event == "Push Hook",
            WebhookProvider::Bitbucket => self.event == "repo:push",
        }
    }
}

/// Look up a header case-insensitively. Headers are supplied as
/// `&[(name, value)]` to keep this module free of any specific HTTP
/// framework's header type.
fn header<'a>(headers: &'a [(&str, &str)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v)
}

/// Verify a GitHub webhook signature (`X-Hub-Signature-256: sha256=<hex>`)
/// against `body` using the shared `secret`. Rejects missing headers,
/// unknown prefixes, and malformed hex. Constant-time compare.
pub fn verify_github(secret: &[u8], headers: &[(&str, &str)], body: &[u8]) -> Result<()> {
    let sig_header = header(headers, "X-Hub-Signature-256")
        .ok_or_else(|| GitError::InvalidState("missing X-Hub-Signature-256".into()))?;
    let hex_sig = sig_header
        .strip_prefix("sha256=")
        .ok_or_else(|| GitError::InvalidState("unexpected signature prefix".into()))?;
    let sig_bytes = hex::decode(hex_sig)
        .map_err(|e| GitError::InvalidState(format!("malformed signature hex: {e}")))?;

    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| GitError::InvalidState(format!("invalid HMAC key: {e}")))?;
    mac.update(body);
    mac.verify_slice(&sig_bytes)
        .map_err(|_| GitError::InvalidState("signature mismatch".into()))
}

/// Verify a GitLab webhook. GitLab uses a shared token in plaintext
/// under `X-Gitlab-Token` rather than an HMAC — we still
/// constant-time compare to avoid leaking the token length via
/// timing.
pub fn verify_gitlab(secret: &[u8], headers: &[(&str, &str)]) -> Result<()> {
    let token = header(headers, "X-Gitlab-Token")
        .ok_or_else(|| GitError::InvalidState("missing X-Gitlab-Token".into()))?;
    use subtle::ConstantTimeEq;
    if token.as_bytes().ct_eq(secret).into() {
        Ok(())
    } else {
        Err(GitError::InvalidState("signature mismatch".into()))
    }
}

/// Verify a Bitbucket webhook. Bitbucket Cloud doesn't sign by
/// default but Bitbucket Server/Data Center sends
/// `X-Hub-Signature: sha256=<hex>` (same scheme as GitHub's new
/// header, but with the legacy header name). Bitbucket Cloud also
/// supports a plaintext token when configured — we accept either.
pub fn verify_bitbucket(secret: &[u8], headers: &[(&str, &str)], body: &[u8]) -> Result<()> {
    if let Some(sig) = header(headers, "X-Hub-Signature") {
        if let Some(hex_sig) = sig.strip_prefix("sha256=") {
            let sig_bytes = hex::decode(hex_sig)
                .map_err(|e| GitError::InvalidState(format!("malformed signature hex: {e}")))?;
            let mut mac = HmacSha256::new_from_slice(secret)
                .map_err(|e| GitError::InvalidState(format!("invalid HMAC key: {e}")))?;
            mac.update(body);
            return mac
                .verify_slice(&sig_bytes)
                .map_err(|_| GitError::InvalidState("signature mismatch".into()));
        }
        if let Some(hex_sig) = sig.strip_prefix("sha1=") {
            let sig_bytes = hex::decode(hex_sig)
                .map_err(|e| GitError::InvalidState(format!("malformed signature hex: {e}")))?;
            let mut mac = HmacSha1::new_from_slice(secret)
                .map_err(|e| GitError::InvalidState(format!("invalid HMAC key: {e}")))?;
            mac.update(body);
            return mac
                .verify_slice(&sig_bytes)
                .map_err(|_| GitError::InvalidState("signature mismatch".into()));
        }
        return Err(GitError::InvalidState(
            "unexpected Bitbucket signature prefix".into(),
        ));
    }
    Err(GitError::InvalidState(
        "missing X-Hub-Signature for Bitbucket webhook".into(),
    ))
}

/// Parse a GitHub webhook payload. Caller is responsible for having
/// verified the signature first.
pub fn parse_github(headers: &[(&str, &str)], body: &[u8]) -> Result<ParsedWebhook> {
    let event = header(headers, "X-GitHub-Event")
        .ok_or_else(|| GitError::InvalidState("missing X-GitHub-Event".into()))?
        .to_string();
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GitError::InvalidState(format!("malformed JSON: {e}")))?;
    let url = json
        .get("repository")
        .and_then(|r| r.get("clone_url"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| GitError::InvalidState("missing repository.clone_url".into()))?;
    Ok(ParsedWebhook {
        provider: WebhookProvider::GitHub,
        repo_url: RepoUrl(url.into()),
        event,
    })
}

pub fn parse_gitlab(headers: &[(&str, &str)], body: &[u8]) -> Result<ParsedWebhook> {
    let event = header(headers, "X-Gitlab-Event")
        .ok_or_else(|| GitError::InvalidState("missing X-Gitlab-Event".into()))?
        .to_string();
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GitError::InvalidState(format!("malformed JSON: {e}")))?;
    // GitLab push payloads use `repository.git_http_url` or
    // `project.git_http_url`. Prefer `repository` (closer to the
    // actual push target); fall back to `project`.
    let url = json
        .get("repository")
        .and_then(|r| r.get("git_http_url"))
        .and_then(|s| s.as_str())
        .or_else(|| {
            json.get("project")
                .and_then(|p| p.get("git_http_url"))
                .and_then(|s| s.as_str())
        })
        .ok_or_else(|| GitError::InvalidState("missing git_http_url".into()))?;
    Ok(ParsedWebhook {
        provider: WebhookProvider::GitLab,
        repo_url: RepoUrl(url.into()),
        event,
    })
}

pub fn parse_bitbucket(headers: &[(&str, &str)], body: &[u8]) -> Result<ParsedWebhook> {
    // Bitbucket Cloud uses `X-Event-Key`; Server uses `X-Event-Key`
    // as well (same header name, different event strings).
    let event = header(headers, "X-Event-Key")
        .ok_or_else(|| GitError::InvalidState("missing X-Event-Key".into()))?
        .to_string();
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| GitError::InvalidState(format!("malformed JSON: {e}")))?;
    // Bitbucket Cloud: repository.links.clone[?name=https].href
    let cloud_url = json
        .get("repository")
        .and_then(|r| r.get("links"))
        .and_then(|l| l.get("clone"))
        .and_then(|c| c.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|entry| {
                let name = entry.get("name").and_then(|s| s.as_str())?;
                if name == "https" || name == "http" {
                    entry
                        .get("href")
                        .and_then(|s| s.as_str())
                        .map(str::to_string)
                } else {
                    None
                }
            })
        });
    let url = cloud_url
        .or_else(|| {
            // Bitbucket Server flattened link form.
            json.get("repository")
                .and_then(|r| r.get("links"))
                .and_then(|l| l.get("self"))
                .and_then(|s| s.as_array())
                .and_then(|arr| arr.first())
                .and_then(|entry| entry.get("href"))
                .and_then(|s| s.as_str())
                .map(str::to_string)
        })
        .ok_or_else(|| GitError::InvalidState("could not extract Bitbucket clone URL".into()))?;
    Ok(ParsedWebhook {
        provider: WebhookProvider::Bitbucket,
        repo_url: RepoUrl(url),
        event,
    })
}
