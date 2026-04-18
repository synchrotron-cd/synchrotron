//! Tests for the exec credential plugin cache.
//!
//! The subprocess invocation is stubbed via a [`Runner`] for the
//! cache-behaviour tests. One end-to-end test uses /bin/sh to prove
//! the production [`tokio_command_runner`] correctly forks, captures
//! stdout, and threads through env vars.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use synchrotron_kube::{
    tokio_command_runner, ExecCredentialCache, ExecPluginConfig, KubeError, Runner,
};

fn cred_json(token: &str, exp: Option<DateTime<Utc>>) -> String {
    match exp {
        Some(t) => format!(
            r#"{{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{{"token":"{token}","expirationTimestamp":"{}"}}}}"#,
            t.to_rfc3339(),
        ),
        None => format!(
            r#"{{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{{"token":"{token}"}}}}"#,
        ),
    }
}

fn counting_runner(responses: Arc<Vec<String>>, calls: Arc<AtomicUsize>) -> Runner {
    Arc::new(move |_cfg| {
        let responses = responses.clone();
        let calls = calls.clone();
        Box::pin(async move {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            Ok(responses[n.min(responses.len() - 1)].clone())
        })
    })
}

#[tokio::test]
async fn caches_token_until_within_margin() {
    let now = SystemTime::now();
    let exp: DateTime<Utc> = (now + Duration::from_secs(600)).into();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = counting_runner(Arc::new(vec![cred_json("tkn-1", Some(exp))]), calls.clone());
    let cache = ExecCredentialCache::new(
        ExecPluginConfig::new("ignored"),
        runner,
        Duration::from_secs(60),
    );

    assert_eq!(cache.token_at(now).await.unwrap(), "tkn-1");
    assert_eq!(cache.token_at(now).await.unwrap(), "tkn-1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refreshes_when_within_margin() {
    let now = SystemTime::now();
    let near_exp: DateTime<Utc> = (now + Duration::from_secs(30)).into();
    let far_exp: DateTime<Utc> = (now + Duration::from_secs(3600)).into();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = counting_runner(
        Arc::new(vec![
            cred_json("tkn-1", Some(near_exp)),
            cred_json("tkn-2", Some(far_exp)),
        ]),
        calls.clone(),
    );
    let cache = ExecCredentialCache::new(
        ExecPluginConfig::new("ignored"),
        runner,
        Duration::from_secs(60),
    );

    assert_eq!(cache.token_at(now).await.unwrap(), "tkn-1");
    // Same `now`, but near_exp - 60s margin already past now → refresh.
    assert_eq!(cache.token_at(now).await.unwrap(), "tkn-2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn missing_expiration_disables_caching() {
    let now = SystemTime::now();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = counting_runner(
        Arc::new(vec![cred_json("tkn-1", None), cred_json("tkn-2", None)]),
        calls.clone(),
    );
    let cache = ExecCredentialCache::new(
        ExecPluginConfig::new("ignored"),
        runner,
        Duration::from_secs(60),
    );
    let _ = cache.token_at(now).await.unwrap();
    let _ = cache.token_at(now).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn empty_token_is_rejected() {
    let now = SystemTime::now();
    let runner: Runner = Arc::new(|_cfg| Box::pin(async move { Ok(cred_json("", None)) }));
    let cache = ExecCredentialCache::new(
        ExecPluginConfig::new("ignored"),
        runner,
        Duration::from_secs(60),
    );
    let err = cache.token_at(now).await.unwrap_err();
    assert!(matches!(err, KubeError::ExecPlugin(_)), "got: {err:?}");
}

#[tokio::test]
async fn malformed_json_is_rejected() {
    let now = SystemTime::now();
    let runner: Runner =
        Arc::new(|_cfg| Box::pin(async move { Ok("not json at all".to_string()) }));
    let cache = ExecCredentialCache::new(
        ExecPluginConfig::new("ignored"),
        runner,
        Duration::from_secs(60),
    );
    let err = cache.token_at(now).await.unwrap_err();
    assert!(matches!(err, KubeError::ExecPlugin(_)), "got: {err:?}");
}

/// End-to-end check that the production runner actually spawns a
/// subprocess, captures stdout, and threads env vars through.
#[tokio::test]
async fn tokio_runner_executes_real_subprocess() {
    if !std::path::Path::new("/bin/sh").exists() {
        return; // skip on non-unix
    }
    let mut cfg = ExecPluginConfig::new("/bin/sh");
    cfg.args = vec!["-c".into(), "printf '%s' \"$TOKEN_BODY\"".into()];
    let body = cred_json("from-subproc", None);
    cfg.env.insert("TOKEN_BODY".into(), body.clone());

    let runner = tokio_command_runner();
    let cache = ExecCredentialCache::new(cfg, runner, Duration::from_secs(60));
    assert_eq!(cache.token().await.unwrap(), "from-subproc");
}

#[tokio::test]
async fn tokio_runner_propagates_nonzero_exit() {
    if !std::path::Path::new("/bin/sh").exists() {
        return;
    }
    let mut cfg = ExecPluginConfig::new("/bin/sh");
    cfg.args = vec!["-c".into(), "echo broken >&2; exit 7".into()];
    let runner = tokio_command_runner();
    let cache = ExecCredentialCache::new(cfg, runner, Duration::from_secs(60));
    let err = cache.token().await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("exited"), "expected exit info, got: {msg}");
}
