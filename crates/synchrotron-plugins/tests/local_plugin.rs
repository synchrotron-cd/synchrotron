//! Integration tests for the local plugin runtime. Each test spawns
//! the in-crate `synchrotron-stub-plugin` binary with an env var that
//! selects its behavior, then drives it through the public API.

use std::path::PathBuf;
use std::time::Duration;

use synchrotron_plugins::local::{Plugin, PluginError, PluginSpec, RenderRequest};

fn stub_path() -> PathBuf {
    env!("CARGO_BIN_EXE_synchrotron-stub-plugin").into()
}

fn spec(mode: &str, timeout: Duration) -> PluginSpec {
    PluginSpec {
        name: format!("stub-{mode}"),
        command: stub_path(),
        args: vec![],
        env: vec![("STUB_MODE".into(), mode.into())],
        workdir: None,
        timeout,
    }
}

fn render_req() -> RenderRequest {
    RenderRequest {
        source_path: PathBuf::from("/does/not/matter"),
        params: serde_json::json!({}),
    }
}

#[tokio::test]
async fn handshake_and_render_happy_path() {
    let mut plugin = Plugin::spawn(spec("ok", Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(plugin.info.plugin_version, "stub-0.1");

    let resp = plugin.render(&render_req()).await.unwrap();
    assert_eq!(resp.manifests.len(), 1);
    assert!(resp.manifests[0].contains("ConfigMap"));

    plugin.shutdown().await.unwrap();
}

#[tokio::test]
async fn render_timeout_returns_timeout_error() {
    let mut plugin = Plugin::spawn(spec("slow_render", Duration::from_millis(200)))
        .await
        .unwrap();
    let err = plugin.render(&render_req()).await.unwrap_err();
    assert!(matches!(err, PluginError::Timeout { .. }), "got {err:?}");
}

#[tokio::test]
async fn render_crash_returns_early_exit() {
    let mut plugin = Plugin::spawn(spec("crash_on_render", Duration::from_secs(5)))
        .await
        .unwrap();
    let err = plugin.render(&render_req()).await.unwrap_err();
    assert!(matches!(err, PluginError::EarlyExit { .. }), "got {err:?}");
}

#[tokio::test]
async fn malformed_response_is_protocol_error() {
    let mut plugin = Plugin::spawn(spec("malformed_render", Duration::from_secs(5)))
        .await
        .unwrap();
    let err = plugin.render(&render_req()).await.unwrap_err();
    assert!(matches!(err, PluginError::Protocol { .. }), "got {err:?}");
}

#[tokio::test]
async fn plugin_side_error_is_surfaced() {
    let mut plugin = Plugin::spawn(spec("plugin_error", Duration::from_secs(5)))
        .await
        .unwrap();
    let err = plugin.render(&render_req()).await.unwrap_err();
    match err {
        PluginError::PluginSide { code, message, .. } => {
            assert_eq!(code, -32000);
            assert_eq!(message, "render failed");
        }
        other => panic!("expected PluginSide, got {other:?}"),
    }
}

#[tokio::test]
async fn stderr_is_captured() {
    // Prove stderr piping doesn't block the handshake. Content
    // verification would require a tracing subscriber adapter, which
    // is overkill here — the acceptance is "no deadlock, no panic".
    let mut spec = spec("ok", Duration::from_secs(5));
    spec.env
        .push(("STUB_STDERR".into(), "hello from stub".into()));
    let mut plugin = Plugin::spawn(spec).await.unwrap();
    let _ = plugin.render(&render_req()).await.unwrap();
    plugin.shutdown().await.unwrap();
}
