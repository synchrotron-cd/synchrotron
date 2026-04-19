//! Helm local plugin binary. Speaks the JSON-RPC 2.0 protocol
//! defined by `synchrotron_plugins::local` over stdin/stdout.
//!
//! Logs go to stderr so the host's stderr capture surfaces them in
//! synchrotron's tracing pipeline.

use std::io::{BufRead, Write};
use std::path::PathBuf;

use serde::Deserialize;
use synchrotron_helm_plugin::{HelmError, HelmRunner, RenderParams, PLUGIN_VERSION};
use tracing_subscriber::EnvFilter;

// JSON-RPC error codes. -32000..-32099 is the "server error" range
// reserved for application-defined errors; we pick a fixed code so
// callers can distinguish helm failures from protocol errors.
const CODE_HELM_FAILURE: i64 = -32001;

#[derive(Debug, Deserialize)]
struct HostRenderParams {
    source_path: PathBuf,
    params: RenderParams,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let runner = HelmRunner::default();
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(req) = serde_json::from_str::<serde_json::Value>(&line) else {
            // Malformed JSON is ignored; nothing useful to reply to.
            continue;
        };
        let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);

        match method {
            "initialize" => {
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "plugin_version": PLUGIN_VERSION }
                });
                write_line(&mut stdout, &resp);
            }
            "render" => {
                let params = req
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let resp = match serde_json::from_value::<HostRenderParams>(params) {
                    Ok(p) => run_render(&runner, p, id),
                    Err(e) => error_response(id, CODE_HELM_FAILURE, format!("bad params: {e}")),
                };
                write_line(&mut stdout, &resp);
            }
            "shutdown" => {
                std::process::exit(0);
            }
            _ => {}
        }
    }
}

fn run_render(
    runner: &HelmRunner,
    p: HostRenderParams,
    id: serde_json::Value,
) -> serde_json::Value {
    match runner.render(&p.source_path, &p.params) {
        Ok(rendered) => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "manifests": [rendered] }
        }),
        Err(e) => {
            let message = match &e {
                HelmError::BinaryNotFound { .. } => format!("{e}"),
                HelmError::NonZeroExit { stderr, .. } => format!("{e}\nstderr tail: {stderr}"),
                _ => format!("{e}"),
            };
            error_response(id, CODE_HELM_FAILURE, message)
        }
    }
}

fn error_response(id: serde_json::Value, code: i64, message: String) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

fn write_line(stdout: &mut std::io::Stdout, value: &serde_json::Value) {
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}
