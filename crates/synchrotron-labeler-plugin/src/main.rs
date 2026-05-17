//! Demo local plugin that decorates every rendered manifest with a
//! set of labels supplied via params. Useful as a worked example of
//! the JSON-RPC plugin protocol for anyone writing their own.
//!
//! Protocol: identical to `synchrotron-helm-plugin` — see
//! `synchrotron_plugins::local` for the wire format. The host calls
//! `initialize` once, then `render` per app, then `shutdown`.
//!
//! Render params (object passed as `params.params`):
//! ```yaml
//! plugin:
//!   name: labeler
//!   parameters:
//!     - { name: team, value: platform }
//!     - { name: env,  value: prod }
//! ```
//! Each `name/value` pair becomes a label whose key is namespaced
//! under `synchrotron.io/` so it doesn't collide with anything the
//! manifests already carry.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use walkdir::WalkDir;

const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
const LABEL_PREFIX: &str = "synchrotron.io/";
const CODE_LABELER_FAILURE: i64 = -32002;

#[derive(Debug, Deserialize)]
struct HostRenderParams {
    source_path: PathBuf,
    #[serde(default)]
    params: serde_json::Value,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(req) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);

        match method {
            "initialize" => {
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "plugin_version": PLUGIN_VERSION },
                });
                write_line(&mut stdout, &resp);
            }
            "render" => {
                let params = req.get("params").cloned().unwrap_or_default();
                let resp = match serde_json::from_value::<HostRenderParams>(params) {
                    Ok(p) => run_render(p, id),
                    Err(e) => error_response(id, CODE_LABELER_FAILURE, format!("bad params: {e}")),
                };
                write_line(&mut stdout, &resp);
            }
            "shutdown" => std::process::exit(0),
            _ => {}
        }
    }
}

fn run_render(p: HostRenderParams, id: serde_json::Value) -> serde_json::Value {
    match label_directory(&p.source_path, &p.params) {
        Ok(rendered) => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "manifests": [rendered] },
        }),
        Err(e) => error_response(id, CODE_LABELER_FAILURE, e),
    }
}

fn label_directory(source: &Path, raw_params: &serde_json::Value) -> Result<String, String> {
    let labels = collect_labels(raw_params);
    let mut docs: Vec<serde_yaml_ng::Value> = Vec::new();
    let walker = WalkDir::new(source)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            matches!(
                e.path().extension().and_then(|s| s.to_str()),
                Some("yaml") | Some("yml")
            )
        });
    for entry in walker {
        let text = std::fs::read_to_string(entry.path())
            .map_err(|e| format!("read {}: {e}", entry.path().display()))?;
        for doc in serde_yaml_ng::Deserializer::from_str(&text) {
            let mut value = serde_yaml_ng::Value::deserialize(doc)
                .map_err(|e| format!("parse {}: {e}", entry.path().display()))?;
            apply_labels(&mut value, &labels);
            // Drop YAML null documents (trailing `---` separators)
            // so the host doesn't see noise it has to filter.
            if !value.is_null() {
                docs.push(value);
            }
        }
    }
    let mut out = String::new();
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            out.push_str("---\n");
        }
        out.push_str(
            &serde_yaml_ng::to_string(doc).map_err(|e| format!("serialize manifest: {e}"))?,
        );
    }
    Ok(out)
}

/// Extract `parameters: [{name, value}]` from the host's PluginRef
/// params and turn them into `synchrotron.io/<name> = value` labels.
fn collect_labels(raw: &serde_json::Value) -> Vec<(String, String)> {
    let obj = match raw {
        serde_json::Value::Object(m) => m,
        _ => return Vec::new(),
    };
    obj.iter()
        .filter_map(|(k, v)| {
            v.as_str()
                .map(|s| (format!("{LABEL_PREFIX}{k}"), s.to_string()))
        })
        .collect()
}

/// Merge `labels` into `value.metadata.labels`, creating the
/// intermediate maps if missing. Resources without a `metadata`
/// field are left alone (List / RawExtension shapes).
fn apply_labels(value: &mut serde_yaml_ng::Value, labels: &[(String, String)]) {
    use serde_yaml_ng::Value;
    let Value::Mapping(top) = value else { return };
    let metadata = top
        .entry(Value::from("metadata"))
        .or_insert_with(|| Value::Mapping(Default::default()));
    let Value::Mapping(meta_map) = metadata else {
        return;
    };
    let labels_entry = meta_map
        .entry(Value::from("labels"))
        .or_insert_with(|| Value::Mapping(Default::default()));
    let Value::Mapping(labels_map) = labels_entry else {
        return;
    };
    for (k, v) in labels {
        labels_map.insert(Value::from(k.as_str()), Value::from(v.as_str()));
    }
}

fn error_response(id: serde_json::Value, code: i64, message: String) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

fn write_line(stdout: &mut std::io::Stdout, value: &serde_json::Value) {
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn labels_added_under_synchrotron_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = std::fs::File::create(dir.path().join("cm.yaml")).unwrap();
        writeln!(
            f,
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: hello\n  namespace: default\ndata:\n  key: value\n"
        )
        .unwrap();
        let params = serde_json::json!({"team": "platform", "env": "prod"});
        let out = label_directory(dir.path(), &params).unwrap();
        assert!(out.contains("synchrotron.io/team: platform"));
        assert!(out.contains("synchrotron.io/env: prod"));
        // Existing fields preserved.
        assert!(out.contains("name: hello"));
        assert!(out.contains("key: value"));
    }

    #[test]
    fn multi_doc_yaml_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = std::fs::File::create(dir.path().join("two.yaml")).unwrap();
        writeln!(
            f,
            "apiVersion: v1\nkind: ConfigMap\nmetadata: {{name: a}}\n---\napiVersion: v1\nkind: Service\nmetadata: {{name: b}}\n"
        )
        .unwrap();
        let out = label_directory(dir.path(), &serde_json::json!({"team": "platform"})).unwrap();
        assert_eq!(out.matches("synchrotron.io/team: platform").count(), 2);
    }
}
