//! End-to-end tests for the Helm plugin binary. Drives the process
//! through its JSON-RPC protocol, not through `synchrotron-plugins`,
//! to keep this crate's test dependencies minimal.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

fn plugin_bin() -> PathBuf {
    env!("CARGO_BIN_EXE_synchrotron-helm-plugin").into()
}

struct PluginProc {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PluginProc {
    fn spawn(helm_binary: Option<&str>) -> Self {
        let mut cmd = Command::new(plugin_bin());
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(h) = helm_binary {
            cmd.env("HELM_BINARY", h);
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn rpc(&mut self, req: serde_json::Value) -> serde_json::Value {
        writeln!(self.stdin, "{req}").unwrap();
        self.stdin.flush().unwrap();
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    fn shutdown(mut self) {
        let _ = writeln!(
            self.stdin,
            "{}",
            serde_json::json!({"jsonrpc":"2.0","method":"shutdown"})
        );
        let _ = self.stdin.flush();
        let _ = self.child.wait();
    }
}

fn helm_installed() -> bool {
    Command::new("helm")
        .arg("version")
        .arg("--short")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn write_fixture_chart(root: &Path) {
    std::fs::write(
        root.join("Chart.yaml"),
        "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
    )
    .unwrap();
    std::fs::create_dir_all(root.join("templates")).unwrap();
    std::fs::write(
        root.join("templates/configmap.yaml"),
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  key: {{ .Values.key | default \"default\" | quote }}\n",
    )
    .unwrap();
    std::fs::write(root.join("values.yaml"), "key: from-values\n").unwrap();
}

#[test]
fn handshake_reports_plugin_version() {
    let mut p = PluginProc::spawn(None);
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": { "protocol_version": 1 }
    }));
    assert_eq!(resp["id"], 1);
    assert!(resp["result"]["plugin_version"].is_string());
    p.shutdown();
}

#[test]
fn missing_helm_binary_reports_error() {
    let mut p = PluginProc::spawn(Some("/nonexistent/helm-xyz"));
    let _ = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": { "protocol_version": 1 }
    }));

    let tmp = tempdir();
    write_fixture_chart(tmp.path());
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "render",
        "params": {
            "source_path": tmp.path(),
            "params": { "release_name": "demo" }
        }
    }));
    assert!(resp["error"].is_object(), "got {resp}");
    let message = resp["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("helm not found") || message.contains("not found"),
        "unexpected error message: {message}"
    );
    p.shutdown();
}

#[test]
fn renders_sample_chart_if_helm_available() {
    if !helm_installed() {
        eprintln!("helm not on PATH; skipping");
        return;
    }
    let mut p = PluginProc::spawn(None);
    let _ = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": { "protocol_version": 1 }
    }));

    let tmp = tempdir();
    write_fixture_chart(tmp.path());
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "render",
        "params": {
            "source_path": tmp.path(),
            "params": {
                "release_name": "demo",
                "values_files": ["values.yaml"],
                "set_values": { "key": "overridden" }
            }
        }
    }));
    let manifests = resp["result"]["manifests"]
        .as_array()
        .unwrap_or_else(|| panic!("no manifests in {resp}"));
    assert_eq!(manifests.len(), 1);
    let rendered = manifests[0].as_str().unwrap();
    assert!(rendered.contains("demo-cm"), "got: {rendered}");
    assert!(rendered.contains("overridden"), "got: {rendered}");
    p.shutdown();
}

// Tiny temp-dir helper to avoid adding tempfile as a dev-dep.
struct TempDir(PathBuf);
impl TempDir {
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn tempdir() -> TempDir {
    let mut p = std::env::temp_dir();
    p.push(format!("synchrotron-helm-it-{}", unique()));
    std::fs::create_dir_all(&p).unwrap();
    TempDir(p)
}
fn unique() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
