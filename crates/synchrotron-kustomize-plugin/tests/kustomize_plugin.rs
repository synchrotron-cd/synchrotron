//! End-to-end tests for the kustomize plugin binary.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

fn plugin_bin() -> PathBuf {
    env!("CARGO_BIN_EXE_synchrotron-kustomize-plugin").into()
}

struct PluginProc {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PluginProc {
    fn spawn(kustomize_binary: Option<&str>) -> Self {
        let mut cmd = Command::new(plugin_bin());
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(k) = kustomize_binary {
            cmd.env("KUSTOMIZE_BINARY", k);
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

fn kustomize_installed() -> bool {
    Command::new("kustomize")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn write_fixture_overlay(root: &Path) {
    // Base directory with a single ConfigMap.
    std::fs::create_dir_all(root.join("base")).unwrap();
    std::fs::write(
        root.join("base/kustomization.yaml"),
        "resources:\n  - configmap.yaml\n",
    )
    .unwrap();
    std::fs::write(
        root.join("base/configmap.yaml"),
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-config\ndata:\n  env: base\n",
    )
    .unwrap();
    // Prod overlay adds a name prefix.
    std::fs::create_dir_all(root.join("overlays/prod")).unwrap();
    std::fs::write(
        root.join("overlays/prod/kustomization.yaml"),
        "resources:\n  - ../../base\nnamePrefix: prod-\n",
    )
    .unwrap();
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
fn rejects_remote_base_by_default() {
    let mut p = PluginProc::spawn(Some("/nonexistent/kustomize"));
    let _ = p.rpc(serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocol_version": 1 }
    }));

    let tmp = tempdir();
    std::fs::write(
        tmp.path().join("kustomization.yaml"),
        "resources:\n  - github.com/acme/base?ref=v1\n",
    )
    .unwrap();
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "render",
        "params": {
            "source_path": tmp.path(),
            "params": {}
        }
    }));
    let msg = resp["error"]["message"].as_str().unwrap();
    assert!(msg.contains("remote base"), "got: {msg}");
    assert!(msg.contains("github.com/acme/base"), "got: {msg}");
    p.shutdown();
}

#[test]
fn missing_kustomize_binary_reports_error() {
    let mut p = PluginProc::spawn(Some("/nonexistent/kustomize-xyz"));
    let _ = p.rpc(serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocol_version": 1 }
    }));

    let tmp = tempdir();
    std::fs::write(
        tmp.path().join("kustomization.yaml"),
        "resources:\n  - cm.yaml\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("cm.yaml"),
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n",
    )
    .unwrap();
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "render",
        "params": {
            "source_path": tmp.path(),
            "params": {}
        }
    }));
    let msg = resp["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("kustomize not found") || msg.contains("not found"),
        "got: {msg}"
    );
    p.shutdown();
}

#[test]
fn renders_overlay_if_kustomize_available() {
    if !kustomize_installed() {
        eprintln!("kustomize not on PATH; skipping");
        return;
    }
    let mut p = PluginProc::spawn(None);
    let _ = p.rpc(serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocol_version": 1 }
    }));

    let tmp = tempdir();
    write_fixture_overlay(tmp.path());
    let resp = p.rpc(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "render",
        "params": {
            "source_path": tmp.path(),
            "params": { "path": "overlays/prod" }
        }
    }));
    let manifests = resp["result"]["manifests"]
        .as_array()
        .unwrap_or_else(|| panic!("no manifests in {resp}"));
    assert_eq!(manifests.len(), 1);
    let rendered = manifests[0].as_str().unwrap();
    assert!(rendered.contains("prod-app-config"), "got: {rendered}");
    p.shutdown();
}

// Tempdir helper, same as helm plugin tests.
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
    p.push(format!("synchrotron-kustomize-it-{}", unique()));
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
