//! End-to-end CLI tests (if7.7).
//!
//! Each test spins up a real `synchrotron-server` axum stack on a
//! random localhost port, then drives the CLI binary against it via
//! `std::process::Command`. Assertions check the stdout the user
//! actually sees — not a re-implementation of the request layer.

use std::process::Command;
use std::time::Duration;

use axum::serve;
use synchrotron_core::db::Database;
use synchrotron_core::EventBus;
use synchrotron_server::api::{self, AppsState};
use tokio::net::TcpListener;
use tokio::time::timeout;

/// Boot a server on a random local port and return its base URL plus
/// the join handle. The handle aborts on drop, taking the server
/// down at the end of each test.
async fn spawn_server() -> (String, tokio::task::JoinHandle<()>) {
    let db = Database::open_in_memory().unwrap();
    let bus = EventBus::new(64);
    let apps_state = AppsState::new(db, bus);
    let app = api::router_with_apps(apps_state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve(listener, app).await.unwrap();
    });
    // Tiny pause so the listener is in the kernel's accept queue
    // before the CLI dials.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (format!("http://{addr}"), handle)
}

/// Path to the freshly-built `synchrotron` binary.
fn cli_bin() -> &'static str {
    env!("CARGO_BIN_EXE_synchrotron")
}

fn run_cli(server: &str, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(cli_bin())
        .arg("--server")
        .arg(server)
        .args(args)
        .output()
        .expect("failed to invoke synchrotron CLI");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    (out.status.success(), stdout, stderr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_command_returns_ok() {
    let (server, handle) = spawn_server().await;
    let (ok, stdout, stderr) = run_cli(&server, &["--output", "json", "health"]);
    assert!(ok, "cli failed: stderr={stderr}");
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["status"], "ok");
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_lifecycle_create_list_get_history_delete() {
    let (server, handle) = spawn_server().await;

    // create
    let (ok, _, stderr) = run_cli(
        &server,
        &[
            "app",
            "create",
            "--name",
            "web",
            "--namespace",
            "argocd",
            "--repo-url",
            "https://example.com/r.git",
            "--path",
            "manifests",
            "--dest-cluster",
            "in-cluster",
            "--dest-namespace",
            "default",
        ],
    );
    assert!(ok, "create failed: {stderr}");

    // list (json) — confirm web is in there
    let (ok, stdout, stderr) = run_cli(&server, &["--output", "json", "app", "list"]);
    assert!(ok, "list failed: {stderr}");
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let apps = json["apps"].as_array().expect("apps array");
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0]["name"], "web");

    // list (table) — confirm header + name appear
    let (ok, stdout, _) = run_cli(&server, &["app", "list"]);
    assert!(ok);
    assert!(stdout.contains("NAME"), "table missing header: {stdout}");
    assert!(stdout.contains("web"), "table missing row: {stdout}");

    // get
    let (ok, stdout, _) = run_cli(&server, &["--output", "yaml", "app", "get", "web"]);
    assert!(ok);
    assert!(stdout.contains("name: web"), "yaml missing field: {stdout}");

    // history (empty, but the endpoint must work)
    let (ok, stdout, _) = run_cli(&server, &["--output", "json", "app", "history", "web"]);
    assert!(ok);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["app"], "web");
    assert!(json["entries"].as_array().unwrap().is_empty());

    // delete
    let (ok, _, _) = run_cli(&server, &["app", "delete", "web"]);
    assert!(ok);

    let (ok, stdout, _) = run_cli(&server, &["--output", "json", "app", "list"]);
    assert!(ok);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(json["apps"].as_array().unwrap().is_empty());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_emits_accepted_envelope() {
    let (server, handle) = spawn_server().await;
    let _ = run_cli(
        &server,
        &[
            "app",
            "create",
            "--name",
            "api",
            "--namespace",
            "argocd",
            "--repo-url",
            "https://example.com/r.git",
            "--path",
            ".",
            "--dest-cluster",
            "in-cluster",
            "--dest-namespace",
            "default",
        ],
    );
    let (ok, stdout, _) = run_cli(&server, &["--output", "json", "sync", "api"]);
    assert!(ok);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(json["app"], "api");
    assert!(
        json["status"].is_string(),
        "sync response missing status: {json}"
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonexistent_app_returns_nonzero_with_server_message() {
    let (server, handle) = spawn_server().await;
    let (ok, _stdout, stderr) = run_cli(&server, &["app", "get", "ghost"]);
    assert!(!ok, "expected failure for missing app");
    assert!(
        stderr.contains("not found") || stderr.contains("ghost"),
        "stderr should surface server error message: {stderr}"
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_streams_events_and_exits_on_close() {
    let (server, handle) = spawn_server().await;
    let _ = run_cli(
        &server,
        &[
            "app",
            "create",
            "--name",
            "web",
            "--namespace",
            "argocd",
            "--repo-url",
            "https://example.com/r.git",
            "--path",
            ".",
            "--dest-cluster",
            "in-cluster",
            "--dest-namespace",
            "default",
        ],
    );

    // Launch the watch in a child process. Trigger a sync to make
    // the server publish ManualSyncRequested, then kill the watch
    // and assert its stdout contains the event.
    let mut child = std::process::Command::new(cli_bin())
        .arg("--server")
        .arg(&server)
        .arg("--output")
        .arg("json")
        .arg("watch")
        .arg("web")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    // Give the watcher time to subscribe before we trigger an event.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = run_cli(&server, &["sync", "web"]);

    // Read until we see one event line, with a 3s ceiling.
    let mut stdout = child.stdout.take().unwrap();
    let read_task = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut buf = String::new();
        let mut tmp = [0u8; 1024];
        loop {
            match stdout.read(&mut tmp) {
                Ok(0) => return buf,
                Ok(n) => {
                    buf.push_str(&String::from_utf8_lossy(&tmp[..n]));
                    if buf.contains("sync_requested") {
                        return buf;
                    }
                }
                Err(_) => return buf,
            }
        }
    });

    let observed = timeout(Duration::from_secs(3), read_task)
        .await
        .expect("watch produced output in time")
        .unwrap();
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        observed.contains("sync_requested"),
        "expected sync_requested event in watch output: {observed}"
    );
    handle.abort();
}
