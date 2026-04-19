//! Test-only plugin binary. Behavior is controlled by environment
//! variables so integration tests can drive the host through its
//! failure paths without ever spawning a real plugin.
//!
//! Env vars:
//! - `STUB_MODE=ok` (default): normal handshake + render.
//! - `STUB_MODE=slow_render`: render sleeps longer than the test
//!   timeout, forcing the host to kill the child.
//! - `STUB_MODE=crash_on_render`: exits immediately on the render
//!   request, exercising the EarlyExit path.
//! - `STUB_MODE=malformed_render`: writes a non-JSON line back.
//! - `STUB_MODE=plugin_error`: returns a JSON-RPC error on render.
//! - `STUB_STDERR`: if set, the first thing the process writes to
//!   stderr so the host's stderr logger is exercised.

use std::io::{BufRead, Write};
use std::thread;
use std::time::Duration;

fn main() {
    if let Ok(msg) = std::env::var("STUB_STDERR") {
        eprintln!("{msg}");
    }

    let mode = std::env::var("STUB_MODE").unwrap_or_else(|_| "ok".into());

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut lines = stdin.lock().lines();

    while let Some(Ok(line)) = lines.next() {
        let req: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let id = req.get("id").cloned();

        match method {
            "initialize" => {
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "plugin_version": "stub-0.1" }
                });
                writeln!(stdout, "{resp}").unwrap();
                stdout.flush().unwrap();
            }
            "render" => match mode.as_str() {
                "slow_render" => {
                    thread::sleep(Duration::from_secs(30));
                }
                "crash_on_render" => {
                    std::process::exit(0);
                }
                "malformed_render" => {
                    writeln!(stdout, "not json at all {{").unwrap();
                    stdout.flush().unwrap();
                }
                "plugin_error" => {
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32000, "message": "render failed" }
                    });
                    writeln!(stdout, "{resp}").unwrap();
                    stdout.flush().unwrap();
                }
                _ => {
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "manifests": [
                                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: stub\n"
                            ]
                        }
                    });
                    writeln!(stdout, "{resp}").unwrap();
                    stdout.flush().unwrap();
                }
            },
            "shutdown" => {
                std::process::exit(0);
            }
            _ => {}
        }
    }
}
