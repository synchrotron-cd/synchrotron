//! Local plugin runtime: subprocess speaking JSON-RPC 2.0 over
//! newline-delimited stdin/stdout.
//!
//! Framing is one JSON object per line in each direction. This is
//! simpler than Content-Length framing (used by LSP) and sufficient
//! for our synchronous request/response model. The plugin must not
//! emit newlines mid-object — standard serde_json output satisfies
//! that because `to_string` never writes `\n`.
//!
//! Lifecycle:
//! 1. [`Plugin::spawn`] forks the subprocess, wires stdio, and runs
//!    the `initialize` handshake.
//! 2. Callers invoke [`Plugin::render`] zero or more times. Calls
//!    are serialized through `&mut self` — the plugin side need not
//!    handle concurrency.
//! 3. [`Plugin::shutdown`] sends a `shutdown` notification, closes
//!    stdin, and waits up to the plugin's timeout for clean exit.
//!    If the process is still alive after that, it is killed.
//!
//! Every request is bounded by the plugin's configured timeout. On
//! timeout the child is killed and the plugin object is consumed —
//! callers must respawn. This matches the "kill + restart on
//! protocol violation or crash" acceptance criterion: an unhealthy
//! plugin process is never reused.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;
use tracing::{debug, warn};

/// Protocol version the host speaks. Bumped whenever request or
/// response shapes change in a breaking way.
pub const PROTOCOL_VERSION: u32 = 1;

/// Configuration for launching a single plugin process.
#[derive(Debug, Clone)]
pub struct PluginSpec {
    /// Display name used in logs and error messages.
    pub name: String,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// Working directory for the child. `None` inherits the host's.
    pub workdir: Option<PathBuf>,
    /// Per-request ceiling. Covers handshake, render, and shutdown.
    pub timeout: Duration,
}

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("plugin {name}: i/o error: {source}")]
    Io {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("plugin {name}: timed out after {:?}", .timeout)]
    Timeout { name: String, timeout: Duration },
    #[error("plugin {name}: protocol error: {message}")]
    Protocol { name: String, message: String },
    #[error("plugin {name}: process exited before responding")]
    EarlyExit { name: String },
    #[error("plugin {name}: plugin returned error {code}: {message}")]
    PluginSide {
        name: String,
        code: i64,
        message: String,
    },
}

#[derive(Debug, Serialize)]
pub struct InitializeParams {
    pub protocol_version: u32,
}

#[derive(Debug, Deserialize)]
pub struct InitializeResult {
    pub plugin_version: String,
}

#[derive(Debug, Serialize)]
pub struct RenderRequest {
    pub source_path: PathBuf,
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct RenderResponse {
    /// One YAML document per entry. The plugin is responsible for
    /// rendering; the host just collects the strings.
    pub manifests: Vec<String>,
}

pub struct Plugin {
    spec: PluginSpec,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    /// Populated on handshake.
    pub info: InitializeResult,
}

impl Plugin {
    /// Spawn the subprocess and run the `initialize` handshake.
    pub async fn spawn(spec: PluginSpec) -> Result<Self, PluginError> {
        let mut cmd = Command::new(&spec.command);
        cmd.args(&spec.args);
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        if let Some(d) = &spec.workdir {
            cmd.current_dir(d);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd.spawn().map_err(|source| PluginError::Io {
            name: spec.name.clone(),
            source,
        })?;

        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = BufReader::new(child.stdout.take().expect("piped stdout"));
        let stderr = child.stderr.take().expect("piped stderr");
        spawn_stderr_logger(spec.name.clone(), stderr);

        let mut plugin = Plugin {
            spec,
            child,
            stdin,
            stdout,
            next_id: 0,
            info: InitializeResult {
                plugin_version: String::new(),
            },
        };

        let params = InitializeParams {
            protocol_version: PROTOCOL_VERSION,
        };
        let info: InitializeResult = plugin.request("initialize", &params).await?;
        plugin.info = info;
        Ok(plugin)
    }

    /// Render manifests for one application source tree.
    pub async fn render(&mut self, req: &RenderRequest) -> Result<RenderResponse, PluginError> {
        self.request("render", req).await
    }

    /// Send a `shutdown` notification, close stdin, and wait for the
    /// process to exit. Kills on timeout. Always consumes `self`.
    pub async fn shutdown(mut self) -> Result<(), PluginError> {
        // Fire-and-forget: a cooperative plugin will exit on shutdown,
        // but we don't require a response — closing stdin is enough.
        let _ = self.write_notification("shutdown").await;
        drop(self.stdin);

        let name = self.spec.name.clone();
        let deadline = self.spec.timeout;
        match timeout(deadline, self.child.wait()).await {
            Ok(Ok(_status)) => Ok(()),
            Ok(Err(source)) => Err(PluginError::Io { name, source }),
            Err(_) => {
                warn!(plugin = %name, "plugin did not exit on shutdown; killing");
                let _ = self.child.kill().await;
                Err(PluginError::Timeout {
                    name,
                    timeout: deadline,
                })
            }
        }
    }

    async fn request<P: Serialize, R: for<'de> Deserialize<'de>>(
        &mut self,
        method: &str,
        params: &P,
    ) -> Result<R, PluginError> {
        self.next_id += 1;
        let id = self.next_id;
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&req).expect("serialize request");

        let name = self.spec.name.clone();
        let to = self.spec.timeout;

        let result = timeout(to, async {
            self.stdin.write_all(line.as_bytes()).await?;
            self.stdin.write_all(b"\n").await?;
            self.stdin.flush().await?;

            let mut buf = String::new();
            let n = self.stdout.read_line(&mut buf).await?;
            if n == 0 {
                return Err(ReqErr::EarlyExit);
            }
            Ok(buf)
        })
        .await;

        let line = match result {
            Ok(Ok(line)) => line,
            Ok(Err(ReqErr::Io(source))) => {
                self.kill_on_failure().await;
                return Err(PluginError::Io { name, source });
            }
            Ok(Err(ReqErr::EarlyExit)) => {
                self.kill_on_failure().await;
                return Err(PluginError::EarlyExit { name });
            }
            Err(_) => {
                self.kill_on_failure().await;
                return Err(PluginError::Timeout { name, timeout: to });
            }
        };

        let resp: RpcResponse<R> =
            serde_json::from_str(line.trim()).map_err(|e| PluginError::Protocol {
                name: name.clone(),
                message: format!("malformed response: {e}"),
            })?;
        if resp.id != Some(id) {
            return Err(PluginError::Protocol {
                name,
                message: format!("response id mismatch: expected {id}, got {:?}", resp.id),
            });
        }
        match (resp.result, resp.error) {
            (Some(r), None) => Ok(r),
            (None, Some(e)) => Err(PluginError::PluginSide {
                name,
                code: e.code,
                message: e.message,
            }),
            _ => Err(PluginError::Protocol {
                name,
                message: "response must contain exactly one of result/error".into(),
            }),
        }
    }

    async fn write_notification(&mut self, method: &str) -> std::io::Result<()> {
        let notif = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        let line = serde_json::to_string(&notif).unwrap();
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await
    }

    async fn kill_on_failure(&mut self) {
        debug!(plugin = %self.spec.name, "killing plugin after failure");
        let _ = self.child.start_kill();
    }
}

enum ReqErr {
    Io(std::io::Error),
    EarlyExit,
}

impl From<std::io::Error> for ReqErr {
    fn from(e: std::io::Error) -> Self {
        ReqErr::Io(e)
    }
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    id: Option<u64>,
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

fn spawn_stderr_logger(name: String, stderr: tokio::process::ChildStderr) {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            warn!(plugin = %name, "{}", line);
        }
    });
}
