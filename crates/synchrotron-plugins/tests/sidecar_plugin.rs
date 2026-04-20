//! Integration tests for the sidecar gRPC runtime. Each test spins
//! up an in-process tonic server on a random loopback port so we
//! exercise the full client/server path without external containers.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use synchrotron_plugins::sidecar::{proto, RetryConfig, Sidecar, SidecarError, PROTOCOL_VERSION};
use synchrotron_plugins::{DispatchError, Registry};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

use proto::plugin_server::{Plugin, PluginServer};
use proto::{
    HealthcheckRequest, HealthcheckResponse, RenderRequest, RenderResponse, VersionRequest,
    VersionResponse,
};

struct StubPlugin {
    protocol_version: u32,
    ready: bool,
    manifests: Vec<String>,
    render_calls: Arc<AtomicU32>,
    version_calls: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl Plugin for StubPlugin {
    async fn version(
        &self,
        _req: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        self.version_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(VersionResponse {
            plugin_version: "0.0.1-test".into(),
            protocol_version: self.protocol_version,
        }))
    }

    async fn healthcheck(
        &self,
        _req: Request<HealthcheckRequest>,
    ) -> Result<Response<HealthcheckResponse>, Status> {
        Ok(Response::new(HealthcheckResponse {
            ready: self.ready,
            message: if self.ready {
                String::new()
            } else {
                "warming up".into()
            },
        }))
    }

    async fn render(
        &self,
        _req: Request<RenderRequest>,
    ) -> Result<Response<RenderResponse>, Status> {
        self.render_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(RenderResponse {
            manifests: self.manifests.clone(),
        }))
    }
}

struct TestServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    render_calls: Arc<AtomicU32>,
    version_calls: Arc<AtomicU32>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_server(stub: StubPlugin) -> TestServer {
    let render_calls = stub.render_calls.clone();
    let version_calls = stub.version_calls.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let stream = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        Server::builder()
            .add_service(PluginServer::new(stub))
            .serve_with_incoming_shutdown(stream, async move {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    TestServer {
        addr,
        shutdown: Some(tx),
        render_calls,
        version_calls,
    }
}

fn endpoint(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

fn fast_retry() -> RetryConfig {
    RetryConfig {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
    }
}

#[tokio::test]
async fn handshake_and_render_happy_path() {
    let server = start_server(StubPlugin {
        protocol_version: PROTOCOL_VERSION,
        ready: true,
        manifests: vec![
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n".into(),
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n".into(),
        ],
        render_calls: Arc::new(AtomicU32::new(0)),
        version_calls: Arc::new(AtomicU32::new(0)),
    })
    .await;

    let mut sc = Sidecar::connect("stub", endpoint(server.addr), &fast_retry())
        .await
        .unwrap();
    sc.healthcheck().await.unwrap();
    let docs = sc
        .render(Path::new("/src"), &serde_json::json!({"k": "v"}))
        .await
        .unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(server.version_calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.render_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn protocol_version_mismatch_rejects() {
    let server = start_server(StubPlugin {
        protocol_version: PROTOCOL_VERSION + 1,
        ready: true,
        manifests: vec![],
        render_calls: Arc::new(AtomicU32::new(0)),
        version_calls: Arc::new(AtomicU32::new(0)),
    })
    .await;

    let err = Sidecar::connect("stub", endpoint(server.addr), &fast_retry())
        .await
        .unwrap_err();
    assert!(
        matches!(err, SidecarError::ProtocolVersion { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn healthcheck_not_ready_surfaces_message() {
    let server = start_server(StubPlugin {
        protocol_version: PROTOCOL_VERSION,
        ready: false,
        manifests: vec![],
        render_calls: Arc::new(AtomicU32::new(0)),
        version_calls: Arc::new(AtomicU32::new(0)),
    })
    .await;

    let mut sc = Sidecar::connect("stub", endpoint(server.addr), &fast_retry())
        .await
        .unwrap();
    let err = sc.healthcheck().await.unwrap_err();
    match err {
        SidecarError::NotReady { message, .. } => assert_eq!(message, "warming up"),
        other => panic!("expected NotReady, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_retries_then_fails_on_unreachable_endpoint() {
    // 127.0.0.1:1 is reserved — connection refused every time.
    let started = std::time::Instant::now();
    let err = Sidecar::connect("stub", "http://127.0.0.1:1", &fast_retry())
        .await
        .unwrap_err();
    assert!(matches!(err, SidecarError::Connect { attempts: 3, .. }));
    // Three attempts with 10ms → 20ms backoff ≈ 30ms minimum.
    assert!(started.elapsed() >= Duration::from_millis(25));
}

#[tokio::test]
async fn registry_dispatches_sidecar_end_to_end() {
    let server = start_server(StubPlugin {
        protocol_version: PROTOCOL_VERSION,
        ready: true,
        manifests: vec!["apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: via-sidecar\n".into()],
        render_calls: Arc::new(AtomicU32::new(0)),
        version_calls: Arc::new(AtomicU32::new(0)),
    })
    .await;

    let yaml = format!(
        "plugins:\n  - name: side\n    kind: sidecar\n    endpoint: {}\n",
        endpoint(server.addr)
    );
    let reg = Registry::from_yaml(&yaml).unwrap();
    let out = reg
        .render("side", Path::new("/src"), serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].name, "via-sidecar");
    assert_eq!(server.render_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn registry_sidecar_unreachable_surfaces_connect_error() {
    let reg = Registry::from_yaml(
        "plugins:\n  - name: s\n    kind: sidecar\n    endpoint: http://127.0.0.1:1\n",
    )
    .unwrap();
    let err = reg
        .render("s", Path::new("/"), serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DispatchError::Sidecar(SidecarError::Connect { .. })),
        "got {err:?}"
    );
}
