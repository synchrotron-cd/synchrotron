//! Outbound sync-outcome notifications.
//!
//! Subscribes to the in-process [`EventBus`], filters
//! [`SystemEvent::SyncOutcome`] events, and POSTs a JSON payload to
//! the per-app webhook URL. Bounded exponential backoff on transient
//! (5xx / network) failures; 4xx is logged once and dropped — the
//! caller's URL is wrong and retrying won't fix it.
//!
//! # Configuration source
//!
//! The notifier doesn't own where the per-app config comes from — it
//! takes a [`ConfigSource`] trait object. Production wires this to the
//! apps DB; tests use [`StaticConfigSource`].
//!
//! # Signing
//!
//! When a config carries an `hmac_secret`, the request includes
//! `X-Synchrotron-Signature: sha256=<hex>` over the raw body. The
//! header name matches the GitHub-style convention used by our
//! inbound webhook code so consumers can share verification helpers.
//!
//! # Delivery semantics
//!
//! At-most-once-with-bounded-retries — the notifier never persists
//! pending deliveries. A process restart drops in-flight retries.
//! That's acceptable for a notification channel (consumers should
//! reconcile state via the API rather than treat webhooks as the
//! source of truth).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use synchrotron_core::{EventBus, SystemEvent};
use synchrotron_types::AppName;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

type HmacSha256 = Hmac<Sha256>;

/// Per-app webhook configuration.
#[derive(Debug, Clone)]
pub struct NotificationConfig {
    pub url: String,
    /// When set, requests carry an `X-Synchrotron-Signature` header.
    pub hmac_secret: Option<Vec<u8>>,
    /// Total attempts cap, including the first try. `1` disables retry.
    pub max_attempts: u32,
}

impl NotificationConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            hmac_secret: None,
            max_attempts: 4,
        }
    }

    pub fn with_hmac(mut self, secret: Vec<u8>) -> Self {
        self.hmac_secret = Some(secret);
        self
    }

    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }
}

/// Lookup hook: given an app name, return its notification config or
/// `None` (notifications disabled for this app).
pub trait ConfigSource: Send + Sync {
    fn lookup(&self, app: &AppName) -> Option<NotificationConfig>;
}

/// In-memory `ConfigSource` for tests and simple deployments.
#[derive(Default, Clone)]
pub struct StaticConfigSource {
    inner: std::collections::HashMap<String, NotificationConfig>,
}

impl StaticConfigSource {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, app: impl Into<String>, cfg: NotificationConfig) {
        self.inner.insert(app.into(), cfg);
    }
}

impl ConfigSource for StaticConfigSource {
    fn lookup(&self, app: &AppName) -> Option<NotificationConfig> {
        self.inner.get(&app.0).cloned()
    }
}

/// Counters surfaced to the host metrics registry. Atomic so the
/// host can scrape without coordination.
#[derive(Debug, Default)]
pub struct NotifierMetrics {
    pub sent: AtomicU64,
    pub failed: AtomicU64,
    pub retries: AtomicU64,
    pub dropped_4xx: AtomicU64,
}

impl NotifierMetrics {
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }
    pub fn dropped_4xx(&self) -> u64 {
        self.dropped_4xx.load(Ordering::Relaxed)
    }
}

/// Wire-format payload. Versioned via the `v` field so future schema
/// changes are detectable by receivers without breaking parsers.
#[derive(Debug, Serialize)]
pub struct OutcomePayload<'a> {
    pub v: u32,
    pub kind: &'static str,
    pub app: &'a str,
    pub cluster: &'a str,
    pub success: bool,
    pub message: Option<&'a str>,
    pub timestamp: String,
}

pub struct Notifier {
    bus: EventBus,
    config: Arc<dyn ConfigSource>,
    client: reqwest::Client,
    metrics: Arc<NotifierMetrics>,
    backoff_base: Duration,
}

impl Notifier {
    pub fn new(bus: EventBus, config: Arc<dyn ConfigSource>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client builds with default config");
        Self {
            bus,
            config,
            client,
            metrics: Arc::new(NotifierMetrics::default()),
            backoff_base: Duration::from_millis(200),
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_backoff_base(mut self, base: Duration) -> Self {
        self.backoff_base = base;
        self
    }

    pub fn metrics(&self) -> Arc<NotifierMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Spawn the notifier loop. The returned handle ends when the bus
    /// is dropped or the task is aborted.
    pub fn spawn(self) -> JoinHandle<()> {
        // Subscribe synchronously so events published immediately
        // after `spawn()` returns are not missed by a not-yet-scheduled
        // task.
        let rx = self.bus.subscribe();
        tokio::spawn(self.run(rx))
    }

    async fn run(self, mut rx: synchrotron_core::EventReceiver) {
        loop {
            let evt = match rx.recv().await {
                Ok(e) => e,
                Err(_) => return,
            };
            let SystemEvent::SyncOutcome {
                app,
                cluster,
                success,
                message,
            } = evt.event
            else {
                continue;
            };
            let Some(cfg) = self.config.lookup(&app) else {
                debug!(app = %app, "no notification config; skipping");
                continue;
            };
            let payload = OutcomePayload {
                v: 1,
                kind: "sync_outcome",
                app: &app.0,
                cluster: &cluster.0,
                success,
                message: message.as_deref(),
                timestamp: chrono::DateTime::<chrono::Utc>::from(evt.at).to_rfc3339(),
            };
            let body = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "failed to serialize outcome payload");
                    self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            self.deliver(&app, &cfg, body).await;
        }
    }

    /// Public so tests can drive a single delivery without spinning
    /// up the bus loop.
    pub async fn deliver(&self, app: &AppName, cfg: &NotificationConfig, body: Vec<u8>) {
        let signature = cfg.hmac_secret.as_ref().map(|s| sign(s, &body));
        let mut delay = self.backoff_base;
        for attempt in 1..=cfg.max_attempts {
            let mut req = self
                .client
                .post(&cfg.url)
                .header("content-type", "application/json")
                .body(body.clone());
            if let Some(sig) = &signature {
                req = req.header("X-Synchrotron-Signature", format!("sha256={sig}"));
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        info!(app = %app, status = status.as_u16(), attempt, "notification delivered");
                        self.metrics.sent.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    if status.is_client_error() {
                        warn!(app = %app, status = status.as_u16(), "notification dropped on 4xx");
                        self.metrics.dropped_4xx.fetch_add(1, Ordering::Relaxed);
                        self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    warn!(app = %app, status = status.as_u16(), attempt, "notification 5xx; will retry");
                }
                Err(e) => {
                    warn!(app = %app, error = %e, attempt, "notification request failed; will retry");
                }
            }
            if attempt == cfg.max_attempts {
                warn!(app = %app, "notification giving up after {} attempts", cfg.max_attempts);
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            self.metrics.retries.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2);
        }
    }
}

fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Verify a signature produced by [`sign`]. Exposed so receivers in
/// the same workspace can validate without re-implementing the
/// HMAC dance.
pub fn verify(secret: &[u8], body: &[u8], header_value: &str) -> bool {
    let Some(hex_sig) = header_value.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(sig) = hex::decode(hex_sig) else {
        return false;
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::HeaderMap, routing::post, Router};
    use std::sync::Mutex;
    use synchrotron_types::ClusterName;
    use tokio::net::TcpListener;

    #[derive(Default, Clone)]
    struct Recorder {
        inner: Arc<Mutex<RecorderInner>>,
    }
    #[derive(Default)]
    struct RecorderInner {
        calls: Vec<RecordedCall>,
        responses: Vec<u16>,
    }
    struct RecordedCall {
        body: Vec<u8>,
        signature: Option<String>,
    }
    impl Recorder {
        fn push_response(&self, status: u16) {
            self.inner.lock().unwrap().responses.push(status);
        }
        fn push_responses(&self, statuses: &[u16]) {
            for s in statuses {
                self.push_response(*s);
            }
        }
        fn calls(&self) -> usize {
            self.inner.lock().unwrap().calls.len()
        }
        fn last_body(&self) -> Option<Vec<u8>> {
            self.inner.lock().unwrap().calls.last().map(|c| c.body.clone())
        }
        fn last_signature(&self) -> Option<String> {
            self.inner
                .lock()
                .unwrap()
                .calls
                .last()
                .and_then(|c| c.signature.clone())
        }
    }

    async fn handler(
        State(rec): State<Recorder>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> axum::http::StatusCode {
        let mut g = rec.inner.lock().unwrap();
        let signature = headers
            .get("x-synchrotron-signature")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        g.calls.push(RecordedCall {
            body: body.to_vec(),
            signature,
        });
        let status = if g.responses.is_empty() {
            200
        } else {
            g.responses.remove(0)
        };
        axum::http::StatusCode::from_u16(status).unwrap()
    }

    async fn spawn_test_server(rec: Recorder) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/hook", post(handler)).with_state(rec);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/hook")
    }

    fn cfg_for(url: String) -> NotificationConfig {
        NotificationConfig::new(url).with_max_attempts(4)
    }

    fn notifier_with(cfg_source: StaticConfigSource) -> Notifier {
        let bus = EventBus::new(16);
        Notifier::new(bus, Arc::new(cfg_source)).with_backoff_base(Duration::from_millis(1))
    }

    #[tokio::test]
    async fn delivers_payload_with_versioned_envelope() {
        let rec = Recorder::default();
        rec.push_response(200);
        let url = spawn_test_server(rec.clone()).await;
        let mut src = StaticConfigSource::new();
        src.insert("billing", cfg_for(url));
        let notifier = notifier_with(src);

        let cfg = NotificationConfig::new("ignored").with_max_attempts(1);
        let real_cfg = notifier.config.lookup(&AppName("billing".into())).unwrap();
        notifier
            .deliver(
                &AppName("billing".into()),
                &real_cfg,
                serde_json::to_vec(&OutcomePayload {
                    v: 1,
                    kind: "sync_outcome",
                    app: "billing",
                    cluster: "prod",
                    success: true,
                    message: None,
                    timestamp: "2026-05-02T00:00:00Z".into(),
                })
                .unwrap(),
            )
            .await;
        let _ = cfg;

        assert_eq!(rec.calls(), 1);
        let body: serde_json::Value = serde_json::from_slice(&rec.last_body().unwrap()).unwrap();
        assert_eq!(body["v"], 1);
        assert_eq!(body["kind"], "sync_outcome");
        assert_eq!(body["app"], "billing");
        assert_eq!(body["success"], true);
        assert_eq!(notifier.metrics().sent(), 1);
        assert_eq!(notifier.metrics().retries(), 0);
    }

    #[tokio::test]
    async fn retries_5xx_with_backoff_then_succeeds() {
        let rec = Recorder::default();
        rec.push_responses(&[503, 502, 200]);
        let url = spawn_test_server(rec.clone()).await;
        let mut src = StaticConfigSource::new();
        src.insert("a", cfg_for(url));
        let notifier = notifier_with(src);
        let cfg = notifier.config.lookup(&AppName("a".into())).unwrap();

        notifier
            .deliver(&AppName("a".into()), &cfg, b"{}".to_vec())
            .await;

        assert_eq!(rec.calls(), 3);
        let m = notifier.metrics();
        assert_eq!(m.sent(), 1);
        assert_eq!(m.failed(), 0);
        assert_eq!(m.retries(), 2);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_on_persistent_5xx() {
        let rec = Recorder::default();
        rec.push_responses(&[500, 500, 500, 500]);
        let url = spawn_test_server(rec.clone()).await;
        let mut src = StaticConfigSource::new();
        src.insert(
            "a",
            NotificationConfig::new(url).with_max_attempts(3),
        );
        let notifier = notifier_with(src);
        let cfg = notifier.config.lookup(&AppName("a".into())).unwrap();

        notifier
            .deliver(&AppName("a".into()), &cfg, b"{}".to_vec())
            .await;

        assert_eq!(rec.calls(), 3);
        let m = notifier.metrics();
        assert_eq!(m.sent(), 0);
        assert_eq!(m.failed(), 1);
        assert_eq!(m.retries(), 2);
    }

    #[tokio::test]
    async fn drops_on_4xx_without_retry() {
        let rec = Recorder::default();
        rec.push_responses(&[404]);
        let url = spawn_test_server(rec.clone()).await;
        let mut src = StaticConfigSource::new();
        src.insert("a", cfg_for(url));
        let notifier = notifier_with(src);
        let cfg = notifier.config.lookup(&AppName("a".into())).unwrap();

        notifier
            .deliver(&AppName("a".into()), &cfg, b"{}".to_vec())
            .await;

        assert_eq!(rec.calls(), 1);
        let m = notifier.metrics();
        assert_eq!(m.sent(), 0);
        assert_eq!(m.dropped_4xx(), 1);
        assert_eq!(m.failed(), 1);
        assert_eq!(m.retries(), 0);
    }

    #[tokio::test]
    async fn signs_request_with_hmac_when_secret_present() {
        let rec = Recorder::default();
        rec.push_response(200);
        let url = spawn_test_server(rec.clone()).await;
        let secret = b"shhh".to_vec();
        let mut src = StaticConfigSource::new();
        src.insert(
            "a",
            NotificationConfig::new(url).with_hmac(secret.clone()),
        );
        let notifier = notifier_with(src);
        let cfg = notifier.config.lookup(&AppName("a".into())).unwrap();
        let body = b"{\"x\":1}".to_vec();

        notifier.deliver(&AppName("a".into()), &cfg, body.clone()).await;

        let sig = rec.last_signature().expect("signature header present");
        assert!(verify(&secret, &body, &sig));
        assert!(!verify(b"wrong", &body, &sig));
    }

    #[tokio::test]
    async fn unsigned_when_no_secret() {
        let rec = Recorder::default();
        rec.push_response(200);
        let url = spawn_test_server(rec.clone()).await;
        let mut src = StaticConfigSource::new();
        src.insert("a", cfg_for(url));
        let notifier = notifier_with(src);
        let cfg = notifier.config.lookup(&AppName("a".into())).unwrap();

        notifier.deliver(&AppName("a".into()), &cfg, b"{}".to_vec()).await;

        assert!(rec.last_signature().is_none());
    }

    #[tokio::test]
    async fn skips_apps_without_config() {
        let bus = EventBus::new(16);
        let src = StaticConfigSource::new();
        let notifier = Notifier::new(bus.clone(), Arc::new(src))
            .with_backoff_base(Duration::from_millis(1));
        let metrics = notifier.metrics();
        let _h = notifier.spawn();

        bus.publish(SystemEvent::SyncOutcome {
            app: AppName("ghost".into()),
            cluster: ClusterName("prod".into()),
            success: true,
            message: None,
        });

        // Give the loop a chance to consume the event.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(metrics.sent(), 0);
        assert_eq!(metrics.failed(), 0);
    }

    #[tokio::test]
    async fn bus_loop_dispatches_sync_outcome() {
        let rec = Recorder::default();
        rec.push_response(200);
        let url = spawn_test_server(rec.clone()).await;
        let bus = EventBus::new(16);
        let mut src = StaticConfigSource::new();
        src.insert("billing", cfg_for(url));
        let notifier = Notifier::new(bus.clone(), Arc::new(src))
            .with_backoff_base(Duration::from_millis(1));
        let metrics = notifier.metrics();
        let _h = notifier.spawn();

        bus.publish(SystemEvent::SyncOutcome {
            app: AppName("billing".into()),
            cluster: ClusterName("prod".into()),
            success: false,
            message: Some("boom".into()),
        });

        for _ in 0..50 {
            if metrics.sent() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(metrics.sent(), 1);
        let body: serde_json::Value =
            serde_json::from_slice(&rec.last_body().unwrap()).unwrap();
        assert_eq!(body["app"], "billing");
        assert_eq!(body["cluster"], "prod");
        assert_eq!(body["success"], false);
        assert_eq!(body["message"], "boom");
    }

    #[test]
    fn config_max_attempts_floor_is_one() {
        let c = NotificationConfig::new("u").with_max_attempts(0);
        assert_eq!(c.max_attempts, 1);
    }

    #[test]
    fn verify_rejects_wrong_prefix() {
        assert!(!verify(b"k", b"x", "sha1=deadbeef"));
        assert!(!verify(b"k", b"x", "deadbeef"));
        assert!(!verify(b"k", b"x", "sha256=zzzz"));
    }
}
