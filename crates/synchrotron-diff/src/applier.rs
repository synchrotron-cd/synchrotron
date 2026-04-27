//! Server-side dry-run normalization.
//!
//! The differ wants to compare *what the API server would store*
//! against *what's currently stored*. Reaching the first half of
//! that means asking the API server to run the apply through every
//! mutating webhook and defaulting pass without persisting the
//! result. Kubernetes exposes that via `dryRun=All` on PATCH/POST
//! requests; this trait wraps the call so the differ stays
//! transport-independent and unit-testable.
//!
//! The actual `kube`-rs implementation lives in a follow-up bead so
//! this crate stays compile-fast and pure-Rust testable. Callers
//! constructing a real reconcile loop will provide the kube-backed
//! impl; tests use a hand-rolled mock that returns canned manifests.

use std::future::Future;
use std::pin::Pin;

use synchrotron_plugins::Manifest;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DryRunError {
    /// The API server rejected the dry-run apply (validation,
    /// admission, or a webhook returning an error). The differ has
    /// no recovery path — the caller decides whether to surface this
    /// as a planning failure or a degraded sync.
    #[error("dry-run apply failed: {0}")]
    Server(String),
    /// Transport failure (TLS, DNS, timeout). Distinct from
    /// `Server` so retry policies can target it specifically.
    #[error("dry-run transport error: {0}")]
    Transport(String),
    /// The server returned a manifest we can't parse back into our
    /// `Manifest` type. Indicates a bug or an unexpected schema.
    #[error("dry-run response could not be decoded: {0}")]
    Decode(String),
}

/// Normalize a manifest by asking the API server to apply it with
/// `dryRun=All` and returning the server-rendered object.
///
/// "Normalize" here covers: defaulting (e.g. `protocol: TCP` filled
/// in on a port), mutating-webhook rewrites (e.g. sidecar injection),
/// and any server-side coercion (e.g. `imagePullPolicy` defaulted
/// from the image tag). A successful response is the manifest as it
/// *would* appear in etcd, modulo `metadata.resourceVersion` and the
/// other server-managed fields the differ filters separately.
pub trait DryRunApplier: Send + Sync {
    fn normalize<'a>(
        &'a self,
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use synchrotron_plugins::manifest::parse_stream;

    /// In-memory mock: returns canned normalized manifests keyed by
    /// `(namespace, name)`. Anything else fails with `Server`.
    pub struct MockApplier {
        canned: HashMap<(Option<String>, String), Manifest>,
    }

    impl MockApplier {
        fn with(manifest: Manifest) -> Self {
            let mut canned = HashMap::new();
            canned.insert(
                (manifest.namespace.clone(), manifest.name.clone()),
                manifest,
            );
            Self { canned }
        }
    }

    impl DryRunApplier for MockApplier {
        fn normalize<'a>(
            &'a self,
            manifest: &'a Manifest,
        ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>> {
            let key = (manifest.namespace.clone(), manifest.name.clone());
            let result = self
                .canned
                .get(&key)
                .cloned()
                .ok_or_else(|| DryRunError::Server(format!("no canned response for {key:?}")));
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn mock_returns_canned_manifest() {
        let canned = parse_stream(
            "t",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\ndata:\n  k: v\n",
        )
        .unwrap()
        .pop()
        .unwrap();
        let applier = MockApplier::with(canned.clone());
        let result = applier.normalize(&canned).await.unwrap();
        assert_eq!(result.name, "cm");
    }

    #[tokio::test]
    async fn mock_errors_on_unknown_manifest() {
        let canned = parse_stream(
            "t",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\n",
        )
        .unwrap()
        .pop()
        .unwrap();
        let other = parse_stream(
            "t",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: app\n",
        )
        .unwrap()
        .pop()
        .unwrap();
        let applier = MockApplier::with(canned);
        let err = applier.normalize(&other).await.unwrap_err();
        assert!(matches!(err, DryRunError::Server(_)));
    }
}
