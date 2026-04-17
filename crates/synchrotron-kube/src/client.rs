use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Client, Config};
use tracing::{debug, info};

use crate::config::ClusterConfig;
use crate::error::KubeError;
use crate::Result;

/// Handle to a single Kubernetes cluster.
///
/// Wraps a [`kube::Client`] with the operator-facing cluster name and the
/// kubeconfig context that produced it, so logging/metrics can attribute
/// requests without leaking the raw kubeconfig. Downstream sub-issues
/// (health probes, informers, etc.) build on `self.client()`.
#[derive(Clone)]
pub struct KubeClient {
    name: String,
    context: String,
    client: Client,
}

impl KubeClient {
    /// Build a client from a [`ClusterConfig`]. This parses the
    /// kubeconfig and constructs HTTP/TLS infrastructure, but does not
    /// perform any request against the cluster.
    pub async fn connect(cfg: &ClusterConfig) -> Result<Self> {
        let (kube_config, context) = match cfg.kubeconfig_path() {
            Some(path) => {
                if !path.exists() {
                    return Err(KubeError::KubeconfigMissing(path.to_path_buf()));
                }
                let kubeconfig =
                    Kubeconfig::read_from(path).map_err(|source| KubeError::KubeconfigRead {
                        path: path.to_path_buf(),
                        source,
                    })?;

                let resolved_context = resolve_context(&kubeconfig, cfg.context.as_deref())?;
                let opts = KubeConfigOptions {
                    context: Some(resolved_context.clone()),
                    ..Default::default()
                };
                let config = Config::from_custom_kubeconfig(kubeconfig, &opts).await?;
                (config, resolved_context)
            }
            None => {
                let config = Config::infer().await?;
                let context = cfg
                    .context
                    .clone()
                    .unwrap_or_else(|| "<default-discovery>".to_string());
                (config, context)
            }
        };

        debug!(
            cluster = %cfg.name,
            context = %context,
            cluster_url = %kube_config.cluster_url,
            "built kube config",
        );

        let client = Client::try_from(kube_config)?;
        info!(cluster = %cfg.name, context = %context, "kube client ready");

        Ok(Self {
            name: cfg.name.0.clone(),
            context,
            client,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Fetches the API server version. Doubles as a simple connectivity
    /// check — the full health-probe loop is a sibling sub-issue
    /// (h48.8.5).
    pub async fn apiserver_version(&self) -> Result<String> {
        let info = self.client.apiserver_version().await?;
        Ok(info.git_version)
    }
}

impl std::fmt::Debug for KubeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeClient")
            .field("name", &self.name)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

fn resolve_context(kubeconfig: &Kubeconfig, requested: Option<&str>) -> Result<String> {
    let candidate = requested
        .map(str::to_string)
        .or_else(|| kubeconfig.current_context.clone())
        .ok_or_else(|| KubeError::ContextNotFound("<current-context unset>".into()))?;

    if kubeconfig.contexts.iter().any(|c| c.name == candidate) {
        Ok(candidate)
    } else {
        Err(KubeError::ContextNotFound(candidate))
    }
}
