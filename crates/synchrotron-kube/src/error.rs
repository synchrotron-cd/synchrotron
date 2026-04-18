use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KubeError {
    #[error("kubeconfig not found at {0}")]
    KubeconfigMissing(PathBuf),

    #[error("failed to read kubeconfig {path}: {source}")]
    KubeconfigRead {
        path: PathBuf,
        #[source]
        source: kube::config::KubeconfigError,
    },

    #[error("failed to build kube config: {0}")]
    ConfigBuild(#[from] kube::config::KubeconfigError),

    #[error("failed to infer in-cluster config: {0}")]
    Infer(#[from] kube::config::InferConfigError),

    #[error("failed to load in-cluster ServiceAccount config: {0}")]
    InCluster(#[source] kube::config::InClusterError),

    #[error("failed to build kube client: {0}")]
    ClientBuild(#[from] kube::Error),

    #[error("context {0:?} not found in kubeconfig")]
    ContextNotFound(String),

    #[error("cluster {0:?} is already registered")]
    AlreadyRegistered(String),
}
