//! Annotated YAML template emitter for documentation and
//! `synchrotron-server config init` style scaffolds.
//!
//! Hand-written rather than derived from the schema: the comments
//! are the point, and they describe operational nuance that the
//! type system can't express (when to set `in_cluster`, how
//! `credentials_secret` resolves, which `kind` values exist).

/// Returns an annotated YAML template covering every section of
/// [`super::schema::Config`]. The template parses cleanly via
/// [`super::load`] when the user fills in the placeholder values.
pub fn config_template() -> String {
    r#"# synchrotron-server configuration.
# All sections are optional; defaults match the values shown.

server:
  # Address the HTTP API listens on. Must include a port.
  listen_addr: 0.0.0.0:8484
  # Path to the SQLite state database. Created if missing.
  db_path: synchrotron.db

# Kubernetes clusters this controller manages. Each entry must set
# either `kubeconfig` or `in_cluster: true` (mutually exclusive).
clusters: []
  # - name: prod
  #   kubeconfig: /etc/synchrotron/kubeconfig
  #   context: prod-east   # optional; defaults to current-context
  # - name: in-cluster
  #   in_cluster: true     # use the pod's projected ServiceAccount

# Git repos polled for desired state. `id` is referenced by apps.
repos: []
  # - id: platform
  #   url: https://github.com/example/platform.git
  #   branch: main                      # optional; default branch if omitted
  #   credentials_secret: platform-git  # optional; resolved by secret store

# Manifest-rendering plugins. `kind` selects the runtime; `config`
# is passed verbatim to that runtime.
# Valid kinds: raw, helm, kustomize, local, sidecar.
plugins: []
  # - name: helm
  #   kind: helm
  #   config: {}

polling:
  # How often the git poller checks each repo for new commits.
  repo_interval_seconds: 180
  # How often auto-heal scans for stale apps.
  auto_heal_interval_seconds: 600

timeouts:
  # Hard cap on a single reconcile run.
  reconcile_seconds: 300
  # Hard cap on a single git fetch.
  git_fetch_seconds: 120

git:
  ssh:
    # OpenSSH-style strict_host_key_checking. Production should keep
    # the default (yes); accept-new enables TOFU; no disables verification.
    strict_host_key_checking: yes
    # Defaults to $HOME/.ssh/known_hosts. A missing file is treated as
    # empty — accept-new mode creates it on first contact.
    # known_hosts: /etc/synchrotron/known_hosts
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{validate, Config};

    #[test]
    fn template_parses_and_validates() {
        let text = config_template();
        let cfg: Config = serde_yaml_ng::from_str(&text).expect("template parses");
        validate(&cfg).expect("template validates");
    }

    #[test]
    fn template_matches_defaults() {
        let cfg: Config = serde_yaml_ng::from_str(&config_template()).expect("parse");
        assert_eq!(cfg, Config::default());
    }
}
