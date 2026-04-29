//! Post-deserialize validation.
//!
//! Each [`ValidationError`] carries a dotted path so messages
//! ("`clusters[2].kubeconfig`: file does not exist") point operators
//! at the exact misconfigured field. Errors accumulate — one bad
//! cluster doesn't hide the next bad repo.

use std::collections::HashSet;

use thiserror::Error;

use super::schema::{ClusterCfg, Config, PluginCfg, Polling, RepoCfg, ServerSection, Timeouts};

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{path}: {message}")]
pub struct ValidationError {
    pub path: String,
    pub message: String,
}

impl ValidationError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Run every check. Returns `Ok(())` if everything is sound, or a
/// list of errors with paths.
pub fn validate(cfg: &Config) -> Result<(), Vec<ValidationError>> {
    let mut errs = Vec::new();
    validate_server(&cfg.server, &mut errs);
    validate_clusters(&cfg.clusters, &mut errs);
    validate_repos(&cfg.repos, &mut errs);
    validate_plugins(&cfg.plugins, &mut errs);
    validate_polling(&cfg.polling, &mut errs);
    validate_timeouts(&cfg.timeouts, &mut errs);
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

fn validate_server(s: &ServerSection, errs: &mut Vec<ValidationError>) {
    if s.listen_addr.trim().is_empty() {
        errs.push(ValidationError::new(
            "server.listen_addr",
            "must not be empty",
        ));
    } else if !s.listen_addr.contains(':') {
        errs.push(ValidationError::new(
            "server.listen_addr",
            "must include a port (e.g. 0.0.0.0:8484)",
        ));
    }
    if s.db_path.as_os_str().is_empty() {
        errs.push(ValidationError::new("server.db_path", "must not be empty"));
    }
}

fn validate_clusters(clusters: &[ClusterCfg], errs: &mut Vec<ValidationError>) {
    let mut seen = HashSet::new();
    for (i, c) in clusters.iter().enumerate() {
        let prefix = format!("clusters[{i}]");
        if c.name.trim().is_empty() {
            errs.push(ValidationError::new(
                format!("{prefix}.name"),
                "must not be empty",
            ));
        } else if !seen.insert(c.name.clone()) {
            errs.push(ValidationError::new(
                format!("{prefix}.name"),
                format!("duplicate cluster name '{}'", c.name),
            ));
        }
        if c.in_cluster && c.kubeconfig.is_some() {
            errs.push(ValidationError::new(
                prefix.clone(),
                "in_cluster=true is mutually exclusive with kubeconfig",
            ));
        }
        if !c.in_cluster && c.kubeconfig.is_none() {
            errs.push(ValidationError::new(
                prefix.clone(),
                "must set either kubeconfig or in_cluster=true",
            ));
        }
    }
}

fn validate_repos(repos: &[RepoCfg], errs: &mut Vec<ValidationError>) {
    let mut seen = HashSet::new();
    for (i, r) in repos.iter().enumerate() {
        let prefix = format!("repos[{i}]");
        if r.id.trim().is_empty() {
            errs.push(ValidationError::new(
                format!("{prefix}.id"),
                "must not be empty",
            ));
        } else if !seen.insert(r.id.clone()) {
            errs.push(ValidationError::new(
                format!("{prefix}.id"),
                format!("duplicate repo id '{}'", r.id),
            ));
        }
        if r.url.trim().is_empty() {
            errs.push(ValidationError::new(
                format!("{prefix}.url"),
                "must not be empty",
            ));
        }
    }
}

fn validate_plugins(plugins: &[PluginCfg], errs: &mut Vec<ValidationError>) {
    let mut seen = HashSet::new();
    for (i, p) in plugins.iter().enumerate() {
        let prefix = format!("plugins[{i}]");
        if p.name.trim().is_empty() {
            errs.push(ValidationError::new(
                format!("{prefix}.name"),
                "must not be empty",
            ));
        } else if !seen.insert(p.name.clone()) {
            errs.push(ValidationError::new(
                format!("{prefix}.name"),
                format!("duplicate plugin name '{}'", p.name),
            ));
        }
    }
}

fn validate_polling(p: &Polling, errs: &mut Vec<ValidationError>) {
    if p.repo_interval_seconds == 0 {
        errs.push(ValidationError::new(
            "polling.repo_interval_seconds",
            "must be > 0",
        ));
    }
    if p.auto_heal_interval_seconds == 0 {
        errs.push(ValidationError::new(
            "polling.auto_heal_interval_seconds",
            "must be > 0",
        ));
    }
}

fn validate_timeouts(t: &Timeouts, errs: &mut Vec<ValidationError>) {
    if t.reconcile_seconds == 0 {
        errs.push(ValidationError::new(
            "timeouts.reconcile_seconds",
            "must be > 0",
        ));
    }
    if t.git_fetch_seconds == 0 {
        errs.push(ValidationError::new(
            "timeouts.git_fetch_seconds",
            "must be > 0",
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        validate(&Config::default()).expect("default config should validate");
    }

    #[test]
    fn empty_listen_addr_is_rejected() {
        let mut cfg = Config::default();
        cfg.server.listen_addr = String::new();
        let err = validate(&cfg).unwrap_err();
        assert!(err.iter().any(|e| e.path == "server.listen_addr"));
    }

    #[test]
    fn listen_addr_without_port_is_rejected() {
        let mut cfg = Config::default();
        cfg.server.listen_addr = "0.0.0.0".into();
        let err = validate(&cfg).unwrap_err();
        assert!(err.iter().any(|e| e.path == "server.listen_addr"));
    }

    #[test]
    fn cluster_with_both_kubeconfig_and_in_cluster_is_rejected() {
        let cfg = Config {
            clusters: vec![ClusterCfg {
                name: "prod".into(),
                kubeconfig: Some("/tmp/kc".into()),
                context: None,
                in_cluster: true,
            }],
            ..Default::default()
        };
        let err = validate(&cfg).unwrap_err();
        assert_eq!(err[0].path, "clusters[0]");
    }

    #[test]
    fn cluster_with_neither_source_is_rejected() {
        let cfg = Config {
            clusters: vec![ClusterCfg {
                name: "prod".into(),
                kubeconfig: None,
                context: None,
                in_cluster: false,
            }],
            ..Default::default()
        };
        let err = validate(&cfg).unwrap_err();
        assert_eq!(err[0].path, "clusters[0]");
    }

    #[test]
    fn duplicate_cluster_names_are_rejected_with_index() {
        let cfg = Config {
            clusters: vec![
                ClusterCfg {
                    name: "prod".into(),
                    kubeconfig: Some("/tmp/kc".into()),
                    context: None,
                    in_cluster: false,
                },
                ClusterCfg {
                    name: "prod".into(),
                    kubeconfig: Some("/tmp/kc".into()),
                    context: None,
                    in_cluster: false,
                },
            ],
            ..Default::default()
        };
        let err = validate(&cfg).unwrap_err();
        assert!(err.iter().any(|e| e.path == "clusters[1].name"));
    }

    #[test]
    fn empty_repo_id_is_rejected() {
        let cfg = Config {
            repos: vec![RepoCfg {
                id: String::new(),
                url: "https://example.com/x.git".into(),
                branch: None,
                credentials_secret: None,
            }],
            ..Default::default()
        };
        let err = validate(&cfg).unwrap_err();
        assert!(err.iter().any(|e| e.path == "repos[0].id"));
    }

    #[test]
    fn zero_interval_is_rejected() {
        let mut cfg = Config::default();
        cfg.polling.repo_interval_seconds = 0;
        let err = validate(&cfg).unwrap_err();
        assert!(err
            .iter()
            .any(|e| e.path == "polling.repo_interval_seconds"));
    }

    #[test]
    fn errors_accumulate_across_sections() {
        let mut cfg = Config::default();
        cfg.server.listen_addr = String::new();
        cfg.timeouts.reconcile_seconds = 0;
        cfg.repos = vec![RepoCfg {
            id: String::new(),
            url: String::new(),
            branch: None,
            credentials_secret: None,
        }];
        let err = validate(&cfg).unwrap_err();
        assert!(err.len() >= 4, "got: {err:#?}");
    }
}
