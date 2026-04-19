//! Core logic for the Kustomize local plugin, factored out of
//! `main.rs` so argument-building and the remote-base allowlist
//! check are unit-testable without spawning kustomize.
//!
//! Security posture: remote bases are a real supply-chain risk in
//! GitOps because an overlay edit can silently pull arbitrary code
//! paths from the internet. By default this plugin rejects any
//! `resources:` / `bases:` / `components:` entry that looks like a
//! URL. Operators who need remote bases must (a) set
//! `allow_remote_bases: true` and (b) provide an explicit prefix
//! `remote_base_allowlist`. "Allow everything" requires an explicit
//! wildcard, not a missing config.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::Deserialize;
use thiserror::Error;

pub const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Parameters the Application spec passes to the Kustomize plugin.
#[derive(Debug, Clone, Deserialize)]
pub struct RenderParams {
    /// Subdirectory within `source_path` holding `kustomization.yaml`.
    /// Defaults to the source root.
    #[serde(default)]
    pub path: Option<String>,

    /// When false (default), references outside the kustomization
    /// root are rejected via `--load-restrictor LoadRestrictionsRootOnly`
    /// and any URL-shaped resource entry is rejected before invoking
    /// kustomize.
    #[serde(default)]
    pub allow_remote_bases: bool,

    /// Prefix matches applied to each URL-shaped resource entry. Any
    /// entry that doesn't match at least one prefix is rejected,
    /// even when `allow_remote_bases` is true. Use `"*"` to allow
    /// anything (explicit wildcard).
    #[serde(default)]
    pub remote_base_allowlist: Vec<String>,

    /// Pass `--enable-helm` to kustomize, letting `helmCharts:`
    /// entries render via helm. Off by default because it pulls
    /// charts from the network.
    #[serde(default)]
    pub enable_helm: bool,
}

#[derive(Debug, Error)]
pub enum KustomizeError {
    #[error("kustomize not found: {source}")]
    BinaryNotFound {
        #[source]
        source: std::io::Error,
    },
    #[error("kustomize i/o error: {source}")]
    Io {
        #[source]
        source: std::io::Error,
    },
    #[error("kustomize exited {code}:\n{stderr}")]
    NonZeroExit { code: i32, stderr: String },
    #[error("kustomization not found: {path}")]
    Missing { path: PathBuf },
    #[error("failed to read {path}: {source}")]
    ReadKustomization {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    ParseKustomization {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("remote base `{entry}` not permitted (allow_remote_bases=false)")]
    RemoteBaseDisabled { entry: String },
    #[error(
        "remote base `{entry}` did not match any prefix in remote_base_allowlist {allowlist:?}"
    )]
    RemoteBaseNotAllowlisted {
        entry: String,
        allowlist: Vec<String>,
    },
}

pub struct KustomizeRunner {
    /// Path or name of the kustomize binary. Defaults to `kustomize`
    /// on PATH; override via `KUSTOMIZE_BINARY`.
    pub binary: OsString,
}

impl Default for KustomizeRunner {
    fn default() -> Self {
        let binary =
            std::env::var_os("KUSTOMIZE_BINARY").unwrap_or_else(|| OsString::from("kustomize"));
        Self { binary }
    }
}

impl KustomizeRunner {
    pub fn resolve_kustomization_dir(
        &self,
        source_path: &Path,
        params: &RenderParams,
    ) -> Result<PathBuf, KustomizeError> {
        let dir = match &params.path {
            Some(rel) => source_path.join(rel),
            None => source_path.to_path_buf(),
        };
        if kustomization_file(&dir).is_none() {
            return Err(KustomizeError::Missing { path: dir });
        }
        Ok(dir)
    }

    pub fn render(
        &self,
        source_path: &Path,
        params: &RenderParams,
    ) -> Result<String, KustomizeError> {
        let dir = self.resolve_kustomization_dir(source_path, params)?;
        check_remote_bases(&dir, params)?;

        let args = build_args(&dir, params);
        let output = run(&self.binary, &args)?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Build the argv for `kustomize build`. Exposed for unit tests.
pub fn build_args(kustomization_dir: &Path, params: &RenderParams) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        OsString::from("build"),
        kustomization_dir.as_os_str().to_os_string(),
    ];
    if !params.allow_remote_bases {
        args.push(OsString::from("--load-restrictor"));
        args.push(OsString::from("LoadRestrictionsRootOnly"));
    }
    if params.enable_helm {
        args.push(OsString::from("--enable-helm"));
    }
    args
}

/// Enforce the remote-base policy against the top-level
/// kustomization.yaml. Nested overlays are covered by kustomize's
/// own `--load-restrictor` flag when `allow_remote_bases: false`.
pub fn check_remote_bases(
    kustomization_dir: &Path,
    params: &RenderParams,
) -> Result<(), KustomizeError> {
    let Some(kf) = kustomization_file(kustomization_dir) else {
        return Err(KustomizeError::Missing {
            path: kustomization_dir.to_path_buf(),
        });
    };
    let text =
        std::fs::read_to_string(&kf).map_err(|source| KustomizeError::ReadKustomization {
            path: kf.clone(),
            source,
        })?;
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&text).map_err(|source| KustomizeError::ParseKustomization {
            path: kf.clone(),
            source,
        })?;

    for field in ["resources", "bases", "components"] {
        if let Some(list) = doc.get(field).and_then(|v| v.as_sequence()) {
            for entry in list {
                let Some(s) = entry.as_str() else { continue };
                if !looks_remote(s) {
                    continue;
                }
                if !params.allow_remote_bases {
                    return Err(KustomizeError::RemoteBaseDisabled {
                        entry: s.to_string(),
                    });
                }
                if !prefix_allowed(s, &params.remote_base_allowlist) {
                    return Err(KustomizeError::RemoteBaseNotAllowlisted {
                        entry: s.to_string(),
                        allowlist: params.remote_base_allowlist.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

pub fn kustomization_file(dir: &Path) -> Option<PathBuf> {
    // kustomize recognizes three filenames; we check in the same
    // order kustomize does.
    for name in ["kustomization.yaml", "kustomization.yml", "Kustomization"] {
        let p = dir.join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn looks_remote(entry: &str) -> bool {
    // Patterns kustomize treats as remote: scheme URLs, explicit
    // go-getter prefixes, and the shorthand github.com/ form used
    // extensively in examples.
    let lower = entry.to_ascii_lowercase();
    lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("git@")
        || lower.starts_with("git::")
        || lower.starts_with("ssh://")
        || lower.starts_with("github.com/")
        || lower.starts_with("gitlab.com/")
        || lower.starts_with("bitbucket.org/")
}

fn prefix_allowed(entry: &str, allowlist: &[String]) -> bool {
    allowlist.iter().any(|p| p == "*" || entry.starts_with(p))
}

fn run(binary: &OsString, args: &[OsString]) -> Result<Output, KustomizeError> {
    let output = Command::new(binary).args(args).output().map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            KustomizeError::BinaryNotFound { source }
        } else {
            KustomizeError::Io { source }
        }
    })?;
    if !output.status.success() {
        return Err(KustomizeError::NonZeroExit {
            code: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn params_default() -> RenderParams {
        RenderParams {
            path: None,
            allow_remote_bases: false,
            remote_base_allowlist: vec![],
            enable_helm: false,
        }
    }

    #[test]
    fn build_args_default_has_load_restrictor() {
        let args = build_args(Path::new("/k"), &params_default());
        assert!(args.contains(&OsString::from("--load-restrictor")));
        assert!(args.contains(&OsString::from("LoadRestrictionsRootOnly")));
    }

    #[test]
    fn build_args_with_remote_bases_drops_load_restrictor() {
        let mut p = params_default();
        p.allow_remote_bases = true;
        let args = build_args(Path::new("/k"), &p);
        assert!(!args.contains(&OsString::from("--load-restrictor")));
    }

    #[test]
    fn build_args_enable_helm() {
        let mut p = params_default();
        p.enable_helm = true;
        let args = build_args(Path::new("/k"), &p);
        assert!(args.contains(&OsString::from("--enable-helm")));
    }

    #[test]
    fn looks_remote_detects_common_shapes() {
        for s in [
            "https://example.com/x",
            "http://example.com",
            "git@github.com:org/r",
            "git::https://example.com",
            "ssh://git@github.com/x/y",
            "github.com/org/repo",
            "gitlab.com/org/repo",
        ] {
            assert!(looks_remote(s), "{s}");
        }
        for s in ["./base", "../other", "subdir/kustomization.yaml"] {
            assert!(!looks_remote(s), "{s}");
        }
    }

    #[test]
    fn check_remote_bases_allows_local_only_config() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("kustomization.yaml"),
            "resources:\n  - deployment.yaml\n  - ./overlays/prod\n",
        )
        .unwrap();
        check_remote_bases(tmp.path(), &params_default()).unwrap();
    }

    #[test]
    fn check_remote_bases_rejects_remote_when_disabled() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("kustomization.yaml"),
            "resources:\n  - github.com/acme/base?ref=v1\n",
        )
        .unwrap();
        let err = check_remote_bases(tmp.path(), &params_default()).unwrap_err();
        assert!(matches!(err, KustomizeError::RemoteBaseDisabled { .. }));
    }

    #[test]
    fn check_remote_bases_enforces_allowlist() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("kustomization.yaml"),
            "resources:\n  - github.com/acme/base?ref=v1\n",
        )
        .unwrap();
        let mut p = params_default();
        p.allow_remote_bases = true;
        p.remote_base_allowlist = vec!["github.com/other/".into()];
        let err = check_remote_bases(tmp.path(), &p).unwrap_err();
        assert!(matches!(
            err,
            KustomizeError::RemoteBaseNotAllowlisted { .. }
        ));
    }

    #[test]
    fn check_remote_bases_allows_matching_prefix() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("kustomization.yaml"),
            "resources:\n  - github.com/acme/base?ref=v1\n",
        )
        .unwrap();
        let mut p = params_default();
        p.allow_remote_bases = true;
        p.remote_base_allowlist = vec!["github.com/acme/".into()];
        check_remote_bases(tmp.path(), &p).unwrap();
    }

    #[test]
    fn check_remote_bases_wildcard_matches_all() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("kustomization.yaml"),
            "components:\n  - https://somewhere/overlay\n",
        )
        .unwrap();
        let mut p = params_default();
        p.allow_remote_bases = true;
        p.remote_base_allowlist = vec!["*".into()];
        check_remote_bases(tmp.path(), &p).unwrap();
    }

    #[test]
    fn resolve_errors_when_kustomization_missing() {
        let tmp = tempdir();
        let runner = KustomizeRunner::default();
        let err = runner
            .resolve_kustomization_dir(tmp.path(), &params_default())
            .unwrap_err();
        assert!(matches!(err, KustomizeError::Missing { .. }));
    }

    // Tempdir helper — kept private, avoids a tempfile dep.
    struct TempDir(PathBuf);
    impl TempDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        let mut p = std::env::temp_dir();
        p.push(format!("synchrotron-kustomize-test-{}", unique()));
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn unique() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
