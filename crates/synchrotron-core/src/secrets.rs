//! Pluggable secret resolution.
//!
//! Operators reference secrets by name in the config (e.g.
//! `RepoCfg.credentials_secret = "github-app-foo"`); a [`SecretStore`]
//! turns that name into bytes the consumer parses into whatever
//! shape it expects (e.g. `synchrotron_git::Credentials` deserialized
//! from a JSON or YAML body).
//!
//! # Slice scope (synchrotron-cd-u0o)
//!
//! Two backends:
//!
//! - [`EnvSecretStore`] — `SYNCHROTRON_SECRET_<UPPER_NAME>`. Useful
//!   for development and for k8s deployments that pass per-secret
//!   environment variables from a Secret.
//! - [`FileSecretStore`] — `<root>/<name>` files, one secret per
//!   file. Matches the standard k8s `volumeMounts.subPath` pattern
//!   for projecting a Secret into the pod's filesystem (`/etc/synchrotron/secrets/<name>`).
//!
//! [`CompositeSecretStore`] tries a list of backends in order and
//! returns the first hit. This lets ops layer "env wins, file is
//! the bulk" or vice versa.
//!
//! Out of scope (file as needed):
//!   - Vault / AWS Secrets Manager / GCP Secret Manager
//!   - Refresh notifications (the consumer reads on each use, so
//!     long-running secrets can rotate behind us — but informed
//!     rotation that triggers an immediate poll is its own story)
//!   - Per-tenant scoping

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Resolved secret value. Most credentials are text (PATs, GitHub
/// App private keys in PEM, kubeconfig YAML); binary secrets are
/// base64-encoded by convention.
pub type SecretValue = String;

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret `{name}` not found")]
    NotFound { name: String },
    #[error("secret `{name}`: I/O error reading {path}: {source}")]
    Io {
        name: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("secret `{name}`: contents are not valid UTF-8")]
    InvalidUtf8 { name: String },
}

pub trait SecretStore: Send + Sync + std::fmt::Debug {
    fn get(&self, name: &str) -> Result<SecretValue, SecretError>;
}

/// Tries a series of backends in order; first hit wins.
/// `NotFound` propagates only when every backend reports `NotFound`.
/// Other errors short-circuit (a malformed file is a failure even
/// if a later backend has the secret — fail loudly).
#[derive(Debug)]
pub struct CompositeSecretStore {
    stores: Vec<Box<dyn SecretStore>>,
}

impl CompositeSecretStore {
    pub fn new(stores: Vec<Box<dyn SecretStore>>) -> Self {
        Self { stores }
    }
}

impl SecretStore for CompositeSecretStore {
    fn get(&self, name: &str) -> Result<SecretValue, SecretError> {
        for store in &self.stores {
            match store.get(name) {
                Ok(v) => return Ok(v),
                Err(SecretError::NotFound { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(SecretError::NotFound {
            name: name.to_string(),
        })
    }
}

/// Looks up `<prefix><UPPER_NAME>` from the process environment.
/// `-` in `name` is normalized to `_` because POSIX env names are
/// `[A-Z0-9_]+`.
#[derive(Debug, Clone)]
pub struct EnvSecretStore {
    prefix: String,
}

impl EnvSecretStore {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    fn env_name(&self, name: &str) -> String {
        format!("{}{}", self.prefix, name.to_uppercase().replace('-', "_"))
    }
}

impl SecretStore for EnvSecretStore {
    fn get(&self, name: &str) -> Result<SecretValue, SecretError> {
        match std::env::var(self.env_name(name)) {
            Ok(v) => Ok(v),
            Err(_) => Err(SecretError::NotFound {
                name: name.to_string(),
            }),
        }
    }
}

/// Reads `<root>/<name>` from the filesystem. Strips a single
/// trailing newline so file-mounted secrets that end with `\n`
/// (basically all of them) don't poison the consumer.
#[derive(Debug, Clone)]
pub struct FileSecretStore {
    root: PathBuf,
}

impl FileSecretStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, name: &str) -> PathBuf {
        // Reject `..` segments so a malicious config can't escape
        // the secret root. (Names with `/` are also rejected by the
        // strip-then-rejoin below — `Path::new("a/b").components()`
        // would split into two normals, but our join here keeps
        // them under root.)
        let clean: PathBuf = Path::new(name)
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(s) => Some(PathBuf::from(s)),
                _ => None,
            })
            .collect();
        self.root.join(clean)
    }
}

impl SecretStore for FileSecretStore {
    fn get(&self, name: &str) -> Result<SecretValue, SecretError> {
        let path = self.path_for(name);
        if !path.exists() {
            return Err(SecretError::NotFound {
                name: name.to_string(),
            });
        }
        let bytes = std::fs::read(&path).map_err(|source| SecretError::Io {
            name: name.to_string(),
            path: path.clone(),
            source,
        })?;
        let s = String::from_utf8(bytes).map_err(|_| SecretError::InvalidUtf8 {
            name: name.to_string(),
        })?;
        // Strip exactly one trailing newline — common with `echo -n`
        // not being used. Don't `.trim()` because a leading space
        // can be meaningful (some tokens start with one in tests).
        Ok(s.strip_suffix('\n').unwrap_or(&s).to_string())
    }
}

/// Always reports `NotFound`. The default for cases where the
/// operator hasn't configured any secret backend; used by tests
/// where no repo references a secret.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSecretStore;

impl SecretStore for NoopSecretStore {
    fn get(&self, name: &str) -> Result<SecretValue, SecretError> {
        Err(SecretError::NotFound {
            name: name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn noop_returns_not_found() {
        let r = NoopSecretStore.get("anything");
        assert!(matches!(r, Err(SecretError::NotFound { .. })));
    }

    #[test]
    fn env_store_resolves_uppercase_with_prefix() {
        // Pick a unique key so parallel tests don't collide.
        let key = "SYNCHROTRON_SECRET_TEST_U0O_ENV_OK";
        // SAFETY: setting a unique env var; race only matters if two
        // tests use the same key, which the unique name avoids.
        std::env::set_var(key, "tok");
        let store = EnvSecretStore::new("SYNCHROTRON_SECRET_");
        assert_eq!(store.get("test-u0o-env-ok").unwrap(), "tok");
        std::env::remove_var(key);
    }

    #[test]
    fn env_store_returns_not_found_for_unset() {
        let store = EnvSecretStore::new("SYNCHROTRON_SECRET_");
        let r = store.get("definitely-unset-zzz");
        assert!(matches!(r, Err(SecretError::NotFound { .. })));
    }

    #[test]
    fn file_store_reads_and_strips_trailing_newline() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("token"), "abc123\n").unwrap();
        let store = FileSecretStore::new(dir.path());
        assert_eq!(store.get("token").unwrap(), "abc123");
    }

    #[test]
    fn file_store_handles_no_trailing_newline() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("token"), "abc123").unwrap();
        let store = FileSecretStore::new(dir.path());
        assert_eq!(store.get("token").unwrap(), "abc123");
    }

    #[test]
    fn file_store_returns_not_found_for_missing_file() {
        let dir = TempDir::new().unwrap();
        let store = FileSecretStore::new(dir.path());
        assert!(matches!(
            store.get("missing"),
            Err(SecretError::NotFound { .. })
        ));
    }

    #[test]
    fn file_store_rejects_path_traversal() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("token"), "abc").unwrap();
        let store = FileSecretStore::new(dir.path());
        // ../token resolves to "token" under root after component
        // filtering, so this is "Ok" — but the more important case
        // is that ../../etc/passwd never escapes the root.
        let bad = store.get("../../etc/passwd");
        assert!(matches!(bad, Err(SecretError::NotFound { .. })));
    }

    #[test]
    fn composite_first_hit_wins() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("token"), "from-file").unwrap();
        let key = "SYNCHROTRON_SECRET_TEST_U0O_COMPOSITE_TOKEN";
        std::env::set_var(key, "from-env");

        // env first → env wins
        let composite = CompositeSecretStore::new(vec![
            Box::new(EnvSecretStore::new("SYNCHROTRON_SECRET_")),
            Box::new(FileSecretStore::new(dir.path())),
        ]);
        assert_eq!(
            composite.get("test-u0o-composite-token").unwrap(),
            "from-env"
        );
        std::env::remove_var(key);
    }

    #[test]
    fn composite_falls_through_on_not_found() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("only-here"), "found").unwrap();
        let composite = CompositeSecretStore::new(vec![
            Box::new(EnvSecretStore::new("SYNCHROTRON_SECRET_NOMATCH_")),
            Box::new(FileSecretStore::new(dir.path())),
        ]);
        assert_eq!(composite.get("only-here").unwrap(), "found");
    }

    #[test]
    fn composite_propagates_non_not_found_errors() {
        // A FileSecretStore pointing at a path where the file
        // exists but is unreadable would surface Io. Hard to
        // simulate portably; instead verify the contract with
        // a stub.
        #[derive(Debug)]
        struct AlwaysIo;
        impl SecretStore for AlwaysIo {
            fn get(&self, name: &str) -> Result<SecretValue, SecretError> {
                Err(SecretError::Io {
                    name: name.to_string(),
                    path: PathBuf::from("/nope"),
                    source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
                })
            }
        }
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("would-find"), "...").unwrap();
        let composite = CompositeSecretStore::new(vec![
            Box::new(AlwaysIo),
            Box::new(FileSecretStore::new(dir.path())),
        ]);
        let r = composite.get("would-find");
        // AlwaysIo's error short-circuits — the later FileSecretStore
        // is never consulted, even though it would have succeeded.
        assert!(matches!(r, Err(SecretError::Io { .. })));
    }
}
