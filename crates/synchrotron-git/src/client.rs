use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use git2::{
    cert::Cert, CertificateCheckStatus, Cred, CredentialType, FetchOptions, ObjectType,
    RemoteCallbacks, Repository, Tree,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::credentials::Credentials;
use crate::error::GitError;
use crate::known_hosts::HostVerifier;
use crate::repo::Repo;
use crate::workspace::Workspace;
use crate::Result;

/// A git commit hash (hex-encoded SHA-1, 40 chars).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sha(pub String);

impl Sha {
    fn from_oid(oid: git2::Oid) -> Self {
        Self(oid.to_string())
    }
}

impl std::fmt::Display for Sha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Outcome of a fetch: whether the tracked branch advanced, plus the
/// before/after commit hashes for caller-side change detection.
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub previous_head: Option<Sha>,
    pub current_head: Sha,
    pub changed: bool,
}

/// Synchronous git client. Operations block; callers running on tokio should
/// wrap calls in `tokio::task::spawn_blocking`.
pub struct GitClient {
    workspace: Workspace,
    /// Shared so the libgit2 `certificate_check` callback can mutate
    /// it (TOFU writes new entries) without leaking lifetimes into
    /// the public API.
    host_verifier: Arc<Mutex<HostVerifier>>,
}

impl GitClient {
    /// Create a client with verification disabled. Suitable for
    /// `file://` integration tests; production callers should use
    /// [`Self::with_host_verifier`] and pass a real
    /// [`HostVerifier`].
    pub fn new(workspace: Workspace) -> Self {
        Self {
            workspace,
            host_verifier: Arc::new(Mutex::new(HostVerifier::insecure())),
        }
    }

    /// Create a client that verifies SSH host keys per the given
    /// [`HostVerifier`]. HTTPS transports are unaffected — libgit2
    /// already validates X.509 certs against the system trust store.
    pub fn with_host_verifier(workspace: Workspace, verifier: HostVerifier) -> Self {
        Self {
            workspace,
            host_verifier: Arc::new(Mutex::new(verifier)),
        }
    }

    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    /// Returns the path to the on-disk bare cache for `repo`, creating
    /// the workspace layout if missing.
    pub fn bare_path(&self, repo: &Repo) -> Result<PathBuf> {
        self.workspace.ensure_layout()?;
        Ok(self.workspace.bare_path(&repo.id))
    }

    /// Performs an initial shallow clone if the cache is missing; otherwise
    /// a no-op. Subsequent updates go through [`Self::fetch`].
    pub fn ensure_cloned(&self, repo: &Repo) -> Result<()> {
        let bare_path = self.bare_path(repo)?;
        if bare_path.join("HEAD").exists() {
            debug!(repo.id = repo.id.as_str(), "bare cache already present");
            return Ok(());
        }

        info!(
            repo.id = repo.id.as_str(),
            url = %repo.url,
            branch = %repo.branch,
            "cloning bare cache",
        );

        let credentials = repo.credentials.clone();
        let callbacks = build_callbacks(credentials, Arc::clone(&self.host_verifier));
        let mut fetch_opts = FetchOptions::new();
        fetch_opts.remote_callbacks(callbacks);
        if let Some(depth) = repo.depth {
            fetch_opts.depth(depth);
        }

        let mut builder = git2::build::RepoBuilder::new();
        builder.bare(true);
        builder.fetch_options(fetch_opts);
        builder.branch(&repo.branch);
        builder.clone(&repo.url.0, &bare_path)?;

        Ok(())
    }

    /// Fetches latest commits from origin for `repo.branch` and reports
    /// the change in HEAD. Caller is responsible for persisting the new
    /// `current_head` (e.g. into SQLite).
    pub fn fetch(&self, repo: &Repo) -> Result<FetchResult> {
        self.ensure_cloned(repo)?;

        let bare_path = self.workspace.bare_path(&repo.id);
        let bare = Repository::open_bare(&bare_path)?;

        let previous_head = read_branch_head(&bare, &repo.branch).ok();

        let credentials = repo.credentials.clone();
        let callbacks = build_callbacks(credentials, Arc::clone(&self.host_verifier));
        let mut fetch_opts = FetchOptions::new();
        fetch_opts.remote_callbacks(callbacks);
        if let Some(depth) = repo.depth {
            fetch_opts.depth(depth);
        }

        let refspec = format!("+refs/heads/{0}:refs/heads/{0}", repo.branch);
        let mut remote = bare.find_remote("origin")?;
        remote.fetch(&[refspec.as_str()], Some(&mut fetch_opts), None)?;

        let current_head = read_branch_head(&bare, &repo.branch)?;
        let changed = previous_head.as_ref() != Some(&current_head);

        debug!(
            repo.id = repo.id.as_str(),
            previous = ?previous_head,
            current = %current_head,
            changed,
            "fetch complete",
        );

        Ok(FetchResult {
            previous_head,
            current_head,
            changed,
        })
    }

    /// Returns the current HEAD of the tracked branch in the cache,
    /// without contacting the remote.
    pub fn current_head(&self, repo: &Repo) -> Result<Sha> {
        let bare = Repository::open_bare(self.workspace.bare_path(&repo.id))?;
        read_branch_head(&bare, &repo.branch)
    }

    /// Materializes the tree at `commit` into `target_dir` by walking the
    /// commit tree and writing blobs to disk. `target_dir` is created if
    /// missing; existing contents are not removed.
    ///
    /// This avoids libgit2 worktree machinery (which is finicky for
    /// short-lived render dirs) and keeps the rendered output a plain
    /// directory tree the plugin layer can consume directly.
    pub fn materialize(&self, repo: &Repo, commit: &Sha, target_dir: &Path) -> Result<()> {
        let bare = Repository::open_bare(self.workspace.bare_path(&repo.id))?;
        let oid = git2::Oid::from_str(&commit.0).map_err(GitError::from)?;
        let commit_obj = bare
            .find_commit(oid)
            .map_err(|_| GitError::CommitNotFound(commit.0.clone()))?;
        let tree = commit_obj.tree()?;

        fs::create_dir_all(target_dir).map_err(|e| GitError::Io {
            path: target_dir.to_path_buf(),
            source: e,
        })?;
        write_tree(&bare, &tree, target_dir)?;
        Ok(())
    }
}

fn build_callbacks(
    credentials: Credentials,
    host_verifier: Arc<Mutex<HostVerifier>>,
) -> RemoteCallbacks<'static> {
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(move |_url, username_from_url, allowed| {
        select_credential(&credentials, username_from_url, allowed)
    });
    callbacks.certificate_check(move |cert, host| verify_certificate(cert, host, &host_verifier));
    callbacks
}

/// libgit2's `certificate_check` hook. For SSH transports the cert
/// is `Cert::Hostkey`; for HTTPS it's `Cert::X509` (which we hand
/// back to libgit2's default trust store via `CertificatePassthrough`).
///
/// The `host` argument from libgit2 is `host[:port]`; we split it
/// before passing to the verifier.
fn verify_certificate(
    cert: &Cert<'_>,
    host: &str,
    host_verifier: &Arc<Mutex<HostVerifier>>,
) -> std::result::Result<CertificateCheckStatus, git2::Error> {
    let hostkey = match cert.as_hostkey() {
        Some(hk) => hk,
        // HTTPS / unknown: defer to libgit2's built-in checks.
        None => return Ok(CertificateCheckStatus::CertificatePassthrough),
    };
    let key_bytes = match hostkey.hostkey() {
        Some(b) => b,
        None => {
            return Err(git2::Error::from_str(
                "SSH host presented no key bytes; refusing to trust",
            ));
        }
    };
    let key_type = match hostkey.hostkey_type() {
        Some(t) => hostkey_type_str(t),
        None => {
            return Err(git2::Error::from_str(
                "SSH host key type unknown; refusing to trust",
            ));
        }
    };

    let (hostname, port) = split_host_port(host);
    let mut verifier = host_verifier
        .lock()
        .map_err(|_| git2::Error::from_str("host verifier mutex poisoned"))?;
    match verifier.check(hostname, port, key_type, key_bytes) {
        Ok(()) => Ok(CertificateCheckStatus::CertificateOk),
        Err(e) => {
            warn!(host = hostname, error = %e, "SSH host key verification failed");
            Err(git2::Error::from_str(&format!(
                "SSH host key verification failed for {hostname}: {e}"
            )))
        }
    }
}

fn hostkey_type_str(t: git2::cert::SshHostKeyType) -> &'static str {
    use git2::cert::SshHostKeyType;
    match t {
        SshHostKeyType::Rsa => "ssh-rsa",
        SshHostKeyType::Dss => "ssh-dss",
        SshHostKeyType::Ecdsa256 => "ecdsa-sha2-nistp256",
        SshHostKeyType::Ecdsa384 => "ecdsa-sha2-nistp384",
        SshHostKeyType::Ecdsa521 => "ecdsa-sha2-nistp521",
        SshHostKeyType::Ed255219 => "ssh-ed25519",
        // libgit2 may report Unknown; we surface the best string we have.
        _ => "ssh-unknown",
    }
}

fn split_host_port(host: &str) -> (&str, u16) {
    if let Some(rest) = host.strip_prefix('[') {
        // [host]:port form
        if let Some((h, p)) = rest.split_once("]:") {
            if let Ok(port) = p.parse::<u16>() {
                return (h, port);
            }
        }
    }
    if let Some((h, p)) = host.rsplit_once(':') {
        if let Ok(port) = p.parse::<u16>() {
            return (h, port);
        }
    }
    (host, 22)
}

/// Translate a [`Credentials`] choice into a libgit2 [`Cred`] for the
/// allowed credential types libgit2 reports for the current transport.
/// Extracted from the callback so it can be unit-tested without a
/// remote.
pub(crate) fn select_credential(
    credentials: &Credentials,
    username_from_url: Option<&str>,
    allowed: CredentialType,
) -> std::result::Result<Cred, git2::Error> {
    match credentials {
        Credentials::HttpBasic { username, password }
            if allowed.contains(CredentialType::USER_PASS_PLAINTEXT) =>
        {
            Cred::userpass_plaintext(username, password)
        }
        Credentials::SshKey {
            username,
            private_key,
            public_key,
            passphrase,
        } if allowed.contains(CredentialType::SSH_KEY) => Cred::ssh_key(
            username,
            public_key.as_deref(),
            private_key,
            passphrase.as_deref(),
        ),
        Credentials::SshAgent { username } if allowed.contains(CredentialType::SSH_KEY) => {
            Cred::ssh_key_from_agent(username)
        }
        // libgit2 invokes the credentials callback for username probing
        // on some transports (e.g. SSH); satisfy it with a default so
        // unauthenticated HTTP(S) clones still work, and SSH gets the
        // user it needs before the SSH_KEY callback round-trip.
        _ if allowed.contains(CredentialType::USERNAME) => {
            let user = match credentials {
                Credentials::SshKey { username, .. } | Credentials::SshAgent { username } => {
                    username.as_str()
                }
                _ => username_from_url.unwrap_or(""),
            };
            Cred::username(user)
        }
        // GitHubApp must be resolved to HttpBasic before reaching this
        // sync callback (see crate::github_app::TokenCache). Failing
        // here surfaces the misuse rather than silently authenticating
        // anonymously.
        Credentials::GitHubApp { .. } => Err(git2::Error::from_str(
            "GitHubApp credentials must be exchanged for an installation token \
             via github_app::TokenCache before fetch",
        )),
        _ => Cred::default(),
    }
}

fn read_branch_head(repo: &Repository, branch: &str) -> Result<Sha> {
    let reference = repo.find_reference(&format!("refs/heads/{branch}"))?;
    let oid = reference
        .target()
        .ok_or_else(|| GitError::InvalidState(format!("ref refs/heads/{branch} has no target")))?;
    Ok(Sha::from_oid(oid))
}

fn write_tree(repo: &Repository, tree: &Tree<'_>, base: &Path) -> Result<()> {
    for entry in tree.iter() {
        let name = entry
            .name()
            .ok_or_else(|| GitError::InvalidState("tree entry has non-utf8 name".into()))?;
        let path = base.join(name);
        match entry.kind() {
            Some(ObjectType::Tree) => {
                fs::create_dir_all(&path).map_err(|e| GitError::Io {
                    path: path.clone(),
                    source: e,
                })?;
                let subtree = repo.find_tree(entry.id())?;
                write_tree(repo, &subtree, &path)?;
            }
            Some(ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                fs::write(&path, blob.content()).map_err(|e| GitError::Io {
                    path: path.clone(),
                    source: e,
                })?;
                apply_filemode(&path, entry.filemode())?;
            }
            // Submodules and other object kinds are not supported in this
            // foundation slice; they'd require recursion into linked repos.
            _ => {}
        }
    }
    Ok(())
}

#[cfg(unix)]
fn apply_filemode(path: &Path, mode: i32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    // Git stores mode as e.g. 0o100644 / 0o100755; we only care about exec bit.
    let exec = mode & 0o111 != 0;
    let target = if exec { 0o755 } else { 0o644 };
    let mut perms = fs::metadata(path)
        .map_err(|e| GitError::Io {
            path: path.to_path_buf(),
            source: e,
        })?
        .permissions();
    perms.set_mode(target);
    fs::set_permissions(path, perms).map_err(|e| GitError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn apply_filemode(_path: &Path, _mode: i32) -> Result<()> {
    Ok(())
}
