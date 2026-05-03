//! Integration test: drive a `GitClient` SSH fetch against a local
//! `sshd` and exercise [`HostVerifier`] in each mode.
//!
//! The unit tests in `known_hosts.rs` cover the parser, the
//! verification matrix, and the TOFU append path against synthetic
//! key bytes. This test closes the loop end-to-end:
//!
//!   * a real OpenSSH server presents a real host key,
//!   * libgit2's `certificate_check` callback fires with key bytes
//!     produced by libssh2 (not hand-crafted in a unit test),
//!   * `HostVerifier::check` decides accept/reject,
//!   * the fetch either completes or errors with the expected variant.
//!
//! # Skips
//!
//! The test silently passes (with `eprintln!`) on hosts that lack
//! `/usr/sbin/sshd` or `ssh-keygen` — common in stripped-down CI
//! containers. The unit-test layer keeps coverage on those builds; the
//! integration layer runs anywhere a developer-style box exists.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use synchrotron_git::{
    Credentials, GitClient, HostKeyMode, HostVerifier, KnownHostsFile, Repo, Workspace,
};
use synchrotron_types::RepoUrl;

const SSHD_BIN: &str = "/usr/sbin/sshd";

fn precheck() -> bool {
    if !Path::new(SSHD_BIN).exists() {
        eprintln!("skipping: {SSHD_BIN} not found");
        return false;
    }
    if Command::new("ssh-keygen")
        .arg("-V")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("skipping: ssh-keygen not on PATH");
        return false;
    }
    true
}

struct SshdFixture {
    dir: tempfile::TempDir,
    port: u16,
    child: Child,
    /// Wire-format RSA host key, base64-encoded — the same string
    /// that `ssh-keygen` writes to `host_rsa.pub` (column 2).
    host_pubkey_b64: String,
    client_priv: PathBuf,
    client_pub: PathBuf,
    repo_path: PathBuf,
    user: String,
}

impl SshdFixture {
    fn spawn() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = dir.path().to_path_buf();

        // RSA host key: libssh2 in this build doesn't recognise ed25519
        // host keys (libgit2 errors with "invalid or unknown remote ssh
        // hostkey" before our certificate_check fires). RSA is the
        // lowest common denominator that works across libssh2 versions.
        let host_key = p.join("host_rsa");
        run(Command::new("ssh-keygen")
            .args(["-q", "-t", "rsa", "-b", "2048", "-N", "", "-f"])
            .arg(&host_key));
        let host_pub = std::fs::read_to_string(p.join("host_rsa.pub")).unwrap();
        let host_pubkey_b64 = host_pub
            .split_whitespace()
            .nth(1)
            .expect("malformed host pub")
            .to_string();

        let client_key = p.join("client_ed25519");
        run(Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&client_key));
        let client_pub = p.join("client_ed25519.pub");

        let auth_keys = p.join("authorized_keys");
        std::fs::copy(&client_pub, &auth_keys).unwrap();

        let repo_path = build_test_repo(&p);

        let user = whoami();
        let port = pick_port();
        let pidfile = p.join("sshd.pid");
        let logfile = p.join("sshd.log");

        let mut cmd = Command::new(SSHD_BIN);
        cmd.arg("-D")
            .args(["-h", host_key.to_str().unwrap()])
            .args(["-p", &port.to_string()])
            .args(["-f", "/dev/null"])
            .args(["-E", logfile.to_str().unwrap()])
            .args(["-o", &format!("PidFile={}", pidfile.display())])
            .args(["-o", &format!("AuthorizedKeysFile={}", auth_keys.display())])
            .args(["-o", "UsePAM=no"])
            .args(["-o", "PasswordAuthentication=no"])
            .args(["-o", "PubkeyAuthentication=yes"])
            .args(["-o", "KbdInteractiveAuthentication=no"])
            .args(["-o", "ChallengeResponseAuthentication=no"])
            .args(["-o", "StrictModes=no"])
            .args(["-o", "X11Forwarding=no"])
            .args(["-o", "PrintMotd=no"])
            .args(["-o", "PermitRootLogin=no"])
            .args(["-o", &format!("AllowUsers={user}")])
            .args(["-o", "Subsystem=sftp internal-sftp"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let child = cmd.spawn().expect("failed to spawn sshd");
        if !wait_for_port(port, Duration::from_secs(10)) {
            let log = std::fs::read_to_string(&logfile).unwrap_or_default();
            panic!("sshd did not start listening on {port}; log:\n{log}");
        }

        Self {
            dir,
            port,
            child,
            host_pubkey_b64,
            client_priv: client_key,
            client_pub,
            repo_path,
            user,
        }
    }

    fn ssh_url(&self) -> RepoUrl {
        RepoUrl(format!(
            "ssh://{}@127.0.0.1:{}{}",
            self.user,
            self.port,
            self.repo_path.display()
        ))
    }

    fn credentials(&self) -> Credentials {
        Credentials::SshKey {
            username: self.user.clone(),
            private_key: self.client_priv.clone(),
            public_key: Some(self.client_pub.clone()),
            passphrase: None,
        }
    }

    /// libgit2's `certificate_check` callback receives only the bare
    /// hostname (no port), so the verifier always queries / appends the
    /// port-22 form even when the server is on a non-default port.
    fn host_form(&self) -> String {
        "127.0.0.1".to_string()
    }
}

impl Drop for SshdFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn build_test_repo(base: &Path) -> PathBuf {
    let work = base.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let work_str = work.to_str().unwrap();
    run(Command::new("git").args(["-C", work_str, "init", "-q", "-b", "main"]));
    run(Command::new("git").args(["-C", work_str, "config", "user.email", "t@e.test"]));
    run(Command::new("git").args(["-C", work_str, "config", "user.name", "test"]));
    std::fs::write(work.join("README"), b"hello\n").unwrap();
    run(Command::new("git").args(["-C", work_str, "add", "README"]));
    run(Command::new("git").args(["-C", work_str, "commit", "-q", "-m", "init"]));
    let bare = base.join("repo.git");
    run(Command::new("git").args([
        "clone",
        "-q",
        "--bare",
        work_str,
        bare.to_str().unwrap(),
    ]));
    bare
}

fn run(cmd: &mut Command) {
    let out = cmd.output().unwrap_or_else(|e| panic!("spawn: {cmd:?}: {e}"));
    assert!(
        out.status.success(),
        "command failed ({:?}): {:?}\nstdout: {}\nstderr: {}",
        cmd,
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .or_else(|_| {
            String::from_utf8(
                Command::new("id").arg("-un").output().unwrap().stdout,
            )
            .map(|s| s.trim().to_string())
            .map_err(|_| std::env::VarError::NotPresent)
        })
        .expect("could not determine current user")
}

fn write_known_hosts_for(fixture: &SshdFixture, key_b64: &str) -> tempfile::NamedTempFile {
    let f = tempfile::NamedTempFile::new().unwrap();
    let line = format!("{} ssh-rsa {}\n", fixture.host_form(), key_b64);
    std::fs::write(f.path(), line).unwrap();
    f
}

fn make_repo(url: RepoUrl, creds: Credentials) -> Repo {
    // libgit2 doesn't support shallow over SSH on libssh2 builds;
    // disable depth for the integration test.
    Repo::new(url, "main", creds).with_depth(None)
}

#[test]
fn strict_with_correct_known_hosts_accepts() {
    if !precheck() {
        return;
    }
    let fx = SshdFixture::spawn();
    let kh = write_known_hosts_for(&fx, &fx.host_pubkey_b64);
    let verifier = HostVerifier::strict(kh.path()).unwrap();

    let work = tempfile::tempdir().unwrap();
    let client = GitClient::with_host_verifier(Workspace::new(work.path()), verifier);
    let repo = make_repo(fx.ssh_url(), fx.credentials());

    let result = client.fetch(&repo).expect("fetch should succeed");
    assert!(!result.current_head.0.is_empty());
}

#[test]
fn strict_with_unknown_host_rejects() {
    if !precheck() {
        return;
    }
    let fx = SshdFixture::spawn();
    let kh = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(kh.path(), "").unwrap();
    let verifier = HostVerifier::strict(kh.path()).unwrap();

    let work = tempfile::tempdir().unwrap();
    let client = GitClient::with_host_verifier(Workspace::new(work.path()), verifier);
    let repo = make_repo(fx.ssh_url(), fx.credentials());

    let err = client.fetch(&repo).expect_err("fetch must fail");
    assert_host_key_rejection(&err.to_string());
}

#[test]
fn strict_with_mismatched_key_rejects() {
    if !precheck() {
        return;
    }
    let fx = SshdFixture::spawn();
    // Generate a *different* RSA host key and use its wire-format
    // bytes — this guarantees a syntactically valid blob that won't
    // match the running server.
    let other_dir = tempfile::tempdir().unwrap();
    let other_key = other_dir.path().join("other_rsa");
    run(Command::new("ssh-keygen")
        .args(["-q", "-t", "rsa", "-b", "2048", "-N", "", "-f"])
        .arg(&other_key));
    let other_pub = std::fs::read_to_string(other_dir.path().join("other_rsa.pub")).unwrap();
    let wrong_b64 = other_pub
        .split_whitespace()
        .nth(1)
        .expect("malformed pub")
        .to_string();

    let kh = write_known_hosts_for(&fx, &wrong_b64);
    let verifier = HostVerifier::strict(kh.path()).unwrap();

    let work = tempfile::tempdir().unwrap();
    let client = GitClient::with_host_verifier(Workspace::new(work.path()), verifier);
    let repo = make_repo(fx.ssh_url(), fx.credentials());

    let err = client.fetch(&repo).expect_err("fetch must fail");
    assert_host_key_rejection(&err.to_string());
}

/// libgit2 substitutes its own generic message ("invalid or unknown
/// remote ssh hostkey") when `certificate_check` returns Err — our
/// verifier's structured message doesn't propagate out of the SSH
/// transport. Treat any of the recognisable signals as proof that the
/// fetch was rejected at the host-key check.
fn assert_host_key_rejection(msg: &str) {
    let m = msg.to_lowercase();
    let rejected = m.contains("hostkey")
        || m.contains("host key")
        || m.contains("unknown ssh host key")
        || m.contains("ssh host key mismatch")
        || m.contains("class=ssh");
    assert!(rejected, "expected host-key rejection, got: {msg}");
}

#[test]
fn accept_new_tofu_appends_and_subsequent_fetches_match() {
    if !precheck() {
        return;
    }
    let fx = SshdFixture::spawn();
    let kh_path = fx.dir.path().join("known_hosts");
    std::fs::write(&kh_path, "").unwrap();
    let verifier = HostVerifier::accept_new(&kh_path).unwrap();

    let work = tempfile::tempdir().unwrap();
    let client = GitClient::with_host_verifier(Workspace::new(work.path()), verifier);
    let repo = make_repo(fx.ssh_url(), fx.credentials());

    client.fetch(&repo).expect("first fetch (TOFU) must accept");

    let recorded = std::fs::read_to_string(&kh_path).unwrap();
    assert!(
        recorded.contains(&fx.host_form()),
        "TOFU should append host form; got: {recorded}"
    );
    assert!(
        recorded.contains(&fx.host_pubkey_b64),
        "TOFU should append exact host pubkey base64; got: {recorded}"
    );

    // A second fetch using the recorded file in strict mode must
    // continue to verify — i.e. TOFU produced a usable entry.
    let strict_verifier = HostVerifier::strict(&kh_path).unwrap();
    let work2 = tempfile::tempdir().unwrap();
    let client2 = GitClient::with_host_verifier(Workspace::new(work2.path()), strict_verifier);
    client2
        .fetch(&repo)
        .expect("strict fetch against TOFU-recorded entry must accept");

    // Sanity: the file we'd parse via KnownHostsFile sees one entry.
    let kh = KnownHostsFile::load(&kh_path).unwrap();
    assert_eq!(kh.len(), 1);
}

#[test]
fn no_mode_skips_verification_entirely() {
    if !precheck() {
        return;
    }
    let fx = SshdFixture::spawn();
    let verifier = HostVerifier::new(HostKeyMode::No, KnownHostsFile::empty());

    let work = tempfile::tempdir().unwrap();
    let client = GitClient::with_host_verifier(Workspace::new(work.path()), verifier);
    let repo = make_repo(fx.ssh_url(), fx.credentials());

    client
        .fetch(&repo)
        .expect("StrictHostKeyChecking=no must skip verification");
}
