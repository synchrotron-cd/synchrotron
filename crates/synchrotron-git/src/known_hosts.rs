//! OpenSSH `known_hosts` parsing and host-key verification.
//!
//! libgit2's default SSH transport accepts whatever host key the
//! server presents — that's TOFU at best, MITM-vulnerable at
//! worst. This module gives the git client a real verification
//! step:
//!
//! 1. Parse `known_hosts` entries (plain and hashed forms).
//! 2. On each fetch, libgit2 hands us the server's host key via
//!    [`RemoteCallbacks::certificate_check`]. We look it up by
//!    `(host, port, key-type)` and compare the key bytes.
//! 3. Per [`HostKeyMode`]: reject unknown hosts (`Yes`/`Ask`),
//!    accept-and-append on first contact (`AcceptNew`), or skip
//!    entirely (`No`). `No` is provided for parity with OpenSSH
//!    config but is unsafe in production — operators have to opt
//!    into it explicitly.
//!
//! Hashed entries use the OpenSSH `|1|<salt>|<hash>` format:
//! HMAC-SHA1 of the hostname keyed by a random salt, both base64.
//! Lookups iterate hashed entries and re-hash the queried host
//! against each salt, so the file remains a one-way map (you can
//! verify a known host without learning the host list).

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use subtle::ConstantTimeEq;
use tracing::warn;

use crate::error::GitError;

type HmacSha1 = Hmac<Sha1>;

/// What to do when the server's host key isn't in `known_hosts`.
/// Field name and string forms match OpenSSH's
/// `StrictHostKeyChecking` so config files port over cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum HostKeyMode {
    /// Reject unknown hosts. Production default.
    #[default]
    Yes,
    /// Identical to `Yes` for this non-interactive process — there's
    /// no operator on the other end of an "ask" prompt — but we
    /// log a warning so misconfiguration is visible.
    Ask,
    /// TOFU: append the key to `known_hosts` on first contact.
    /// Subsequent fetches verify against the recorded key.
    #[serde(rename = "accept-new")]
    AcceptNew,
    /// Skip verification entirely. Unsafe; only for local file://
    /// fixtures or operator-controlled airgapped environments.
    No,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyStatus {
    /// Host + key-type + key bytes all match an entry.
    Match,
    /// No entry for this `(host, key-type)`.
    Unknown,
    /// An entry exists for this `(host, key-type)` but the key
    /// bytes don't match. Almost certainly an attack or a real
    /// host-key rotation.
    Mismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HostPattern {
    /// Comma-separated list of plain hostnames/IPs. May include
    /// `[host]:port` forms for non-22 ports.
    Plain(Vec<String>),
    /// `|1|salt|hash` — hash of a single hostname.
    Hashed { salt: Vec<u8>, hash: Vec<u8> },
}

#[derive(Debug, Clone)]
struct Entry {
    pattern: HostPattern,
    key_type: String,
    key_b64: String,
}

/// In-memory view of a `known_hosts` file. Cheap to construct and
/// re-load; not designed for files of unbounded size.
#[derive(Debug, Clone)]
pub struct KnownHostsFile {
    path: PathBuf,
    entries: Vec<Entry>,
}

impl KnownHostsFile {
    /// Load `path`. A missing file is treated as an empty database
    /// — TOFU mode then writes new entries to `path` on first
    /// contact. Other I/O errors are propagated.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, GitError> {
        let path = path.into();
        let mut entries = Vec::new();
        match File::open(&path) {
            Ok(f) => {
                for (lineno, line) in BufReader::new(f).lines().enumerate() {
                    let line = line.map_err(|e| GitError::Io {
                        path: path.clone(),
                        source: e,
                    })?;
                    if let Some(entry) = parse_line(&line) {
                        entries.push(entry);
                    } else if !line.trim().is_empty() && !line.trim_start().starts_with('#') {
                        warn!(
                            path = %path.display(),
                            line = lineno + 1,
                            "ignoring malformed known_hosts entry"
                        );
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(GitError::Io {
                    path: path.clone(),
                    source: e,
                });
            }
        }
        Ok(Self { path, entries })
    }

    /// In-memory file backed by no path. Useful for tests.
    pub fn empty() -> Self {
        Self {
            path: PathBuf::new(),
            entries: Vec::new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Verify a host key. `key_type` is the OpenSSH algorithm
    /// string (e.g. `ssh-ed25519`); `key_bytes` is the raw key
    /// material — *not* base64 — that libgit2 hands us.
    pub fn verify(&self, host: &str, port: u16, key_type: &str, key_bytes: &[u8]) -> VerifyStatus {
        let host_forms = host_lookup_forms(host, port);
        let mut found_for_host = false;
        for entry in &self.entries {
            if !pattern_matches(&entry.pattern, &host_forms) {
                continue;
            }
            if entry.key_type != key_type {
                continue;
            }
            found_for_host = true;
            let stored = match B64.decode(entry.key_b64.as_bytes()) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if stored.ct_eq(key_bytes).into() {
                return VerifyStatus::Match;
            }
        }
        if found_for_host {
            VerifyStatus::Mismatch
        } else {
            VerifyStatus::Unknown
        }
    }

    /// Append a new entry. The host is recorded in plain form (not
    /// hashed) — TOFU users who want hashing can run
    /// `ssh-keygen -H` over the file out-of-band. Persists to
    /// `self.path` if non-empty.
    pub fn append(
        &mut self,
        host: &str,
        port: u16,
        key_type: &str,
        key_bytes: &[u8],
    ) -> Result<(), GitError> {
        let host_form = canonical_host_form(host, port);
        let key_b64 = B64.encode(key_bytes);
        let line = format!("{host_form} {key_type} {key_b64}\n");

        if !self.path.as_os_str().is_empty() {
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| GitError::Io {
                    path: self.path.clone(),
                    source: e,
                })?;
            f.write_all(line.as_bytes()).map_err(|e| GitError::Io {
                path: self.path.clone(),
                source: e,
            })?;
        }

        self.entries.push(Entry {
            pattern: HostPattern::Plain(vec![host_form]),
            key_type: key_type.to_string(),
            key_b64,
        });
        Ok(())
    }
}

/// Top-level verifier wiring [`HostKeyMode`] together with a
/// [`KnownHostsFile`]. Produced from config; passed into the git
/// client so `certificate_check` callbacks know how to behave.
#[derive(Debug, Clone)]
pub struct HostVerifier {
    pub mode: HostKeyMode,
    pub file: KnownHostsFile,
}

impl HostVerifier {
    pub fn new(mode: HostKeyMode, file: KnownHostsFile) -> Self {
        Self { mode, file }
    }

    /// Strict verification using the file at `path`. Convenience
    /// for the common production case.
    pub fn strict(path: impl Into<PathBuf>) -> Result<Self, GitError> {
        Ok(Self::new(HostKeyMode::Yes, KnownHostsFile::load(path)?))
    }

    /// TOFU: trust on first use, then verify forever after.
    pub fn accept_new(path: impl Into<PathBuf>) -> Result<Self, GitError> {
        Ok(Self::new(
            HostKeyMode::AcceptNew,
            KnownHostsFile::load(path)?,
        ))
    }

    /// Disable verification. Unsafe — used by integration tests
    /// that drive `file://` transports where no host key is
    /// presented.
    pub fn insecure() -> Self {
        Self::new(HostKeyMode::No, KnownHostsFile::empty())
    }

    /// Decide what to do for `(host, port, key_type, key_bytes)`.
    /// Returns `Ok(())` to accept, `Err(...)` to reject.
    /// `AcceptNew` mutates `self.file` (and persists) on first
    /// contact.
    pub fn check(
        &mut self,
        host: &str,
        port: u16,
        key_type: &str,
        key_bytes: &[u8],
    ) -> Result<(), GitError> {
        if matches!(self.mode, HostKeyMode::No) {
            warn!(
                host,
                "host key verification disabled (StrictHostKeyChecking=no)"
            );
            return Ok(());
        }
        match self.file.verify(host, port, key_type, key_bytes) {
            VerifyStatus::Match => Ok(()),
            VerifyStatus::Mismatch => Err(GitError::HostKeyMismatch {
                host: host.to_string(),
                key_type: key_type.to_string(),
            }),
            VerifyStatus::Unknown => match self.mode {
                HostKeyMode::AcceptNew => {
                    warn!(
                        host,
                        key_type, "TOFU: appending new host key to known_hosts"
                    );
                    self.file.append(host, port, key_type, key_bytes)
                }
                HostKeyMode::Yes | HostKeyMode::Ask => Err(GitError::UnknownHostKey {
                    host: host.to_string(),
                    key_type: key_type.to_string(),
                }),
                HostKeyMode::No => unreachable!("handled above"),
            },
        }
    }
}

/// Try to parse one `known_hosts` line. Returns `None` for blank
/// lines, comments, or anything we can't make sense of (we log
/// the raw form at the call site).
fn parse_line(line: &str) -> Option<Entry> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut tokens = trimmed.split_whitespace();
    let mut first = tokens.next()?;
    // Skip optional `@cert-authority` / `@revoked` markers — we
    // don't currently distinguish them, but we shouldn't blow up
    // on their presence either.
    if first.starts_with('@') {
        first = tokens.next()?;
    }
    let key_type = tokens.next()?;
    if !is_known_key_type(key_type) {
        return None;
    }
    let key_b64 = tokens.next()?;

    let pattern = if let Some(rest) = first.strip_prefix("|1|") {
        let mut parts = rest.split('|');
        let salt = B64.decode(parts.next()?).ok()?;
        let hash = B64.decode(parts.next()?).ok()?;
        HostPattern::Hashed { salt, hash }
    } else {
        HostPattern::Plain(first.split(',').map(str::to_string).collect())
    };

    // Sanity: real entries have a base64-decodable key blob. This
    // filters out free-form garbage that happens to have 3+ tokens.
    let decoded = B64.decode(key_b64.as_bytes()).ok()?;
    if decoded.is_empty() {
        return None;
    }

    Some(Entry {
        pattern,
        key_type: key_type.to_string(),
        key_b64: key_b64.to_string(),
    })
}

fn is_known_key_type(s: &str) -> bool {
    matches!(
        s,
        "ssh-rsa"
            | "ssh-dss"
            | "ssh-ed25519"
            | "ssh-ed448"
            | "ecdsa-sha2-nistp256"
            | "ecdsa-sha2-nistp384"
            | "ecdsa-sha2-nistp521"
            | "sk-ecdsa-sha2-nistp256@openssh.com"
            | "sk-ssh-ed25519@openssh.com"
    )
}

fn pattern_matches(pattern: &HostPattern, host_forms: &[String]) -> bool {
    match pattern {
        HostPattern::Plain(entries) => entries.iter().any(|p| host_forms.iter().any(|h| h == p)),
        HostPattern::Hashed { salt, hash } => host_forms.iter().any(|h| {
            let mut mac = match HmacSha1::new_from_slice(salt) {
                Ok(m) => m,
                Err(_) => return false,
            };
            mac.update(h.as_bytes());
            mac.finalize().into_bytes().as_slice().ct_eq(hash).into()
        }),
    }
}

/// Forms a host could appear under in `known_hosts`. We try both
/// the bare hostname and the `[host]:port` form when port != 22.
fn host_lookup_forms(host: &str, port: u16) -> Vec<String> {
    let mut forms = vec![host.to_string()];
    if port != 22 {
        forms.push(format!("[{host}]:{port}"));
    }
    forms
}

/// Form to write back when appending a new entry.
fn canonical_host_form(host: &str, port: u16) -> String {
    if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ED25519_KEY_TYPE: &str = "ssh-ed25519";
    const ED25519_KEY: &[u8] =
        b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00 0123456789abcdef0123456789abcdef";
    const ED25519_KEY_OTHER: &[u8] =
        b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00 ffffffffffffffffffffffffffffffff";

    fn write_kh(text: &str) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), text).unwrap();
        f
    }

    fn entry_for(host: &str, key: &[u8]) -> String {
        format!("{} {} {}\n", host, ED25519_KEY_TYPE, B64.encode(key))
    }

    #[test]
    fn plain_entry_matches() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Match
        );
    }

    #[test]
    fn unknown_host() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("gitlab.com", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Unknown
        );
    }

    #[test]
    fn mismatched_key_for_known_host() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY_OTHER),
            VerifyStatus::Mismatch
        );
    }

    #[test]
    fn comma_separated_hostnames() {
        let f = write_kh(&format!(
            "github.com,140.82.121.4 {} {}\n",
            ED25519_KEY_TYPE,
            B64.encode(ED25519_KEY)
        ));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("140.82.121.4", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Match
        );
    }

    #[test]
    fn non_default_port_uses_bracket_form() {
        let f = write_kh(&format!(
            "[git.internal]:2222 {} {}\n",
            ED25519_KEY_TYPE,
            B64.encode(ED25519_KEY)
        ));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("git.internal", 2222, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Match
        );
    }

    #[test]
    fn hashed_entry_matches() {
        // Build a hashed entry the same way ssh-keygen -H would.
        let salt = b"random-16-byte-s"; // 16 bytes
        let mut mac = HmacSha1::new_from_slice(salt).unwrap();
        mac.update(b"github.com");
        let hash = mac.finalize().into_bytes();
        let line = format!(
            "|1|{}|{} {} {}\n",
            B64.encode(salt),
            B64.encode(hash),
            ED25519_KEY_TYPE,
            B64.encode(ED25519_KEY)
        );
        let f = write_kh(&line);
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            kh.verify("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Match
        );
        assert_eq!(
            kh.verify("gitlab.com", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Unknown
        );
    }

    #[test]
    fn marker_lines_parse() {
        let f = write_kh(&format!(
            "@cert-authority *.example.com {} {}\n",
            ED25519_KEY_TYPE,
            B64.encode(ED25519_KEY)
        ));
        let kh = KnownHostsFile::load(f.path()).unwrap();
        // We don't enforce CA semantics yet, but the entry must
        // parse rather than being silently dropped.
        assert_eq!(kh.len(), 1);
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let f = write_kh("not a real entry\n# comment\n\n");
        let kh = KnownHostsFile::load(f.path()).unwrap();
        assert!(kh.is_empty());
    }

    #[test]
    fn missing_file_loads_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let kh = KnownHostsFile::load(dir.path().join("nope")).unwrap();
        assert!(kh.is_empty());
    }

    #[test]
    fn strict_rejects_unknown() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let mut v = HostVerifier::strict(f.path()).unwrap();
        let err = v
            .check("gitlab.com", 22, ED25519_KEY_TYPE, ED25519_KEY)
            .unwrap_err();
        assert!(matches!(err, GitError::UnknownHostKey { .. }));
    }

    #[test]
    fn strict_rejects_mismatch() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let mut v = HostVerifier::strict(f.path()).unwrap();
        let err = v
            .check("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY_OTHER)
            .unwrap_err();
        assert!(matches!(err, GitError::HostKeyMismatch { .. }));
    }

    #[test]
    fn ask_mode_behaves_like_strict() {
        let f = write_kh(&entry_for("github.com", ED25519_KEY));
        let mut v = HostVerifier::new(HostKeyMode::Ask, KnownHostsFile::load(f.path()).unwrap());
        assert!(v
            .check("gitlab.com", 22, ED25519_KEY_TYPE, ED25519_KEY)
            .is_err());
    }

    #[test]
    fn accept_new_appends_and_persists() {
        let f = write_kh("");
        let mut v = HostVerifier::accept_new(f.path()).unwrap();
        v.check("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY)
            .expect("first contact accepted");

        // Mismatch on subsequent fetch is rejected.
        let err = v
            .check("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY_OTHER)
            .unwrap_err();
        assert!(matches!(err, GitError::HostKeyMismatch { .. }));

        // Persisted to disk: re-reading sees the entry.
        let reloaded = KnownHostsFile::load(f.path()).unwrap();
        assert_eq!(
            reloaded.verify("github.com", 22, ED25519_KEY_TYPE, ED25519_KEY),
            VerifyStatus::Match
        );
    }

    #[test]
    fn insecure_accepts_anything() {
        let mut v = HostVerifier::insecure();
        v.check("anywhere", 22, "ssh-rsa", b"whatever").unwrap();
    }

    #[test]
    fn append_writes_bracket_form_for_nondefault_port() {
        let f = write_kh("");
        let mut v = HostVerifier::accept_new(f.path()).unwrap();
        v.check("git.internal", 2222, ED25519_KEY_TYPE, ED25519_KEY)
            .unwrap();
        let text = std::fs::read_to_string(f.path()).unwrap();
        assert!(
            text.contains("[git.internal]:2222"),
            "expected bracket form in: {text}"
        );
    }
}
