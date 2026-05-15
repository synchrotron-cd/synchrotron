# Git Auth — Threat Model

Lightweight STRIDE-style review of the `synchrotron-git` auth and
host-verification surface. Goal: shared understanding of what we're
defending against and where each defense lives in the code, not
formal certification.

> **Status**: living document. Update when an auth mode is added or
> a verification path changes.

## Scope

Anything between Synchrotron and a remote git server:

- [`crates/synchrotron-git/src/credentials.rs`](../../crates/synchrotron-git/src/credentials.rs) — `Credentials` enum (None / HttpBasic / SshKey / GitHubApp).
- [`crates/synchrotron-git/src/known_hosts.rs`](../../crates/synchrotron-git/src/known_hosts.rs) — `HostVerifier`, `HostKeyMode`, OpenSSH `known_hosts` parsing.
- [`crates/synchrotron-git/src/client.rs`](../../crates/synchrotron-git/src/client.rs) — libgit2 callbacks that wire credentials and host verification into each fetch.
- [`crates/synchrotron-git/src/github_app.rs`](../../crates/synchrotron-git/src/github_app.rs) + [`github_app_http.rs`](../../crates/synchrotron-git/src/github_app_http.rs) — GitHub App installation-token exchange.
- [`crates/synchrotron-git/src/webhooks.rs`](../../crates/synchrotron-git/src/webhooks.rs) — inbound webhook signature verification (GitHub HMAC-SHA256, GitLab token, Bitbucket SHA1).
- Out of scope: kube auth (separate threat model when one is filed),
  the operator API (depends on how Synchrotron is deployed in front
  of the public internet).

## Assets

| asset | sensitivity | where it lives |
|---|---|---|
| Repository contents (private code, CI config, manifests) | high | rendered into `Workspace` work dirs; in memory in `AppCache` and `DesiredStore`; on the filesystem of the controller pod |
| Repo credentials (HTTP password, SSH private key, GitHub App private key) | high | in process memory inside `Credentials` enum; on disk under operator-controlled paths (`SshKey { private_key_path }`) or as the GitHub App key file |
| Webhook secrets | medium | configured per-RepoCfg, used to verify HMAC of incoming pushes |
| Known-hosts file (`~/.ssh/known_hosts` or controller-managed equivalent) | medium-low (integrity-sensitive) | filesystem; mutated under `HostKeyMode::AcceptNew` |
| GitHub App installation tokens | medium (short-lived, ~1 hour) | in-process cache only (`oidc_http`-shaped pattern); not persisted |

## Trust boundaries

```
   ┌──────────────────────────────────────────────────────────────────┐
   │  Synchrotron controller                                          │
   │                                                                  │
   │  ┌────────────────┐    ┌──────────────┐    ┌────────────────┐  │
   │  │  RepoCfg /     │    │  Credentials │    │  HostVerifier  │  │
   │  │  WebhookCfg    │──▶ │  (resolved)  │──▶ │   (libgit2     │  │
   │  │  (operator)    │    │              │    │   callback)    │  │
   │  └────────────────┘    └──────────────┘    └───────┬────────┘  │
   │                                                    │            │
   └────────────────────────────────────────────────────┼────────────┘
                                                       │
                                          ────────────▶│
                                          unauth net   │
                                                       ▼
                                            ┌──────────────────┐
                                            │  Git server      │
                                            │ (untrusted host) │
                                            └──────────────────┘
```

The trust boundary is the network egress to the remote git server.
Everything inside the controller is treated as one trust domain;
operator-supplied config is trusted (operators can already do
arbitrary harm — that's separately mitigated by Kubernetes RBAC on
the controller's CRDs and Secrets, not by us).

## Threats and mitigations

The table below groups threats by the STRIDE category they primarily
fall under. "Mitigated where" points at the file or function that
owns the defense.

### Spoofing

| threat | mitigation | mitigated where |
|---|---|---|
| Malicious server impersonates a legitimate git host (DNS hijack, MITM, BGP) | Mandatory host-key verification (`HostKeyMode::Yes`, the default). Mismatched keys produce `GitError::HostKeyMismatch` and abort the fetch. | `HostVerifier::verify` + libgit2 `certificate_check` callback in `client::build_callbacks` |
| First-contact attacker poisons the known-hosts file under TOFU | Documented as a known weakness of `HostKeyMode::AcceptNew`. Production default is `Yes`. The mode is a per-repo opt-in so a single misconfigured repo doesn't loosen the rest. | `HostKeyMode` docs + per-repo configuration |
| Attacker forges a webhook to trigger fetches against an attacker-controlled URL | Webhook payloads carry only a repo identifier; the URL Synchrotron fetches comes from operator-supplied config, not the payload. The HMAC is verified before the trigger registers. | `webhooks::verify_github` / `verify_gitlab` / `verify_bitbucket` |
| Attacker replays a captured webhook | We don't currently dedupe by delivery ID. Practical impact: an extra fetch (no-op if HEAD unchanged), no state change. **Filed as follow-up** if the repo set ever includes URLs whose mere fetch would leak metadata. | n/a |

### Tampering

| threat | mitigation | mitigated where |
|---|---|---|
| Server returns a malformed pack file | libgit2 verifies pack integrity (SHA-1 over the contents). Errors surface as `GitError::Git`. | libgit2 |
| Operator-supplied private key is corrupted between config load and use | `Credentials::SshKey` references a path; libgit2 reads it per fetch, so corruption surfaces as `GitError::AuthFailed`. We don't pin the file's hash — that would require operator workflow we don't have yet. | `client::select_credential` |
| In-flight modification of fetched contents | TLS for HTTPS clones; SSH crypto for SSH clones — both via libgit2's transport. Plain `git://` is not a supported config. | libgit2 transport selection |

### Repudiation

Out of scope: Synchrotron doesn't sign its own outputs. If you need
signed commits / signed manifests, use `commit-signing` upstream and
verify in a CI step before letting Synchrotron pick up the commit.

### Information disclosure

| threat | mitigation | mitigated where |
|---|---|---|
| Credentials leak into logs via error messages | All `GitError` variants strip token / password contents. The `tracing` calls log repo URLs but never `Credentials` directly (`Credentials` does not implement `Display`). | `error.rs` + every `tracing` site in `synchrotron-git` |
| Credentials leak into core dump / crash report | Same surface as any Rust process. Apply OS-level core-dump policy (CIS guidance on `RLIMIT_CORE`). Not actively mitigated in code. | Operator domain |
| Repository contents leak via the workspace dir on a shared filesystem | Workspace lives under `cfg.git_workspace_root` (default beneath the DB). Operators should mount it on a tmpfs or an EmptyDir, not a shared volume. | Operator deployment guidance — to be added to `docs/upgrades.md` |
| GitHub App installation token leaks | Tokens are in-memory only, never persisted, and refreshed before expiry. The private signing key is the operator's responsibility (file mode 0600, mounted via Secret). | `github_app::Refresher` |

### Denial of service

| threat | mitigation | mitigated where |
|---|---|---|
| Slow / hung remote stalls a poll | Fetches go through a `Semaphore`-bounded `Orchestrator`. Hung fetches consume slots; once the semaphore is full new pollers wait. The poller's `interval` ensures forward progress eventually. | `orchestrator::wrap_with_semaphore` |
| Webhook flood from a CI fan-out triggers excessive renders | `Coalescer` (synchrotron-core) collapses bursts within a configurable window (`Polling.coalesce_seconds`). | `synchrotron-core::coalesce` |
| Attacker targets the webhook endpoint with garbage to consume HMAC verification CPU | HMAC of small bodies is cheap; the webhook router already rate-limits at the HTTP server level when one is configured (operator concern). | n/a |

### Elevation of privilege

| threat | mitigation | mitigated where |
|---|---|---|
| Compromised git server returns manifests that escalate inside the cluster | Synchrotron applies whatever the manifests say with the controller's ServiceAccount. Operators MUST scope that ServiceAccount via Kubernetes RBAC; we don't do this for them. The destination cluster is part of the Application, so operators can use namespace boundaries / cluster RBAC to limit blast radius. | Operator RBAC (Helm chart's `rbac.role` controls the controller's ServiceAccount) |
| Path traversal in `app.source.path` lets an app render outside its repo's working tree | Defense in depth: `synchrotron-server::pipeline::subpath` rejects `..` and absolute components. Config-layer validation rejects suspicious paths at load time. | `pipeline::subpath` + `config::validate` |
| Credential confusion: HTTP creds attempted on an SSH URL or vice versa | `client::select_credential` matches the libgit2-reported `allowed` flags against the configured credential variant; mismatch yields `GitError::AuthFailed` rather than fall-through. | `client::select_credential` |

## Adding a new auth method — checklist

When extending the `Credentials` enum:

1. Implement `Debug` to redact secrets (or assert no secret fields
   are public — the existing variants follow this convention).
2. Wire selection in `client::select_credential` so a misconfigured
   variant returns `AuthFailed` rather than silently falling
   through to a different credential.
3. Add a host-verification path consideration. SSH variants always
   pair with `HostVerifier`; HTTPS variants rely on the system
   trust store (consider whether a per-repo CA bundle is needed).
4. Add a unit test for the credential selection (matching the
   `select_credential_*` tests).
5. If the new method involves a token refresh (OAuth, similar):
   take the `github_app::Refresher` shape — in-memory cache, never
   persist, log only token expiry not contents.
6. Update the table above with the new threat surface and the
   mitigation site in code.

## Open follow-ups

- Webhook delivery-ID dedup (reduces metadata-leak surface for
  repos whose fetch reveals access patterns).
- Workspace tmpfs guidance in `docs/upgrades.md`.
- Threat model for the kube auth surface (separate doc).
- Threat model for the controller's HTTP API surface
  (depends on deployment topology — meta concern).
