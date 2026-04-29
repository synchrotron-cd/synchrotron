# Build & Release

Synchrotron-CD ships as **statically linked musl binaries** for
Linux. There is no runtime dependency on libc, openssl, or a
package manager — drop the binary into a container scratch image
or a bare host and run it.

## Targets

| Target                          | Triple                          | Build mode |
| ------------------------------- | ------------------------------- | ---------- |
| Linux amd64                     | `x86_64-unknown-linux-musl`     | native (musl-tools) |
| Linux arm64                     | `aarch64-unknown-linux-musl`    | cross-compile (`cross`) |

## Binaries shipped per release

Each release tarball contains four binaries:

- `synchrotron-server` — the controller daemon.
- `synchrotron` — operator CLI.
- `synchrotron-helm-plugin` — Helm rendering plugin.
- `synchrotron-kustomize-plugin` — Kustomize rendering plugin.

## Local release build

```bash
# Native amd64 musl
sudo apt-get install -y musl-tools
rustup target add x86_64-unknown-linux-musl
cargo build --release --locked --target x86_64-unknown-linux-musl

# Cross arm64 musl
cargo install cross --locked --git https://github.com/cross-rs/cross
cross build --release --locked --target aarch64-unknown-linux-musl
```

Release-profile knobs (in [`Cargo.toml`](./Cargo.toml)):

- `lto = "fat"` — whole-program optimization across crates.
- `codegen-units = 1` — single codegen unit for maximum inlining.
- `strip = "symbols"` — drop debug/symbol info from the final binary.
- `opt-level = 3` — standard speed-optimized release.

These trade ~3-5x build time for noticeably smaller, faster
binaries. Debug-builds are unaffected.

## Size budget

The deployment artifact is the *tarball* containing all four
binaries. The targets below are upper bounds — regressions above
these warrant investigation before merging.

| Artifact                                  | Budget |
| ----------------------------------------- | -----: |
| `synchrotron-server` (stripped, static)   |  60 MB |
| `synchrotron` (stripped, static)          |  25 MB |
| `synchrotron-helm-plugin`                 |  15 MB |
| `synchrotron-kustomize-plugin`            |  15 MB |
| Combined tarball (`.tar.gz`)              |  45 MB |

Why these numbers: kube-rs, tokio, and rustls dominate the server
binary; the plugins are thin wrappers around external tools and
should stay small. If you exceed a budget, prefer feature-gating
heavy deps over loosening the budget — `cargo bloat --release`
and `cargo tree -d` are the diagnostic starting points.

Budgets are revisited at each minor-version bump.

## Release pipeline

Tag-driven via [`.github/workflows/release.yml`](./.github/workflows/release.yml):

1. **Trigger** — push a `v*` tag (e.g. `v0.2.0`). Manual
   `workflow_dispatch` runs the build matrix without publishing.
2. **Build** — matrix job per target produces the tarball plus a
   per-file SHA-256 sidecar.
3. **Provenance** — `actions/attest-build-provenance@v2` records
   a SLSA v1 attestation against the GitHub OIDC-backed signer.
   Consumers verify with `gh attestation verify <tarball> --owner <org>`.
4. **Publish** — aggregated `SHA256SUMS` and tarballs attach to
   the GitHub release. Release notes auto-generate from commits.

## Verifying a release

```bash
# Verify checksums
sha256sum -c SHA256SUMS

# Verify provenance (requires gh CLI)
gh attestation verify synchrotron-<ver>-<target>.tar.gz \
  --owner <org>
```
