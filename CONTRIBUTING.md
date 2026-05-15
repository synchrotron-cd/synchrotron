# Contributing to Synchrotron

Thanks for looking at the project. This file is the practical
quickstart for getting set up locally and the conventions the
codebase follows. For project goals and architecture see
[`DESIGN.md`](DESIGN.md); for the operator-facing build/release
process see [`BUILD.md`](BUILD.md); for AI-assisted contribution
norms see [`AGENTS.md`](AGENTS.md).

## Local environment

You need:

- **Rust** — toolchain pinned by [`rust-toolchain.toml`](rust-toolchain.toml).
  `rustup` will install the right version on any cargo invocation
  inside the repo.
- **just** — the [`justfile`](justfile) is the canonical entry
  point for build/test/lint/bench. `cargo install just`.
- **kind** (optional) — for the kube integration tests. Any local
  Kubernetes cluster works; the `kind_*` tests in
  `crates/synchrotron-{kube,reconcile}` skip themselves unless
  `SYNCHROTRON_KIND_TEST=1` is exported.

```bash
just build         # cargo build --workspace
just test          # cargo test --workspace, kind tests skip
just lint          # cargo clippy --workspace --all-targets -- -D warnings
just fmt-check     # cargo fmt --all -- --check
just audit         # cargo deny check (mirrors .github/workflows/audit.yml)
```

The full set of recipes is in `justfile`.

## Performance work

End-to-end load harness in
[`crates/synchrotron-bench`](crates/synchrotron-bench). The
[`RUNBOOK`](crates/synchrotron-bench/RUNBOOK.md) documents the
methodology, scenarios, and the y0v perf budget.

```bash
just bench smoke           # ~1s sanity check
just bench 10k-apps        # 30s steady-state baseline
just bench webhook-burst   # webhook→sync p95
just bench-budget          # CI memory-budget guard (130 KB/app)
just bench-webhook         # CI webhook-latency guard (5000 ms p95)
```

## CI

Every PR runs:

| workflow | what it covers |
|---|---|
| `ci.yml` | rustfmt, clippy `-D warnings`, `cargo test --workspace --no-fail-fast` (offline) |
| `bench.yml` | criterion microbenches + scenario runs + budget guards |
| `audit.yml` | cargo-deny (advisories, licenses, bans, sources) + weekly cron |
| `kind.yml` | `SYNCHROTRON_KIND_TEST=1` integration tests against an ephemeral kind cluster |
| `helm.yml` | chart lint + template + install (only fires on chart changes) |

`main` is branch-protected: PRs require `rustfmt`, `clippy`, `test`,
and `criterion + scenarios` to pass before merging.

Dependabot ([`.github/dependabot.yml`](.github/dependabot.yml))
opens grouped PRs weekly for cargo deps and monthly for GitHub
Actions.

## Issue tracking — `bd` (beads)

Issues live in `.beads/issues.jsonl`, a local issue database
checked into the repo. The full command reference is in
[`AGENTS.md`](AGENTS.md); the everyday flow is:

```bash
bd ready                       # what can I pick up?
bd show <id>                   # full issue + dependencies
bd update <id> --claim         # assign to yourself
bd update <id> --status=in_progress
bd close <id> --reason="…"     # mark done with a one-line summary
```

Filing follow-ups is encouraged when you find something out of
scope for the task you're working on:

```bash
bd create --title="…" --type=task --priority=2 \
          --description="What and why."
```

See `bd help` for everything else.

## Commit message convention

Format: `<type>(<scope>): <summary> (<bead-id>)`

`type` is one of `feat`, `fix`, `perf`, `refactor`, `test`,
`bench`, `docs`, `chore`, `ci`. `scope` is the affected crate(s)
without the `synchrotron-` prefix (e.g. `reconcile`, `kube,server`).
`bead-id` is the closing or primary bead. Body explains the *why*
and any non-obvious tradeoffs; aim for an end-of-month reader who
has no context.

Examples (from `git log`):

```
feat(reconcile,kube,server): per-cluster kube executors (79e)
perf(plugins): byte-backed ManifestBody with lazy Value (slice 2 of d2p, v3z)
ci: pin Rust toolchain to 1.95.0 (85h)
```

## Pull requests

Workflow:

1. `bd ready` → pick an issue, `bd update --claim`, `bd update --status=in_progress`.
2. Branch (`git checkout -b feat/<short-name>`), implement, run
   `just lint && just test` locally.
3. `bd close` the issue with a 1-2 sentence summary of what
   landed, then commit.
4. Open the PR. CI runs ci/bench/audit (and kind/helm if relevant
   paths changed).
5. Merge after the required checks go green. There's no required
   reviewer right now (solo project); when collaborators join we'll
   add a code-owners file.

If the bead's scope grew during implementation, file a follow-up
and link it in the close-reason rather than letting the PR sprawl.

## Style

- `cargo fmt --all` is the source of truth.
- `cargo clippy --workspace --all-targets -- -D warnings` must be
  clean — toolchain is pinned, so new lints arrive only on
  intentional bumps.
- Comments default to none. Add one when the *why* is non-obvious
  (a hidden constraint, a workaround, a surprising behavior).
  Don't narrate what the code does.
- No emojis in code or commits unless explicitly asked.
- Prefer editing existing files to creating new ones.

## License

Apache-2.0. By contributing you agree your contributions are
licensed under the same.
