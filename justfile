# Default recipe
default: check

# Build all crates
build:
    cargo build

# Build release binaries
release:
    cargo build --release

# Run all checks
check: fmt-check lint test

# Format check
fmt-check:
    cargo fmt --all -- --check

# Format
fmt:
    cargo fmt --all

# Lint
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Run tests
test:
    cargo test --workspace

# Run the server (dev mode)
serve:
    RUST_LOG=info cargo run --bin synchrotron-server

# Run a CLI command (pass args after --)
cli *ARGS:
    cargo run --bin synchrotron -- {{ARGS}}

# Clean build artifacts
clean:
    cargo clean

# Run a perf scenario (e.g. `just bench smoke` or `just bench 10k-apps`)
bench SCENARIO="smoke":
    mkdir -p reports
    cargo run --release -p synchrotron-bench -- \
      --scenario crates/synchrotron-bench/scenarios/{{SCENARIO}}.yaml \
      --out reports/{{SCENARIO}}.json
