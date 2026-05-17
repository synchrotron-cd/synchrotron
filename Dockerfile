# syntax=docker/dockerfile:1.7
#
# Multi-stage build for synchrotron-server.
#
# Stage 1 builds a statically-linked musl binary for whichever
# target arch buildx asks for (set via TARGETARCH). Stage 2 copies
# that binary into a distroless image — distroless/static-debian12
# is a ~2 MB base that ships glibc-free + a `nonroot` UID.
#
# Multi-arch builds work via buildx + QEMU; no cross-compile linker
# acrobatics needed because each arch's stage 1 runs natively in
# its own emulated container. Slower than a real cross-compile but
# trivially correct.

FROM rust:1.95-bookworm AS builder
# No `--platform=$BUILDPLATFORM` — that would force the builder to
# run on the host arch and cross-compile to TARGETARCH, which would
# need an aarch64-linux-musl-gcc cross-toolchain the rust base
# image doesn't ship. Instead let buildx run the builder natively
# on each platform (under QEMU for the non-host arch). Slower, but
# trivially correct.
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends musl-tools pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# Layered copy so iterating on src/ doesn't bust the deps cache.
# The Cargo.lock + manifests determine the dep set; copying them
# alone first lets `cargo fetch` cache-warm without recopying
# every change in src.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN case "$TARGETARCH" in \
      amd64) TGT=x86_64-unknown-linux-musl ;; \
      arm64) TGT=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
 && rustup target add "$TGT" \
 && cargo build --release --locked --target "$TGT" \
      --bin synchrotron-server \
      --bin synchrotron-helm-plugin \
      --bin synchrotron-kustomize-plugin \
      --bin synchrotron-labeler-plugin \
 && cp "target/$TGT/release/synchrotron-server"        /synchrotron-server \
 && cp "target/$TGT/release/synchrotron-helm-plugin"   /synchrotron-helm-plugin \
 && cp "target/$TGT/release/synchrotron-kustomize-plugin" /synchrotron-kustomize-plugin \
 && cp "target/$TGT/release/synchrotron-labeler-plugin" /synchrotron-labeler-plugin

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
LABEL org.opencontainers.image.title="synchrotron-server"
LABEL org.opencontainers.image.source="https://github.com/synchrotron-cd/synchrotron"
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder /synchrotron-server               /synchrotron-server
COPY --from=builder /synchrotron-helm-plugin          /synchrotron-helm-plugin
COPY --from=builder /synchrotron-kustomize-plugin     /synchrotron-kustomize-plugin
COPY --from=builder /synchrotron-labeler-plugin       /synchrotron-labeler-plugin
USER nonroot:nonroot
EXPOSE 8484
ENTRYPOINT ["/synchrotron-server"]
