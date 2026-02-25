# Synchrotron-CD Design Document

## Project Vision

Synchrotron-CD is a lightweight, fast, and scalable GitOps continuous deployment system. It serves as a modern alternative to Argo CD, designed from first principles to prioritize performance, simplicity, and resource efficiency.

## Guiding Principles

### 1. Lightweight
- Minimal dependencies and resource consumption
- Small binary size and memory footprint
- Simple deployment model without complex infrastructure

### 2. Fast
- Quick GitOps reconciliation cycles
- Rapid deployment pipelines
- Efficient change detection and application

### 3. Scalable
- Handle thousands of applications across multiple clusters
- Horizontal scaling without operational complexity
- Efficient resource utilization at scale

### 4. Simple
- Straightforward operational model
- Clear configuration and deployment workflows
- Minimal cognitive overhead for operators

## Architectural Goals

- **Performance as a first-class concern**: Every design decision considers performance impact
- **Memory efficiency by design**: Target sub-512MB memory footprint for deployments with thousands of apps
- **Stateless components**: Enable horizontal scaling and easy failover
- **Event-driven reconciliation**: React quickly to changes rather than polling
- **Composable architecture**: Individual components can be used independently
- **Plugin extensibility**: Support multiple templating engines and deployment strategies without core modifications

## Design Areas (To Be Detailed)

### Core Components
- [ ] Git synchronization engine
- [ ] Application state reconciliation
- [ ] Plugin system for templating/rendering (Helm, Kustomize, Jsonnet, etc.)
- [ ] Deployment orchestration
- [ ] Webhook and event handling
- [ ] In-memory manifest cache with LRU eviction

### Technology Decisions
- [x] Language: Rust (zero-cost abstractions, memory efficiency, minimal binary size)
- [x] State Database: RocksDB (embedded KV store, bounded memory, optimized for write-heavy workloads)
- [x] Memory Management: In-memory manifest cache with LRU eviction under 512MB total
- [ ] Plugin architecture (RPC/WASM for templating engines)
- [ ] Communication protocols
- [ ] Cluster interaction mechanisms

### Operational Model
- [ ] Installation and setup
- [ ] Configuration management
- [ ] Monitoring and observability
- [ ] Multi-cluster deployment patterns

### Performance Targets
- [ ] Git reconciliation latency
- [ ] Maximum managed applications
- [ ] Memory usage per application
- [ ] Cluster scaling limits

## Decision Log

This section documents major architectural decisions and their rationale.

### D1: Rust + RocksDB Stack
**Date**: 2026-02-24  
**Decision**: Use Rust for the main application with RocksDB for state persistence.

**Rationale**:
- **Memory Efficiency**: Must stay under 512MB RAM for deployments with thousands of apps. Rust's zero-cost abstractions and lack of garbage collection provide predictable memory usage.
- **Sync Performance**: Goroutines are lightweight, but Rust's async/await with tokio provides comparable concurrency with better memory bounds.
- **Binary Size**: Rust produces ~3-5MB static binary vs 10MB+ for Go, reducing deployment footprint.
- **RocksDB Choice**: Embedded KV store designed for large datasets with bounded memory. Streaming reads prevent loading entire state into memory. Optimized for write-heavy workloads (perfect for sync operations).

**Trade-offs**:
- Steeper learning curve for Rust vs Go
- Compile times longer than Go

### D2: Memory Management Strategy
**Date**: 2026-02-24  
**Decision**: Use bounded in-memory manifest cache with LRU eviction to balance sync speed and auto-heal CPU efficiency while staying under 512MB total memory.

**Rationale**:
- **Sync Performance**: Fresh sync operations stream manifests from RocksDB and populate cache, keeping I/O efficient.
- **Auto-heal Efficiency**: Auto-heal runs every 30-60s reading exclusively from in-memory cache, minimizing CPU and DB overhead.
- **Memory Bounds**: LRU eviction ensures cache respects 512MB total memory budget (RocksDB ~50MB + runtime ~50MB = cache ~350-400MB).
- **Cache Invalidation**: Sync completion invalidates cache entries, ensuring eventual consistency.

**Implementation**:
- LRU cache for parsed manifests keyed by `(app, git-commit-hash)`
- Configurable TTL for eviction (default: 1 hour)
- Metrics for cache hit/miss rate

### D3: Plugin Architecture for Templating
**Date**: 2026-02-24  
**Decision**: Implement plugin system for manifest templating and rendering engines.

**Rationale**:
- **Flexibility**: Teams use different templating approaches (Helm, Kustomize, Jsonnet, custom tools). Plugins allow each to be opt-in.
- **Decoupling**: Core sync engine doesn't depend on specific templating tool versions.
- **Extensibility**: Users can add custom rendering without modifying core.

**Plugin Types**:
- Templating plugins (input: values + template source, output: rendered manifest)
- Pre-sync hooks (validate, lint, transform)
- Post-deploy hooks (verify, notify)

**Transport**:
- **Local plugins**: RPC over stdio (subprocess) for fast, simple plugins (Helm, Kustomize wrappers)
- **Sidecar plugins**: gRPC or HTTP to sidecar containers for resource-heavy or complex plugins (custom frameworks, integrations)
- **Config-driven**: Plugin config specifies transport (local binary path vs container image + port)

**Benefits**:
- Local: Zero overhead, instant startup
- Sidecar: Fully isolated, can be language-agnostic, scales independently
- Same plugin interface for both, just different transports

---

**Last Updated**: 2026-02-24
**Status**: In Progress
