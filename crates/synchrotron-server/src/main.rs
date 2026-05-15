use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use synchrotron_core::events::EventBus;
use synchrotron_core::metrics::Metrics;
use synchrotron_core::telemetry::{init as telemetry_init, TelemetryConfig};
use synchrotron_kube::{
    spawn_default_informers, AuthSource, ClusterConfig as KubeClusterConfig, KubeApplierAdapter,
    KubeClient, KubeSsaApplier, LiveStore, StoreLiveSource,
};
use synchrotron_plugins::{AppCache, AppRenderer, Registry};
use synchrotron_reconcile::{
    AlwaysHealthy, AppResolver, DesiredStore, EventTrigger, JobCtx, PoolConfig, ReconcileExecutor,
    Reconciler, StoreDesiredSource, WaveExecConfig, WorkerPool,
};
use synchrotron_server::api;
use synchrotron_server::config::{reload, Config, ConfigHandle};
use synchrotron_server::pipeline;
use synchrotron_types::AppName;

/// DB-backed [`AppResolver`]. Maps a repo URL (the wire form
/// shipped on `RepoChanged` / `WebhookTriggered` events) to the
/// [`AppName`]s that reference it, by listing applications from the
/// SQLite ledger and filtering. List-and-filter is fine at current
/// scale (10k apps in 1.1 GB per the y0v perf work; the list is in
/// memory after the SQLite roundtrip and the filter is a string
/// compare) — once the app set grows, a repo→apps index becomes
/// worthwhile.
struct DbAppResolver {
    db: Arc<Mutex<synchrotron_core::db::Database>>,
}

impl AppResolver for DbAppResolver {
    fn apps_for_repo(&self, repo: &str) -> Vec<AppName> {
        let db = self.db.lock().expect("db mutex poisoned");
        match db.list_applications() {
            Ok(apps) => apps
                .into_iter()
                .filter(|a| a.source.repo_url.0 == repo)
                .map(|a| a.name)
                .collect(),
            Err(e) => {
                warn!(error = %e, "AppResolver: list_applications failed; treating as empty");
                Vec::new()
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry_init(TelemetryConfig::default())?;

    // Resolve the config path from CLI arg, then $SYNCHROTRON_CONFIG,
    // else fall back to a synthesized default. Only file-backed
    // handles get a SIGHUP reloader.
    let path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SYNCHROTRON_CONFIG").ok());
    let handle = match path {
        Some(p) => {
            let h = ConfigHandle::load(&p)?;
            #[cfg(unix)]
            reload::spawn_sighup_reloader(h.clone());
            info!(path = %p, "config loaded");
            h
        }
        None => {
            warn!("no config path supplied; using built-in defaults");
            ConfigHandle::from_config(Config::default(), "<default>")
        }
    };

    let cfg = handle.current().await;

    // Probe the DB up front so config errors surface before the
    // engine assembly runs; AppsState::new opens the long-lived
    // handle below.
    {
        let _probe = synchrotron_core::db::Database::open(&cfg.server.db_path)?;
    }
    info!("database initialized");

    // Event bus is the in-process pub/sub used by the reconciler and
    // notifier. Capacity is generous; the apps API only publishes
    // ManualSyncRequested at human cadence, but informer-driven
    // SyncOutcome events can fan out per cluster, so we want a
    // healthy ring.
    let bus = EventBus::new(1024);
    let metrics = Arc::new(Metrics::new());
    let readiness = api::ReadinessGate::new();
    let registry = readiness.registry();
    // The DB is up by the time we get here (open() above would have
    // returned Err otherwise). Subsystems that come and go (cluster
    // connectors, reconciler) report their own state as they spawn.
    registry.report("db", api::ComponentState::Up);

    // ---- engine assembly (synchrotron-cd-70x: slice 4 of d2p) ----
    //
    // The wire-pipeline epic landed three slices of primitives:
    //   211 — Reconciler::reconcile_and_apply_app
    //   oes — LiveStore + StoreLiveSource + LiveStoreUpdater
    //   c4c — AppRenderer + DesiredStore + StoreDesiredSource
    //
    // This slice constructs the graph in process. The dynamic
    // drivers — actually spawning informers per (cluster, GVK) and
    // running git → render → DesiredStore.put on a loop — are
    // tracked separately (see closing notes below); the static graph
    // landing here is what they plug into.

    // Plugin registry: build from config. PluginCfg is the user-
    // facing shape; Registry::from_yaml takes the same canonical
    // form, so we re-serialize. Cheap (config plugin list is small).
    let plugins_yaml = serde_yaml_ng::to_string(&serde_yaml_ng::value::Value::Mapping(
        std::iter::once((
            serde_yaml_ng::Value::String("plugins".into()),
            serde_yaml_ng::to_value(&cfg.plugins)?,
        ))
        .collect(),
    ))?;
    let plugin_registry = Arc::new(Registry::from_yaml(&plugins_yaml)?);
    info!(
        plugins = plugin_registry.names().len(),
        "plugin registry loaded"
    );

    let app_cache = Arc::new(AppCache::with_defaults());
    let app_renderer = Arc::new(AppRenderer::new(plugin_registry, app_cache));

    // Desired-side store (slice c4c). The render driver, when it
    // lands, calls `.put_vec(app, manifests)` after each render.
    let desired_store = Arc::new(DesiredStore::new());
    let desired_source = Arc::new(StoreDesiredSource(desired_store.clone()));

    // Live-side store (slice oes). Cluster names from config are
    // registered up front so `live(app, cluster)` returns Ok(empty)
    // for known clusters before any informer event has landed,
    // rather than NotFound (which the reconciler maps to
    // ClusterNotFound — a user-facing config error).
    let live_store = Arc::new(LiveStore::new());
    for c in &cfg.clusters {
        live_store.register_cluster(synchrotron_types::ClusterName(c.name.clone()));
    }
    let live_source = Arc::new(StoreLiveSource(live_store.clone()));
    info!(
        clusters = cfg.clusters.len(),
        "live-store registered cluster names"
    );

    // Build a kube Applier + ReconcileExecutor per configured
    // cluster (synchrotron-cd-79e), and spawn the per-GVK informers
    // that feed the LiveStore (synchrotron-cd-wba). A cluster whose
    // Client fails to build (missing kubeconfig, unreachable
    // in-cluster SA token, etc) is logged and skipped — the
    // Reconciler stays plan-only for that cluster. Other clusters
    // still apply.
    let mut reconciler_builder =
        Reconciler::new(desired_source.clone(), live_source.clone(), bus.clone())
            .with_metrics(metrics.clone());
    let mut cluster_informer_handles = Vec::with_capacity(cfg.clusters.len());
    for c in &cfg.clusters {
        let kube_cfg = if c.in_cluster {
            KubeClusterConfig {
                name: synchrotron_kube::ClusterName(c.name.clone()),
                source: AuthSource::InCluster,
            }
        } else if let Some(kc) = &c.kubeconfig {
            let mut kcfg = KubeClusterConfig::from_kubeconfig(c.name.clone(), kc.clone());
            if let Some(ctx) = &c.context {
                kcfg = kcfg.with_context(ctx.clone());
            }
            kcfg
        } else {
            KubeClusterConfig::default_discovery(c.name.clone())
        };
        match KubeClient::connect(&kube_cfg).await {
            Ok(kc) => {
                let ssa = Arc::new(KubeSsaApplier::new(kc.client().clone(), "synchrotron"));
                let applier = Arc::new(KubeApplierAdapter::new(ssa));
                let executor = ReconcileExecutor {
                    applier,
                    // AlwaysHealthy until the informer-backed
                    // health checker lands (it'll read aggregate
                    // status off the LiveStore that wba is feeding).
                    health: Arc::new(AlwaysHealthy),
                    config: WaveExecConfig::default(),
                };
                reconciler_builder = reconciler_builder.with_cluster_executor(
                    synchrotron_types::ClusterName(c.name.clone()),
                    executor,
                );
                info!(cluster = %c.name, "kube executor wired");

                // Spawn one Informer<DynamicObject> per default GVK
                // and route every event into the LiveStore via a
                // LiveStoreUpdater (slice wba). Informer + router
                // tasks live on the returned handle for the
                // process lifetime.
                let handles = spawn_default_informers(
                    synchrotron_types::ClusterName(c.name.clone()),
                    kc,
                    live_store.clone(),
                )
                .await;
                info!(
                    cluster = %c.name,
                    informers = handles.informers.len(),
                    "kube informers spawned"
                );
                cluster_informer_handles.push(handles);
            }
            Err(e) => {
                warn!(cluster = %c.name, error = %e, "kube client connect failed; cluster stays plan-only");
            }
        }
    }
    // Hold the informer handles for the process lifetime — drop
    // would abort every supervisor.
    let _cluster_informer_handles = cluster_informer_handles;
    let reconciler = Arc::new(reconciler_builder);

    // Worker pool: bounded concurrency + per-app FIFO. Handler
    // calls reconcile_and_apply_app, which falls back to plan-only
    // for clusters without an executor.
    let pool_db_handle = {
        // Open an extra DB handle for the pool's per-dispatch
        // app-record lookup. Cheap; SQLite open is in-process.
        Arc::new(Mutex::new(synchrotron_core::db::Database::open(
            &cfg.server.db_path,
        )?))
    };
    let pool = {
        let reconciler = reconciler.clone();
        let db = pool_db_handle.clone();
        Arc::new(WorkerPool::new(
            PoolConfig {
                max_concurrent: 64,
                per_app_queue_cap: 16,
            },
            move |ctx: JobCtx| {
                let reconciler = reconciler.clone();
                let db = db.clone();
                Box::pin(async move {
                    let app = AppName(ctx.app_id.clone());
                    // Look up the destination cluster from the
                    // app record. Missing record (app deleted
                    // between enqueue and dispatch) → no-op.
                    let cluster = {
                        let db = db.lock().expect("db mutex poisoned");
                        match db.get_application(&app.0) {
                            Ok(Some(a)) => a.destination.cluster,
                            Ok(None) => {
                                warn!(app = %app.0, "dispatch: app not found in DB; skipping");
                                return;
                            }
                            Err(e) => {
                                warn!(app = %app.0, error = %e, "dispatch: DB lookup failed");
                                return;
                            }
                        }
                    };
                    let _ = reconciler.reconcile_and_apply_app(&app, &cluster).await;
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            },
        ))
    };
    info!("worker pool spawned (max_concurrent=64)");

    // Apps state for the HTTP API. Wire the diff engine using the
    // same desired/live sources so /apps/{name}/diff stops returning
    // the empty-with-note shape — slice 4 unblocks this for free.
    let apps_state = api::AppsState::new(
        synchrotron_core::db::Database::open(&cfg.server.db_path)?,
        bus.clone(),
    )
    .with_diff_engine(Arc::new(api::diff_engine::PlannerDiffEngine::new(
        desired_source.clone(),
        live_source.clone(),
    )));

    // Trigger: subscribes to the bus, drains RepoChanged /
    // WebhookTriggered events, and enqueues reconciles. The
    // resolver looks up apps from the DB by repo URL.
    let resolver = Arc::new(DbAppResolver {
        db: apps_state.db.clone(),
    });
    let _trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);
    registry.report("reconciler", api::ComponentState::Up);
    info!("event trigger spawned (RepoChanged / WebhookTriggered → pool)");

    // Desired-state pipeline (synchrotron-cd-tvy): git Pollers per
    // repo, bus bridges, and the render loop that updates the
    // DesiredStore on RepoChanged. Built from existing cfg; the
    // pollers stay alive while `_pipeline` does (which is the whole
    // process — the binding name is `_` to silence unused-warning,
    // since the live tasks are what we want, not the handle).
    let git_workspace = synchrotron_git::Workspace::new(
        cfg.server
            .db_path
            .parent()
            .map(|p| p.join("git"))
            .unwrap_or_else(|| std::path::PathBuf::from(".synchrotron-git")),
    );
    git_workspace.ensure_layout()?;
    let git_client = Arc::new(synchrotron_git::GitClient::new(git_workspace.clone()));
    let _pipeline = pipeline::spawn(pipeline::PipelineDeps {
        repos: cfg.repos.clone(),
        polling: cfg.polling.clone(),
        workspace: git_workspace,
        git_client,
        renderer: app_renderer,
        desired_store: desired_store.clone(),
        db: apps_state.db.clone(),
        bus: bus.clone(),
    });
    info!(repos = cfg.repos.len(), "render pipeline spawned");

    let app = api::router_with_apps(apps_state)
        .merge(api::metrics_router(metrics))
        .merge(api::probes_router(readiness.clone()));

    let listener = tokio::net::TcpListener::bind(&cfg.server.listen_addr).await?;
    info!("listening on {}", cfg.server.listen_addr);
    // Initialization complete: DB open, config loaded, listener
    // bound, engine graph constructed. Flip readiness so kubelet
    // routes traffic.
    readiness.mark_ready();
    axum::serve(listener, app).await?;

    Ok(())
}
