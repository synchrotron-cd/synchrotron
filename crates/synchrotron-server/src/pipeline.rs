//! Git → render → DesiredStore driver (synchrotron-cd-tvy).
//!
//! This is the runtime half of the wire-pipeline epic on the
//! desired-state side. The static graph (slice 70x) constructs
//! [`DesiredStore`] and an [`AppRenderer`]; this module fills it.
//!
//! Two pieces work in tandem:
//!
//! 1. **Pollers**, one per configured repo. Each Poller wraps a
//!    [`GitClient::fetch`] call on the configured `interval`,
//!    emitting [`PollEvent`]s on its broadcast channel. On
//!    `Fetched { current_head != previous_head }` the poller has
//!    detected new commits.
//!
//! 2. **Bus bridge**, one task per Poller, translating
//!    `PollEvent::Fetched` into `SystemEvent::RepoChanged` on the
//!    process-wide [`EventBus`]. Reusing the bus means the existing
//!    `EventTrigger` (which fans webhook + repo-change events out
//!    to the worker pool) also fans these into reconciles —
//!    no duplicate plumbing.
//!
//! 3. **Render loop**, a single task subscribed to the bus. On
//!    `RepoChanged` it lists applications referencing the repo,
//!    materializes the repo at the new head into the workspace,
//!    runs each app through [`AppRenderer`], and stores the
//!    result in [`DesiredStore`].
//!
//! # Scope (tvy)
//!
//! What this module does NOT yet do:
//!
//! - **Seed render at startup.** Apps that exist before the first
//!   poll see an empty DesiredStore until their repo polls. The
//!   first poll cycle fills the cache; users wanting an instant
//!   first reconcile can `bd trigger` (webhook) the repo. Adding
//!   an explicit "render every known app at boot" is a small
//!   follow-up; tying it to apps showing up in the DB after
//!   startup is a bigger one.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use synchrotron_core::db::Database;
use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_core::secrets::{SecretError, SecretStore};
use synchrotron_git::poller::fetch_fn_from_client;
use synchrotron_git::{
    Credentials, GitClient, PollEvent, Poller, PollerConfig, Repo, Sha, Workspace,
};
use synchrotron_plugins::{AppRenderSpec, AppRenderer};
use synchrotron_reconcile::DesiredStore;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::{Polling, RepoCfg};

/// Owns the desired-state driver: git Pollers, bus bridges, and the
/// render loop. Keep [`PipelineHandles`] alive for the lifetime of
/// the process — dropping it aborts every spawned task.
pub struct PipelineHandles {
    pub pollers: Vec<Poller>,
    pub bridges: Vec<JoinHandle<()>>,
    pub render_loop: JoinHandle<()>,
}

/// Build inputs for [`spawn`].
pub struct PipelineDeps {
    pub repos: Vec<RepoCfg>,
    pub polling: Polling,
    pub workspace: Workspace,
    pub git_client: Arc<GitClient>,
    pub renderer: Arc<AppRenderer>,
    pub desired_store: Arc<DesiredStore>,
    pub db: Arc<Mutex<Database>>,
    pub bus: EventBus,
    /// Resolves `RepoCfg.credentials_secret` into [`Credentials`].
    /// Slice u0o; defaults to a `NoopSecretStore`-backed one when
    /// the operator hasn't configured any backend, in which case
    /// any `credentials_secret = Some(...)` aborts the
    /// pipeline build.
    pub secrets: Arc<dyn SecretStore>,
}

/// Wire the pipeline. Spawns one Poller and one bridge task per
/// configured repo (sequentially clone-initializing each so a slow
/// remote doesn't block other repos for too long is a known
/// tradeoff — the alternative is spawning blocking clones in
/// parallel, which we'll do once we feel the pinch), then the
/// single render loop.
pub fn spawn(deps: PipelineDeps) -> PipelineHandles {
    let url_by_repo_id: HashMap<String, Repo> =
        build_repo_map(&deps.repos, &deps.git_client, deps.secrets.as_ref());

    let mut pollers = Vec::with_capacity(url_by_repo_id.len());
    let mut bridges = Vec::with_capacity(url_by_repo_id.len());

    let poller_cfg = PollerConfig {
        interval: std::time::Duration::from_secs(deps.polling.repo_interval_seconds),
        // Default jitter — the config schema doesn't expose this
        // yet. 10% is the value PollerConfig::default uses too.
        jitter_ratio: 0.1,
    };

    for repo in url_by_repo_id.values() {
        let fetch = fetch_fn_from_client(deps.git_client.clone(), repo.clone());
        let poller = Poller::spawn(repo.id.as_str().to_string(), fetch, poller_cfg.clone());
        let bridge = spawn_bridge(repo.url.0.clone(), poller.subscribe(), deps.bus.clone());
        info!(repo = %repo.url.0, branch = %repo.branch, "poller spawned");
        pollers.push(poller);
        bridges.push(bridge);
    }

    let render_loop = spawn_render_loop(deps);
    PipelineHandles {
        pollers,
        bridges,
        render_loop,
    }
}

fn build_repo_map(
    cfgs: &[RepoCfg],
    client: &GitClient,
    secrets: &dyn SecretStore,
) -> HashMap<String, Repo> {
    let mut out = HashMap::with_capacity(cfgs.len());
    for r in cfgs {
        let url = synchrotron_types::RepoUrl(r.url.clone());
        let branch = r.branch.clone().unwrap_or_else(|| "main".to_string());
        let creds = match resolve_credentials(r.credentials_secret.as_deref(), secrets) {
            Ok(c) => c,
            Err(e) => {
                // Don't kill the whole server for one bad repo, but
                // be loud — fetches will fail without creds.
                warn!(
                    repo = %r.url,
                    secret = ?r.credentials_secret,
                    error = %e,
                    "credentials resolution failed; falling back to anonymous (fetches will likely fail)"
                );
                Credentials::None
            }
        };
        let repo = Repo::new(url, branch, creds);
        // Pre-clone so the first poll's diff against the bare repo
        // has somewhere to land. ensure_cloned is idempotent.
        if let Err(e) = client.ensure_cloned(&repo) {
            warn!(repo = %r.url, error = %e, "initial clone failed; poller will retry on cadence");
        }
        out.insert(repo.id.as_str().to_string(), repo);
    }
    out
}

/// Resolve a `credentials_secret` reference into a typed
/// [`Credentials`]. The secret value is parsed as JSON first, then
/// YAML — operators tend to put kubeconfig-shaped YAML into k8s
/// Secrets, while CI tooling tends to produce JSON.
fn resolve_credentials(
    secret_name: Option<&str>,
    secrets: &dyn SecretStore,
) -> Result<Credentials, ResolveCredsError> {
    let Some(name) = secret_name else {
        return Ok(Credentials::None);
    };
    let value = secrets.get(name).map_err(ResolveCredsError::Lookup)?;
    serde_json::from_str::<Credentials>(&value)
        .or_else(|_| serde_yaml_ng::from_str::<Credentials>(&value))
        .map_err(|e| ResolveCredsError::Parse(e.to_string()))
}

#[derive(Debug, thiserror::Error)]
enum ResolveCredsError {
    #[error("secret lookup failed: {0}")]
    Lookup(#[from] SecretError),
    #[error("secret parse failed: {0}")]
    Parse(String),
}

/// Translate one Poller's events into bus publishes.
fn spawn_bridge(
    repo_url: String,
    mut rx: tokio::sync::broadcast::Receiver<PollEvent>,
    bus: EventBus,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(PollEvent::Fetched(result)) => {
                    let moved = result
                        .previous_head
                        .as_ref()
                        .map(|p| p.0 != result.current_head.0)
                        .unwrap_or(true);
                    if !moved {
                        debug!(repo = %repo_url, head = %result.current_head.0, "poll: head unchanged");
                        continue;
                    }
                    debug!(repo = %repo_url, head = %result.current_head.0, "poll: head moved");
                    bus.publish(SystemEvent::RepoChanged {
                        repo: repo_url.clone(),
                        new_head: result.current_head.0.clone(),
                    });
                }
                Ok(PollEvent::FetchFailed(msg)) => {
                    warn!(repo = %repo_url, error = %msg, "poll: fetch failed");
                    bus.publish(SystemEvent::RepoFetchFailed {
                        repo: repo_url.clone(),
                        error: msg,
                    });
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(repo = %repo_url, lagged = n, "bus bridge lagged");
                }
            }
        }
    })
}

/// Single-consumer loop that translates RepoChanged into renders.
fn spawn_render_loop(deps: PipelineDeps) -> JoinHandle<()> {
    let url_to_repo: HashMap<String, Repo> = deps
        .repos
        .iter()
        .map(|r| {
            let repo = Repo::new(
                synchrotron_types::RepoUrl(r.url.clone()),
                r.branch.clone().unwrap_or_else(|| "main".to_string()),
                Credentials::None,
            );
            (r.url.clone(), repo)
        })
        .collect();

    let bus = deps.bus.clone();
    let workspace = deps.workspace.clone();
    let git_client = deps.git_client.clone();
    let renderer = deps.renderer.clone();
    let desired_store = deps.desired_store.clone();
    let db = deps.db.clone();

    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(evt) => match evt.event {
                    SystemEvent::RepoChanged { repo, new_head } => {
                        let Some(repo_struct) = url_to_repo.get(&repo).cloned() else {
                            debug!(repo, "RepoChanged for repo not in config; ignoring");
                            continue;
                        };
                        render_apps_for_repo(
                            &git_client,
                            &workspace,
                            &renderer,
                            &desired_store,
                            &db,
                            &bus,
                            &repo_struct,
                            &repo,
                            &new_head,
                        )
                        .await;
                    }
                    SystemEvent::AppChanged { app, repo } => {
                        // Render-on-create path (5bv): the poller only
                        // fires RepoChanged when HEAD moves, so a new
                        // app against a quiet repo never lands in the
                        // desired store. Use the bare repo's cached
                        // HEAD — no network fetch needed.
                        let Some(repo_struct) = url_to_repo.get(&repo).cloned() else {
                            debug!(app = %app.0, repo, "AppChanged for repo not in config; ignoring");
                            continue;
                        };
                        let head = {
                            let gc = git_client.clone();
                            let rs = repo_struct.clone();
                            tokio::task::spawn_blocking(move || gc.current_head(&rs))
                                .await
                                .expect("current_head task panicked")
                        };
                        let head = match head {
                            Ok(h) => h.0,
                            Err(e) => {
                                warn!(app = %app.0, repo, error = %e, "AppChanged: current_head failed (repo not yet cloned?)");
                                continue;
                            }
                        };
                        render_apps_for_repo(
                            &git_client,
                            &workspace,
                            &renderer,
                            &desired_store,
                            &db,
                            &bus,
                            &repo_struct,
                            &repo,
                            &head,
                        )
                        .await;
                    }
                    _ => {}
                },
                Err(synchrotron_core::events::RecvError::Closed) => {
                    debug!("event bus closed; render loop exiting");
                    return;
                }
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn render_apps_for_repo(
    git_client: &Arc<GitClient>,
    workspace: &Workspace,
    renderer: &Arc<AppRenderer>,
    desired_store: &Arc<DesiredStore>,
    db: &Arc<Mutex<Database>>,
    bus: &EventBus,
    repo_struct: &Repo,
    repo_url: &str,
    new_head: &str,
) {
    let apps = {
        let db = db.lock().expect("db mutex poisoned");
        match db.list_applications() {
            Ok(list) => list
                .into_iter()
                .filter(|a| a.source.repo_url.0 == repo_url)
                .collect::<Vec<_>>(),
            Err(e) => {
                warn!(error = %e, "render loop: list_applications failed");
                return;
            }
        }
    };
    if apps.is_empty() {
        debug!(repo = repo_url, "RepoChanged: no apps reference this repo");
        return;
    }

    // Materialize once per repo+commit; all apps for the repo
    // share the same checkout. Path scopes them apart via
    // app.source.path.
    let work_dir = workspace.root().join("work").join(format!(
        "{}-{}",
        repo_struct.id.as_str(),
        short(new_head)
    ));
    let materialize_res = {
        let gc = git_client.clone();
        let repo = repo_struct.clone();
        let head = Sha(new_head.to_string());
        let target = work_dir.clone();
        tokio::task::spawn_blocking(move || gc.materialize(&repo, &head, &target))
            .await
            .expect("materialize task panicked")
    };
    if let Err(e) = materialize_res {
        warn!(repo = repo_url, head = new_head, error = %e, "materialize failed");
        return;
    }

    for app in apps {
        let app_name_str = app.name.0.clone();
        let plugin = app
            .source
            .plugin
            .as_ref()
            .map(|p| p.name.clone())
            .unwrap_or_else(|| "raw".to_string());
        let params = plugin_params(app.source.plugin.as_ref());
        let source_path = subpath(&work_dir, &app.source.path);

        let spec = AppRenderSpec {
            app_id: app_name_str.clone(),
            commit_hash: new_head.to_string(),
            plugin,
            source_path,
            params,
        };
        match renderer.render(&spec).await {
            Ok(manifests) => {
                desired_store.put_with_revision(
                    app.name.clone(),
                    manifests,
                    Some(new_head.to_string()),
                );
                info!(app = %app_name_str, head = new_head, "desired store updated");
                // Kick the reconciler now that desired state is
                // populated. Ordering matters: publish AFTER the
                // store write so the reconcile pass sees the entry.
                bus.publish(SystemEvent::ManualSyncRequested {
                    app: app.name.clone(),
                });
            }
            Err(e) => {
                warn!(app = %app_name_str, head = new_head, error = %e, "render failed; desired-store entry unchanged");
            }
        }
    }
}

fn plugin_params(p: Option<&synchrotron_types::PluginRef>) -> serde_json::Value {
    match p {
        None => serde_json::json!({}),
        Some(pr) => {
            // Map (name, value) pairs into a flat object so plugin
            // runtimes consuming serde_json see them as keys.
            let mut map = serde_json::Map::new();
            for kv in &pr.parameters {
                map.insert(kv.name.clone(), serde_json::Value::String(kv.value.clone()));
            }
            serde_json::Value::Object(map)
        }
    }
}

fn subpath(work_dir: &std::path::Path, app_path: &str) -> PathBuf {
    // Normalize. Reject `..` segments so an app can't escape its
    // repo workdir via a crafted path (defense in depth — the
    // config layer should validate this too).
    let clean: PathBuf = std::path::Path::new(app_path)
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => Some(PathBuf::from(s)),
            std::path::Component::CurDir => None,
            std::path::Component::ParentDir => None,
            std::path::Component::RootDir | std::path::Component::Prefix(_) => None,
        })
        .collect();
    if clean.as_os_str().is_empty() {
        work_dir.to_path_buf()
    } else {
        work_dir.join(clean)
    }
}

/// Short SHA for log lines / work-dir naming. 7 chars matches
/// what `git log` shows.
fn short(sha: &str) -> &str {
    if sha.len() > 7 {
        &sha[..7]
    } else {
        sha
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subpath_strips_parent_dir() {
        let work = std::path::Path::new("/tmp/work");
        assert_eq!(subpath(work, "manifests"), work.join("manifests"));
        assert_eq!(subpath(work, "../escape"), work.join("escape"));
        assert_eq!(subpath(work, "./a/./b"), work.join("a").join("b"));
        assert_eq!(subpath(work, ""), work.to_path_buf());
        assert_eq!(
            subpath(work, "/etc/passwd"),
            work.join("etc").join("passwd")
        );
    }

    #[test]
    fn short_truncates_long_sha() {
        assert_eq!(short("abcdef1234567890"), "abcdef1");
    }

    #[test]
    fn short_passes_through_short_input() {
        assert_eq!(short("abc"), "abc");
    }

    #[test]
    fn plugin_params_empty_when_none() {
        let v = plugin_params(None);
        assert!(v.is_object() && v.as_object().unwrap().is_empty());
    }

    #[test]
    fn plugin_params_maps_kv_pairs() {
        let pr = synchrotron_types::PluginRef {
            name: "helm".into(),
            parameters: vec![
                synchrotron_types::PluginParam {
                    name: "replicas".into(),
                    value: "3".into(),
                },
                synchrotron_types::PluginParam {
                    name: "image".into(),
                    value: "nginx:1.27".into(),
                },
            ],
        };
        let v = plugin_params(Some(&pr));
        let obj = v.as_object().unwrap();
        assert_eq!(obj["replicas"], serde_json::Value::String("3".into()));
        assert_eq!(obj["image"], serde_json::Value::String("nginx:1.27".into()));
    }
}
