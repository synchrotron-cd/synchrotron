//! Scenario runner: build state, drive reconciles, collect samples.
//!
//! Scheduling model is "sweeps": one sweep enqueues every app
//! exactly once, round-robined across clusters. Sweeps loop until
//! `iterations` is hit or `duration_seconds` elapses. Stats are
//! captured only after `warmup_sweeps` to keep cold-start noise
//! out of the percentiles.
//!
//! Latency is measured around the synchronous `reconcile_app` call
//! inside the worker pool handler — that's the real per-app cost
//! the engine incurs. Memory is sampled once per second from
//! `/proc/self/statm`.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use synchrotron_core::events::EventBus;
use synchrotron_reconcile::{PoolConfig, Reconciler, Trigger, WorkerPool};

use crate::config::ScenarioConfig;
use crate::report::{LatencyStats, MemSample, MemoryStats, ReconcileCounts, Report};
use crate::sources::{SyntheticDesired, SyntheticLive, SyntheticState};

pub async fn run_scenario(cfg: ScenarioConfig) -> anyhow::Result<Report> {
    let started_wall = chrono::Utc::now();
    let started = Instant::now();

    tracing::info!(
        scenario = %cfg.name,
        apps = cfg.apps,
        clusters = cfg.clusters,
        manifests_per_app = cfg.manifests_per_app,
        "building synthetic state"
    );

    let state = Arc::new(SyntheticState::build(
        cfg.apps,
        cfg.manifests_per_app,
        cfg.clusters,
        cfg.drift_ratio,
    ));

    let bus = EventBus::new(1024);
    let reconciler = Arc::new(Reconciler::new(
        Arc::new(SyntheticDesired(state.clone())),
        Arc::new(SyntheticLive(state.clone())),
        bus,
    ));

    // Map app-id (string) → (AppName, ClusterName) up front so the
    // pool handler can look them up by the string ID it receives.
    let app_lookup: Arc<Vec<(synchrotron_types::AppName, synchrotron_types::ClusterName)>> =
        Arc::new(
            state
                .app_names
                .iter()
                .enumerate()
                .map(|(i, app)| {
                    let cluster = state.cluster_names[i % state.cluster_names.len()].clone();
                    (app.clone(), cluster)
                })
                .collect(),
        );
    let by_id: Arc<std::collections::HashMap<String, usize>> = Arc::new(
        state
            .app_names
            .iter()
            .enumerate()
            .map(|(i, app)| (app.0.clone(), i))
            .collect(),
    );

    // Shared collectors. `latency_us` is locked once per reconcile;
    // contention is fine relative to the work being measured (a
    // full plan over ~25 manifests).
    let latency_us = Arc::new(Mutex::new(Vec::<u64>::with_capacity(
        cfg.apps * cfg.iterations.unwrap_or(10) as usize,
    )));
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    // Sweep number on which sampling started. `u32::MAX` = not yet.
    let recording_from_sweep = Arc::new(AtomicU64::new(u64::MAX));
    let current_sweep = Arc::new(AtomicU64::new(0));

    let handler = {
        let reconciler = reconciler.clone();
        let app_lookup = app_lookup.clone();
        let by_id = by_id.clone();
        let latency_us = latency_us.clone();
        let completed = completed.clone();
        let failed = failed.clone();
        let recording_from = recording_from_sweep.clone();
        let current_sweep = current_sweep.clone();
        move |ctx: synchrotron_reconcile::JobCtx| {
            let reconciler = reconciler.clone();
            let app_lookup = app_lookup.clone();
            let by_id = by_id.clone();
            let latency_us = latency_us.clone();
            let completed = completed.clone();
            let failed = failed.clone();
            let recording_from = recording_from.clone();
            let current_sweep = current_sweep.clone();
            Box::pin(async move {
                let Some(&idx) = by_id.get(&ctx.app_id) else {
                    return;
                };
                let (app, cluster) = &app_lookup[idx];
                let t0 = Instant::now();
                let outcome = reconciler.reconcile_app(app, cluster);
                let elapsed = t0.elapsed();

                // Only record after warmup.
                let record_threshold = recording_from.load(Ordering::Relaxed);
                let sweep_now = current_sweep.load(Ordering::Relaxed);
                if sweep_now >= record_threshold {
                    if outcome.success() {
                        completed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                    let us = elapsed.as_micros() as u64;
                    latency_us.lock().unwrap().push(us);
                }
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        }
    };

    let pool = Arc::new(WorkerPool::new(
        PoolConfig {
            max_concurrent: cfg.concurrency,
            // Generous per-app cap — the driver enqueues each app
            // once per sweep and waits for drain before the next,
            // so we shouldn't see >1 queued per app, but allow
            // headroom for asymmetric drain.
            per_app_queue_cap: 16,
        },
        handler,
    ));

    // Memory sampler.
    let mem_samples = Arc::new(Mutex::new(Vec::<MemSample>::new()));
    let peak_rss = Arc::new(AtomicU64::new(0));
    let stop_sampler = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler_handle = {
        let mem_samples = mem_samples.clone();
        let peak_rss = peak_rss.clone();
        let stop_sampler = stop_sampler.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.tick().await; // first tick fires immediately
            loop {
                if stop_sampler.load(Ordering::Relaxed) {
                    break;
                }
                let rss = read_rss_bytes().unwrap_or(0);
                peak_rss.fetch_max(rss, Ordering::Relaxed);
                mem_samples.lock().unwrap().push(MemSample {
                    t_seconds: started.elapsed().as_secs_f64(),
                    rss_bytes: rss,
                });
                tick.tick().await;
            }
        })
    };

    // Drive sweeps.
    let total_sweeps = run_sweeps(
        &pool,
        &state.app_names,
        &cfg,
        &current_sweep,
        &recording_from_sweep,
        &completed,
        &failed,
    )
    .await?;

    stop_sampler.store(true, Ordering::Relaxed);
    let _ = sampler_handle.await;

    let elapsed = started.elapsed();

    let latency_samples = std::mem::take(&mut *latency_us.lock().unwrap());
    let latency = LatencyStats::from_micros(latency_samples);

    let mem_samples = std::mem::take(&mut *mem_samples.lock().unwrap());
    let final_rss = mem_samples.last().map(|s| s.rss_bytes).unwrap_or(0);
    let memory = MemoryStats {
        peak_rss_bytes: peak_rss.load(Ordering::Relaxed),
        final_rss_bytes: final_rss,
        samples: mem_samples,
    };

    let counts = ReconcileCounts {
        completed: completed.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
        sweeps: total_sweeps,
    };

    let throughput = if elapsed.as_secs_f64() > 0.0 {
        counts.completed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    Ok(Report {
        scenario: cfg.name.clone(),
        config: cfg,
        started_at: started_wall.to_rfc3339(),
        elapsed_seconds: elapsed.as_secs_f64(),
        reconciles: counts,
        latency_us: latency,
        memory,
        throughput_per_second: throughput,
    })
}

/// Loop over sweeps until iteration / time budget is exhausted.
/// Returns the total sweep count actually run (warmup included).
async fn run_sweeps(
    pool: &WorkerPool,
    apps: &[synchrotron_types::AppName],
    cfg: &ScenarioConfig,
    current_sweep: &AtomicU64,
    recording_from_sweep: &AtomicU64,
    completed: &AtomicU64,
    failed: &AtomicU64,
) -> anyhow::Result<u64> {
    recording_from_sweep.store(cfg.warmup_sweeps as u64, Ordering::Relaxed);

    let deadline = cfg.duration_seconds.map(|s| Instant::now() + Duration::from_secs(s));
    let target_iterations = cfg.iterations.map(|n| n as u64 + cfg.warmup_sweeps as u64);

    let mut sweep: u64 = 0;
    loop {
        if let Some(target) = target_iterations {
            if sweep >= target {
                break;
            }
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                break;
            }
        }
        current_sweep.store(sweep, Ordering::Relaxed);

        // Snapshot pre-sweep counters so we know when the sweep
        // has fully drained.
        let before = completed.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
        let recording = sweep >= cfg.warmup_sweeps as u64;

        for app in apps {
            // Tight enqueue loop. If the pool's per-app cap fills
            // (shouldn't, given we wait for drain) we spin briefly.
            loop {
                match pool.enqueue(app.0.clone(), Trigger::Manual) {
                    Ok(()) => break,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        }

        // Wait for this sweep to drain.
        let need = if recording { apps.len() as u64 } else { 0 };
        if recording {
            loop {
                let now = completed.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
                if now - before >= need {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        } else {
            // During warmup we don't increment counters in the
            // handler, so fall back to polling pool stats.
            loop {
                let s = pool.stats();
                if s.queue_depth == 0 && s.in_flight == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }

        sweep += 1;
    }

    Ok(sweep)
}

/// Read RSS in bytes from `/proc/self/statm`. Field 1 is "resident"
/// in pages; multiply by `sysconf(_SC_PAGESIZE)`.
fn read_rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
    let page_size: u64 = page_size();
    Some(resident_pages.saturating_mul(page_size))
}

fn page_size() -> u64 {
    // SAFETY: sysconf is thread-safe, and `_SC_PAGESIZE` is always
    // supported on Linux (returns >0).
    unsafe { libc_sysconf_pagesize() }
}

// Avoid pulling in a `libc` dep just for one call: bind via libc's
// soname. Falls back to 4096 if anything is unusual.
#[allow(non_snake_case)]
unsafe fn libc_sysconf_pagesize() -> u64 {
    extern "C" {
        fn sysconf(name: i32) -> i64;
    }
    // _SC_PAGESIZE is 30 on Linux (stable across glibc/musl).
    const SC_PAGESIZE: i32 = 30;
    let v = sysconf(SC_PAGESIZE);
    if v > 0 {
        v as u64
    } else {
        4096
    }
}
