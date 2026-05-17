//! c7z: confirm SyncOutcome / AppHealthAssessed events update the
//! applications row.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use synchrotron_core::db::Database;
use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_server::status_writer;
use synchrotron_types::{
    AppDestination, AppName, AppSource, AppStatus, Application, ClusterName, HealthStatusCode,
    RepoUrl, SyncPolicy,
};
use uuid::Uuid;

fn seed_app(db: &Database, name: &str) {
    let now = chrono::Utc::now();
    db.insert_application(&Application {
        id: Uuid::new_v4(),
        name: AppName(name.into()),
        namespace: "default".into(),
        source: AppSource {
            repo_url: RepoUrl("git://example/repo.git".into()),
            path: "manifests".into(),
            target_revision: "main".into(),
            plugin: None,
        },
        destination: AppDestination {
            cluster: ClusterName("in-cluster".into()),
            namespace: "default".into(),
        },
        sync_policy: SyncPolicy::default(),
        status: AppStatus::default(),
        created_at: now,
        updated_at: now,
    })
    .unwrap();
}

async fn wait_for<F: Fn() -> bool>(predicate: F, label: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("predicate {label} never became true within 2s");
}

#[tokio::test]
async fn sync_outcome_success_marks_app_synced() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
    }
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: true,
        message: None,
    });

    wait_for(
        || {
            let app = db.lock().unwrap().get_application("web").unwrap().unwrap();
            matches!(app.status.sync, synchrotron_types::SyncStatusCode::Synced)
                && app.status.last_synced_at.is_some()
        },
        "sync_status=Synced + last_synced_at populated",
    )
    .await;
}

#[tokio::test]
async fn sync_outcome_failure_marks_app_sync_failed() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
    }
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: false,
        message: Some("boom".into()),
    });

    wait_for(
        || {
            let app = db.lock().unwrap().get_application("web").unwrap().unwrap();
            matches!(
                app.status.sync,
                synchrotron_types::SyncStatusCode::SyncFailed
            )
        },
        "sync_status=SyncFailed",
    )
    .await;
}

#[tokio::test]
async fn health_assessment_writes_status_and_message() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
    }
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::AppHealthAssessed {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        status: HealthStatusCode::Degraded,
        message: Some("pod CrashLoopBackOff".into()),
    });

    wait_for(
        || {
            let app = db.lock().unwrap().get_application("web").unwrap().unwrap();
            matches!(app.status.health, HealthStatusCode::Degraded)
                && app.status.health_message.as_deref() == Some("pod CrashLoopBackOff")
        },
        "health=Degraded with message",
    )
    .await;
}

#[tokio::test]
async fn sync_and_health_dont_clobber_each_other() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
    }
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: true,
        message: None,
    });
    bus.publish(SystemEvent::AppHealthAssessed {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        status: HealthStatusCode::Healthy,
        message: None,
    });

    wait_for(
        || {
            let app = db.lock().unwrap().get_application("web").unwrap().unwrap();
            matches!(app.status.sync, synchrotron_types::SyncStatusCode::Synced)
                && matches!(app.status.health, HealthStatusCode::Healthy)
        },
        "both sync and health set",
    )
    .await;
}
