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
        trigger: "manual".into(),
        revision: None,
        resources_synced: 0,
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
        trigger: "manual".into(),
        revision: None,
        resources_synced: 0,
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
async fn sync_outcome_appends_a_sync_history_record() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    let app_id = {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
        guard.get_application("web").unwrap().unwrap().id
    };
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: true,
        message: None,
        trigger: "manual".into(),
        revision: None,
        resources_synced: 0,
    });
    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: false,
        message: Some("boom".into()),
        trigger: "manual".into(),
        revision: None,
        resources_synced: 0,
    });

    wait_for(
        || {
            let history = db.lock().unwrap().get_sync_history(&app_id, 10).unwrap();
            history.len() == 2
        },
        "two history rows",
    )
    .await;

    let history = db.lock().unwrap().get_sync_history(&app_id, 10).unwrap();
    // Ordered DESC by started_at, so [0] is the failure, [1] is the success.
    let statuses: Vec<_> = history.iter().map(|h| h.status.clone()).collect();
    assert!(statuses.contains(&synchrotron_core::db::SyncRecordStatus::Succeeded));
    assert!(statuses.contains(&synchrotron_core::db::SyncRecordStatus::Failed));
}

#[tokio::test]
async fn sync_outcome_revision_lands_in_app_and_history() {
    let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
    let app_id = {
        let guard = db.lock().unwrap();
        seed_app(&guard, "web");
        guard.get_application("web").unwrap().unwrap().id
    };
    let bus = EventBus::new(16);
    let _writer = status_writer::spawn(bus.clone(), db.clone());

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("in-cluster".into()),
        success: true,
        message: None,
        trigger: "webhook".into(),
        revision: Some("deadbeef".into()),
        resources_synced: 5,
    });

    wait_for(
        || {
            let app = db.lock().unwrap().get_application("web").unwrap().unwrap();
            app.status.last_synced_revision.as_deref() == Some("deadbeef")
        },
        "last_synced_revision populated",
    )
    .await;

    let history = db.lock().unwrap().get_sync_history(&app_id, 10).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].revision, "deadbeef");
    assert_eq!(history[0].resources_synced, 5);
    assert_eq!(
        history[0].trigger,
        synchrotron_core::db::SyncTrigger::Webhook
    );
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
        trigger: "manual".into(),
        revision: None,
        resources_synced: 0,
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
