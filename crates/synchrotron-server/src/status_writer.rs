//! Persist event-bus status signals back to the applications row.
//!
//! The reconciler publishes `SyncOutcome` on every reconcile pass
//! (success or failure); the health engine publishes
//! `AppHealthAssessed` when it produces an aggregate verdict. Both
//! are pure events — without this writer, the `sync_status` and
//! `health_status` columns on the application row stay at
//! `Unknown`/null indefinitely. See synchrotron-cd-c7z.
//!
//! Two narrow UPDATE statements (one per column family) avoid
//! racing each other when both events fire for the same app.

use std::sync::{Arc, Mutex};

use synchrotron_core::db::Database;
use synchrotron_core::events::{EventBus, RecvError, SystemEvent};
use synchrotron_types::SyncStatusCode;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub fn spawn(bus: EventBus, db: Arc<Mutex<Database>>) -> JoinHandle<()> {
    // Subscribe BEFORE spawning so we don't race a caller that
    // publishes immediately after `spawn` returns. broadcast::Sender
    // only buffers events for receivers that already exist.
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(evt) => match evt.event {
                    SystemEvent::SyncOutcome {
                        app,
                        cluster: _,
                        success,
                        message: _,
                    } => {
                        let status = if success {
                            SyncStatusCode::Synced
                        } else {
                            SyncStatusCode::SyncFailed
                        };
                        // last_synced_revision isn't on the event yet
                        // — once the reconciler threads it through,
                        // pass Some(rev) here.
                        let res = {
                            let db = db.lock().expect("db mutex poisoned");
                            db.update_application_sync(&app.0, &status, None)
                        };
                        match res {
                            Ok(true) => {
                                debug!(app = %app.0, status = status.as_str(), "sync status written")
                            }
                            Ok(false) => {
                                debug!(app = %app.0, "sync status: app no longer in DB; dropping")
                            }
                            Err(e) => warn!(app = %app.0, error = %e, "sync status write failed"),
                        }
                    }
                    SystemEvent::AppHealthAssessed {
                        app,
                        cluster: _,
                        status,
                        message,
                    } => {
                        let res = {
                            let db = db.lock().expect("db mutex poisoned");
                            db.update_application_health(&app.0, &status, message.as_deref())
                        };
                        match res {
                            Ok(true) => {
                                debug!(app = %app.0, status = status.as_str(), "health status written")
                            }
                            Ok(false) => {
                                debug!(app = %app.0, "health status: app no longer in DB; dropping")
                            }
                            Err(e) => warn!(app = %app.0, error = %e, "health status write failed"),
                        }
                    }
                    _ => {}
                },
                Err(RecvError::Closed) => {
                    debug!("event bus closed; status writer exiting");
                    return;
                }
            }
        }
    })
}
