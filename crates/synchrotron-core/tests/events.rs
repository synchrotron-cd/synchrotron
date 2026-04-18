//! Tests for the internal event bus. Cover: multi-consumer delivery,
//! publish-with-no-subscribers, lagged-consumer skip-forward, and
//! producer isolation from slow consumers.

use std::time::Duration;

use synchrotron_core::{EventBus, SystemEvent, WebhookSource};
use tokio::time;

fn repo_changed(repo: &str) -> SystemEvent {
    SystemEvent::RepoChanged {
        repo: repo.into(),
        new_head: "a".repeat(40),
    }
}

#[tokio::test]
async fn fans_out_to_multiple_subscribers() {
    let bus = EventBus::new(32);
    let mut rx1 = bus.subscribe();
    let mut rx2 = bus.subscribe();

    assert_eq!(bus.publish(repo_changed("r1")), 2);

    let e1 = rx1.recv().await.unwrap();
    let e2 = rx2.recv().await.unwrap();
    assert_eq!(e1.event, repo_changed("r1"));
    assert_eq!(e2.event, repo_changed("r1"));
}

#[tokio::test]
async fn publish_with_no_subscribers_is_fine() {
    let bus = EventBus::new(8);
    // No panic, no error — steady state for polling-only deployments.
    assert_eq!(bus.publish(repo_changed("r1")), 0);
    assert_eq!(bus.subscriber_count(), 0);
}

#[tokio::test]
async fn lagged_consumer_skips_forward_silently() {
    let bus = EventBus::new(4);
    let mut rx = bus.subscribe();

    // Publish 10 events into a 4-deep ring buffer. The first 6 are
    // dropped for our receiver; we should end up reading the last 4.
    for i in 0..10 {
        bus.publish(SystemEvent::RepoChanged {
            repo: format!("r{i}"),
            new_head: "a".repeat(40),
        });
    }

    let mut seen = Vec::new();
    for _ in 0..4 {
        // Small timeout so a bug (hang forever) fails the test loudly.
        let evt = time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        if let SystemEvent::RepoChanged { repo, .. } = evt.event {
            seen.push(repo);
        }
    }
    assert_eq!(seen, vec!["r6", "r7", "r8", "r9"]);
}

#[tokio::test]
async fn slow_consumer_does_not_block_producer() {
    let bus = EventBus::new(2);
    // Subscribe but never read. With capacity=2, further publishes
    // must still return promptly rather than blocking the producer.
    let _stuck = bus.subscribe();

    let start = std::time::Instant::now();
    for i in 0..1000 {
        bus.publish(SystemEvent::RepoChanged {
            repo: format!("r{i}"),
            new_head: "a".repeat(40),
        });
    }
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "producer blocked by stuck subscriber"
    );
}

#[tokio::test]
async fn try_recv_returns_none_when_empty() {
    let bus = EventBus::new(4);
    let mut rx = bus.subscribe();
    assert!(rx.try_recv().is_none());

    bus.publish(SystemEvent::WebhookTriggered {
        repo: "r1".into(),
        source: WebhookSource::GitHub,
    });
    let got = rx.try_recv().unwrap();
    assert!(matches!(
        got.event,
        SystemEvent::WebhookTriggered {
            source: WebhookSource::GitHub,
            ..
        }
    ));
    assert!(rx.try_recv().is_none());
}

#[tokio::test]
async fn late_subscriber_only_sees_future_events() {
    let bus = EventBus::new(8);
    bus.publish(repo_changed("past"));
    let mut rx = bus.subscribe();
    assert!(rx.try_recv().is_none());

    bus.publish(repo_changed("future"));
    let evt = rx.recv().await.unwrap();
    assert_eq!(evt.event, repo_changed("future"));
}
