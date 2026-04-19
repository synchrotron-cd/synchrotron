//! Tests for the per-key event coalescer.

use std::time::{Duration, Instant};

use synchrotron_core::Coalescer;

#[test]
fn first_event_is_admitted() {
    let c: Coalescer<&'static str> = Coalescer::new(Duration::from_secs(2));
    assert!(c.admit("alpha"));
}

#[test]
fn burst_for_same_key_collapses_to_one() {
    let c: Coalescer<&'static str> = Coalescer::new(Duration::from_secs(2));
    let t0 = Instant::now();
    assert!(c.admit_at("alpha", t0));
    for i in 1..100 {
        let ti = t0 + Duration::from_millis(i * 5); // well under 2s window
        assert!(
            !c.admit_at("alpha", ti),
            "iteration {i} should be suppressed"
        );
    }
}

#[test]
fn different_keys_are_independent() {
    let c: Coalescer<&'static str> = Coalescer::new(Duration::from_secs(2));
    let t0 = Instant::now();
    assert!(c.admit_at("alpha", t0));
    assert!(c.admit_at("beta", t0));
    assert!(c.admit_at("gamma", t0));
    // But within the window each individual key is still throttled.
    assert!(!c.admit_at("alpha", t0 + Duration::from_millis(10)));
}

#[test]
fn admits_again_after_window_elapses() {
    let c: Coalescer<&'static str> = Coalescer::new(Duration::from_millis(100));
    let t0 = Instant::now();
    assert!(c.admit_at("alpha", t0));
    assert!(!c.admit_at("alpha", t0 + Duration::from_millis(50)));
    assert!(c.admit_at("alpha", t0 + Duration::from_millis(150)));
}

#[test]
fn sweep_reclaims_expired_keys() {
    let c: Coalescer<u32> = Coalescer::new(Duration::from_millis(100));
    let t0 = Instant::now();
    for i in 0..10 {
        assert!(c.admit_at(i, t0));
    }
    assert_eq!(c.tracked_keys(), 10);

    let reclaimed = c.sweep(t0 + Duration::from_millis(200));
    assert_eq!(reclaimed, 10);
    assert_eq!(c.tracked_keys(), 0);
}

#[test]
fn coalesces_tuple_keys_for_app_cluster_pairs() {
    // Exercise the intended app/cluster-keyed use case.
    let c: Coalescer<(String, String)> = Coalescer::new(Duration::from_secs(1));
    let t0 = Instant::now();
    assert!(c.admit_at(("app-1".into(), "prod".into()), t0));
    assert!(c.admit_at(("app-1".into(), "stage".into()), t0));
    assert!(!c.admit_at(
        ("app-1".into(), "prod".into()),
        t0 + Duration::from_millis(10)
    ));
}
