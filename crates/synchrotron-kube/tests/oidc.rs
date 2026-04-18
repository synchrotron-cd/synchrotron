//! Tests for the OIDC token cache. The HTTP exchange is stubbed via
//! a test [`Refresher`] so cache state is fully deterministic.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use synchrotron_kube::{KubeError, OidcConfig, OidcToken, OidcTokenCache, Refresher};

fn cfg() -> OidcConfig {
    OidcConfig {
        issuer_url: "https://issuer.example".into(),
        client_id: "synchrotron".into(),
        client_secret: Some("shh".into()),
    }
}

fn seed(now: SystemTime, ttl: Duration) -> OidcToken {
    OidcToken {
        id_token: "id-0".into(),
        refresh_token: "rt-0".into(),
        expires_at: now + ttl,
    }
}

fn counting_refresher(counter: Arc<AtomicUsize>) -> Refresher {
    Arc::new(move |_cfg, prev_rt| {
        let counter = counter.clone();
        Box::pin(async move {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            // Issuer rotates the refresh token; chain it off the prior
            // value so we can prove it was actually threaded through.
            Ok(OidcToken {
                id_token: format!("id-{n}"),
                refresh_token: format!("{prev_rt}->rt-{n}"),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
            })
        })
    })
}

#[tokio::test]
async fn returns_seeded_token_when_fresh() {
    let now = SystemTime::now();
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = OidcTokenCache::new(
        cfg(),
        seed(now, Duration::from_secs(600)),
        counting_refresher(calls.clone()),
        Duration::from_secs(60),
    );

    let tok = cache.get_at(now).await.unwrap();
    assert_eq!(tok.id_token, "id-0");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn refreshes_within_margin_of_expiry() {
    let now = SystemTime::now();
    // Token expires in 30s, margin is 60s → already considered stale.
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = OidcTokenCache::new(
        cfg(),
        seed(now, Duration::from_secs(30)),
        counting_refresher(calls.clone()),
        Duration::from_secs(60),
    );

    let tok = cache.get_at(now).await.unwrap();
    assert_eq!(tok.id_token, "id-1");
    assert_eq!(tok.refresh_token, "rt-0->rt-1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rotated_refresh_token_persists_across_calls() {
    let now = SystemTime::now();
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = OidcTokenCache::new(
        cfg(),
        seed(now, Duration::from_secs(0)),
        counting_refresher(calls.clone()),
        Duration::from_secs(0),
    );

    let _ = cache.get_at(now).await.unwrap();
    // Force another stale read by passing a far-future `now`.
    let later = now + Duration::from_secs(7200);
    let tok = cache.get_at(later).await.unwrap();
    // Second call must have used the rotated token from call 1.
    assert!(
        tok.refresh_token.starts_with("rt-0->rt-1->rt-2"),
        "expected chained rotation, got {}",
        tok.refresh_token
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn empty_refresh_token_is_rejected() {
    let now = SystemTime::now();
    let bad: Refresher = Arc::new(|_cfg, _rt| {
        Box::pin(async move {
            Ok(OidcToken {
                id_token: "id".into(),
                refresh_token: String::new(),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
            })
        })
    });
    let cache = OidcTokenCache::new(
        cfg(),
        seed(now, Duration::from_secs(0)),
        bad,
        Duration::from_secs(0),
    );
    let err = cache.get_at(now).await.unwrap_err();
    assert!(matches!(err, KubeError::OidcRefresh(_)), "got: {err:?}");
}
