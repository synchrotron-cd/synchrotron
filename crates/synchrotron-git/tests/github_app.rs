//! Tests for the GitHub App auth flow: JWT minting, token cache
//! refresh-before-expiry, and a stub fetcher to drive the cache
//! deterministically without hitting GitHub.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use synchrotron_git::github_app::{
    mint_jwt, GitHubAppConfig, InstallationToken, TokenCache, TokenFetcher,
};
use tokio::sync::Mutex;

// 2048-bit RSA test key, generated with `openssl genrsa 2048`. Used
// only by these tests; never trusted by anything.
const TEST_PRIVATE_KEY: &str = include_str!("data/test_app_key.pem");
const TEST_PUBLIC_KEY: &str = include_str!("data/test_app_key.pub.pem");

fn test_cfg() -> GitHubAppConfig {
    GitHubAppConfig::new(123, 456, TEST_PRIVATE_KEY.as_bytes().to_vec())
        .with_api_base("https://example.test")
}

#[derive(Deserialize)]
struct Claims {
    iat: i64,
    exp: i64,
    iss: String,
}

#[test]
fn mint_jwt_signs_with_correct_claims() {
    let cfg = test_cfg();
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let token = mint_jwt(&cfg, now).unwrap();

    let key = DecodingKey::from_rsa_pem(TEST_PUBLIC_KEY.as_bytes()).unwrap();
    let mut validation = Validation::new(Algorithm::RS256);
    validation.validate_exp = false;
    validation.required_spec_claims.clear();
    let decoded = jsonwebtoken::decode::<Claims>(&token, &key, &validation).unwrap();

    assert_eq!(decoded.claims.iss, "123");
    assert_eq!(decoded.claims.iat, 1_700_000_000 - 60);
    assert_eq!(decoded.claims.exp, 1_700_000_000 + 9 * 60);
}

#[test]
fn token_url_is_well_formed() {
    let cfg = test_cfg();
    assert_eq!(
        cfg.token_url(),
        "https://example.test/app/installations/456/access_tokens"
    );
}

#[test]
fn installation_token_freshness_respects_margin() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let tok = InstallationToken {
        token: "t".into(),
        expires_at: now + Duration::from_secs(120),
    };
    // 60s margin: still fresh because 1000 < (1120 - 60 = 1060).
    assert!(tok.is_fresh(now, Duration::from_secs(60)));
    // 90s margin: stale because 1000 >= (1120 - 90 = 1030)? No, 1000 < 1030 so still fresh.
    assert!(tok.is_fresh(now, Duration::from_secs(90)));
    // 120s margin: stale because 1000 >= (1120 - 120 = 1000) — boundary.
    assert!(!tok.is_fresh(now, Duration::from_secs(120)));
    // 180s margin: stale.
    assert!(!tok.is_fresh(now, Duration::from_secs(180)));
}

fn stub_fetcher(
    sequence: Arc<Mutex<Vec<InstallationToken>>>,
    calls: Arc<Mutex<u32>>,
) -> TokenFetcher {
    Arc::new(move |_jwt, _url| {
        let sequence = sequence.clone();
        let calls = calls.clone();
        Box::pin(async move {
            *calls.lock().await += 1;
            let mut g = sequence.lock().await;
            if g.is_empty() {
                Err(synchrotron_git::GitError::InvalidState(
                    "test: stub exhausted".into(),
                ))
            } else {
                Ok(g.remove(0))
            }
        })
    })
}

#[tokio::test]
async fn cache_returns_cached_token_until_margin() {
    let cfg = test_cfg();
    let now = UNIX_EPOCH + Duration::from_secs(2_000);
    let tok = InstallationToken {
        token: "first".into(),
        expires_at: now + Duration::from_secs(3600),
    };
    let sequence = Arc::new(Mutex::new(vec![tok.clone()]));
    let calls = Arc::new(Mutex::new(0u32));
    let cache = TokenCache::new(
        cfg,
        stub_fetcher(sequence, calls.clone()),
        Duration::from_secs(60),
    );

    let a = cache.get_at(now).await.unwrap();
    let b = cache.get_at(now + Duration::from_secs(1000)).await.unwrap();
    assert_eq!(a.token, "first");
    assert_eq!(b.token, "first");
    assert_eq!(
        *calls.lock().await,
        1,
        "fetcher should have been called once"
    );
}

#[tokio::test]
async fn cache_refreshes_when_within_margin() {
    let cfg = test_cfg();
    let now = UNIX_EPOCH + Duration::from_secs(2_000);
    let first = InstallationToken {
        token: "first".into(),
        expires_at: now + Duration::from_secs(120),
    };
    let second = InstallationToken {
        token: "second".into(),
        expires_at: now + Duration::from_secs(7200),
    };
    let sequence = Arc::new(Mutex::new(vec![first, second]));
    let calls = Arc::new(Mutex::new(0u32));
    let cache = TokenCache::new(
        cfg,
        stub_fetcher(sequence, calls.clone()),
        Duration::from_secs(60),
    );

    let a = cache.get_at(now).await.unwrap();
    assert_eq!(a.token, "first");
    // Advance into the refresh margin (within 60s of expiry).
    let b = cache.get_at(now + Duration::from_secs(80)).await.unwrap();
    assert_eq!(b.token, "second");
    assert_eq!(*calls.lock().await, 2);
}
