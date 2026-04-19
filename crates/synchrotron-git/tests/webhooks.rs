//! Tests for webhook parsing + HMAC verification. Signatures are
//! computed against known-good payloads to pin the constant-time
//! comparison path end-to-end.

use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::Sha256;

use synchrotron_git::webhooks::{
    parse_bitbucket, parse_github, parse_gitlab, verify_bitbucket, verify_github, verify_gitlab,
    WebhookProvider,
};

fn gh_signature(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn bb_sha1_signature(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).unwrap();
    mac.update(body);
    format!("sha1={}", hex::encode(mac.finalize().into_bytes()))
}

const GH_PUSH_BODY: &str =
    r#"{"ref":"refs/heads/main","repository":{"clone_url":"https://github.com/acme/app.git"}}"#;

#[test]
fn github_valid_signature_verifies() {
    let secret = b"hunter2";
    let sig = gh_signature(secret, GH_PUSH_BODY.as_bytes());
    let headers = [
        ("X-Hub-Signature-256", sig.as_str()),
        ("X-GitHub-Event", "push"),
    ];
    verify_github(secret, &headers, GH_PUSH_BODY.as_bytes()).unwrap();
    let parsed = parse_github(&headers, GH_PUSH_BODY.as_bytes()).unwrap();
    assert_eq!(parsed.provider, WebhookProvider::GitHub);
    assert_eq!(parsed.repo_url.0, "https://github.com/acme/app.git");
    assert!(parsed.is_push());
}

#[test]
fn github_tampered_body_rejected() {
    let secret = b"hunter2";
    let sig = gh_signature(secret, GH_PUSH_BODY.as_bytes());
    let headers = [
        ("X-Hub-Signature-256", sig.as_str()),
        ("X-GitHub-Event", "push"),
    ];
    let tampered = GH_PUSH_BODY.replace("acme", "evil");
    let err = verify_github(secret, &headers, tampered.as_bytes()).unwrap_err();
    assert!(err.to_string().contains("signature mismatch"));
}

#[test]
fn github_missing_signature_header_rejected() {
    let secret = b"hunter2";
    let headers = [("X-GitHub-Event", "push")];
    let err = verify_github(secret, &headers, GH_PUSH_BODY.as_bytes()).unwrap_err();
    assert!(err.to_string().contains("missing"));
}

#[test]
fn github_legacy_sha1_prefix_rejected() {
    let secret = b"hunter2";
    // SHA-1 header shape but we don't accept it — deprecated by GitHub.
    let headers = [
        ("X-Hub-Signature-256", "sha1=deadbeef"),
        ("X-GitHub-Event", "push"),
    ];
    let err = verify_github(secret, &headers, GH_PUSH_BODY.as_bytes()).unwrap_err();
    assert!(err.to_string().contains("prefix"));
}

#[test]
fn github_non_push_event_not_filtered_by_verify() {
    // verify_github verifies regardless of event type; `is_push` is
    // the event filter.
    let secret = b"hunter2";
    let sig = gh_signature(secret, GH_PUSH_BODY.as_bytes());
    let headers = [
        ("X-Hub-Signature-256", sig.as_str()),
        ("X-GitHub-Event", "ping"),
    ];
    verify_github(secret, &headers, GH_PUSH_BODY.as_bytes()).unwrap();
    let parsed = parse_github(&headers, GH_PUSH_BODY.as_bytes()).unwrap();
    assert!(!parsed.is_push());
}

#[test]
fn gitlab_token_compare_ok_and_mismatch() {
    let secret = b"glsecret";
    let headers_ok = [
        ("X-Gitlab-Token", "glsecret"),
        ("X-Gitlab-Event", "Push Hook"),
    ];
    verify_gitlab(secret, &headers_ok).unwrap();

    let headers_bad = [("X-Gitlab-Token", "wrong"), ("X-Gitlab-Event", "Push Hook")];
    let err = verify_gitlab(secret, &headers_bad).unwrap_err();
    assert!(err.to_string().contains("mismatch"));
}

#[test]
fn gitlab_parses_repository_git_http_url() {
    let body =
        r#"{"object_kind":"push","repository":{"git_http_url":"https://gitlab.example/g/p.git"}}"#;
    let headers = [("X-Gitlab-Event", "Push Hook")];
    let parsed = parse_gitlab(&headers, body.as_bytes()).unwrap();
    assert_eq!(parsed.repo_url.0, "https://gitlab.example/g/p.git");
    assert!(parsed.is_push());
}

#[test]
fn gitlab_falls_back_to_project_git_http_url() {
    let body = r#"{"project":{"git_http_url":"https://gitlab.example/g/p.git"}}"#;
    let headers = [("X-Gitlab-Event", "Push Hook")];
    let parsed = parse_gitlab(&headers, body.as_bytes()).unwrap();
    assert_eq!(parsed.repo_url.0, "https://gitlab.example/g/p.git");
}

#[test]
fn bitbucket_server_sha256_verifies() {
    let secret = b"bbsecret";
    let body = br#"{"eventKey":"repo:refs_changed","repository":{"links":{"self":[{"href":"https://bb.example/scm/p/r.git"}]}}}"#;
    let sig = {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    };
    let headers = [
        ("X-Hub-Signature", sig.as_str()),
        ("X-Event-Key", "repo:refs_changed"),
    ];
    verify_bitbucket(secret, &headers, body).unwrap();
    let parsed = parse_bitbucket(&headers, body).unwrap();
    assert_eq!(parsed.repo_url.0, "https://bb.example/scm/p/r.git");
}

#[test]
fn bitbucket_sha1_fallback_verifies() {
    let secret = b"bbsecret";
    let body = br#"{"eventKey":"repo:push","repository":{"links":{"clone":[{"name":"https","href":"https://bb.example/x/y.git"}]}}}"#;
    let sig = bb_sha1_signature(secret, body);
    let headers = [
        ("X-Hub-Signature", sig.as_str()),
        ("X-Event-Key", "repo:push"),
    ];
    verify_bitbucket(secret, &headers, body).unwrap();
    let parsed = parse_bitbucket(&headers, body).unwrap();
    assert_eq!(parsed.provider, WebhookProvider::Bitbucket);
    assert_eq!(parsed.repo_url.0, "https://bb.example/x/y.git");
    assert!(parsed.is_push());
}

#[test]
fn bitbucket_missing_signature_rejected() {
    let secret = b"bbsecret";
    let body = br#"{}"#;
    let headers = [("X-Event-Key", "repo:push")];
    let err = verify_bitbucket(secret, &headers, body).unwrap_err();
    assert!(err.to_string().contains("missing"));
}
