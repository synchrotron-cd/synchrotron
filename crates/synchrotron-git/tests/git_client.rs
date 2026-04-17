//! Integration tests for the synchrotron-git foundation slice.
//!
//! These run against an upstream git repo created in a tempdir, addressed
//! by local path. No network or daemon required.

use std::fs;
use std::path::{Path, PathBuf};

use git2::{IndexAddOption, Repository, Signature};
use synchrotron_git::{Credentials, GitClient, Repo, Workspace};
use synchrotron_types::RepoUrl;
use tempfile::TempDir;

/// Build an "upstream" repo on disk to act as a clone source.
struct Upstream {
    _dir: TempDir,
    path: PathBuf,
    repo: Repository,
}

impl Upstream {
    fn init() -> Self {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        let repo = Repository::init(&path).unwrap();
        // Force the default branch name so tests don't depend on the host's
        // git init.defaultBranch setting.
        repo.set_head("refs/heads/main").unwrap();
        Self {
            _dir: dir,
            path,
            repo,
        }
    }

    /// Write `files` (path → contents), stage them, and create a commit on
    /// the current branch. Returns the new HEAD oid as a hex string.
    fn commit(&self, message: &str, files: &[(&str, &str)]) -> String {
        for (rel, contents) in files {
            let abs = self.path.join(rel);
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&abs, contents).unwrap();
        }

        let mut index = self.repo.index().unwrap();
        index.add_all(["*"], IndexAddOption::DEFAULT, None).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = self.repo.find_tree(tree_oid).unwrap();

        let sig = Signature::now("Test", "test@example.com").unwrap();
        let parents: Vec<git2::Commit> = match self.repo.head() {
            Ok(head) => vec![head.peel_to_commit().unwrap()],
            Err(_) => vec![],
        };
        let parent_refs: Vec<&git2::Commit> = parents.iter().collect();

        let oid = self
            .repo
            .commit(Some("HEAD"), &sig, &sig, message, &tree, &parent_refs)
            .unwrap();
        oid.to_string()
    }

    fn url(&self) -> RepoUrl {
        RepoUrl(self.path.to_string_lossy().into_owned())
    }
}

fn workspace(dir: &Path) -> Workspace {
    Workspace::new(dir.join("workspace"))
}

#[test]
fn ensure_cloned_creates_bare_cache() {
    let upstream = Upstream::init();
    let head = upstream.commit("initial", &[("README.md", "hi\n")]);

    let work = TempDir::new().unwrap();
    let client = GitClient::new(workspace(work.path()));
    let repo = Repo::new(upstream.url(), "main", Credentials::None).with_depth(None);

    client.ensure_cloned(&repo).unwrap();

    let bare = client.bare_path(&repo).unwrap();
    assert!(bare.join("HEAD").exists(), "bare cache HEAD should exist");
    assert_eq!(client.current_head(&repo).unwrap().0, head);

    // Idempotent.
    client.ensure_cloned(&repo).unwrap();
}

#[test]
fn fetch_reports_no_change_when_upstream_unchanged() {
    let upstream = Upstream::init();
    let head = upstream.commit("initial", &[("a.txt", "1")]);

    let work = TempDir::new().unwrap();
    let client = GitClient::new(workspace(work.path()));
    let repo = Repo::new(upstream.url(), "main", Credentials::None).with_depth(None);

    let first = client.fetch(&repo).unwrap();
    assert_eq!(first.current_head.0, head);
    // First fetch after clone: previous == current (clone already populated
    // the ref), so nothing changed.
    assert!(!first.changed);

    let second = client.fetch(&repo).unwrap();
    assert_eq!(second.current_head.0, head);
    assert!(!second.changed);
    assert_eq!(second.previous_head.unwrap().0, head);
}

#[test]
fn fetch_detects_new_upstream_commit() {
    let upstream = Upstream::init();
    let first_head = upstream.commit("initial", &[("a.txt", "1")]);

    let work = TempDir::new().unwrap();
    let client = GitClient::new(workspace(work.path()));
    let repo = Repo::new(upstream.url(), "main", Credentials::None).with_depth(None);
    client.ensure_cloned(&repo).unwrap();

    let second_head = upstream.commit("update", &[("a.txt", "2")]);
    assert_ne!(first_head, second_head);

    let result = client.fetch(&repo).unwrap();
    assert!(result.changed);
    assert_eq!(result.previous_head.unwrap().0, first_head);
    assert_eq!(result.current_head.0, second_head);
}

#[test]
fn materialize_writes_tree_to_target_dir() {
    let upstream = Upstream::init();
    let head = upstream.commit(
        "initial",
        &[
            ("README.md", "hello\n"),
            ("deploy/app.yaml", "kind: Deployment\n"),
            ("deploy/svc.yaml", "kind: Service\n"),
        ],
    );

    let work = TempDir::new().unwrap();
    let client = GitClient::new(workspace(work.path()));
    let repo = Repo::new(upstream.url(), "main", Credentials::None).with_depth(None);
    client.ensure_cloned(&repo).unwrap();

    let target = work.path().join("render");
    client
        .materialize(&repo, &synchrotron_git::Sha(head), &target)
        .unwrap();

    assert_eq!(
        fs::read_to_string(target.join("README.md")).unwrap(),
        "hello\n"
    );
    assert_eq!(
        fs::read_to_string(target.join("deploy/app.yaml")).unwrap(),
        "kind: Deployment\n"
    );
    assert_eq!(
        fs::read_to_string(target.join("deploy/svc.yaml")).unwrap(),
        "kind: Service\n"
    );
}

#[test]
fn materialize_unknown_commit_errors() {
    let upstream = Upstream::init();
    upstream.commit("initial", &[("a.txt", "1")]);

    let work = TempDir::new().unwrap();
    let client = GitClient::new(workspace(work.path()));
    let repo = Repo::new(upstream.url(), "main", Credentials::None).with_depth(None);
    client.ensure_cloned(&repo).unwrap();

    let bogus = synchrotron_git::Sha("0".repeat(40));
    let target = work.path().join("render");
    assert!(client.materialize(&repo, &bogus, &target).is_err());
}
