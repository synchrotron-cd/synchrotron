//! Built-in raw-YAML manifest source.
//!
//! Walks a directory (post-git-sync) recursively, reads every
//! `.yaml`/`.yml` file, splits multi-document streams, and produces
//! [`Manifest`]s. No templating, no value substitution — the bytes
//! on disk are the manifests.
//!
//! Output order is deterministic: files are sorted lexicographically
//! and within each file documents appear in source order. This lets
//! the manifest cache key on content rather than iteration order.

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tracing::debug;
use walkdir::WalkDir;

use crate::manifest::{parse_stream, Manifest, ManifestParseError};

#[derive(Debug, Error)]
pub enum RawLoadError {
    #[error("i/o error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Parse(#[from] ManifestParseError),
}

/// Load all manifests from a directory tree.
///
/// Non-YAML files are skipped silently. Empty/null YAML documents are
/// skipped. A document that parses but lacks `apiVersion`, `kind`,
/// or `metadata.name` is an error — silently dropping half-formed
/// manifests would hide bugs.
pub fn load_dir(root: &Path) -> Result<Vec<Manifest>, RawLoadError> {
    // Fail loudly when the directory doesn't exist — otherwise a
    // typo in `app.source.path` silently renders an empty manifest
    // set, the planner sees no drift, and the app reports Synced
    // with zero resources. An existing-but-empty directory is still
    // a valid no-op (operator's choice). See synchrotron-cd-3wa.
    if !root.is_dir() {
        return Err(RawLoadError::Io {
            path: root.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("source path `{}` is not a directory", root.display()),
            ),
        });
    }

    let mut files: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|p| is_yaml(p))
        .collect();
    files.sort();

    let mut out = Vec::new();
    for path in files {
        let text = fs::read_to_string(&path).map_err(|source| RawLoadError::Io {
            path: path.clone(),
            source,
        })?;
        let label = path.display().to_string();
        out.extend(parse_stream(&label, &text)?);
    }
    debug!(root = %root.display(), count = out.len(), "raw yaml source loaded");
    Ok(out)
}

fn is_yaml(p: &Path) -> bool {
    matches!(
        p.extension().and_then(|e| e.to_str()),
        Some("yaml") | Some("yml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Gvk;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_file(dir: &Path, rel: &str, contents: &str) -> PathBuf {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn empty_dir_returns_no_manifests() {
        let dir = TempDir::new().unwrap();
        let out = load_dir(dir.path()).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn missing_dir_is_an_error_not_an_empty_manifest_set() {
        // 3wa: a typo in app.source.path used to silently render
        // empty and report Synced. Now it surfaces as NotFound.
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("does-not-exist");
        match load_dir(&missing) {
            Err(RawLoadError::Io { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn file_path_instead_of_dir_is_also_an_error() {
        let dir = TempDir::new().unwrap();
        let file = write_file(
            dir.path(),
            "cm.yaml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: y}\n",
        );
        match load_dir(&file) {
            Err(RawLoadError::Io { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected NotFound (path-is-file), got {other:?}"),
        }
    }

    #[test]
    fn single_file_single_doc() {
        let dir = TempDir::new().unwrap();
        write_file(
            dir.path(),
            "cm.yaml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-config\n  namespace: default\n",
        );
        let out = load_dir(dir.path()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].gvk, Gvk::parse("v1", "ConfigMap"));
        assert_eq!(out[0].name, "app-config");
        assert_eq!(out[0].namespace.as_deref(), Some("default"));
    }

    #[test]
    fn multi_doc_and_trailing_separator() {
        let dir = TempDir::new().unwrap();
        write_file(
            dir.path(),
            "multi.yaml",
            "apiVersion: v1\n\
             kind: ConfigMap\n\
             metadata:\n  name: a\n\
             ---\n\
             apiVersion: apps/v1\n\
             kind: Deployment\n\
             metadata:\n  name: b\n  namespace: web\n\
             ---\n",
        );
        let out = load_dir(dir.path()).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "a");
        assert_eq!(out[0].gvk.group, "");
        assert_eq!(out[1].name, "b");
        assert_eq!(out[1].gvk.group, "apps");
        assert_eq!(out[1].gvk.version, "v1");
    }

    #[test]
    fn recursive_walk_and_deterministic_order() {
        let dir = TempDir::new().unwrap();
        write_file(
            dir.path(),
            "b/second.yaml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: second\n",
        );
        write_file(
            dir.path(),
            "a/first.yml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: first\n",
        );
        write_file(dir.path(), "ignored.txt", "not yaml");
        let out = load_dir(dir.path()).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "first");
        assert_eq!(out[1].name, "second");
    }

    #[test]
    fn invalid_yaml_is_parse_error() {
        let dir = TempDir::new().unwrap();
        let path = write_file(dir.path(), "bad.yaml", "apiVersion: v1\nkind: : :\n");
        let err = load_dir(dir.path()).unwrap_err();
        match err {
            RawLoadError::Parse(ManifestParseError::Yaml { source_label, .. }) => {
                assert_eq!(source_label, path.display().to_string());
            }
            other => panic!("expected Parse(Yaml), got {other:?}"),
        }
    }

    #[test]
    fn missing_required_field_errors() {
        let dir = TempDir::new().unwrap();
        write_file(
            dir.path(),
            "nameless.yaml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n",
        );
        let err = load_dir(dir.path()).unwrap_err();
        match err {
            RawLoadError::Parse(ManifestParseError::MissingField { field, .. }) => {
                assert_eq!(field, "metadata.name");
            }
            other => panic!("expected MissingField, got {other:?}"),
        }
    }

    #[test]
    fn null_documents_are_skipped() {
        let dir = TempDir::new().unwrap();
        write_file(
            dir.path(),
            "null.yaml",
            "---\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: only\n",
        );
        let out = load_dir(dir.path()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "only");
    }
}
