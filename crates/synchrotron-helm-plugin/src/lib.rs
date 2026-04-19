//! Core logic for the Helm local plugin, factored out of `main.rs`
//! so the argument-building path is unit-testable without spawning
//! an actual helm process.
//!
//! The plugin accepts [`RenderParams`] in the JSON-RPC `render`
//! request and shells out to `helm template` (plus optional
//! `helm dependency update`). Stderr from helm is captured and
//! returned in the JSON-RPC error body on failure so operators can
//! debug chart issues from synchrotron logs.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::Deserialize;
use thiserror::Error;

pub const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Parameters the Application spec passes to the Helm plugin.
///
/// All paths are resolved relative to the `source_path` the host
/// hands us (the post-git-sync tree). That keeps the plugin side
/// free of assumptions about where Synchrotron stores checkouts.
#[derive(Debug, Clone, Deserialize)]
pub struct RenderParams {
    pub release_name: String,

    /// Subdirectory within `source_path` containing `Chart.yaml`.
    /// Defaults to the source root.
    #[serde(default)]
    pub chart_path: Option<String>,

    /// `-n` / `--namespace` passed through to helm. None means
    /// helm's default ("default") applies.
    #[serde(default)]
    pub namespace: Option<String>,

    /// One or more `-f` values files, resolved against the chart
    /// directory.
    #[serde(default)]
    pub values_files: Vec<String>,

    /// Flat `key=value` set overrides. Ordering is deterministic
    /// (BTreeMap) so cache keys don't flap.
    #[serde(default)]
    pub set_values: BTreeMap<String, String>,

    /// Run `helm dependency update` before `helm template`. Always
    /// done when `Chart.lock` is missing; this flag forces it
    /// unconditionally for teams that don't commit the lock.
    #[serde(default)]
    pub dependency_update: bool,
}

#[derive(Debug, Error)]
pub enum HelmError {
    #[error("helm not found: {source}")]
    BinaryNotFound {
        #[source]
        source: std::io::Error,
    },
    #[error("helm i/o error: {source}")]
    Io {
        #[source]
        source: std::io::Error,
    },
    #[error("helm {phase} exited {code}:\n{stderr}")]
    NonZeroExit {
        phase: &'static str,
        code: i32,
        stderr: String,
    },
    #[error("chart directory not found: {path}")]
    ChartMissing { path: PathBuf },
}

/// Resolves the helm binary, locations, and runs the render.
pub struct HelmRunner {
    /// Path or name of the helm binary. Defaults to `helm` on PATH;
    /// override via the `HELM_BINARY` env var when spawning.
    pub binary: OsString,
}

impl Default for HelmRunner {
    fn default() -> Self {
        let binary = std::env::var_os("HELM_BINARY").unwrap_or_else(|| OsString::from("helm"));
        Self { binary }
    }
}

impl HelmRunner {
    /// Resolve the chart directory for the given source + params.
    /// Errors if the directory doesn't contain a Chart.yaml — a
    /// clearer message than whatever helm would emit.
    pub fn resolve_chart_dir(
        &self,
        source_path: &Path,
        params: &RenderParams,
    ) -> Result<PathBuf, HelmError> {
        let dir = match &params.chart_path {
            Some(rel) => source_path.join(rel),
            None => source_path.to_path_buf(),
        };
        if !dir.join("Chart.yaml").is_file() {
            return Err(HelmError::ChartMissing { path: dir });
        }
        Ok(dir)
    }

    /// Execute render end-to-end, returning the YAML document emitted
    /// by `helm template` as a single multi-doc string. Runs
    /// `helm dependency update` first when requested or when
    /// `Chart.lock` is missing alongside a populated `Chart.yaml`
    /// dependencies list.
    pub fn render(&self, source_path: &Path, params: &RenderParams) -> Result<String, HelmError> {
        let chart_dir = self.resolve_chart_dir(source_path, params)?;

        if params.dependency_update || needs_dep_update(&chart_dir) {
            let args = build_dep_update_args(&chart_dir);
            run(&self.binary, &args, "dependency update")?;
        }

        let args = build_template_args(&chart_dir, params);
        let output = run(&self.binary, &args, "template")?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Build the argv for `helm template`. Exposed for unit tests.
pub fn build_template_args(chart_dir: &Path, params: &RenderParams) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        OsString::from("template"),
        OsString::from(&params.release_name),
        chart_dir.as_os_str().to_os_string(),
    ];
    if let Some(ns) = &params.namespace {
        args.push(OsString::from("-n"));
        args.push(OsString::from(ns));
    }
    for f in &params.values_files {
        args.push(OsString::from("-f"));
        args.push(OsString::from(chart_dir.join(f)));
    }
    // BTreeMap iteration is sorted, giving deterministic arg order.
    for (k, v) in &params.set_values {
        args.push(OsString::from("--set"));
        args.push(OsString::from(format!("{k}={v}")));
    }
    args
}

pub fn build_dep_update_args(chart_dir: &Path) -> Vec<OsString> {
    vec![
        OsString::from("dependency"),
        OsString::from("update"),
        chart_dir.as_os_str().to_os_string(),
    ]
}

/// Returns true when the chart declares dependencies in Chart.yaml
/// but no Chart.lock is present. Conservative: any read error falls
/// back to "no, don't run dep update" — `helm template` will fail
/// with its own clearer diagnostic.
pub fn needs_dep_update(chart_dir: &Path) -> bool {
    let chart_yaml = chart_dir.join("Chart.yaml");
    let chart_lock = chart_dir.join("Chart.lock");
    if chart_lock.is_file() {
        return false;
    }
    let Ok(text) = std::fs::read_to_string(&chart_yaml) else {
        return false;
    };
    // Cheap substring check — avoids pulling in a YAML dep just for
    // this probe. A line starting with `dependencies:` at the root
    // is the standard form.
    text.lines().any(|l| {
        let trimmed = l.trim_end();
        trimmed == "dependencies:" || trimmed.starts_with("dependencies: ")
    })
}

fn run(binary: &OsString, args: &[OsString], phase: &'static str) -> Result<Output, HelmError> {
    let output = Command::new(binary).args(args).output().map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            HelmError::BinaryNotFound { source }
        } else {
            HelmError::Io { source }
        }
    })?;
    if !output.status.success() {
        return Err(HelmError::NonZeroExit {
            phase,
            code: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn params(release: &str) -> RenderParams {
        RenderParams {
            release_name: release.into(),
            chart_path: None,
            namespace: None,
            values_files: vec![],
            set_values: BTreeMap::new(),
            dependency_update: false,
        }
    }

    #[test]
    fn template_args_minimal() {
        let chart = PathBuf::from("/tmp/chart");
        let args = build_template_args(&chart, &params("app"));
        assert_eq!(
            args,
            vec![
                OsString::from("template"),
                OsString::from("app"),
                OsString::from("/tmp/chart"),
            ]
        );
    }

    #[test]
    fn template_args_include_namespace() {
        let chart = PathBuf::from("/tmp/chart");
        let mut p = params("app");
        p.namespace = Some("web".into());
        let args = build_template_args(&chart, &p);
        assert!(args.contains(&OsString::from("-n")));
        assert!(args.contains(&OsString::from("web")));
    }

    #[test]
    fn values_files_resolved_against_chart_dir() {
        let chart = PathBuf::from("/src/mychart");
        let mut p = params("app");
        p.values_files = vec!["values-prod.yaml".into()];
        let args = build_template_args(&chart, &p);
        let joined = chart.join("values-prod.yaml");
        assert!(
            args.iter().any(|a| a == joined.as_os_str()),
            "missing resolved values path in {args:?}"
        );
    }

    #[test]
    fn set_values_are_deterministic() {
        let chart = PathBuf::from("/tmp/chart");
        let mut p = params("app");
        p.set_values.insert("z".into(), "3".into());
        p.set_values.insert("a".into(), "1".into());
        p.set_values.insert("m".into(), "2".into());
        let args = build_template_args(&chart, &p);
        // Find the positions of each --set arg.
        let sets: Vec<&OsString> = args
            .iter()
            .zip(args.iter().skip(1))
            .filter_map(|(a, b)| (a == &OsString::from("--set")).then_some(b))
            .collect();
        assert_eq!(
            sets,
            vec![
                &OsString::from("a=1"),
                &OsString::from("m=2"),
                &OsString::from("z=3")
            ]
        );
    }

    #[test]
    fn resolve_chart_dir_errors_when_missing_chart_yaml() {
        let tmp = tempdir();
        let runner = HelmRunner::default();
        let err = runner
            .resolve_chart_dir(tmp.path(), &params("app"))
            .unwrap_err();
        assert!(matches!(err, HelmError::ChartMissing { .. }));
    }

    #[test]
    fn needs_dep_update_detects_dependencies_without_lock() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("Chart.yaml"),
            "apiVersion: v2\nname: x\nversion: 0.1.0\ndependencies:\n  - name: y\n",
        )
        .unwrap();
        assert!(needs_dep_update(tmp.path()));
    }

    #[test]
    fn needs_dep_update_skips_when_lock_present() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("Chart.yaml"),
            "apiVersion: v2\nname: x\nversion: 0.1.0\ndependencies:\n  - name: y\n",
        )
        .unwrap();
        fs::File::create(tmp.path().join("Chart.lock"))
            .unwrap()
            .write_all(b"")
            .unwrap();
        assert!(!needs_dep_update(tmp.path()));
    }

    #[test]
    fn needs_dep_update_false_for_chart_without_deps() {
        let tmp = tempdir();
        fs::write(
            tmp.path().join("Chart.yaml"),
            "apiVersion: v2\nname: x\nversion: 0.1.0\n",
        )
        .unwrap();
        assert!(!needs_dep_update(tmp.path()));
    }

    // Avoid a tempfile dep just for these unit tests — construct a
    // scratch dir under std::env::temp_dir and clean up on drop.
    struct TempDir(PathBuf);
    impl TempDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        let mut p = std::env::temp_dir();
        p.push(format!("synchrotron-helm-test-{}", unique()));
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn unique() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
