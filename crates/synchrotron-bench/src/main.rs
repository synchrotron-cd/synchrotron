//! `synchrotron-bench` — run a scenario, write a JSON report.

use std::path::PathBuf;

use clap::Parser;
use synchrotron_bench::{run_scenario, ScenarioConfig};

#[derive(Parser, Debug)]
#[command(version, about = "Synchrotron load-scenario runner")]
struct Args {
    /// Path to scenario YAML.
    #[arg(long)]
    scenario: PathBuf,

    /// Where to write the JSON report. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Also print a one-line summary to stderr.
    #[arg(long, default_value_t = true)]
    summary: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let cfg = ScenarioConfig::load(&args.scenario)?;
    let report = run_scenario(cfg).await?;

    let json = serde_json::to_string_pretty(&report)?;
    match &args.out {
        Some(p) => std::fs::write(p, &json)?,
        None => println!("{json}"),
    }

    if args.summary {
        eprintln!(
            "scenario={} sweeps={} completed={} failed={} elapsed={:.2}s p50={}us p95={}us p99={}us peak_rss={}MB tput={:.1}/s",
            report.scenario,
            report.reconciles.sweeps,
            report.reconciles.completed,
            report.reconciles.failed,
            report.elapsed_seconds,
            report.latency_us.p50,
            report.latency_us.p95,
            report.latency_us.p99,
            report.memory.peak_rss_bytes / 1_048_576,
            report.throughput_per_second,
        );
    }

    Ok(())
}
