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

    /// CI budget check: fail if `peak_rss_bytes / apps` exceeds
    /// this many KB. Defaults to disabled (0). The current
    /// production-realistic budget is tracked in y0v.3.1; until
    /// that lands, set this to ~250 KB to guard against
    /// regressions without falsely failing.
    #[arg(long, default_value_t = 0)]
    max_rss_kb_per_app: u64,

    /// CI budget check for webhook-burst scenarios: fail if the
    /// webhook→sync p95 latency exceeds this many ms. Defaults to
    /// disabled (0). The y0v.4 acceptance target is 5000 ms.
    #[arg(long, default_value_t = 0)]
    max_webhook_p95_ms: u64,
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

    if args.max_rss_kb_per_app > 0 {
        let kb_per_app = report.memory.peak_rss_bytes / 1024 / report.config.apps as u64;
        if kb_per_app > args.max_rss_kb_per_app {
            eprintln!(
                "BUDGET FAIL: {} KB/app exceeds limit of {} KB/app",
                kb_per_app, args.max_rss_kb_per_app
            );
            std::process::exit(2);
        }
    }

    if args.max_webhook_p95_ms > 0 {
        match &report.webhook_latency_ms {
            Some(s) if s.p95 > args.max_webhook_p95_ms => {
                eprintln!(
                    "BUDGET FAIL: webhook p95 {} ms exceeds limit of {} ms",
                    s.p95, args.max_webhook_p95_ms
                );
                std::process::exit(2);
            }
            None => {
                eprintln!(
                    "BUDGET FAIL: --max-webhook-p95-ms set but scenario produced no webhook latency stats"
                );
                std::process::exit(2);
            }
            _ => {}
        }
    }

    if args.summary {
        if let Some(w) = &report.webhook_latency_ms {
            eprintln!(
                "scenario={} bursts={} completed={} failed={} elapsed={:.2}s webhook_p50={}ms p95={}ms p99={}ms peak_rss={}MB",
                report.scenario,
                report.reconciles.sweeps,
                report.reconciles.completed,
                report.reconciles.failed,
                report.elapsed_seconds,
                w.p50,
                w.p95,
                w.p99,
                report.memory.peak_rss_bytes / 1_048_576,
            );
        } else {
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
    }

    Ok(())
}
