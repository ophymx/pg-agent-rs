//! Acceptance suite driver. See testing/README.md.
//!
//! THE MODEL UNDER TEST IS LEASE-DRIVEN ROLES. Nodes boot with
//! `[raft] enabled = true, shadow = false` — the greenfield deployment
//! shape — and every failure/recovery scenario runs under the raft
//! consensus lease, with pgpool present strictly as the router it is
//! post-cutover. The pgpool-led promote path is deleted from the
//! codebase, not merely untested.
//!
//! Usage (via testing/acceptance.sh, which builds and execs this):
//!   testing/acceptance.sh              # build, up, run, down
//!   KEEP=1 testing/acceptance.sh       # leave the cluster running
//!   SKIP_BUILD=1 testing/acceptance.sh # reuse dist/.deb + image

mod audit;
mod checks;
mod cluster;
mod events;
mod load;
mod pg;
mod scenarios;

use std::process::ExitCode;
use std::time::Instant;

use checks::Ctx;
use events::EventLog;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("acceptance: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<bool> {
    // The wrapper script cd's to the repo root; verify rather than
    // guess, since everything below uses relative paths.
    anyhow::ensure!(
        std::path::Path::new(cluster::COMPOSE).exists(),
        "run from the repo root (testing/compose.yaml not found)"
    );
    let keep = std::env::var_os("KEEP").is_some();
    let skip_build = std::env::var_os("SKIP_BUILD").is_some();
    let t0 = Instant::now();

    // Which package this cell installs. The host has to decide before
    // the image exists (the package goes INTO the build context), so
    // unlike every other layout fact this one cannot come from
    // /etc/pg-agent-matrix/env — it comes from the environment
    // testing/matrix.sh sets, and init_facts() cross-checks that the
    // image that came out agrees.
    let family = std::env::var("PG_FAMILY").unwrap_or_else(|_| "debian".to_string());
    let (fmt, glob, staged) = match family.as_str() {
        "rhel" => (
            "rpm",
            "ls -t dist/pg-agent-rs-*.x86_64.rpm | head -1",
            "testing/docker/pg-agent.rpm",
        ),
        _ => (
            "deb",
            "ls -t dist/pg-agent-rs_*_amd64.deb | head -1",
            "testing/docker/pg-agent.deb",
        ),
    };

    say_raw("build", t0);
    if !skip_build {
        cluster::host(&["./scripts/build-pkgs.sh", fmt]).await?;
    }
    let pkg = cluster::host(&["bash", "-c", glob])
        .await?
        .trim()
        .to_string();
    anyhow::ensure!(!pkg.is_empty(), "no .{fmt} in dist/ (build first?)");
    cluster::host(&["cp", &pkg, staged]).await?;
    cluster::host(&["./testing/gen-certs.sh"]).await?;

    say_raw("cluster up", t0);
    cluster::compose_down().await;
    cluster::compose_up().await?;

    // Learn this cell's layout before anything asks for a unit name or
    // a data directory. The image is the authority (see cluster::Facts).
    cluster::init_facts().await?;

    // Event tails from container start; PG connections once the nodes
    // have addresses.
    let log = EventLog::new();
    for n in cluster::NODES {
        log.spawn_node_tails(n);
    }
    let pg = std::sync::Arc::new(pg::Pg::discover().await?);
    let mut cx = Ctx::new(log, pg);

    scenarios::run_all(&mut cx).await;

    cx.say("result");
    let green = cx.summary();
    if green && !keep {
        cluster::compose_down().await;
    } else if !green {
        println!("(cluster left running for debugging — pga-db0/1/2)");
    } else {
        println!("(KEEP=1: cluster left running — pga-db0/1/2)");
    }
    Ok(green)
}

fn say_raw(title: &str, t0: Instant) {
    println!(
        "\n\x1b[1m== {title}\x1b[0m \x1b[90m[t={}s]\x1b[0m",
        t0.elapsed().as_secs()
    );
}
