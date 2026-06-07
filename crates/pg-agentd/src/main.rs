//! pg_agentd — coordinator daemon. See SPEC §12 for the lifecycle.

use clap::Parser;
use pg_agent_core::config::DEFAULT_CONFIG_FILE;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::error;

/// pg_agentd — pg_agent's coordinator daemon.
#[derive(Debug, Parser)]
#[command(name = "pg_agentd", version, about, long_about = None)]
struct Cli {
    /// Path to config.toml (default: /etc/pg_agent/config.toml).
    #[arg(long, env = "PG_AGENTD_CONFIG")]
    config: Option<PathBuf>,

    /// Override unix_socket path from config.
    #[arg(long, env = "PG_AGENTD_SOCKET")]
    socket: Option<PathBuf>,

    /// Development mode: unlock `allow_insecure_remote_peer` from config.toml.
    #[arg(long)]
    dev: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();
    let _cli = Cli::parse();

    if let Err(err) = run().await {
        error!(?err, "fatal");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run() -> anyhow::Result<()> {
    // TODO(v1): load config, apply env + CLI overrides, build CertReloader,
    // wire AgentDeps (LocalDb / PeerPool / PgStandbyExec / PcpCli /
    // DbusSystemd / FileReplayMarkerStore / FileWalStore /
    // FileMaintenanceStore), construct Agent, repair $PGDATA hook symlinks,
    // install SIGHUP cert-reload task, install SIGINT/SIGTERM shutdown
    // task, Agent::serve(token).await.
    //
    // sd_notify("READY=1") fires inside Agent::serve once both listeners
    // are bound.
    let _ = DEFAULT_CONFIG_FILE;
    Ok(())
}

fn init_logging() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}
