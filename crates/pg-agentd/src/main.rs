//! pg_agentd — coordinator daemon. See SPEC §12 for the lifecycle.
//!
//! `main` is the composition root: load + project config, construct the
//! [`AgentDeps`] trait objects, bind every listener, install the signal
//! handlers, then hand off to [`Agent::serve`]. The serve method is the
//! one that calls `sd_notify::ready()` — by the time it does, every
//! listener fd already exists in the kernel (the bind-before-notify race
//! is closed structurally via [`Listeners::bind`]).

use clap::Parser;
use pg_agent_core::{
    agent::{Agent, AgentDeps, Listeners, Options},
    certreload::CertReloader,
    config::{Config, DEFAULT_CONFIG_FILE},
    errors::AgentError,
    localdb::PgLocalDb,
    maintenance::{FileMaintenanceStore, DEFAULT_SWEEP_INTERVAL},
    pcp::PcpCli,
    peers::{PeerPool, PeerRegistry},
    pgstandby::StandbyExec,
    replay_markers::{FileReplayMarkerStore, DEFAULT_RETENTION},
    symlinks::{ensure_hook_symlinks, find_pg_agentc},
    systemd::DbusSystemd,
    walstore::FileWalStore,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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

    /// Development mode: allow plaintext peer connections to non-loopback
    /// hostnames. The only escape hatch from mandatory mTLS — deliberately
    /// CLI-only (no config-file knob).
    #[arg(long)]
    dev: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();
    if let Err(err) = run().await {
        error!(?err, "fatal");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE));

    // Config: load + env + CLI overrides + defaults + resolve local id + validate.
    let mut config = Config::load(&config_path)?;
    config.apply_env_overrides();
    if let Some(socket) = cli.socket.clone() {
        config.unix_socket = Some(socket.to_string_lossy().into_owned());
    }
    config.dev_mode = cli.dev;
    config.apply_defaults();
    config.resolve_local_node_id()?;
    config.validate()?;
    info!(
        path = %config_path.display(),
        node_id = config.local_node_id,
        "config loaded"
    );

    // Projections used by Agent.
    let serve = config.to_serve_settings();
    let node_pool = config.to_node_pool();
    let postgres = config.to_postgres_runtime();

    // Cert reloader (Optional — only when TLS is configured).
    let cert_reloader = if serve.tls_configured {
        Some(Arc::new(CertReloader::new(serve.tls.clone())?))
    } else {
        None
    };

    // Build dependencies.
    let sd = Arc::new(
        DbusSystemd::new(
            config.postgres.service.clone().unwrap(),
            config.pcp.pgpool_service.clone().unwrap(),
        )
        .await?,
    );

    let db = Arc::new(
        PgLocalDb::connect(config.postgres.socket_dir.clone().unwrap(), postgres.port).await?,
    );

    let pcp = Arc::new(PcpCli::new(&config.pcp));

    // Resolve `pg_agentc` once at startup: sibling of pg_agentd in the
    // Debian package layout, else PATH fallback. Threaded through
    // StandbyExec so the post-basebackup hook-symlink repair points at
    // the same binary that the daemon-startup repair uses.
    let pg_agentc_bin = find_pg_agentc().map_err(|e| anyhow::anyhow!("locate pg_agentc: {e}"))?;
    info!(path = %pg_agentc_bin.display(), "pg_agentc located");

    // Daemon-startup hook-symlink repair (SPEC §10.2 + §17). pgpool may
    // exec these any time after pg_agentd.service activates, so the
    // symlinks must exist before listeners come up.
    ensure_hook_symlinks(&postgres.data_dir, &pg_agentc_bin)
        .map_err(|e| anyhow::anyhow!("hook symlink setup: {e}"))?;

    let standby = Arc::new(StandbyExec::new(
        config.postgres.pghome.clone().unwrap(),
        postgres.data_dir.clone(),
        config.postgres.replication_tls.clone(),
        pg_agentc_bin,
    ));

    // State directories under <state_dir>/{replay,maintenance}/.
    let state_dir = config.state_dir.clone().unwrap();
    let replay_dir = state_dir.join("replay");
    let maintenance_dir = state_dir.join("maintenance");
    create_state_subdirs(&[&replay_dir, &maintenance_dir]).await?;

    let replay = Arc::new(FileReplayMarkerStore::new(replay_dir, DEFAULT_RETENTION));
    let wal = Arc::new(FileWalStore::new(
        postgres.data_dir.clone(),
        config.postgres.archive_dir.clone().unwrap(),
    ));
    let maintenance_store = Arc::new(FileMaintenanceStore::new(
        maintenance_dir,
        chrono::Duration::days(7),
    ));

    // PeerPool: mTLS when cert material is configured, plain TCP only
    // for `--dev` / single-node. The validate() path already rejects
    // "remote peers + no TLS + no --dev".
    let peers: Arc<dyn PeerRegistry> = match cert_reloader.clone() {
        Some(reloader) => PeerPool::new(reloader, serve.agent_port)?,
        None => PeerPool::new_dev(serve.agent_port),
    };

    let deps = AgentDeps {
        db,
        peers,
        standby,
        pcp,
        sd,
        replay,
        wal,
    };

    let opts = Options {
        serve: serve.clone(),
        node_pool,
        postgres,
        maintenance_store,
        maintenance_sweep_interval: DEFAULT_SWEEP_INTERVAL,
        cert_reloader: cert_reloader.clone(),
    };

    // Bind listeners synchronously — every fd exists once this returns.
    let listeners = Listeners::bind(&serve)
        .await
        .map_err(|e: AgentError| anyhow::anyhow!("bind listeners: {e}"))?;

    let shutdown = CancellationToken::new();
    install_shutdown_handler(shutdown.clone())?;
    if let Some(r) = cert_reloader {
        install_sighup_reload(r);
    }

    let agent = Agent::new(deps, opts);
    agent.serve(listeners, shutdown).await
}

async fn create_state_subdirs(dirs: &[&PathBuf]) -> anyhow::Result<()> {
    for d in dirs {
        tokio::fs::create_dir_all(d)
            .await
            .map_err(|e| anyhow::anyhow!("create state dir {}: {e}", d.display()))?;
    }
    Ok(())
}

/// SIGINT + SIGTERM → `shutdown.cancel()`. Spawned task lives for the
/// lifetime of the process; cancelling the token also closes the signal
/// streams (drop on the spawned async block).
fn install_shutdown_handler(shutdown: CancellationToken) -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow::anyhow!("install SIGINT handler: {e}"))?;
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow::anyhow!("install SIGTERM handler: {e}"))?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigint.recv() => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        shutdown.cancel();
    });
    Ok(())
}

/// SIGHUP → [`CertReloader::reload`]. Each SIGHUP atomically swaps the
/// active cert bundle; new connections pick up the new material, in-flight
/// ones drain on the old. Per SPEC §7 the reload window is bounded by the
/// 12 h peer-connection age cap, so a SIGHUP propagates everywhere within
/// ~12 h without us tearing down healthy channels.
fn install_sighup_reload(reloader: Arc<CertReloader>) {
    tokio::spawn(async move {
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                warn!(?e, "install SIGHUP handler failed; cert reload disabled");
                return;
            }
        };
        while hup.recv().await.is_some() {
            match reloader.reload() {
                Ok(true) => info!("cert reload: new material picked up"),
                Ok(false) => info!("cert reload: no change on disk"),
                Err(e) => warn!(?e, "cert reload: failed; keeping previous bundle"),
            }
        }
    });
}

fn init_logging() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}
