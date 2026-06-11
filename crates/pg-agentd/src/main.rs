//! pg_agentd — coordinator daemon. See SPEC §12 for the lifecycle.
//!
//! Two modes:
//!
//! - **`pg_agentd`** (no subcommand) or **`pg_agentd serve`** — run the
//!   coordinator. `main` is the composition root: load + project config,
//!   construct the [`AgentDeps`] trait objects, bind every listener,
//!   install signal handlers, then hand off to [`Agent::serve`]. The
//!   serve method is the one that calls `sd_notify::ready()` — by the
//!   time it does, every listener fd already exists in the kernel (the
//!   bind-before-notify race is closed structurally via
//!   [`Listeners::bind`]).
//!
//! - **`pg_agentd validate-env`** — run the localhost preflight checks
//!   (see SPEC §14) and exit. No listeners bound, no signal handlers,
//!   no daemon. Intended for `ExecStartPre=` and Ansible deploy gates,
//!   like `nginx -t`.

use clap::{Parser, Subcommand};
use pg_agent_core::{
    agent::{Agent, AgentDeps, Listeners, Options},
    certreload::CertReloader,
    config::{Config, DEFAULT_CONFIG_FILE},
    errors::AgentError,
    localdb::{LocalDb, PgLocalDb},
    maintenance::{FileMaintenanceStore, DEFAULT_SWEEP_INTERVAL},
    pcp::PcpCli,
    peers::{PeerPool, PeerRegistry},
    pgstandby::StandbyExec,
    preflight,
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
    #[arg(long, env = "PG_AGENTD_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Override unix_socket path from config. (Only honoured by `serve`.)
    #[arg(long, env = "PG_AGENTD_SOCKET", global = true)]
    socket: Option<PathBuf>,

    /// Development mode: allow plaintext peer connections to non-loopback
    /// hostnames. The only escape hatch from mandatory mTLS — deliberately
    /// CLI-only (no config-file knob). (Only honoured by `serve`.)
    #[arg(long, global = true)]
    dev: bool,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the coordinator daemon (default when no subcommand given).
    Serve,

    /// Validate the localhost environment (SPEC §14): TLS material,
    /// pgpool_node_id consistency, libpq home defaults, recovery tools,
    /// PostgreSQL tuning, roles, extension. Exits 0 if every check
    /// passes with no ERRs. Designed for systemd `ExecStartPre=` and
    /// Ansible deploy gates — the `nginx -t` equivalent.
    ///
    /// Mesh-level checks (peer mTLS reachability) live in
    /// `pg_agentctl cluster status`, not here.
    ValidateEnv {
        /// Emit machine-readable JSON instead of the human report.
        #[arg(long)]
        json: bool,

        /// Skip the local-DB checks (use during pre-bootstrap when PG
        /// is not yet up).
        #[arg(long)]
        skip_db: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();
    install_rustls_provider();
    let cli = Cli::parse();
    let result = match cli.cmd {
        None | Some(Cmd::Serve) => run_serve(&cli).await,
        Some(Cmd::ValidateEnv { json, skip_db }) => {
            return validate_env(&cli, json, skip_db).await;
        }
    };
    if let Err(err) = result {
        error!(?err, "fatal");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Shared config-loading path. `serve` and `validate-env` both need the
/// same projection so they validate the same thing.
fn load_config(cli: &Cli) -> anyhow::Result<(Config, PathBuf)> {
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE));

    let mut config = Config::load(&config_path)?;
    config.apply_env_overrides();
    if let Some(socket) = cli.socket.clone() {
        config.unix_socket = Some(socket.to_string_lossy().into_owned());
    }
    config.dev_mode = cli.dev;
    config.apply_defaults();
    config.resolve_local_node_id()?;
    config.validate()?;
    Ok((config, config_path))
}

async fn validate_env(cli: &Cli, json: bool, skip_db: bool) -> ExitCode {
    let (config, config_path) = match load_config(cli) {
        Ok(v) => v,
        Err(err) => {
            // Config-load failure is itself a validation failure —
            // surface it on stderr and exit 1, matching nginx -t.
            eprintln!("validate-env: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    info!(
        path = %config_path.display(),
        node_id = config.local_node_id,
        "config loaded"
    );

    // Try to open a local DB connection unless explicitly skipped.
    // Connect failure (PG not up yet, socket mismatch) is reported as
    // a single WARN by the preflight body — the right shape for
    // pre-bootstrap runs where PG may not be running yet.
    //
    // `PgLocalDb::connect` is lazy (no I/O), so the connect-refused case
    // doesn't actually surface here — it would surface inside every
    // individual setting/role query and report N ERRs instead of one
    // WARN, which then fails the ExecStartPre. pg_agentd's whole job
    // when PG is down is to supervise it back up, so this would
    // crashloop the daemon out of its own recovery role. Probe with
    // is_in_recovery() up front: if PG is genuinely unreachable, fall
    // through to db=None so the preflight emits its single WARN.
    let db: Option<Arc<dyn LocalDb>> = if skip_db {
        None
    } else {
        let socket_dir = config
            .postgres
            .socket_dir
            .clone()
            .expect("apply_defaults sets socket_dir");
        let port = config.postgres.port.expect("apply_defaults sets port");
        match PgLocalDb::connect(&socket_dir, port).await {
            Ok(db) => {
                let probe = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    db.is_in_recovery(),
                )
                .await;
                match probe {
                    Ok(Ok(_)) => Some(Arc::new(db) as Arc<dyn LocalDb>),
                    Ok(Err(e)) => {
                        eprintln!(
                            "warning: local DB unreachable ({e}); skipping DB-backed checks"
                        );
                        None
                    }
                    Err(_) => {
                        eprintln!(
                            "warning: local DB probe timed out after 2s; skipping DB-backed checks"
                        );
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("warning: local DB unreachable ({e}); skipping DB-backed checks");
                None
            }
        }
    };

    let report = preflight::preflight(&config, db).await;

    if json {
        match serde_json::to_string_pretty(&report.to_json()) {
            Ok(s) => println!("{s}"),
            Err(e) => {
                eprintln!("validate-env: serialise report: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if let Err(e) = report.print(&mut std::io::stdout()) {
        eprintln!("validate-env: write report: {e}");
        return ExitCode::FAILURE;
    }

    if report.has_errors() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_serve(cli: &Cli) -> anyhow::Result<()> {
    let (config, config_path) = load_config(cli)?;
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
        config.postgres.pg_install_prefix.clone().unwrap(),
        postgres.data_dir.clone(),
        config.postgres.replication.clone(),
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
        phantom_check_required_peers: config.startup.effective_required_peers(),
        supervisor_pgpool_enabled: config.supervisor.effective_pgpool_enabled(),
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

/// Pin the process-global rustls `CryptoProvider` to `ring`, matching the
/// `rustls = { features = ["ring"] }` workspace dep. Without this, any
/// transitive dependency that also pulls `aws-lc-rs` into the binary
/// (e.g. via tonic's TLS feature defaults) leaves
/// `CryptoProvider::get_default()` unable to pick one and the first
/// rustls call panics with "Could not automatically determine the
/// process-level CryptoProvider…". The CertReloader / PeerPool tests
/// already do the same install_default; production needs it too.
///
/// Idempotent in spirit: a second install attempt would return `Err`
/// because a default is already set, but we never call this twice.
fn install_rustls_provider() {
    if let Err(_existing) = rustls::crypto::ring::default_provider().install_default() {
        // A provider was already installed by something earlier in this
        // process. Should not happen for `pg_agentd`, but treat it as
        // a no-op rather than a fatal — the only consequence is that
        // this binary will end up using whichever provider was
        // installed first.
        warn!("rustls crypto provider already installed; leaving the existing one in place");
    }
}

fn init_logging() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Stderr — not stdout. `validate-env --json` writes its JSON
    // report to stdout, and operators pipe it into jq / Ansible
    // `from_json`; a stray tracing line would corrupt the parse.
    // For `serve`, journald captures stderr the same as stdout, so
    // there's no operational difference.
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}
