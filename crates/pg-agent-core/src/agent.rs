//! `Agent` — the long-running runtime. Owns the shared deps and the
//! background subsystems (LocalServer + PeerServer + MaintenanceWorker +
//! `/healthz` listener). See SPEC §12.
//!
//! # Lifecycle (race-critical)
//!
//! 1. Caller binds listeners via [`Listeners::bind`] — every fd exists in
//!    the kernel after this returns.
//! 2. Caller constructs [`Agent::new`] (cheap, no I/O).
//! 3. Caller invokes [`Agent::serve`] which spawns the subsystems, calls
//!    [`crate::sdnotify::ready`], then awaits shutdown OR first failure.
//!
//! The bind-before-notify invariant is structural: you cannot call
//! [`Agent::serve`] without a [`Listeners`] value in hand, and the only
//! way to obtain one is [`Listeners::bind`]. See [`crate::sdnotify`] for
//! the underlying race (pgpool starts the moment we say READY and dials
//! our Unix socket — so the socket must exist by then).

use crate::{
    certreload::CertReloader,
    config::{NodePool, PostgresRuntime, ServeSettings},
    errors::AgentError,
    healthz::{self, HealthSnapshotter, SHUTDOWN_GRACE, STALE_AFTER},
    localdb::{LocalDb, ReplicationLag},
    localserver::LocalServer,
    maintenance::{MaintenanceStore, MaintenanceWorker, DEFAULT_SWEEP_INTERVAL},
    pcp::Pcp,
    peers::PeerRegistry,
    peerserver::PeerServer,
    pgstandby::StandbyOps,
    replay_markers::ReplayMarkerStore,
    sdnotify,
    systemd::Systemd,
    walstore::WalStore,
};
use async_trait::async_trait;
use pg_agent_proto::pgagentpb as pb;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// NodeInfo
// ---------------------------------------------------------------------------

/// Read-only "ask the node about itself" surface shared by both proto
/// services. `LocalServer` and `PeerServer` hold `Arc<dyn NodeInfo>` so they
/// can be tested without standing up a full Agent (and so each transport
/// sees only the read-only subset of Agent's surface).
#[async_trait]
pub trait NodeInfo: Send + Sync {
    async fn get_status(&self) -> anyhow::Result<pb::NodeStatus>;
    async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse>;
}

// ---------------------------------------------------------------------------
// Deps + Options
// ---------------------------------------------------------------------------

/// Trait-object collaborators. Cloning is cheap — every field is an `Arc`.
#[derive(Clone)]
pub struct AgentDeps {
    pub db: Arc<dyn LocalDb>,
    pub peers: Arc<dyn PeerRegistry>,
    pub standby: Arc<dyn StandbyOps>,
    pub pcp: Arc<dyn Pcp>,
    pub sd: Arc<dyn Systemd>,
    pub replay: Arc<dyn ReplayMarkerStore>,
    pub wal: Arc<dyn WalStore>,
}

/// Configuration projected from `Config` at daemon startup. Single
/// construction site (`pg-agentd::main`); use a struct literal.
pub struct Options {
    pub serve: ServeSettings,
    pub node_pool: NodePool,
    pub postgres: PostgresRuntime,
    pub maintenance_store: Arc<dyn MaintenanceStore>,
    pub maintenance_sweep_interval: Duration,
    /// Shared mTLS material for the peer server and outbound peer clients.
    /// `None` means no TLS — caller is responsible for passing `--dev` and
    /// ensuring `serve.reject_insecure_remote_peer()` is false. The check
    /// runs again inside [`Agent::serve`] as a defense in depth.
    pub cert_reloader: Option<Arc<CertReloader>>,
}

impl Options {
    pub fn with_defaults(
        serve: ServeSettings,
        node_pool: NodePool,
        postgres: PostgresRuntime,
        maintenance_store: Arc<dyn MaintenanceStore>,
    ) -> Self {
        Self {
            serve,
            node_pool,
            postgres,
            maintenance_store,
            maintenance_sweep_interval: DEFAULT_SWEEP_INTERVAL,
            cert_reloader: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Listeners — type-level "bind happened" witness
// ---------------------------------------------------------------------------

/// Pre-bound listeners. The only way to construct one is [`Listeners::bind`],
/// which performs every bind synchronously and returns only on success.
/// [`Agent::serve`] requires this as an argument — so by the time we reach
/// the `sd_notify(READY=1)` call inside `serve`, every listener's fd is
/// already in the kernel. The bind-before-notify invariant becomes
/// structural rather than discipline (see SPEC §12 + [`crate::sdnotify`]).
pub struct Listeners {
    pub unix: UnixListener,
    pub peer: TcpListener,
    /// `None` when `serve.healthz.enabled == false`.
    pub healthz: Option<TcpListener>,
}

impl Listeners {
    /// Bind every listener the agent intends to serve. Removes any stale
    /// Unix socket file first (systemd guarantees single-instance, so a
    /// leftover file is from a prior crash). Fails fast on the first bind
    /// error; nothing partial survives — successfully-bound listeners drop
    /// at function exit when we return `Err`.
    pub async fn bind(serve: &ServeSettings) -> Result<Self, AgentError> {
        let socket_path = std::path::Path::new(&serve.unix_socket);
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }
        let unix = UnixListener::bind(socket_path)?;

        let peer = TcpListener::bind(&serve.peer_listen_addr).await?;

        let healthz = if serve.healthz.enabled {
            Some(TcpListener::bind(&serve.healthz.listen_addr).await?)
        } else {
            None
        };

        Ok(Self {
            unix,
            peer,
            healthz,
        })
    }
}

// ---------------------------------------------------------------------------
// Agent
// ---------------------------------------------------------------------------

pub struct Agent {
    pub(crate) deps: AgentDeps,
    pub(crate) opts: Options,
}

impl Agent {
    /// Build a shared agent. Cheap — no I/O. Returns `Arc<Self>` because
    /// subsystems clone it as `Arc<dyn NodeInfo>`.
    pub fn new(deps: AgentDeps, opts: Options) -> Arc<Self> {
        Arc::new(Self { deps, opts })
    }

    /// Brings up every subsystem, calls `sd_notify(READY=1)`, then awaits
    /// either a shutdown signal (via `shutdown.cancel()`) or the first
    /// subsystem failure. On either path, fires `sd_notify(STOPPING=1)`,
    /// cancels the shutdown token, and drains the remaining subsystems
    /// with a [`SHUTDOWN_GRACE`] deadline.
    ///
    /// The `listeners` argument is the type-level proof that every bind
    /// already happened — see [`Listeners`] and [`crate::sdnotify`].
    pub async fn serve(
        self: Arc<Self>,
        listeners: Listeners,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        // Defense in depth: the config-validation layer already enforces
        // this, but a misconfigured caller passing a hand-built ServeSettings
        // shouldn't bypass it.
        if self.opts.serve.reject_insecure_remote_peer() {
            return Err(AgentError::InsecureRemotePeer.into());
        }

        // Healthz snapshotter — initial probe runs synchronously so the
        // very first request after READY sees a real snapshot, not 503.
        let snapshotter = Arc::new(HealthSnapshotter::new(
            self.deps.db.clone(),
            self.deps.pcp.clone(),
        ));
        snapshotter.probe_once().await;

        // Maintenance worker. NodePool is small + Clone; no Arc needed.
        let maintenance_worker = MaintenanceWorker::new(
            self.opts.maintenance_store.clone(),
            self.deps.peers.clone(),
            self.opts.node_pool.clone(),
            self.deps.db.clone(),
            self.deps.replay.clone(),
            self.opts.maintenance_sweep_interval,
        );

        // Spawn subsystems. Each one returns anyhow::Result<()>; the ()-
        // returning workers are wrapped so the JoinSet has a uniform type.
        let mut js: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // LocalServer (Unix socket gRPC).
        {
            let me: Arc<dyn NodeInfo> = self.clone();
            let s = shutdown.clone();
            js.spawn(async move { LocalServer::new(me).serve(listeners.unix, s).await });
        }

        // PeerServer (mTLS gRPC).
        {
            let me: Arc<dyn NodeInfo> = self.clone();
            let s = shutdown.clone();
            let reloader = self.opts.cert_reloader.clone();
            js.spawn(async move { PeerServer::new(me).serve(listeners.peer, reloader, s).await });
        }

        // MaintenanceWorker (no listener; ticks on sweep_every).
        {
            let s = shutdown.clone();
            js.spawn(async move {
                maintenance_worker.run(s).await;
                Ok(())
            });
        }

        // HealthSnapshotter background loop.
        {
            let s = shutdown.clone();
            let snap = snapshotter.clone();
            js.spawn(async move {
                snap.run(s).await;
                Ok(())
            });
        }

        // /healthz HTTP server — only if enabled.
        if let Some(healthz_listener) = listeners.healthz {
            let s = shutdown.clone();
            let snap = snapshotter.clone();
            js.spawn(async move {
                healthz::serve_healthz(healthz_listener, snap, STALE_AFTER, s)
                    .await
                    .map_err(Into::into)
            });
        }

        // Listeners are bound, subsystems are spawned — safe to tell systemd
        // we're ready. See [`crate::sdnotify`] for why this MUST come last.
        sdnotify::ready();
        info!("pg_agentd: serving");

        // Wait for shutdown OR first subsystem to exit.
        let outcome: anyhow::Result<()> = tokio::select! {
            _ = shutdown.cancelled() => {
                info!("pg_agentd: shutdown signal received");
                Ok(())
            }
            Some(res) = js.join_next() => match res {
                Ok(Ok(())) => Err(anyhow::anyhow!(
                    "subsystem exited unexpectedly before shutdown"
                )),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e.into()),
            }
        };

        // Graceful drain. STOPPING tells systemd to treat TimeoutStopSec=
        // as the relevant deadline.
        sdnotify::stopping();
        shutdown.cancel();
        let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
            while js.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            warn!(
                "pg_agentd: shutdown drain exceeded {:?}; aborting remaining tasks",
                SHUTDOWN_GRACE
            );
            js.abort_all();
        }

        outcome
    }
}

// ---------------------------------------------------------------------------
// NodeInfo impl
// ---------------------------------------------------------------------------

#[async_trait]
impl NodeInfo for Agent {
    /// Probe postgres + pgpool service status (via systemd) and postgres's
    /// own `is_in_recovery` + replication lag (via libpq) in parallel.
    /// Failures of any individual probe are logged + folded into the
    /// returned `NodeStatus` flags rather than raised — this RPC is part
    /// of the readiness contract and must always answer.
    ///
    /// `is_ready` is the conjunction of "every contributing probe
    /// succeeded": both service statuses + `is_in_recovery` + replication
    /// lag. A node where any probe failed is degraded, not ready, even if
    /// the visible facts (e.g. service running) look fine. See SPEC §5.9.
    async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
        let (pg_status_res, pgpool_status_res, in_recovery_res, lag_res) = tokio::join!(
            self.deps.sd.status_postgres(),
            self.deps.sd.status_pgpool(),
            self.deps.db.is_in_recovery(),
            self.deps.db.replication_lag(),
        );

        let (pg_running, pg_status_ok) = pg_status_res
            .inspect_err(|e| warn!(?e, "get_status: postgres service status query failed"))
            .map_or((false, false), |r| (r, true));

        let (pgpool_running, pgpool_status_ok) = pgpool_status_res
            .inspect_err(|e| warn!(?e, "get_status: pgpool service status query failed"))
            .map_or((false, false), |r| (r, true));

        let (in_recovery, recovery_ok) = in_recovery_res
            .inspect_err(|e| warn!(?e, "get_status: is_in_recovery query failed"))
            .map_or((false, false), |v| (v, true));

        let (lag, lag_ok) = lag_res
            .inspect_err(|e| warn!(?e, "get_status: replication_lag query failed"))
            .map_or((ReplicationLag::default(), false), |l| (l, true));

        let ready = recovery_ok && lag_ok && pg_status_ok && pgpool_status_ok;

        Ok(pb::NodeStatus {
            is_running: pg_running,
            is_in_recovery: in_recovery,
            is_ready: ready,
            replication_lag_bytes: lag.bytes,
            replication_state: lag.state,
            is_postgres_running: pg_running,
            is_pgpool_running: pgpool_running,
            is_postgres_status_ok: pg_status_ok,
            is_pgpool_status_ok: pgpool_status_ok,
        })
    }

    async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
        Ok(pb::NodeConfigResponse {
            pg_port: self.opts.postgres.port as i32,
            pg_data_dir: self.opts.postgres.data_dir.to_string_lossy().into_owned(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HealthzSettings;
    use crate::localdb::{LocalDb, ReplicationLag};
    use crate::pcp::{NodeInfo as PcpNodeInfo, Pcp};
    use crate::peers::{PeerClient, PeerRegistry};
    use crate::pgstandby::{
        BasebackupOpts, ProgressCb, RewindOpts, StandbyOps, WriteRecoveryConfOpts,
    };
    use crate::replay_markers::ReplayMarkerStore;
    use crate::systemd::Systemd;
    use crate::walstore::WalStore;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};

    // ----- Stub deps ----------------------------------------------------

    #[derive(Default)]
    struct StubDb {
        in_recovery: bool,
        in_recovery_fails: AtomicBool,
        lag_fails: AtomicBool,
        lag: ReplicationLag,
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            if self.in_recovery_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: is_in_recovery boom");
            }
            Ok(self.in_recovery)
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            if self.lag_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: replication_lag boom");
            }
            Ok(self.lag.clone())
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubSd {
        pg_running: bool,
        pgpool_running: bool,
        pg_fails: AtomicBool,
        pgpool_fails: AtomicBool,
    }

    #[async_trait]
    impl Systemd for StubSd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn status_postgres(&self) -> anyhow::Result<bool> {
            if self.pg_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: status_postgres boom");
            }
            Ok(self.pg_running)
        }
        async fn status_pgpool(&self) -> anyhow::Result<bool> {
            if self.pgpool_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: status_pgpool boom");
            }
            Ok(self.pgpool_running)
        }
        async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct StubPcp;
    #[async_trait]
    impl Pcp for StubPcp {
        async fn attach_node(&self, _: i32) -> anyhow::Result<()> {
            Ok(())
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<PcpNodeInfo>> {
            Ok(Vec::new())
        }
    }

    struct StubPeers;
    #[async_trait]
    impl PeerRegistry for StubPeers {
        async fn client(
            &self,
            _: &crate::config::NodeConfig,
        ) -> anyhow::Result<Arc<dyn PeerClient>> {
            anyhow::bail!("stub: no peer client")
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct StubStandby;
    #[async_trait]
    impl StandbyOps for StubStandby {
        async fn basebackup(&self, _: BasebackupOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn rewind(&self, _: RewindOpts, _: Option<ProgressCb>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn write_recovery_conf(&self, _: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct StubReplay;
    #[async_trait]
    impl ReplayMarkerStore for StubReplay {
        async fn has(&self, _: &str, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn mark_done(&self, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn sweep(&self, _: chrono::DateTime<chrono::Utc>) {}
    }

    struct StubWal;
    #[async_trait]
    impl WalStore for StubWal {
        async fn open_archive(
            &self,
            _: &str,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, crate::errors::AgentError>
        {
            Err(crate::errors::AgentError::WalNotFound("stub".into()))
        }
        async fn write_restore(
            &self,
            _: &std::path::Path,
            _: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        ) -> Result<(), crate::errors::AgentError> {
            Ok(())
        }
    }

    struct StubMaint;
    #[async_trait]
    impl MaintenanceStore for StubMaint {
        async fn append(
            &self,
            _: crate::maintenance::MaintenancePayload,
        ) -> anyhow::Result<crate::maintenance::MaintenanceIntent> {
            anyhow::bail!("stub")
        }
        async fn list_pending(&self) -> anyhow::Result<Vec<crate::maintenance::MaintenanceIntent>> {
            Ok(Vec::new())
        }
        async fn list(
            &self,
            _: &[crate::maintenance::MaintenanceStatus],
        ) -> anyhow::Result<(
            Vec<crate::maintenance::MaintenanceIntent>,
            Vec<crate::maintenance::SkippedIntent>,
        )> {
            Ok((Vec::new(), Vec::new()))
        }
        async fn get(&self, _: &str) -> anyhow::Result<crate::maintenance::MaintenanceIntent> {
            anyhow::bail!("stub")
        }
        async fn mark_attempt(
            &self,
            _: &str,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_done(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_abandoned(&self, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reschedule(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn make_agent(deps: AgentDeps) -> Arc<Agent> {
        let serve = ServeSettings {
            unix_socket: "/tmp/never".into(),
            agent_port: 9701,
            peer_listen_addr: "127.0.0.1:0".into(),
            tls: crate::config::TlsConfig::default(),
            tls_configured: false,
            has_remote_peers: false,
            dev_mode: true,
            healthz: HealthzSettings {
                enabled: false,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: PathBuf::from("/var/lib/postgresql/17/main"),
            repl_user: "repl_user".into(),
        };
        let pool = NodePool {
            members: vec![crate::config::NodeConfig {
                id: 0,
                hostname: "localhost".into(),
            }],
            local_node_id: 0,
        };
        Agent::new(
            deps,
            Options {
                serve,
                node_pool: pool,
                postgres: pg,
                maintenance_store: Arc::new(StubMaint),
                maintenance_sweep_interval: Duration::from_secs(30),
                cert_reloader: None,
            },
        )
    }

    fn make_deps(db: Arc<dyn LocalDb>, sd: Arc<dyn Systemd>) -> AgentDeps {
        AgentDeps {
            db,
            peers: Arc::new(StubPeers),
            standby: Arc::new(StubStandby),
            pcp: Arc::new(StubPcp),
            sd,
            replay: Arc::new(StubReplay),
            wal: Arc::new(StubWal),
        }
    }

    // ----- get_status verdict matrix ------------------------------------

    #[tokio::test]
    async fn get_status_primary_ready() {
        let db = StubDb {
            in_recovery: false,
            ..Default::default()
        };
        let sd = StubSd {
            pg_running: true,
            pgpool_running: true,
            ..Default::default()
        };
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let s = agent.get_status().await.unwrap();
        assert!(s.is_running);
        assert!(!s.is_in_recovery);
        assert!(s.is_ready);
        assert!(s.is_postgres_status_ok);
        assert!(s.is_pgpool_status_ok);
        assert_eq!(s.replication_lag_bytes, 0);
    }

    #[tokio::test]
    async fn get_status_replica_with_lag() {
        let db = StubDb {
            in_recovery: true,
            lag: ReplicationLag {
                bytes: 12345,
                state: "streaming".into(),
            },
            ..Default::default()
        };
        let sd = StubSd {
            pg_running: true,
            pgpool_running: true,
            ..Default::default()
        };
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let s = agent.get_status().await.unwrap();
        assert!(s.is_ready);
        assert!(s.is_in_recovery);
        assert_eq!(s.replication_lag_bytes, 12345);
        assert_eq!(s.replication_state, "streaming");
    }

    #[tokio::test]
    async fn get_status_lag_query_fails_yields_not_ready() {
        let db = StubDb {
            in_recovery: true,
            lag_fails: AtomicBool::new(true),
            ..Default::default()
        };
        let sd = StubSd {
            pg_running: true,
            pgpool_running: true,
            ..Default::default()
        };
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let s = agent.get_status().await.unwrap();
        // Lag query failed but is_in_recovery succeeded — surface the
        // recovery state honestly + flip readiness off.
        assert!(s.is_in_recovery);
        assert!(!s.is_ready);
        assert_eq!(s.replication_lag_bytes, 0);
        assert_eq!(s.replication_state, "");
    }

    #[tokio::test]
    async fn get_status_recovery_query_fails_yields_not_ready() {
        let db = StubDb {
            in_recovery_fails: AtomicBool::new(true),
            ..Default::default()
        };
        let sd = StubSd {
            pg_running: true,
            pgpool_running: true,
            ..Default::default()
        };
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let s = agent.get_status().await.unwrap();
        assert!(!s.is_ready);
        assert!(!s.is_in_recovery);
    }

    #[tokio::test]
    async fn get_status_service_status_failure_marks_status_not_ok() {
        let db = StubDb {
            in_recovery: false,
            ..Default::default()
        };
        let sd = StubSd {
            pg_running: true,
            pgpool_fails: AtomicBool::new(true),
            ..Default::default()
        };
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let s = agent.get_status().await.unwrap();
        assert!(s.is_postgres_status_ok);
        assert!(!s.is_pgpool_status_ok);
        assert!(!s.is_ready);
    }

    // ----- get_node_config ----------------------------------------------

    #[tokio::test]
    async fn get_node_config_projects_options() {
        let db = StubDb::default();
        let sd = StubSd::default();
        let agent = make_agent(make_deps(Arc::new(db), Arc::new(sd)));
        let r = agent.get_node_config().await.unwrap();
        assert_eq!(r.pg_port, 5432);
        assert_eq!(r.pg_data_dir, "/var/lib/postgresql/17/main");
    }

    // ----- Listeners ----------------------------------------------------

    #[tokio::test]
    async fn listeners_bind_clears_stale_unix_socket() {
        let tmp = tempfile::tempdir().unwrap();
        let sock = tmp.path().join("pg_agentd.sock");
        std::fs::write(&sock, b"leftover").unwrap();
        assert!(sock.exists());

        let serve = ServeSettings {
            unix_socket: sock.to_string_lossy().into_owned(),
            agent_port: 0,
            peer_listen_addr: "127.0.0.1:0".into(),
            tls: crate::config::TlsConfig::default(),
            tls_configured: false,
            has_remote_peers: false,
            dev_mode: true,
            healthz: HealthzSettings {
                enabled: false,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let listeners = Listeners::bind(&serve).await.unwrap();
        assert!(listeners.healthz.is_none());
        drop(listeners);
    }

    #[tokio::test]
    async fn listeners_bind_creates_healthz_when_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let sock = tmp.path().join("pg_agentd.sock");
        let serve = ServeSettings {
            unix_socket: sock.to_string_lossy().into_owned(),
            agent_port: 0,
            peer_listen_addr: "127.0.0.1:0".into(),
            tls: crate::config::TlsConfig::default(),
            tls_configured: false,
            has_remote_peers: false,
            dev_mode: true,
            healthz: HealthzSettings {
                enabled: true,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let listeners = Listeners::bind(&serve).await.unwrap();
        assert!(listeners.healthz.is_some());
    }

    // ----- serve ----------------------------------------------------------

    #[tokio::test]
    async fn serve_exits_promptly_on_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let sock = tmp.path().join("pg_agentd.sock");
        let serve = ServeSettings {
            unix_socket: sock.to_string_lossy().into_owned(),
            agent_port: 0,
            peer_listen_addr: "127.0.0.1:0".into(),
            tls: crate::config::TlsConfig::default(),
            tls_configured: false,
            has_remote_peers: false,
            dev_mode: true,
            healthz: HealthzSettings {
                enabled: true,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let listeners = Listeners::bind(&serve).await.unwrap();
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: PathBuf::from("/var/lib/postgresql/17/main"),
            repl_user: "repl_user".into(),
        };
        let pool = NodePool {
            members: vec![crate::config::NodeConfig {
                id: 0,
                hostname: "localhost".into(),
            }],
            local_node_id: 0,
        };
        let agent = Agent::new(
            make_deps(Arc::new(StubDb::default()), Arc::new(StubSd::default())),
            Options {
                serve,
                node_pool: pool,
                postgres: pg,
                maintenance_store: Arc::new(StubMaint),
                maintenance_sweep_interval: Duration::from_secs(30),
                cert_reloader: None,
            },
        );
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { agent.serve(listeners, s).await });
        // Give the spawn loop a moment to enter steady state.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let res = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("serve did not exit within 2s of shutdown")
            .unwrap();
        assert!(res.is_ok(), "serve returned {res:?}");
    }

    #[tokio::test]
    async fn serve_rejects_insecure_remote_peer() {
        let tmp = tempfile::tempdir().unwrap();
        let sock = tmp.path().join("pg_agentd.sock");
        let serve = ServeSettings {
            unix_socket: sock.to_string_lossy().into_owned(),
            agent_port: 0,
            peer_listen_addr: "127.0.0.1:0".into(),
            tls: crate::config::TlsConfig::default(),
            tls_configured: false,
            has_remote_peers: true, // <-- the trigger
            dev_mode: false,        // <-- and no --dev escape
            healthz: HealthzSettings {
                enabled: false,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let listeners = Listeners::bind(&serve).await.unwrap();
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: PathBuf::from("/x"),
            repl_user: "r".into(),
        };
        let pool = NodePool {
            members: vec![crate::config::NodeConfig {
                id: 0,
                hostname: "localhost".into(),
            }],
            local_node_id: 0,
        };
        let agent = Agent::new(
            make_deps(Arc::new(StubDb::default()), Arc::new(StubSd::default())),
            Options {
                serve,
                node_pool: pool,
                postgres: pg,
                maintenance_store: Arc::new(StubMaint),
                maintenance_sweep_interval: Duration::from_secs(30),
                cert_reloader: None,
            },
        );
        let res = agent.serve(listeners, CancellationToken::new()).await;
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("remote peers present but TLS is not configured"),
            "unexpected error: {err}"
        );
    }
}
