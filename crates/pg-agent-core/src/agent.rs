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
    peerserver::{PeerServer, PeerTlsConfig},
    pgstandby::StandbyOps,
    replay_markers::ReplayMarkerStore,
    sdnotify,
    systemd::Systemd,
    walstore::WalStore,
};
use async_trait::async_trait;
use pg_agent_proto::pgagentpb as pb;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Durable journal for multi-phase state-change orchestrations
    /// (today: `cluster_handoff`). Distinct contract from `replay`
    /// (after-success dedup) — see `crate::inflight_ops` module docs.
    pub inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
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
    /// Minimum count of peers that must answer the startup phantom-primary
    /// check (with a known timeline) for it to produce a `Confirmed`
    /// verdict. Projected from `Config::startup.effective_required_peers()`.
    /// 0 disables the quorum gate — see `StartupConfig` docs for the
    /// 2-node operational trade-off.
    pub phantom_check_required_peers: usize,
    /// Enable the `PgpoolSupervisor` task. Projected from
    /// `Config::supervisor.effective_pgpool_enabled()`. When true (the
    /// default), the supervisor is spawned by `Agent::serve` if and only
    /// if the startup phantom-primary verdict is `Confirmed` or
    /// `NotApplicable` — phantom states must not have pgpool routing
    /// traffic at them.
    pub supervisor_pgpool_enabled: bool,
    /// Shared mTLS material for the peer server and outbound peer clients.
    /// `None` means no TLS — caller is responsible for passing `--dev` and
    /// ensuring `serve.reject_insecure_remote_peer()` is false. The check
    /// runs again inside [`Agent::serve`] as a defense in depth.
    pub cert_reloader: Option<Arc<CertReloader>>,
    /// The HA subsystem. `None` is reachable only from unit tests that
    /// build an `Agent` to exercise an RPC handler; `pg_agentd` always
    /// constructs it. See [`HaWiring`].
    pub ha: Option<HaWiring>,
}

/// Consensus, the executor, and the loop's timing — as one value,
/// because they are one thing.
///
/// They used to be three independent `Option`s, which spelled four
/// states the daemon could be in and only one it should ever have been
/// in. The other three were the staged migration's: a loop reading a
/// process-local store authoritative for nothing, a loop that narrated
/// decisions with no executor to act on them (shadow mode), consensus
/// running under no loop at all. Each was reachable by a config file,
/// and none of them is a cluster anyone wants to be paged for.
///
/// Binding them together makes those states unrepresentable rather than
/// merely discouraged: there is no lease without something to act on
/// it, and no executor without a lease to act for.
pub struct HaWiring {
    /// Serves `PgAgentRaft` on the peer listener and backs the loop,
    /// `cluster pause`, and cold-start reconciliation with the
    /// replicated state machine.
    ///
    /// Built before `Agent::new` because it opens redb and starts
    /// openraft's core task, and `Agent::new` is documented as cheap
    /// and I/O-free.
    pub raft: Arc<crate::raftconsensus::RaftRuntime>,
    /// The PostgreSQL handle every decision acts through.
    pub instance: Arc<dyn pgman::instance::PostgresInstance>,
    /// Projected from the `[raft]` timing knobs.
    pub timing: crate::ha::HaTiming,
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
            phantom_check_required_peers: crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
            supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
            cert_reloader: None,
            ha: None,
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
    /// Flips `true` after `verify_primary_at_startup` resolves
    /// (regardless of verdict — operator visibility is the gate, not
    /// "everything passed"). Shared with `serve_healthz` so that until
    /// the check runs, every `/healthz` request returns 503: HAProxy
    /// must not route at a node whose role hasn't been validated
    /// against the cluster yet.
    pub(crate) startup_verified: Arc<AtomicBool>,
    /// Freshness of this node's own contact with each peer, recorded by
    /// the HA loop's fan-out and reported to peers in `NodeStatus` —
    /// the second opinion candidacy consults before deposing a holder
    /// it cannot see (finding 25).
    pub(crate) peer_seen: Arc<crate::cluster_view::PeerSeen>,
}

impl Agent {
    /// Build a shared agent. Cheap — no I/O. Returns `Arc<Self>` because
    /// subsystems clone it as `Arc<dyn NodeInfo>`.
    pub fn new(deps: AgentDeps, opts: Options) -> Arc<Self> {
        Arc::new(Self {
            deps,
            opts,
            startup_verified: Arc::new(AtomicBool::new(false)),
            peer_seen: Arc::new(crate::cluster_view::PeerSeen::new()),
        })
    }

    /// Project the inbound peer mTLS config: bundle the cert reloader
    /// with the SAN allowlist derived from `NodePool::members`. Returns
    /// `None` when TLS isn't configured — the peer server then takes the
    /// dev-mode plain-TCP path (only valid when there are no remote peers).
    fn build_peer_tls_config(&self) -> Option<PeerTlsConfig> {
        let reloader = self.opts.cert_reloader.clone()?;
        let allowed_peer_sans = self
            .opts
            .node_pool
            .members
            .iter()
            .map(|n| n.hostname.clone())
            .collect();
        Some(PeerTlsConfig {
            reloader,
            allowed_peer_sans,
        })
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

        // Shared between the role executor (writer) and the healthz
        // snapshotter (reader): the finding-15 wedged-follow flag.
        let follow_wedged = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Healthz snapshotter — initial probe runs synchronously so the
        // very first request after READY sees a real snapshot, not 503.
        let snapshotter = Arc::new(
            HealthSnapshotter::new(self.deps.db.clone(), self.deps.pcp.clone())
                .with_follow_wedged(follow_wedged.clone()),
        );
        snapshotter.probe_once().await;

        // Maintenance worker. NodePool is small + Clone; no Arc needed.
        let maintenance_worker = MaintenanceWorker::new(
            self.opts.maintenance_store.clone(),
            self.deps.peers.clone(),
            self.opts.node_pool.clone(),
            self.deps.db.clone(),
            self.deps.replay.clone(),
            self.deps.inflight.clone(),
            self.opts.maintenance_sweep_interval,
        );

        // Spawn subsystems. Each one returns anyhow::Result<()>; the ()-
        // returning workers are wrapped so the JoinSet has a uniform type.
        let mut js: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // LocalServer (Unix socket gRPC).
        {
            let me: Arc<dyn NodeInfo> = self.clone();
            let s = shutdown.clone();
            let db = self.deps.db.clone();
            let peers = self.deps.peers.clone();
            let maint = self.opts.maintenance_store.clone();
            let wal = self.deps.wal.clone();
            let replay = self.deps.replay.clone();
            let inflight = self.deps.inflight.clone();
            let pcp = self.deps.pcp.clone();
            let sd = self.deps.sd.clone();
            let standby = self.deps.standby.clone();
            let pool = self.opts.node_pool.clone();
            let pg = self.opts.postgres.clone();
            let raft = self.opts.ha.as_ref().map(|ha| ha.raft.clone());
            let peer_seen = self.peer_seen.clone();
            js.spawn(async move {
                let mut server = LocalServer::new(
                    me, db, peers, maint, wal, replay, inflight, pcp, sd, standby, pool, pg,
                )
                .with_peer_seen(peer_seen);
                if let Some(rt) = raft {
                    server = server.with_raft(rt);
                }
                server.serve(listeners.unix, s).await
            });
        }

        // PeerServer (mTLS gRPC). Build the inbound TLS config from the
        // cert reloader + pool SAN allowlist; `None` is dev-mode plain TCP.
        {
            let me: Arc<dyn NodeInfo> = self.clone();
            let s = shutdown.clone();
            let tls = self.build_peer_tls_config();
            let sd = self.deps.sd.clone();
            let db = self.deps.db.clone();
            let standby = self.deps.standby.clone();
            let wal = self.deps.wal.clone();
            let inflight = self.deps.inflight.clone();
            let pcp = self.deps.pcp.clone();
            let raft = self.opts.ha.as_ref().map(|ha| ha.raft.clone());
            js.spawn(async move {
                let mut server = PeerServer::new(me, sd, db, standby, wal, inflight, pcp);
                if let Some(rt) = raft {
                    server = server.with_raft(rt.raft.clone(), rt.reader.clone());
                }
                server.serve(listeners.peer, tls, s).await
            });
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
            let verified = self.startup_verified.clone();
            js.spawn(async move {
                healthz::serve_healthz(healthz_listener, snap, STALE_AFTER, verified, s)
                    .await
                    .map_err(Into::into)
            });
        }

        // Phantom-primary detection (SPEC §...; ROADMAP recovery and
        // reconciliation). Runs AFTER subsystems spawn so peers can
        // answer our outbound GetStatus via the now-bound local
        // PeerServer if they're booting concurrently, and BEFORE
        // sd_notify(READY) so systemd's "started" signal aligns with
        // "role has been verified against the cluster." `startup_verified`
        // gates /healthz independently: even a Confirmed verdict
        // requires the flag to flip before /healthz returns 200.
        let verdict = self.verify_primary_with_retries().await;
        match &verdict {
            PrimaryVerdict::Confirmed => {
                info!("phantom-primary check: confirmed");
            }
            PrimaryVerdict::NotApplicable => {
                info!("phantom-primary check: not applicable (standby or PG not running)");
            }
            PrimaryVerdict::Phantom { local_tl, peers } => {
                let peer_evidence: Vec<String> = peers
                    .iter()
                    .map(|p| format!("{}@TL{}", p.hostname, p.timeline_id))
                    .collect();
                warn!(
                    local_tl,
                    peers = ?peer_evidence,
                    "phantom-primary check: detected — peer reports higher timeline; stopping postgres"
                );
                if let Err(e) = stop_postgres_with_retry(self.deps.sd.as_ref()).await {
                    return Err(anyhow::anyhow!(
                        "phantom-primary detected on local TL {local_tl}; \
                         stop_postgres failed: {e}; refusing to mark daemon ready \
                         with PG still up"
                    ));
                }
            }
            PrimaryVerdict::SplitBrain { local_tl, peers } => {
                let peer_evidence: Vec<String> = peers
                    .iter()
                    .map(|p| format!("{}@TL{}", p.hostname, p.timeline_id))
                    .collect();
                warn!(
                    local_tl,
                    peers = ?peer_evidence,
                    "phantom-primary check: split brain — peer asserts primary on same timeline; stopping postgres"
                );
                if let Err(e) = stop_postgres_with_retry(self.deps.sd.as_ref()).await {
                    return Err(anyhow::anyhow!(
                        "split brain detected on local TL {local_tl}; \
                         stop_postgres failed: {e}; refusing to mark daemon ready \
                         with PG still up"
                    ));
                }
            }
            PrimaryVerdict::Unverifiable { reason } => {
                warn!(
                    reason,
                    "phantom-primary check: unverifiable; stopping postgres conservatively"
                );
                if let Err(e) = stop_postgres_with_retry(self.deps.sd.as_ref()).await {
                    return Err(anyhow::anyhow!(
                        "phantom-primary unverifiable ({reason}); stop_postgres failed: \
                         {e}; refusing to mark daemon ready with PG still up"
                    ));
                }
            }
        }
        // The gate flips after the verdict resolves regardless of
        // outcome — operator visibility, not "everything passed." A
        // Phantom/SplitBrain/Unverifiable verdict still produces a 503
        // because PG is now stopped (snapshotter reports it), and the
        // gate releasing lets that 503 reflect actual cluster state
        // instead of the bootstrap-pending placeholder.
        self.startup_verified.store(true, Ordering::SeqCst);

        // Cold-start reconciliation (finding 21) — see the method docs.
        // Synchronous, BEFORE the HA loop spawns: a holder starting its
        // PostgreSQL through crash recovery must not race the loop's
        // "holder with unknown local state" fence. `start_postgres`
        // waits for readiness (pg_ctl -w), so the loop's first tick
        // sees the recovered primary and retains.
        if let Some(wiring) = &self.opts.ha {
            let store: Arc<dyn crate::consensus::ConsensusStore> = wiring.raft.store.clone();
            self.cold_start_reconcile(&wiring.instance, &store).await;
        }

        // PgpoolSupervisor — only spawn on a verdict that says "this
        // node should be serving." A Phantom/SplitBrain/Unverifiable
        // node must NOT have its local pgpool started, because pgpool's
        // backend view is stale relative to whichever node the cluster
        // actually chose; routing through it would surface a wrong
        // primary to anything reading via HAProxy.
        let supervisor_eligible = matches!(
            verdict,
            PrimaryVerdict::Confirmed | PrimaryVerdict::NotApplicable
        );
        // The HA loop — spawned regardless of the phantom verdict.
        // A node whose PostgreSQL the phantom check just stopped is
        // still a cluster member: it can follow, it can be a candidate,
        // and it is the node most in need of the lease telling it what
        // it is. Withholding the loop from exactly that node would
        // leave it stopped with nothing able to reconcile it.
        if let Some(wiring) = &self.opts.ha {
            let timing = wiring.timing.clone();
            let executor = Arc::new(crate::roleexec::RoleExecutor::new(
                wiring.instance.clone(),
                self.deps.db.clone(),
                self.deps.peers.clone(),
                self.opts.node_pool.clone(),
                self.deps.inflight.clone(),
                self.deps.pcp.clone(),
                // Promote budget = leader_ttl: the clock rivals run
                // against a fresh holder (see roleexec docs).
                timing.leader_ttl,
                &self.opts.postgres,
                follow_wedged.clone(),
            ));
            let store: Arc<dyn crate::consensus::ConsensusStore> = wiring.raft.store.clone();
            let ha = Arc::new(
                crate::ha::HaLoop::new(
                    store,
                    self.deps.db.clone(),
                    self.deps.peers.clone(),
                    self.opts.node_pool.clone(),
                    timing,
                )
                // The loop records who it reached each tick; get_status
                // ships those ages to peers as the second opinion.
                .with_peer_seen(self.peer_seen.clone())
                .with_executor(executor),
            );
            let s = shutdown.clone();
            js.spawn(async move {
                ha.run(s).await;
                Ok(())
            });
        }

        if self.opts.supervisor_pgpool_enabled && supervisor_eligible {
            let supervisor = Arc::new(crate::pgpool_supervisor::PgpoolSupervisor::new(
                self.deps.sd.clone(),
            ));
            // Best-effort one-shot at startup: a clean boot converges
            // in milliseconds instead of waiting `TICK` for the loop's
            // first iteration. Logs and proceeds on any error — the
            // continuous loop will retry.
            if let Err(e) = supervisor.ensure_running_once().await {
                warn!(
                    ?e,
                    "pgpool_supervisor: startup probe failed; continuous loop will retry"
                );
            }
            let s = shutdown.clone();
            let sup = supervisor.clone();
            js.spawn(async move {
                sup.run(s).await;
                Ok(())
            });
        }

        // Listeners are bound, subsystems are spawned — safe to tell systemd
        // we're ready. See [`crate::sdnotify`] for why this MUST come last.
        sdnotify::ready();
        info!("pg_agentd: serving");

        // Wait for shutdown OR first subsystem to exit.
        //
        // Race-correctness: when SIGTERM arrives, BOTH arms can become
        // ready essentially simultaneously — the signal handler cancels
        // the token, AND each subsystem (which is listening on the same
        // token in its own loop) starts returning `Ok(())` cleanly.
        // `tokio::select!` picks among ready arms at random, so the
        // `join_next` arm can fire instead of `shutdown.cancelled()`.
        // The `Ok(Ok(()))` branch must therefore distinguish "subsystem
        // exited while shutdown was already in progress" (expected) from
        // "subsystem exited cold" (the failure mode this watch arm
        // exists to catch). Without the `is_cancelled()` check, every
        // SIGTERM has a non-trivial chance of surfacing as a spurious
        // `status=1/FAILURE` exit and getting bounced by
        // `Restart=on-failure`. See the 2026-06-11 incident: parallel
        // `pg-agent-rs` apt restarts cascaded into a split-brain
        // because the spurious restarts gave pgpool's `failover_command`
        // a window to fire.
        let outcome: anyhow::Result<()> = tokio::select! {
            _ = shutdown.cancelled() => {
                info!("pg_agentd: shutdown signal received");
                Ok(())
            }
            Some(res) = js.join_next() => match res {
                Ok(Ok(())) => {
                    if shutdown.is_cancelled() {
                        info!(
                            "pg_agentd: subsystem returned after shutdown signal — \
                             graceful drain"
                        );
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "subsystem exited unexpectedly before shutdown"
                        ))
                    }
                }
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
// Phantom-primary startup check
// ---------------------------------------------------------------------------

/// Best-effort `stop_postgres` with one retry. Used by the
/// phantom-primary action arm: if we couldn't even stop PG, we have a
/// node with a stale-primary PostgreSQL still accepting writes from
/// anyone who bypasses pgpool — the worst possible outcome of the
/// failure mode this check exists to prevent. So a second failure
/// surfaces as `Err`, which `serve()` propagates → daemon exits
/// nonzero, systemd surfaces the failure, operator gets paged. Silent
/// log lines would not.
async fn stop_postgres_with_retry(sd: &dyn Systemd) -> anyhow::Result<()> {
    crate::retry::retry_result("stop_postgres", 2, Duration::ZERO, || sd.stop_postgres()).await
}

/// Total budget for the startup peer fan-out. Five seconds matches the
/// per-peer dial timeout in [`crate::peers::DEFAULT_CONNECT_TIMEOUT`]:
/// long enough for healthy LAN + TLS handshake to every reachable peer
/// in parallel; tight enough that a permanently-partitioned cluster
/// member doesn't drag startup. Hard-coded; if a deployment turns out
/// to need tuning, promote to a `[startup]` knob then.
const STARTUP_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// On a first-pass `Unverifiable` verdict, retry `verify_primary_at_startup`
/// up to this many additional times with [`STARTUP_CHECK_RETRY_DELAY`]
/// between attempts before treating the verdict as final. Covers the
/// rolling-deploy race observed on 2026-06-12: every node's pg_agentd
/// restarts within the same second, each daemon's localdb pool isn't
/// ready yet so peers respond with `timeline_id=0`, the quorum gate
/// fails, and a healthy primary gets a conservative stop. By the
/// second or third retry the peers have settled.
///
/// Total worst-case extra wait is `STARTUP_CHECK_RETRIES *
/// STARTUP_CHECK_RETRY_DELAY` — kept short enough that systemd's
/// `TimeoutStartSec` (default 90 s) still tolerates it on top of the
/// 5 s fan-out budget per pass. Phantom/SplitBrain verdicts skip the
/// retry — those are positive evidence; no amount of waiting changes
/// them.
const STARTUP_CHECK_RETRIES: u8 = 3;
const STARTUP_CHECK_RETRY_DELAY: Duration = Duration::from_secs(3);

/// How long [`Agent::cold_start_reconcile`] polls for a quorum lease
/// read before leaving a primary-shaped pgdata down. The poll blocks
/// `serve` ahead of `sd_notify(READY)`, so it MUST stay well inside
/// the unit's `TimeoutStartSec` (30 s in packaging/) — a 60 s first
/// cut got the daemon killed mid-poll by systemd on every greenfield
/// boot, because a virgin Debian pgdata (default cluster: PG_VERSION,
/// no standby.signal) is indistinguishable from a primary's and the
/// pre-membership store never answers. Short is also sufficient: on a
/// synchronized site restart the standby-shaped peers start without
/// consulting the store, so a quorum of raft servers exists within a
/// few seconds. A holder that boots long before its peers exhausts
/// this budget and stays down — and the site still heals, because the
/// auto-started standbys give candidacy a live flush position and the
/// majority promotes past the cold ex-holder (operator rejoin, per
/// the demote policy).
const COLD_START_QUORUM_BUDGET: Duration = Duration::from_secs(10);

/// One peer's contribution to a [`PrimaryVerdict`]. Only reachable
/// peers with a known timeline (`timeline_id > 0`) make it into the
/// observation list — unreachable peers and pre-feature peers are
/// counted via the quorum gate, not by being represented here.
#[derive(Debug, Clone)]
pub struct PeerObservation {
    pub id: i32,
    pub hostname: String,
    pub timeline_id: i32,
    pub is_in_recovery: bool,
}

/// Result of `verify_primary_at_startup`. Pure data — the
/// action policy (stop PG, log, exit) lives in [`Agent::serve`]. Keeps
/// the door open for `auto_demote_on_phantom` in a future revision
/// without disturbing the verdict shape.
#[derive(Debug)]
pub enum PrimaryVerdict {
    /// Local node is the primary AND at least
    /// `phantom_check_required_peers` peers answered with timelines ≤
    /// ours and none contradict (no peer reports a higher timeline; no
    /// peer reports the same timeline while also asserting primary).
    Confirmed,
    /// Local node is in recovery, or PG is confirmed-stopped — there's
    /// no primary role being asserted yet, so no verdict to render.
    NotApplicable,
    /// A peer reports a strictly higher timeline. Most common case: we
    /// were offline through a failover that promoted a peer.
    Phantom {
        local_tl: i32,
        peers: Vec<PeerObservation>,
    },
    /// A peer asserts primary on our same timeline. Both nodes think
    /// they own writes — split brain at the same TL. Rare but possible
    /// (two simultaneous promotions, operator error).
    SplitBrain {
        local_tl: i32,
        peers: Vec<PeerObservation>,
    },
    /// Couldn't gather enough evidence: PG-status query failed, local
    /// timeline unreadable, or fewer than `phantom_check_required_peers`
    /// peers responded with a known timeline. The conservative
    /// interpretation — refuse to assert primary, stop PG.
    Unverifiable { reason: String },
}

impl Agent {
    /// One-shot startup check that the local node is *really* the
    /// primary the cluster knows about — not a returned-from-the-dead
    /// phantom on a stale timeline. See [`PrimaryVerdict`] for the
    /// decision shape and the module-level rationale.
    ///
    /// Called from [`Agent::serve`] exactly once, after subsystems are
    /// spawned (so peers can answer our `GetStatus` outbound fan-out
    /// via the local PeerServer if they're racing the same check) and
    /// before [`crate::sdnotify::ready`]. Returns immediately on the
    /// `NotApplicable` paths (PG down, we're in recovery).
    /// Retry-wrapped wrapper around [`Self::verify_primary_at_startup`].
    /// Phantom / SplitBrain / Confirmed / NotApplicable verdicts return
    /// immediately. An `Unverifiable` verdict is retried up to
    /// [`STARTUP_CHECK_RETRIES`] more times with a delay between
    /// attempts — the peer evidence may stabilize once their daemons
    /// finish bootstrapping. The final verdict is whatever the last
    /// attempt produced.
    pub(crate) async fn verify_primary_with_retries(&self) -> PrimaryVerdict {
        self.verify_primary_with_retries_params(STARTUP_CHECK_RETRIES, STARTUP_CHECK_RETRY_DELAY)
            .await
    }

    /// Cold-start reconciliation (finding 21). A full-site power blip
    /// reboots every node with PostgreSQL down — and nothing in the
    /// steady-state design starts a Down instance: followers honor the
    /// demote policy (`converge_follow` ignores `Down`), and a holder
    /// whose PostgreSQL is down can only tick `WouldDemote`. Without
    /// this step an established cluster stays down FOREVER after a
    /// blip: lease intact in the persisted raft store, every executor
    /// waiting for an operator.
    ///
    /// The evidence that makes starting safe, by pgdata shape:
    /// - **standby-shaped** (`standby.signal` present): safe to start
    ///   unconditionally — it starts into recovery and follows whoever
    ///   the executor points it at; divergence lands in the wedge
    ///   tripwire (finding 15), never in a serving primary.
    /// - **primary-shaped**: starts ONLY on a quorum-fresh lease read
    ///   saying this node still holds the lease. While the site was
    ///   dark no rival could take over — a takeover needs the very
    ///   quorum that was down — so the persisted lease is proof. A
    ///   deposed ex-holder reads `holder != self` and stays down (the
    ///   demote policy, unchanged).
    /// - **uninitialized** (no `PG_VERSION`): nothing startable —
    ///   greenfield pre-init, or a reclone that a blip interrupted
    ///   (which is also not the holder, so it stays down either way).
    ///
    /// Runs synchronously in [`Agent::serve`] BETWEEN the phantom
    /// check and the HA-loop spawn: the loop's first tick then sees
    /// the started primary and retains, instead of racing a fence
    /// against a PostgreSQL still in crash recovery.
    ///
    /// Takes its two dependencies as arguments rather than reading
    /// `self.opts`: the reconcile is a decision about local PostgreSQL
    /// given a lease, and passing both in lets the tests drive every
    /// branch (holder / not holder / no quorum) against a real store
    /// double instead of a runtime they cannot construct.
    pub(crate) async fn cold_start_reconcile(
        &self,
        instance: &Arc<dyn pgman::instance::PostgresInstance>,
        store: &Arc<dyn crate::consensus::ConsensusStore>,
    ) {
        if !matches!(instance.state().await, pgman::instance::InstanceState::Down) {
            return;
        }
        let pgdata = &self.opts.postgres.data_dir;
        if !pgdata.join("PG_VERSION").exists() {
            return;
        }
        if pgdata.join("standby.signal").exists() {
            info!("cold start: standby-shaped pgdata with PostgreSQL down; starting as standby");
            if let Err(e) = self.deps.sd.start_postgres().await {
                warn!(err = %e, "cold start: standby start failed; leaving it to the operator");
            }
            return;
        }
        // Primary-shaped. Only the lease can say whether this node is
        // still the cluster's primary — poll for a quorum read, since
        // on a full-site restart the peers' raft servers are booting
        // too and the store answers only once a majority is back.
        let local_id = self.opts.node_pool.local_node_id;
        let deadline = std::time::Instant::now() + COLD_START_QUORUM_BUDGET;
        loop {
            match store.read_state().await {
                Ok(state) => {
                    match state.lease {
                        Some(lease) if lease.holder == local_id => {
                            info!(
                                term = lease.term,
                                "cold start: this node holds the persisted lease; starting \
                                 PostgreSQL as primary (crash recovery may run)"
                            );
                            if let Err(e) = self.deps.sd.start_postgres().await {
                                warn!(
                                    err = %e,
                                    "cold start: primary start failed; the HA loop will \
                                     report the holder unhealthy and the majority decides"
                                );
                            }
                        }
                        Some(lease) => {
                            info!(
                                holder = lease.holder,
                                "cold start: primary-shaped pgdata but another node holds \
                                 the lease; staying down (operator path: cluster recover)"
                            );
                        }
                        None => {
                            info!(
                                "cold start: primary-shaped pgdata and a vacant lease; \
                                 staying down (candidacy needs a running standby, not a \
                                 cold ex-primary)"
                            );
                        }
                    }
                    return;
                }
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        warn!(
                            err = %e,
                            budget = ?COLD_START_QUORUM_BUDGET,
                            "cold start: store unreadable past the budget with \
                             primary-shaped pgdata; leaving PostgreSQL down (operator \
                             decision)"
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Parameterised retry — production goes through
    /// [`Self::verify_primary_with_retries`] which substitutes the
    /// module constants; tests pass small values so the suite doesn't
    /// pay the production delay.
    ///
    /// Each attempt is logged so an operator can correlate a slow
    /// startup with rolling-deploy peer noise.
    pub(crate) async fn verify_primary_with_retries_params(
        &self,
        max_retries: u8,
        delay: Duration,
    ) -> PrimaryVerdict {
        let mut last = self.verify_primary_at_startup().await;
        for attempt in 1..=max_retries {
            match &last {
                PrimaryVerdict::Unverifiable { reason } => {
                    info!(
                        attempt,
                        max_attempts = max_retries,
                        ?delay,
                        reason,
                        "phantom-primary check: unverifiable; retrying after delay \
                         (peers may be mid-restart)"
                    );
                    tokio::time::sleep(delay).await;
                    last = self.verify_primary_at_startup().await;
                }
                _ => return last,
            }
        }
        last
    }

    pub(crate) async fn verify_primary_at_startup(&self) -> PrimaryVerdict {
        // 1. PG must be running primary for there to be a role to verify.
        //    "Could not query systemd" → conservative Unverifiable; "PG
        //    confirmed stopped" → NotApplicable (snapshotter will pick up
        //    if PG comes online later).
        match self.deps.sd.status_postgres().await {
            Ok(false) => return PrimaryVerdict::NotApplicable,
            Ok(true) => {}
            Err(e) => {
                return PrimaryVerdict::Unverifiable {
                    reason: format!("systemd status_postgres: {e}"),
                };
            }
        }

        match self.deps.db.is_in_recovery().await {
            Ok(true) => return PrimaryVerdict::NotApplicable,
            Ok(false) => {}
            Err(e) => {
                return PrimaryVerdict::Unverifiable {
                    reason: format!("is_in_recovery: {e}"),
                };
            }
        }

        let local_tl = match self.deps.db.timeline_id().await {
            Ok(tl) if tl > 0 => tl,
            Ok(_) => {
                return PrimaryVerdict::Unverifiable {
                    reason: "local timeline_id resolved to 0".into(),
                };
            }
            Err(e) => {
                return PrimaryVerdict::Unverifiable {
                    reason: format!("timeline_id: {e}"),
                };
            }
        };

        // 2. Fan out GetStatus to every non-local peer, all in parallel
        //    under one wall-clock budget. Per-peer errors are folded
        //    into the result list as "unreachable" (just dropped here —
        //    the quorum gate counts what survived).
        let local_id = self.opts.node_pool.local_node_id;
        let peers: Vec<crate::config::NodeConfig> = self
            .opts
            .node_pool
            .members
            .iter()
            .filter(|n| n.id != local_id)
            .cloned()
            .collect();

        // Per-peer degradation: a peer that outlives the budget shows
        // up as an Err view and simply contributes no observation —
        // the required-peers floor below decides whether what remains
        // is enough evidence. (Previously a single slow peer erased
        // every answered peer's evidence and forced Unverifiable.)
        // A real observation like any other: recording it here means a
        // node that has just booted can already witness for its peers
        // before its HA loop has ticked once.
        let views = crate::cluster_view::collect_statuses(
            self.deps.peers.clone(),
            &peers,
            STARTUP_CHECK_TIMEOUT,
            Some(&self.peer_seen),
        )
        .await;
        let mut observations: Vec<PeerObservation> = Vec::new();
        for view in views {
            match view.status {
                Ok(status) if status.timeline_id > 0 => observations.push(PeerObservation {
                    id: view.node.id,
                    hostname: view.node.hostname.clone(),
                    timeline_id: status.timeline_id,
                    is_in_recovery: status.is_in_recovery,
                }),
                Ok(_) => {
                    // Peer responded but timeline is 0 — pre-feature peer
                    // or its own timeline probe failed. Counts as "did
                    // not respond with evidence" for the quorum gate.
                    warn!(
                        peer = %view.node.hostname,
                        "phantom-primary check: peer reported timeline_id=0; no evidence"
                    );
                }
                Err(e) => {
                    warn!(
                        peer = %view.node.hostname,
                        ?e,
                        "phantom-primary check: peer status unavailable"
                    );
                }
            }
        }

        // 3. Classify. Phantom wins over SplitBrain (higher-TL evidence
        //    is the more specific failure mode); both win over the
        //    quorum gate.
        let mut phantom_peers: Vec<PeerObservation> = Vec::new();
        let mut split_peers: Vec<PeerObservation> = Vec::new();
        for obs in &observations {
            if obs.timeline_id > local_tl {
                phantom_peers.push(obs.clone());
            } else if obs.timeline_id == local_tl && !obs.is_in_recovery {
                split_peers.push(obs.clone());
            }
        }
        if !phantom_peers.is_empty() {
            return PrimaryVerdict::Phantom {
                local_tl,
                peers: phantom_peers,
            };
        }
        if !split_peers.is_empty() {
            return PrimaryVerdict::SplitBrain {
                local_tl,
                peers: split_peers,
            };
        }
        if observations.len() < self.opts.phantom_check_required_peers {
            return PrimaryVerdict::Unverifiable {
                reason: format!(
                    "only {}/{} peers answered with a known timeline",
                    observations.len(),
                    self.opts.phantom_check_required_peers
                ),
            };
        }
        PrimaryVerdict::Confirmed
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
        let (
            pg_status_res,
            pgpool_status_res,
            in_recovery_res,
            lag_res,
            timeline_res,
            wal_lsn_res,
            flush_lsn_res,
        ) = tokio::join!(
            self.deps.sd.status_postgres(),
            self.deps.sd.status_pgpool(),
            self.deps.db.is_in_recovery(),
            self.deps.db.replication_lag(),
            self.deps.db.timeline_id(),
            self.deps.db.current_wal_lsn(),
            self.deps.db.flush_lsn(),
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

        // Timeline + WAL LSN failures are NOT folded into is_ready —
        // they're informational metrics, and a transient hiccup
        // shouldn't flap readiness. Consumers treat 0 as "unknown" and
        // skip the cross-node comparison for that node.
        let timeline = timeline_res
            .inspect_err(|e| warn!(?e, "get_status: timeline_id query failed"))
            .unwrap_or(0);
        let wal_lsn = wal_lsn_res
            .inspect_err(|e| warn!(?e, "get_status: current_wal_lsn query failed"))
            .unwrap_or(0);
        let flush_lsn = flush_lsn_res
            .inspect_err(|e| warn!(?e, "get_status: flush_lsn query failed"))
            .unwrap_or(0);

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
            timeline_id: timeline,
            current_wal_lsn: wal_lsn,
            last_flush_lsn: flush_lsn,
            peer_primary_seen_age_ms: self.peer_seen.ages_ms(),
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
        timeline: i32,
        timeline_fails: AtomicBool,
        wal_lsn: u64,
        wal_lsn_fails: AtomicBool,
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_conf(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
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
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            if self.timeline_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: timeline_id boom");
            }
            Ok(self.timeline)
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            if self.wal_lsn_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: current_wal_lsn boom");
            }
            Ok(self.wal_lsn)
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            self.current_wal_lsn().await
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
        /// # of stop_postgres calls observed.
        stop_calls: std::sync::atomic::AtomicUsize,
        /// # of leading stop_postgres calls that should fail before
        /// success (counts down).
        stop_fail_count: std::sync::atomic::AtomicUsize,
        /// # of start_pgpool calls observed. Used by `agent::serve` tests
        /// to assert the supervisor was (or wasn't) spawned. Richer
        /// supervisor-behaviour assertions live in `pgpool_supervisor` with
        /// its own dedicated stub.
        start_pgpool_calls: std::sync::atomic::AtomicUsize,
        /// # of start_postgres calls observed (cold-start reconcile tests).
        start_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl Systemd for StubSd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            self.start_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            self.stop_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let remaining = self
                .stop_fail_count
                .load(std::sync::atomic::Ordering::SeqCst);
            if remaining > 0 {
                self.stop_fail_count
                    .store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
                anyhow::bail!("stub: stop_postgres boom");
            }
            Ok(())
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            self.start_pgpool_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    }

    struct StubPcp;
    #[async_trait]
    impl Pcp for StubPcp {
        async fn attach_node(&self, _: i32) -> anyhow::Result<()> {
            Ok(())
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            Ok(())
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<PcpNodeInfo>> {
            Ok(Vec::new())
        }
    }

    /// PeerClient stub answering only `get_status` from a canned value;
    /// every other method (`promote`, `stop`, `basebackup`, …) panics
    /// so a test that strays into them fails loudly. The phantom-primary
    /// check uses only `get_status`.
    struct CannedPeerClient {
        status: pb::NodeStatus,
    }
    #[async_trait]
    impl PeerClient for CannedPeerClient {
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
            Ok(self.status.clone())
        }
        async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
            unreachable!("phantom-primary check does not call get_node_config")
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn start(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn attach_node(&self, _: i32, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn stop(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn rewind(&self, _: crate::pgstandby::RewindOpts) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn basebackup(&self, _: crate::pgstandby::BasebackupOpts) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn configure_standby(
            &self,
            _: crate::pgstandby::WriteRecoveryConfOpts,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn fetch_wal(
            &self,
            _: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct StubPeers {
        /// Per-node-id canned response. Absent → `client()` errors
        /// (default behaviour matches legacy unit-struct stub).
        responses: std::sync::Mutex<std::collections::HashMap<i32, pb::NodeStatus>>,
    }

    impl StubPeers {
        fn with_responses(rs: Vec<(i32, pb::NodeStatus)>) -> Self {
            let mut map = std::collections::HashMap::new();
            for (id, s) in rs {
                map.insert(id, s);
            }
            Self {
                responses: std::sync::Mutex::new(map),
            }
        }
    }

    #[async_trait]
    impl PeerRegistry for StubPeers {
        async fn client(
            &self,
            node: &crate::config::NodeConfig,
        ) -> anyhow::Result<Arc<dyn PeerClient>> {
            let map = self.responses.lock().unwrap();
            match map.get(&node.id) {
                Some(status) => Ok(Arc::new(CannedPeerClient {
                    status: status.clone(),
                })),
                None => anyhow::bail!("stub: no peer client for id={}", node.id),
            }
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Build a NodeStatus for fan-out canned responses.
    fn ns(timeline_id: i32, is_in_recovery: bool) -> pb::NodeStatus {
        ns_with_lsn(timeline_id, is_in_recovery, 0)
    }

    fn ns_with_lsn(timeline_id: i32, is_in_recovery: bool, current_wal_lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            is_running: true,
            is_in_recovery,
            is_ready: true,
            replication_lag_bytes: 0,
            replication_state: String::new(),
            is_postgres_running: true,
            is_pgpool_running: true,
            is_postgres_status_ok: true,
            is_pgpool_status_ok: true,
            timeline_id,
            current_wal_lsn,
            last_flush_lsn: current_wal_lsn,
            peer_primary_seen_age_ms: Default::default(),
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
        async fn detach_recovery_conf(&self) -> anyhow::Result<()> {
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

    /// Minimal stub: empty store, all reads return None. Tests that
    /// exercise the inflight contract live in `inflight_ops::tests`
    /// and `localserver::tests`; agent::tests doesn't need richer
    /// behaviour from this stub.
    struct StubInflight;
    #[async_trait]
    impl crate::inflight_ops::InflightOpStore for StubInflight {
        async fn begin(
            &self,
            payload: crate::inflight_ops::InflightPayload,
            phase: &str,
            _exclusive: bool,
        ) -> anyhow::Result<crate::inflight_ops::InflightOp> {
            let now = chrono::Utc::now();
            Ok(crate::inflight_ops::InflightOp {
                id: format!("stub-{}", payload.op_name()),
                status: crate::inflight_ops::InflightStatus::InProgress,
                payload,
                phase: phase.to_string(),
                started_at: now,
                updated_at: now,
                completed_at: None,
                discharged_at: None,
                last_error: None,
            })
        }
        async fn update_phase(&self, _: &str, _: &str, _: Option<String>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn complete(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn abandon(&self, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn discharge(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn find(
            &self,
            _: &str,
            _: &str,
        ) -> anyhow::Result<Option<crate::inflight_ops::InflightOp>> {
            Ok(None)
        }
        async fn get(&self, id: &str) -> anyhow::Result<crate::inflight_ops::InflightOp> {
            anyhow::bail!("stub: get({id})")
        }
        async fn list(
            &self,
            _: &[crate::inflight_ops::InflightStatus],
        ) -> anyhow::Result<(
            Vec<crate::inflight_ops::InflightOp>,
            Vec<crate::inflight_ops::SkippedInflightOp>,
        )> {
            Ok((vec![], vec![]))
        }
        async fn sweep(&self, _: chrono::DateTime<chrono::Utc>) {}
    }

    struct StubWal;
    #[async_trait]
    impl WalStore for StubWal {
        async fn open_archive(
            &self,
            _: &str,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, pgman::walstore::WalStoreError>
        {
            Err(pgman::walstore::WalStoreError::WalNotFound("stub".into()))
        }
        async fn write_restore(
            &self,
            _: &std::path::Path,
            _: Box<dyn tokio::io::AsyncBufRead + Send + Unpin>,
        ) -> Result<(), pgman::walstore::WalStoreError> {
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
                phantom_check_required_peers: crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
                supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
                cert_reloader: None,
                ha: None,
            },
        )
    }

    fn make_deps(db: Arc<dyn LocalDb>, sd: Arc<dyn Systemd>) -> AgentDeps {
        make_deps_with_peers(db, sd, Arc::new(StubPeers::default()))
    }

    /// Agent wired for cold-start reconcile tests: a real (temp) pgdata
    /// path. The instance and the store are passed to the reconcile
    /// directly (see `cold_start_reconcile`), so these tests exercise
    /// the same code the daemon runs — including the primary-shaped
    /// arm, which used to be untestable here because the harness had no
    /// raft and the method returned early on its absence.
    fn make_agent_cold(deps: AgentDeps, data_dir: PathBuf) -> Arc<Agent> {
        let base = make_agent(deps);
        let mut opts_agent = Arc::into_inner(base).expect("sole owner");
        opts_agent.opts.postgres.data_dir = data_dir;
        Arc::new(opts_agent)
    }

    fn cold_instance(
        state: pgman::instance::InstanceState,
    ) -> Arc<dyn pgman::instance::PostgresInstance> {
        Arc::new(FixedStateInstance(state))
    }

    /// An empty store: the lease is vacant, every read succeeds. The
    /// primary-shaped arm must still refuse to start, because a vacant
    /// lease is not this node's claim to be primary.
    fn vacant_store() -> Arc<dyn crate::consensus::ConsensusStore> {
        Arc::new(crate::consensus::InMemoryConsensusStore::new())
    }

    /// Instance whose `state()` is fixed; the reconcile must never
    /// touch any other method.
    struct FixedStateInstance(pgman::instance::InstanceState);
    #[async_trait]
    impl pgman::instance::PostgresInstance for FixedStateInstance {
        async fn state(&self) -> pgman::instance::InstanceState {
            self.0.clone()
        }
        async fn promote_and_wait(&self, _: Duration) -> anyhow::Result<()> {
            unreachable!("cold start never promotes")
        }
        async fn ensure_stopped(&self) -> anyhow::Result<()> {
            unreachable!("cold start never stops")
        }
        async fn follow(&self, _: &pgman::instance::UpstreamSpec) -> anyhow::Result<()> {
            unreachable!("cold start never follows")
        }
        async fn rebuild_as_standby(
            &self,
            _: &pgman::instance::UpstreamSpec,
        ) -> anyhow::Result<()> {
            unreachable!("cold start never rebuilds")
        }
        async fn stop_receiving(&self) -> anyhow::Result<()> {
            unreachable!("cold start never detaches")
        }
    }

    // ----- cold-start reconcile (finding 21) -----------------------------

    fn cold_deps() -> (AgentDeps, Arc<StubSd>) {
        let sd = Arc::new(StubSd::default());
        (make_deps(Arc::new(StubDb::default()), sd.clone()), sd)
    }

    fn starts(sd: &StubSd) -> usize {
        sd.start_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn cold_start_starts_standby_shaped_pgdata() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("PG_VERSION"), "17\n").unwrap();
        std::fs::write(dir.path().join("standby.signal"), "").unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        agent
            .cold_start_reconcile(
                &cold_instance(pgman::instance::InstanceState::Down),
                &vacant_store(),
            )
            .await;
        assert_eq!(starts(&sd), 1, "standby-shaped pgdata is safe to start");
    }

    #[tokio::test]
    async fn cold_start_ignores_uninitialized_pgdata() {
        let dir = tempfile::tempdir().unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        agent
            .cold_start_reconcile(
                &cold_instance(pgman::instance::InstanceState::Down),
                &vacant_store(),
            )
            .await;
        assert_eq!(starts(&sd), 0, "nothing startable pre-init / mid-reclone");
    }

    /// Primary-shaped pgdata, vacant lease. Nobody has claimed the
    /// cluster, and a cold ex-primary is the one node that must not
    /// claim it by simply starting: candidacy is for running standbys
    /// that can prove a WAL position.
    #[tokio::test]
    async fn cold_start_leaves_primary_shaped_down_on_a_vacant_lease() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("PG_VERSION"), "17\n").unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        agent
            .cold_start_reconcile(
                &cold_instance(pgman::instance::InstanceState::Down),
                &vacant_store(),
            )
            .await;
        assert_eq!(starts(&sd), 0, "primary-shaped needs the lease's say-so");
    }

    /// The finding-21 case the harness could not reach before: the site
    /// came back, this node still holds the persisted lease, so it is
    /// still the primary and starts (crash recovery and all).
    #[tokio::test]
    async fn cold_start_starts_primary_shaped_pgdata_when_we_hold_the_lease() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("PG_VERSION"), "17\n").unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        let store = vacant_store();
        // local_node_id is 0 in this harness — seed the lease to us.
        store.try_takeover(0, None).await.expect("seed the lease");
        agent
            .cold_start_reconcile(&cold_instance(pgman::instance::InstanceState::Down), &store)
            .await;
        assert_eq!(starts(&sd), 1, "the lease says this node is the primary");
    }

    /// Deposed while the site was dark. The lease names someone else,
    /// so this pgdata is a stale timeline — starting it would put a
    /// second primary on the network.
    #[tokio::test]
    async fn cold_start_leaves_a_deposed_ex_primary_down() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("PG_VERSION"), "17\n").unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        let store = vacant_store();
        store.try_takeover(1, None).await.expect("seed the lease");
        agent
            .cold_start_reconcile(&cold_instance(pgman::instance::InstanceState::Down), &store)
            .await;
        assert_eq!(starts(&sd), 0, "another node holds the lease");
    }

    #[tokio::test]
    async fn cold_start_noops_when_postgres_is_running() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("PG_VERSION"), "17\n").unwrap();
        std::fs::write(dir.path().join("standby.signal"), "").unwrap();
        let (deps, sd) = cold_deps();
        let agent = make_agent_cold(deps, dir.path().to_path_buf());
        agent
            .cold_start_reconcile(
                &cold_instance(pgman::instance::InstanceState::Standby { streaming: true }),
                &vacant_store(),
            )
            .await;
        assert_eq!(starts(&sd), 0, "a running instance needs no reconciling");
    }

    fn make_deps_with_peers(
        db: Arc<dyn LocalDb>,
        sd: Arc<dyn Systemd>,
        peers: Arc<dyn PeerRegistry>,
    ) -> AgentDeps {
        AgentDeps {
            db,
            peers,
            standby: Arc::new(StubStandby),
            pcp: Arc::new(StubPcp),
            sd,
            replay: Arc::new(StubReplay),
            inflight: Arc::new(StubInflight),
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
                phantom_check_required_peers: crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
                supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
                cert_reloader: None,
                ha: None,
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

    /// Regression for the 2026-06-11 shutdown race: `tokio::select!`
    /// between `shutdown.cancelled()` and `js.join_next()` could fire
    /// the `join_next` arm even when shutdown was already in progress,
    /// returning `Err("subsystem exited unexpectedly")` and triggering
    /// systemd's `Restart=on-failure`. The fix checks
    /// `shutdown.is_cancelled()` inside the `Ok(Ok(()))` match arm.
    /// To maximise the chance of catching any regression in the
    /// select arms, this test cancels with zero sleep (subsystems will
    /// race serve() to the select) and loops several times.
    #[tokio::test]
    async fn serve_returns_ok_under_sigterm_race() {
        for iter in 0..16 {
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
                    phantom_check_required_peers:
                        crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
                    supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
                    cert_reloader: None,
                    ha: None,
                },
            );
            let shutdown = CancellationToken::new();
            let s = shutdown.clone();
            let handle = tokio::spawn(async move { agent.serve(listeners, s).await });
            // No sleep — the subsystems and the outer select! both
            // race to observe the cancellation.
            shutdown.cancel();
            let res = tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("serve did not exit within 2s of shutdown")
                .unwrap();
            assert!(
                res.is_ok(),
                "serve returned {res:?} on iteration {iter} — \
                 race regression: subsystem-exit arm fired during shutdown"
            );
        }
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
                phantom_check_required_peers: crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
                supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
                cert_reloader: None,
                ha: None,
            },
        );
        let res = agent.serve(listeners, CancellationToken::new()).await;
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("remote peers present but TLS is not configured"),
            "unexpected error: {err}"
        );
    }

    // ----- pgpool supervisor spawn matrix -------------------------------

    /// Build a minimal serve-ready Agent + listeners with the supplied
    /// deps, options overrides applied via closure.
    async fn make_serve_setup(
        db: Arc<dyn LocalDb>,
        sd: Arc<dyn Systemd>,
        peers: Arc<dyn PeerRegistry>,
        node_pool: NodePool,
        supervisor_pgpool_enabled: bool,
        phantom_check_required_peers: usize,
    ) -> (Arc<Agent>, Listeners, tempfile::TempDir) {
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
                enabled: false,
                listen_addr: "127.0.0.1:0".into(),
            },
        };
        let listeners = Listeners::bind(&serve).await.unwrap();
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: PathBuf::from("/var/lib/postgresql/17/main"),
            repl_user: "repl_user".into(),
        };
        let agent = Agent::new(
            make_deps_with_peers(db, sd, peers),
            Options {
                serve,
                node_pool,
                postgres: pg,
                maintenance_store: Arc::new(StubMaint),
                maintenance_sweep_interval: Duration::from_secs(30),
                phantom_check_required_peers,
                supervisor_pgpool_enabled,
                cert_reloader: None,
                ha: None,
            },
        );
        (agent, listeners, tmp)
    }

    fn single_node_pool() -> NodePool {
        NodePool {
            members: vec![crate::config::NodeConfig {
                id: 0,
                hostname: "localhost".into(),
            }],
            local_node_id: 0,
        }
    }

    fn two_node_pool() -> NodePool {
        NodePool {
            members: vec![
                crate::config::NodeConfig {
                    id: 0,
                    hostname: "self".into(),
                },
                crate::config::NodeConfig {
                    id: 1,
                    hostname: "peer1".into(),
                },
            ],
            local_node_id: 0,
        }
    }

    #[tokio::test]
    async fn serve_spawns_pgpool_supervisor_on_confirmed() {
        // Single-node + required_peers=0 => Confirmed without any peer
        // responses (the operational 2-node opt-out path). pgpool stub
        // reports inactive so ensure_running_once issues a start.
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            pgpool_running: false,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::default());
        let (agent, listeners, _tmp) =
            make_serve_setup(db, sd.clone(), peers, single_node_pool(), true, 0).await;
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { agent.serve(listeners, s).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            sd.start_pgpool_calls.load(Ordering::SeqCst) >= 1,
            "expected supervisor to invoke start_pgpool at least once on Confirmed verdict"
        );
    }

    #[tokio::test]
    async fn serve_does_not_spawn_pgpool_supervisor_on_phantom() {
        // 2-node + peer reports higher TL => Phantom verdict.
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            pgpool_running: false,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(8, false))]));
        let (agent, listeners, _tmp) =
            make_serve_setup(db, sd.clone(), peers, two_node_pool(), true, 1).await;
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { agent.serve(listeners, s).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert_eq!(
            sd.start_pgpool_calls.load(Ordering::SeqCst),
            0,
            "supervisor must not be spawned on Phantom verdict"
        );
        // The Phantom branch did call stop_postgres.
        assert!(sd.stop_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn serve_respects_supervisor_disabled_flag() {
        // Confirmed verdict, but supervisor_pgpool_enabled=false => no spawn.
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            pgpool_running: false,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::default());
        let (agent, listeners, _tmp) =
            make_serve_setup(db, sd.clone(), peers, single_node_pool(), false, 0).await;
        let shutdown = CancellationToken::new();
        let s = shutdown.clone();
        let handle = tokio::spawn(async move { agent.serve(listeners, s).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert_eq!(
            sd.start_pgpool_calls.load(Ordering::SeqCst),
            0,
            "supervisor must respect supervisor_pgpool_enabled=false"
        );
    }

    // ----- verify_primary_at_startup verdict matrix ---------------------

    /// Helper: build a 2-node pool (local id=0, peer id=1) and an Agent
    /// with the given (db, sd, peers) deps.
    fn make_agent_with_one_peer(
        db: Arc<dyn LocalDb>,
        sd: Arc<dyn Systemd>,
        peers: Arc<dyn PeerRegistry>,
    ) -> Arc<Agent> {
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
            members: vec![
                crate::config::NodeConfig {
                    id: 0,
                    hostname: "self".into(),
                },
                crate::config::NodeConfig {
                    id: 1,
                    hostname: "peer1".into(),
                },
            ],
            local_node_id: 0,
        };
        Agent::new(
            make_deps_with_peers(db, sd, peers),
            Options {
                serve,
                node_pool: pool,
                postgres: pg,
                maintenance_store: Arc::new(StubMaint),
                maintenance_sweep_interval: Duration::from_secs(30),
                phantom_check_required_peers: crate::config::DEFAULT_PHANTOM_CHECK_REQUIRED_PEERS,
                supervisor_pgpool_enabled: crate::config::DEFAULT_PGPOOL_SUPERVISOR_ENABLED,
                cert_reloader: None,
                ha: None,
            },
        )
    }

    #[tokio::test]
    async fn phantom_confirmed_when_peer_is_standby_on_same_tl() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(7, true))]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(matches!(v, PrimaryVerdict::Confirmed), "got {v:?}");
        assert_eq!(
            sd.stop_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "stop should not be called on Confirmed"
        );
    }

    #[tokio::test]
    async fn phantom_detected_when_peer_reports_higher_timeline() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(8, false))]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent.verify_primary_at_startup().await;
        match v {
            PrimaryVerdict::Phantom { local_tl, peers } => {
                assert_eq!(local_tl, 7);
                assert_eq!(peers.len(), 1);
                assert_eq!(peers[0].timeline_id, 8);
                assert_eq!(peers[0].hostname, "peer1");
            }
            other => panic!("expected Phantom, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn split_brain_detected_when_peer_asserts_primary_on_same_tl() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(7, false))]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent.verify_primary_at_startup().await;
        match v {
            PrimaryVerdict::SplitBrain { local_tl, peers } => {
                assert_eq!(local_tl, 7);
                assert_eq!(peers.len(), 1);
                assert!(!peers[0].is_in_recovery);
            }
            other => panic!("expected SplitBrain, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unverifiable_when_no_peers_reachable() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        // Empty responses → client() bails for every peer.
        let peers = Arc::new(StubPeers::default());
        let agent = make_agent_with_one_peer(db, sd, peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(
            matches!(v, PrimaryVerdict::Unverifiable { .. }),
            "got {v:?}"
        );
    }

    /// Peer registry that returns a different canned response per call
    /// to `client()`. Each call pops the front of the per-peer
    /// sequence; once exhausted, the last value is returned forever.
    /// Used to simulate a peer whose own daemon is mid-bootstrap on the
    /// first verification attempt and stabilises by the second —
    /// regression scaffolding for the 2026-06-12 rolling-deploy race
    /// where db1's primary got stopped because its peers reported
    /// `timeline_id=0` (their localdb pool wasn't ready yet).
    #[derive(Default)]
    struct SequencedPeers {
        seq: std::sync::Mutex<std::collections::HashMap<i32, Vec<pb::NodeStatus>>>,
    }
    impl SequencedPeers {
        fn new(rs: Vec<(i32, Vec<pb::NodeStatus>)>) -> Self {
            let mut map = std::collections::HashMap::new();
            for (id, v) in rs {
                assert!(!v.is_empty(), "SequencedPeers: empty sequence for id={id}");
                map.insert(id, v);
            }
            Self {
                seq: std::sync::Mutex::new(map),
            }
        }
    }
    #[async_trait]
    impl PeerRegistry for SequencedPeers {
        async fn client(
            &self,
            node: &crate::config::NodeConfig,
        ) -> anyhow::Result<Arc<dyn PeerClient>> {
            let mut seqs = self.seq.lock().unwrap();
            let v = seqs
                .get_mut(&node.id)
                .ok_or_else(|| anyhow::anyhow!("sequenced: no peer for id={}", node.id))?;
            let status = if v.len() > 1 {
                v.remove(0)
            } else {
                v[0].clone()
            };
            Ok(Arc::new(CannedPeerClient { status }))
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Rolling-deploy race fix: when the first attempt's peer answers
    /// with `timeline_id=0` (its localdb pool wasn't ready), the
    /// retry-wrapped verification should NOT stop PG. The second
    /// attempt sees the stabilised peer (real timeline) and resolves
    /// to Confirmed.
    #[tokio::test]
    async fn verify_with_retries_recovers_after_peer_settles() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        // First call: peer's localdb still warming up → timeline=0.
        // Second call: peer settled → timeline=7, standby. Confirmed.
        let peers = Arc::new(SequencedPeers::new(vec![(
            1,
            vec![ns(0, true), ns(7, true)],
        )]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent
            .verify_primary_with_retries_params(3, Duration::from_millis(1))
            .await;
        assert!(matches!(v, PrimaryVerdict::Confirmed), "got {v:?}");
        assert_eq!(
            sd.stop_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "stop must not be called when retry resolves to Confirmed"
        );
    }

    /// If every retry still resolves to Unverifiable, the final
    /// verdict is Unverifiable. Caller stops PG conservatively as
    /// before — the retry only buys time for transient noise to
    /// clear, it doesn't make persistent unverifiability safe.
    #[tokio::test]
    async fn verify_with_retries_remains_unverifiable_when_peers_never_settle() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        // Every call returns timeline=0; never any evidence.
        let peers = Arc::new(SequencedPeers::new(vec![(1, vec![ns(0, true)])]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent
            .verify_primary_with_retries_params(2, Duration::from_millis(1))
            .await;
        assert!(
            matches!(v, PrimaryVerdict::Unverifiable { .. }),
            "got {v:?}"
        );
    }

    /// Phantom verdicts are positive evidence — no amount of retrying
    /// changes them. The retry wrapper must return immediately on a
    /// Phantom verdict so a real split-brain returner stops PG fast.
    #[tokio::test]
    async fn verify_with_retries_returns_phantom_immediately() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        // Peer is on a higher timeline → Phantom.
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(8, false))]));
        let agent = make_agent_with_one_peer(db, sd, peers);
        let before = std::time::Instant::now();
        let v = agent
            .verify_primary_with_retries_params(3, Duration::from_millis(500))
            .await;
        let elapsed = before.elapsed();
        assert!(matches!(v, PrimaryVerdict::Phantom { .. }), "got {v:?}");
        // Should NOT have paid the retry delay even once.
        assert!(
            elapsed < Duration::from_millis(400),
            "Phantom verdict should skip retry delays; took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn not_applicable_when_local_in_recovery() {
        let db = Arc::new(StubDb {
            in_recovery: true,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(99, false))]));
        let agent = make_agent_with_one_peer(db, sd.clone(), peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(matches!(v, PrimaryVerdict::NotApplicable), "got {v:?}");
        assert_eq!(
            sd.stop_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "standby must not trigger stop"
        );
    }

    #[tokio::test]
    async fn not_applicable_when_pg_stopped() {
        let db = Arc::new(StubDb::default());
        let sd = Arc::new(StubSd {
            pg_running: false,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::default());
        let agent = make_agent_with_one_peer(db, sd, peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(matches!(v, PrimaryVerdict::NotApplicable), "got {v:?}");
    }

    #[tokio::test]
    async fn unverifiable_when_systemd_status_errors() {
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd_raw = StubSd {
            pg_running: true,
            ..Default::default()
        };
        sd_raw.pg_fails.store(true, Ordering::SeqCst);
        let sd = Arc::new(sd_raw);
        let peers = Arc::new(StubPeers::default());
        let agent = make_agent_with_one_peer(db, sd, peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(
            matches!(v, PrimaryVerdict::Unverifiable { .. }),
            "got {v:?}"
        );
    }

    #[tokio::test]
    async fn unverifiable_when_local_timeline_query_fails() {
        let db_raw = StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        };
        db_raw.timeline_fails.store(true, Ordering::SeqCst);
        let db = Arc::new(db_raw);
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(7, true))]));
        let agent = make_agent_with_one_peer(db, sd, peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(
            matches!(v, PrimaryVerdict::Unverifiable { .. }),
            "got {v:?}"
        );
    }

    #[tokio::test]
    async fn peer_reporting_timeline_zero_is_no_evidence() {
        // Single peer that reports timeline_id=0 (pre-feature or its
        // own probe failed). With required_peers=1, no evidence → Unverifiable.
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::with_responses(vec![(1, ns(0, true))]));
        let agent = make_agent_with_one_peer(db, sd, peers);
        let v = agent.verify_primary_at_startup().await;
        assert!(
            matches!(v, PrimaryVerdict::Unverifiable { .. }),
            "got {v:?}"
        );
    }

    #[tokio::test]
    async fn confirmed_when_required_peers_zero_and_no_responses() {
        // 2-node operational opt-out: required_peers=0 lets a node come
        // up alone when its peer is partitioned.
        let db = Arc::new(StubDb {
            in_recovery: false,
            timeline: 7,
            ..Default::default()
        });
        let sd = Arc::new(StubSd {
            pg_running: true,
            ..Default::default()
        });
        let peers = Arc::new(StubPeers::default());
        let mut agent_arc = make_agent_with_one_peer(db, sd, peers);
        // Mutate the required_peers field on the Options.
        // Safe — we hold the only Arc reference at this point.
        Arc::get_mut(&mut agent_arc)
            .expect("sole owner of Arc")
            .opts
            .phantom_check_required_peers = 0;
        let v = agent_arc.verify_primary_at_startup().await;
        assert!(matches!(v, PrimaryVerdict::Confirmed), "got {v:?}");
    }

    #[tokio::test]
    async fn stop_postgres_retries_once_then_fails_hard() {
        // Two leading failures → retry helper surfaces Err on the second.
        let sd = StubSd {
            pg_running: true,
            ..Default::default()
        };
        sd.stop_fail_count
            .store(2, std::sync::atomic::Ordering::SeqCst);
        let sd: Arc<dyn Systemd> = Arc::new(sd);
        let err = stop_postgres_with_retry(sd.as_ref())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("after 2 attempts")
                && err.contains("attempt 1")
                && err.contains("attempt 2"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn stop_postgres_succeeds_after_one_failure() {
        let sd = StubSd {
            pg_running: true,
            ..Default::default()
        };
        sd.stop_fail_count
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let sd: Arc<dyn Systemd> = Arc::new(sd);
        stop_postgres_with_retry(sd.as_ref()).await.unwrap();
    }
}
