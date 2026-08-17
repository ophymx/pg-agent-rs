//! `/healthz` plain-HTTP listener. See SPEC §9 for the design rationale
//! (one endpoint, status code is the contract, no TLS).
//!
//! # One endpoint, status code is the contract
//!
//! `GET /healthz` returns:
//!
//! - **200** iff the latest snapshot is fresh (`age < STALE_AFTER`) AND
//!   pgpool reports at least one backend with `status_code < 3` (i.e.
//!   `"up"` or `"waiting"` — both are routable per pgpool docs).
//! - **503** otherwise: stale snapshot (probe loop wedged → process
//!   wedged), pgpool unreachable, or every backend `down`.
//! - **405** for methods other than GET / HEAD (handled by axum's
//!   method router; we register both verbs on the same handler).
//!
//! The JSON body carries operational state for operators
//! (`role`, per-backend list, replication lag, snapshot age) but it is
//! **not part of the status-code contract**. Error strings from failed
//! probes go to `tracing::warn` instead of the body — see SPEC §9.2 for
//! why this is plain HTTP rather than HTTPS.
//!
//! # Hot path
//!
//! `handle_healthz` does an `ArcSwap::load` of the snapshot, computes
//! the 200/503 verdict, marshals JSON. **No DB or PCP calls on the
//! request path.** The background snapshotter (`HealthSnapshotter::run`)
//! does the actual probing every ~1 s on its own task.
//!
//! # Lifecycle
//!
//! Daemon main:
//!
//! 1. `TcpListener::bind` the healthz port synchronously.
//! 2. `HealthSnapshotter::new(db, pcp)`.
//! 3. `snapshotter.probe_once().await` — initial sync probe so the
//!    very first request after `sd_notify::ready()` sees a real
//!    snapshot, not 503 (SPEC §9.3, §12).
//! 4. Spawn `snapshotter.run(shutdown)` for the ticking loop.
//! 5. Spawn `serve_healthz(listener, snapshotter, STALE_AFTER, shutdown)`.
//! 6. `sd_notify::ready()`.

use crate::localdb::{LocalDb, ReplicationLag};
use crate::pcp::{NodeInfo, Pcp};
use arc_swap::ArcSwapOption;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Tuning constants (rationale in SPEC §9.3)
// ---------------------------------------------------------------------------

/// Snapshot probe cadence. Must be shorter than HAProxy's `fastinter 2s`
/// so state transitions surface within a single check.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Per-sub-probe deadline. Keeps a hung DB / PCP from stalling the loop.
/// Tight enough that even a worst-case 500 ms hang on postgres still
/// lets pgpool's probe complete within the same tick.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Snapshot age past which we flip `/healthz` to 503 — distinguishes
/// "probe loop wedged" from "pgpool just answered and said it's down".
pub const STALE_AFTER: Duration = Duration::from_secs(30);

/// Reserved for the daemon's graceful-shutdown deadline around the
/// healthz serve task.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Snapshot shape
// ---------------------------------------------------------------------------

/// Local node's role from postgres's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthRole {
    /// `pg_is_in_recovery()` errored or timed out — we genuinely don't know.
    #[default]
    Unknown,
    /// Postgres reports `pg_is_in_recovery() = false`.
    Primary,
    /// Postgres reports `pg_is_in_recovery() = true`.
    Replica,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PostgresProbe {
    /// `is_in_recovery` query succeeded within the probe deadline.
    pub reachable: bool,
    /// Only meaningful when `reachable == true`.
    pub in_recovery: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PgpoolProbe {
    /// `pcp_node_info -a` succeeded within the probe deadline.
    pub reachable: bool,
    /// Per-backend snapshot, one entry per `[[pool]]` member in pgpool's
    /// configured order. Empty when `reachable == false`.
    pub backends: Vec<BackendStatus>,
}

/// Curated subset of [`NodeInfo`] for the JSON body. The full 11-field
/// view is available via `Pcp::node_info_all` for consumers that want
/// more (preflight, eventual `/metrics`, `pg_agentctl cluster status`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct BackendStatus {
    pub id: i32,
    pub hostname: String,
    /// `"primary"` / `"standby"` (streaming replication) or `"main"` /
    /// `"replica"` (other modes), as pgpool reports it.
    pub role: String,
    /// `"up"` / `"waiting"` / `"down"` — pgpool's textual status. `"up"`
    /// and `"waiting"` both count toward the readiness gate.
    pub status: String,
    /// `"streaming"` / `"catchup"` / `"none"` / etc.
    pub replication_state: String,
}

impl BackendStatus {
    pub fn from_node_info(n: &NodeInfo) -> Self {
        Self {
            id: n.id,
            hostname: n.hostname.clone(),
            role: n.role.clone(),
            status: n.status_name.clone(),
            replication_state: n.replication_state.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReplicationProbe {
    pub lag_bytes: i64,
    /// `"streaming"` / `"catchup"` / `""` (no receiver / primary).
    pub wal_receiver_state: String,
}

/// In-memory snapshot fed by the background probe loop and read by the
/// HTTP handler. `timestamp` is not on the wire — it's used to compute
/// `snapshot_age_ms` and the stale-gate verdict at request time.
/// Quorum-commit posture (docs/quorum-commit.md §5), primaries only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncCommitState {
    /// Standby / role unknown / probe failed — no claim.
    #[default]
    #[serde(rename = "n/a")]
    NotApplicable,
    /// `synchronous_standby_names` empty: bootstrap pre-first-standby,
    /// or the operator's allow-async escape hatch. Acknowledged writes
    /// are single-copy promises while this shows.
    Disarmed,
    /// Armed and at least one member standby connected: acknowledged
    /// commits are on ≥ 2 nodes.
    Armed,
    /// Armed with NO member standby connected: commits are hanging.
    /// This is a page.
    Blocked,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub timestamp: DateTime<Utc>,
    pub role: HealthRole,
    pub postgres: PostgresProbe,
    pub pgpool: PgpoolProbe,
    pub replication: ReplicationProbe,
    pub sync_commit: SyncCommitState,
    /// The executor's finding-15 wedge flag: a follow it confirmed has
    /// not streamed past the grace window. Redundancy is degraded
    /// until the node is rebuilt (`cluster recover`).
    pub follow_wedged: bool,
}

// ---------------------------------------------------------------------------
// HealthSnapshotter
// ---------------------------------------------------------------------------

pub struct HealthSnapshotter {
    db: Arc<dyn LocalDb>,
    pcp: Arc<dyn Pcp>,
    interval: Duration,
    probe_timeout: Duration,
    snap: ArcSwapOption<HealthSnapshot>,
    /// Shared with the role executor; read into each snapshot.
    follow_wedged: Arc<AtomicBool>,
}

impl HealthSnapshotter {
    pub fn new(db: Arc<dyn LocalDb>, pcp: Arc<dyn Pcp>) -> Self {
        Self::with_params(db, pcp, POLL_INTERVAL, PROBE_TIMEOUT)
    }

    pub fn with_params(
        db: Arc<dyn LocalDb>,
        pcp: Arc<dyn Pcp>,
        interval: Duration,
        probe_timeout: Duration,
    ) -> Self {
        Self {
            db,
            pcp,
            interval,
            probe_timeout,
            snap: ArcSwapOption::from(None),
            follow_wedged: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wire the role executor's finding-15 wedge flag so it surfaces
    /// in every snapshot as `follow_wedged`.
    pub fn with_follow_wedged(mut self, flag: Arc<AtomicBool>) -> Self {
        self.follow_wedged = flag;
        self
    }

    /// Snapshot, or `None` if [`HealthSnapshotter::probe_once`] hasn't
    /// completed yet. The handler treats `None` as 503.
    pub fn load(&self) -> Option<Arc<HealthSnapshot>> {
        self.snap.load_full()
    }

    /// Run a single probe and store the result. Called once
    /// synchronously by daemon main before `sd_notify::ready()` (so the
    /// very first request after READY sees a real snapshot, not 503),
    /// then on every tick inside `run`.
    pub async fn probe_once(&self) {
        let snap = build_snapshot(
            self.db.as_ref(),
            self.pcp.as_ref(),
            self.probe_timeout,
            self.follow_wedged.load(Ordering::SeqCst),
        )
        .await;
        debug!(
            role = ?snap.role,
            postgres_reachable = snap.postgres.reachable,
            pgpool_reachable = snap.pgpool.reachable,
            backends = snap.pgpool.backends.len(),
            "healthsnap: stored snapshot"
        );
        self.snap.store(Some(Arc::new(snap)));
    }

    /// Run the probe loop until `shutdown` is cancelled. Each tick fires
    /// after `interval` of the previous tick's completion.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(self.interval) => {
                    self.probe_once().await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Probes (free fns — pure modulo Utc::now and the trait calls)
// ---------------------------------------------------------------------------

async fn build_snapshot(
    db: &dyn LocalDb,
    pcp: &dyn Pcp,
    probe_timeout: Duration,
    follow_wedged: bool,
) -> HealthSnapshot {
    // Probe postgres and pgpool concurrently. tokio::join! drives both
    // futures in the same task — independent deadlines, no cross-stall.
    let (pg_result, pgpool_probe) = tokio::join!(
        probe_postgres(db, probe_timeout),
        probe_pgpool(pcp, probe_timeout),
    );
    let (role, postgres, replication) = pg_result;
    let sync_commit = if role == HealthRole::Primary {
        probe_sync_commit(db, probe_timeout).await
    } else {
        SyncCommitState::NotApplicable
    };
    HealthSnapshot {
        timestamp: Utc::now(),
        role,
        postgres,
        pgpool: pgpool_probe,
        replication,
        sync_commit,
        follow_wedged,
    }
}

/// Quorum-commit posture on a primary: `disarmed` when
/// `synchronous_standby_names` is empty, otherwise `armed` iff at
/// least one member standby is connected — `blocked` means commits
/// are currently hanging for want of an ack source. Inferred from
/// state, never probed with a write.
async fn probe_sync_commit(db: &dyn LocalDb, probe_timeout: Duration) -> SyncCommitState {
    let names =
        match tokio::time::timeout(probe_timeout, db.setting("synchronous_standby_names")).await {
            Ok(Ok(v)) => v,
            _ => return SyncCommitState::NotApplicable,
        };
    if names.trim().is_empty() {
        return SyncCommitState::Disarmed;
    }
    match tokio::time::timeout(probe_timeout, db.connected_standby_names()).await {
        Ok(Ok(connected)) if !connected.is_empty() => SyncCommitState::Armed,
        Ok(Ok(_)) => SyncCommitState::Blocked,
        _ => SyncCommitState::NotApplicable,
    }
}

/// Returns the role + postgres reachability state + replication lag
/// state derived from `LocalDb::is_in_recovery` + `replication_lag`.
/// Failures (error, timeout) collapse to `reachable=false` +
/// `role=Unknown` + default `ReplicationProbe`; the error message goes
/// to `tracing::warn` rather than the wire body.
async fn probe_postgres(
    db: &dyn LocalDb,
    probe_timeout: Duration,
) -> (HealthRole, PostgresProbe, ReplicationProbe) {
    let in_recovery = match tokio::time::timeout(probe_timeout, db.is_in_recovery()).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            warn!(?e, "healthsnap: is_in_recovery failed");
            return (
                HealthRole::Unknown,
                PostgresProbe {
                    reachable: false,
                    in_recovery: false,
                },
                ReplicationProbe::default(),
            );
        }
        Err(_) => {
            warn!(
                timeout_ms = probe_timeout.as_millis() as u64,
                "healthsnap: is_in_recovery timed out"
            );
            return (
                HealthRole::Unknown,
                PostgresProbe {
                    reachable: false,
                    in_recovery: false,
                },
                ReplicationProbe::default(),
            );
        }
    };

    let role = if in_recovery {
        HealthRole::Replica
    } else {
        HealthRole::Primary
    };

    let lag = match tokio::time::timeout(probe_timeout, db.replication_lag()).await {
        Ok(Ok(l)) => l,
        Ok(Err(e)) => {
            warn!(?e, "healthsnap: replication_lag failed");
            ReplicationLag::default()
        }
        Err(_) => {
            warn!(
                timeout_ms = probe_timeout.as_millis() as u64,
                "healthsnap: replication_lag timed out"
            );
            ReplicationLag::default()
        }
    };

    (
        role,
        PostgresProbe {
            reachable: true,
            in_recovery,
        },
        ReplicationProbe {
            lag_bytes: lag.bytes,
            wal_receiver_state: lag.state,
        },
    )
}

async fn probe_pgpool(pcp: &dyn Pcp, probe_timeout: Duration) -> PgpoolProbe {
    let nodes = match tokio::time::timeout(probe_timeout, pcp.node_info_all()).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            // Fires every healthsnap tick (~1s) when pgpool isn't
            // running — common during cluster_init and after a
            // legitimate pgpool stop. Operators discover the state
            // via /healthz; the per-tick log line was just noise.
            debug!(?e, "healthsnap: pcp_node_info -a failed");
            return PgpoolProbe {
                reachable: false,
                backends: vec![],
            };
        }
        Err(_) => {
            debug!(
                timeout_ms = probe_timeout.as_millis() as u64,
                "healthsnap: pcp_node_info -a timed out"
            );
            return PgpoolProbe {
                reachable: false,
                backends: vec![],
            };
        }
    };
    let backends = nodes.iter().map(BackendStatus::from_node_info).collect();
    PgpoolProbe {
        reachable: true,
        backends,
    }
}

// ---------------------------------------------------------------------------
// Wire body + handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct HealthState {
    snapshotter: Arc<HealthSnapshotter>,
    stale_after: Duration,
    /// Flips to `true` after `Agent::verify_primary_at_startup` resolves.
    /// While `false`, `/healthz` short-circuits to 503 so HAProxy can't
    /// route traffic at a node whose role hasn't been validated against
    /// the rest of the cluster yet.
    startup_verified: Arc<AtomicBool>,
}

#[derive(Debug, Serialize)]
struct HealthBody {
    ready: bool,
    snapshot_age_ms: i64,
    role: HealthRole,
    postgres: PostgresProbe,
    pgpool: PgpoolProbe,
    replication: ReplicationProbe,
    sync_commit: SyncCommitState,
    follow_wedged: bool,
}

/// Pure verdict + body builder — tests call this directly without
/// spinning up axum. The handler is a thin wrapper around it.
fn compute_health(
    snap: Option<Arc<HealthSnapshot>>,
    stale_after: Duration,
    now: DateTime<Utc>,
) -> (StatusCode, HealthBody) {
    let Some(s) = snap else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HealthBody {
                ready: false,
                snapshot_age_ms: 0,
                role: HealthRole::Unknown,
                postgres: PostgresProbe::default(),
                pgpool: PgpoolProbe::default(),
                replication: ReplicationProbe::default(),
                sync_commit: SyncCommitState::NotApplicable,
                follow_wedged: false,
            },
        );
    };

    let age = now - s.timestamp;
    let age_ms = age.num_milliseconds().max(0);
    let stale = chrono::Duration::from_std(stale_after).unwrap_or(chrono::Duration::seconds(30));
    let fresh = age < stale;
    let any_up = s.pgpool.reachable && s.pgpool.backends.iter().any(|b| backend_is_up(&b.status));
    let ready = fresh && any_up;

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = HealthBody {
        ready,
        snapshot_age_ms: age_ms,
        role: s.role,
        postgres: s.postgres.clone(),
        pgpool: s.pgpool.clone(),
        replication: s.replication.clone(),
        sync_commit: s.sync_commit,
        follow_wedged: s.follow_wedged,
    };
    (status, body)
}

/// pgpool's textual status name → "is this backend routable?". Both
/// `"up"` (status code 2) and `"waiting"` (status code 1) count as
/// routable; `"down"` (status code 3) does not.
fn backend_is_up(status: &str) -> bool {
    matches!(status, "up" | "waiting")
}

async fn handle_healthz(State(state): State<HealthState>) -> Response {
    if !state.startup_verified.load(Ordering::SeqCst) {
        let body = HealthBody {
            ready: false,
            snapshot_age_ms: 0,
            role: HealthRole::Unknown,
            postgres: PostgresProbe::default(),
            pgpool: PgpoolProbe::default(),
            replication: ReplicationProbe::default(),
            sync_commit: SyncCommitState::NotApplicable,
            follow_wedged: false,
        };
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    let snap = state.snapshotter.load();
    let (status, body) = compute_health(snap, state.stale_after, Utc::now());
    (status, Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// Server wiring
// ---------------------------------------------------------------------------

/// Build the axum router. Exposed (pub(crate)) so tests can hit it
/// directly via `tower::ServiceExt::oneshot` without binding a socket.
pub(crate) fn build_router(
    snapshotter: Arc<HealthSnapshotter>,
    stale_after: Duration,
    startup_verified: Arc<AtomicBool>,
) -> Router {
    let state = HealthState {
        snapshotter,
        stale_after,
        startup_verified,
    };
    Router::new()
        // GET + HEAD share the handler; axum strips the body for HEAD.
        // Methods other than GET/HEAD auto-405 via the method router.
        .route("/healthz", get(handle_healthz).head(handle_healthz))
        .with_state(state)
}

/// Serve `/healthz` until `shutdown` fires. Axum's
/// `with_graceful_shutdown` stops accepting new connections and waits
/// for in-flight requests to finish; the daemon main wraps this in a
/// `tokio::time::timeout(SHUTDOWN_GRACE, ...)` to bound the wait.
///
/// `startup_verified` gates `/healthz` returning 200 — until
/// `Agent::verify_primary_at_startup` resolves the verdict, every
/// request gets 503 regardless of snapshot state. Prevents HAProxy from
/// routing at a node whose role has not yet been validated against the
/// rest of the cluster.
pub async fn serve_healthz(
    listener: tokio::net::TcpListener,
    snapshotter: Arc<HealthSnapshotter>,
    stale_after: Duration,
    startup_verified: Arc<AtomicBool>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    info!(?addr, "healthz: listening");
    let app = build_router(snapshotter, stale_after, startup_verified);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    // ----- stubs -----------------------------------------------------------

    /// LocalDb stub — only `is_in_recovery` + `replication_lag` are used
    /// by the snapshotter; every other trait method panics so a test
    /// that wanders into them fails loudly.
    struct StubDb {
        in_recovery: Mutex<Result<bool, String>>,
        replication_lag: Mutex<Result<ReplicationLag, String>>,
    }
    impl StubDb {
        fn primary() -> Self {
            Self {
                in_recovery: Mutex::new(Ok(false)),
                replication_lag: Mutex::new(Ok(ReplicationLag::default())),
            }
        }
        fn replica_with_lag(bytes: i64, state: &str) -> Self {
            Self {
                in_recovery: Mutex::new(Ok(true)),
                replication_lag: Mutex::new(Ok(ReplicationLag {
                    bytes,
                    state: state.to_string(),
                })),
            }
        }
        fn unreachable() -> Self {
            Self {
                in_recovery: Mutex::new(Err("connection refused".into())),
                replication_lag: Mutex::new(Err("connection refused".into())),
            }
        }
    }
    #[async_trait]
    impl LocalDb for StubDb {
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
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            match self.in_recovery.lock().unwrap().clone() {
                Ok(v) => Ok(v),
                Err(m) => anyhow::bail!(m),
            }
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            match self.replication_lag.lock().unwrap().clone() {
                Ok(v) => Ok(v),
                Err(m) => anyhow::bail!(m),
            }
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("snapshotter doesn't use promote")
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            // The sync-commit probe reads synchronous_standby_names on
            // primaries; empty = disarmed is the neutral answer.
            Ok(String::new())
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    struct StubPcp {
        nodes: Mutex<Result<Vec<NodeInfo>, String>>,
    }
    impl StubPcp {
        fn with_nodes(nodes: Vec<NodeInfo>) -> Self {
            Self {
                nodes: Mutex::new(Ok(nodes)),
            }
        }
        fn always_fail(msg: &str) -> Self {
            Self {
                nodes: Mutex::new(Err(msg.to_string())),
            }
        }
    }
    #[async_trait]
    impl Pcp for StubPcp {
        async fn attach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<NodeInfo>> {
            match self.nodes.lock().unwrap().clone() {
                Ok(v) => Ok(v),
                Err(m) => anyhow::bail!(m),
            }
        }
    }

    fn make_node(id: i32, hostname: &str, status_code: u8, status: &str, role: &str) -> NodeInfo {
        NodeInfo {
            id,
            hostname: hostname.into(),
            port: 5432,
            status_code,
            lb_weight: 1.0,
            status_name: status.into(),
            actual_status: status.into(),
            role: role.into(),
            actual_role: role.into(),
            replication_delay: "0".into(),
            replication_state: if role == "primary" {
                "none".into()
            } else {
                "streaming".into()
            },
            sync_state: "none".into(),
        }
    }

    // ----- enum + helpers --------------------------------------------------

    #[test]
    fn health_role_serialises_lowercase() {
        assert_eq!(
            serde_json::to_string(&HealthRole::Primary).unwrap(),
            "\"primary\""
        );
        assert_eq!(
            serde_json::to_string(&HealthRole::Replica).unwrap(),
            "\"replica\""
        );
        assert_eq!(
            serde_json::to_string(&HealthRole::Unknown).unwrap(),
            "\"unknown\""
        );
    }

    #[test]
    fn backend_status_from_node_info_projects_subset() {
        let n = make_node(2, "server3", 2, "up", "standby");
        let b = BackendStatus::from_node_info(&n);
        assert_eq!(b.id, 2);
        assert_eq!(b.hostname, "server3");
        assert_eq!(b.role, "standby");
        assert_eq!(b.status, "up");
        assert_eq!(b.replication_state, "streaming");
    }

    #[test]
    fn backend_is_up_recognises_routable_states() {
        assert!(backend_is_up("up"));
        assert!(backend_is_up("waiting"));
        assert!(!backend_is_up("down"));
        assert!(!backend_is_up(""));
        assert!(!backend_is_up("future-state"));
    }

    // ----- probes (with stubs) ---------------------------------------------

    #[tokio::test]
    async fn probe_postgres_primary_returns_primary_role() {
        let db = StubDb::primary();
        let (role, pg, rep) = probe_postgres(&db, Duration::from_secs(1)).await;
        assert_eq!(role, HealthRole::Primary);
        assert!(pg.reachable);
        assert!(!pg.in_recovery);
        assert_eq!(rep.lag_bytes, 0);
        assert_eq!(rep.wal_receiver_state, "");
    }

    #[tokio::test]
    async fn probe_postgres_replica_carries_lag() {
        let db = StubDb::replica_with_lag(2048, "streaming");
        let (role, pg, rep) = probe_postgres(&db, Duration::from_secs(1)).await;
        assert_eq!(role, HealthRole::Replica);
        assert!(pg.reachable);
        assert!(pg.in_recovery);
        assert_eq!(rep.lag_bytes, 2048);
        assert_eq!(rep.wal_receiver_state, "streaming");
    }

    #[tokio::test]
    async fn probe_postgres_unreachable_sets_role_unknown() {
        let db = StubDb::unreachable();
        let (role, pg, rep) = probe_postgres(&db, Duration::from_secs(1)).await;
        assert_eq!(role, HealthRole::Unknown);
        assert!(!pg.reachable);
        assert_eq!(rep, ReplicationProbe::default());
    }

    #[tokio::test]
    async fn probe_pgpool_projects_backends() {
        let pcp = StubPcp::with_nodes(vec![
            make_node(0, "s1", 2, "up", "primary"),
            make_node(1, "s2", 2, "up", "standby"),
            make_node(2, "s3", 3, "down", "standby"),
        ]);
        let probe = probe_pgpool(&pcp, Duration::from_secs(1)).await;
        assert!(probe.reachable);
        assert_eq!(probe.backends.len(), 3);
        assert_eq!(probe.backends[0].status, "up");
        assert_eq!(probe.backends[2].status, "down");
    }

    #[tokio::test]
    async fn probe_pgpool_unreachable_yields_empty_backends() {
        let pcp = StubPcp::always_fail("PCP socket refused");
        let probe = probe_pgpool(&pcp, Duration::from_secs(1)).await;
        assert!(!probe.reachable);
        assert!(probe.backends.is_empty());
    }

    // ----- compute_health verdict matrix -----------------------------------

    fn healthy_snapshot() -> Arc<HealthSnapshot> {
        Arc::new(HealthSnapshot {
            timestamp: Utc::now(),
            role: HealthRole::Primary,
            postgres: PostgresProbe {
                reachable: true,
                in_recovery: false,
            },
            pgpool: PgpoolProbe {
                reachable: true,
                backends: vec![
                    BackendStatus {
                        id: 0,
                        hostname: "s1".into(),
                        role: "primary".into(),
                        status: "up".into(),
                        replication_state: "none".into(),
                    },
                    BackendStatus {
                        id: 1,
                        hostname: "s2".into(),
                        role: "standby".into(),
                        status: "up".into(),
                        replication_state: "streaming".into(),
                    },
                ],
            },
            replication: ReplicationProbe::default(),
            sync_commit: SyncCommitState::Armed,
            follow_wedged: false,
        })
    }

    #[test]
    fn compute_health_returns_503_when_no_snapshot() {
        let (status, body) = compute_health(None, STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.ready);
        assert_eq!(body.role, HealthRole::Unknown);
        assert_eq!(body.snapshot_age_ms, 0);
    }

    #[test]
    fn compute_health_returns_200_when_fresh_and_backends_up() {
        let (status, body) = compute_health(Some(healthy_snapshot()), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::OK);
        assert!(body.ready);
        assert_eq!(body.role, HealthRole::Primary);
        assert!(body.snapshot_age_ms < 100);
    }

    #[test]
    fn compute_health_returns_503_when_stale() {
        let mut snap = (*healthy_snapshot()).clone();
        snap.timestamp = Utc::now() - chrono::Duration::seconds(60); // > STALE_AFTER
        let (status, body) = compute_health(Some(Arc::new(snap)), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.ready);
        // Body still carries the role + backends — stale doesn't mean
        // we lose the operator's diagnostic info, just the readiness.
        assert_eq!(body.role, HealthRole::Primary);
        assert!(body.snapshot_age_ms >= 60_000);
    }

    #[test]
    fn compute_health_returns_503_when_no_backends_up() {
        let mut snap = (*healthy_snapshot()).clone();
        for b in &mut snap.pgpool.backends {
            b.status = "down".into();
        }
        let (status, body) = compute_health(Some(Arc::new(snap)), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.ready);
    }

    #[test]
    fn compute_health_returns_200_when_at_least_one_waiting_backend() {
        // "waiting" is up-but-no-connections (status_code 1) — still
        // routable per pgpool docs, must count toward readiness.
        let mut snap = (*healthy_snapshot()).clone();
        for b in &mut snap.pgpool.backends {
            b.status = "down".into();
        }
        snap.pgpool.backends[0].status = "waiting".into();
        let (status, _) = compute_health(Some(Arc::new(snap)), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn compute_health_returns_503_when_pgpool_unreachable() {
        let mut snap = (*healthy_snapshot()).clone();
        snap.pgpool.reachable = false;
        snap.pgpool.backends.clear();
        let (status, _) = compute_health(Some(Arc::new(snap)), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // ----- wire shape ------------------------------------------------------

    #[test]
    fn body_serialises_to_spec_shape() {
        let (_status, body) = compute_health(Some(healthy_snapshot()), STALE_AFTER, Utc::now());
        let json: serde_json::Value = serde_json::to_value(&body).unwrap();

        // Top-level fields per SPEC §9.4.
        assert_eq!(json["ready"], true);
        assert_eq!(json["role"], "primary");
        assert!(json["snapshot_age_ms"].is_i64());

        // Nested probes, no error strings exposed.
        assert_eq!(json["postgres"]["reachable"], true);
        assert_eq!(json["postgres"]["in_recovery"], false);
        assert!(json["postgres"].get("error").is_none());

        assert_eq!(json["pgpool"]["reachable"], true);
        assert!(json["pgpool"].get("error").is_none());
        let backends = json["pgpool"]["backends"].as_array().unwrap();
        assert_eq!(backends.len(), 2);
        assert_eq!(backends[0]["id"], 0);
        assert_eq!(backends[0]["status"], "up");
        assert_eq!(backends[0]["role"], "primary");

        assert_eq!(json["replication"]["lag_bytes"], 0);
        // timestamp must NOT appear on the wire — internal only.
        assert!(json.get("timestamp").is_none());
    }

    // ----- snapshotter lifecycle -------------------------------------------

    #[tokio::test]
    async fn snapshotter_load_returns_none_before_first_probe() {
        let db = Arc::new(StubDb::primary());
        let pcp = Arc::new(StubPcp::with_nodes(vec![]));
        let s = HealthSnapshotter::new(db, pcp);
        assert!(s.load().is_none());
    }

    #[tokio::test]
    async fn snapshotter_probe_once_stores_snapshot() {
        let db = Arc::new(StubDb::primary());
        let pcp = Arc::new(StubPcp::with_nodes(vec![make_node(
            0, "s1", 2, "up", "primary",
        )]));
        let s = HealthSnapshotter::new(db, pcp);
        s.probe_once().await;
        let snap = s.load().expect("snapshot stored");
        assert_eq!(snap.role, HealthRole::Primary);
        assert!(snap.pgpool.reachable);
        assert_eq!(snap.pgpool.backends.len(), 1);
    }

    #[tokio::test]
    async fn snapshotter_handles_both_probes_failing() {
        let db = Arc::new(StubDb::unreachable());
        let pcp = Arc::new(StubPcp::always_fail("boom"));
        let s = HealthSnapshotter::new(db, pcp);
        s.probe_once().await;
        let snap = s
            .load()
            .expect("snapshot still stored (with reachable=false)");
        assert_eq!(snap.role, HealthRole::Unknown);
        assert!(!snap.postgres.reachable);
        assert!(!snap.pgpool.reachable);
        // /healthz would now return 503 against this snapshot.
        let (status, _) = compute_health(Some(snap), STALE_AFTER, Utc::now());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn snapshotter_run_stops_on_shutdown() {
        let db = Arc::new(StubDb::primary());
        let pcp = Arc::new(StubPcp::with_nodes(vec![make_node(
            0, "s1", 2, "up", "primary",
        )]));
        let s = Arc::new(HealthSnapshotter::with_params(
            db,
            pcp,
            Duration::from_millis(20),
            Duration::from_millis(100),
        ));
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn({
            let s = s.clone();
            let shutdown = shutdown.clone();
            async move { s.run(shutdown).await }
        });
        // Let a few ticks fire.
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        // run() must return promptly after cancellation.
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("run() did not exit within 1s of shutdown")
            .expect("task panicked");
    }
}
