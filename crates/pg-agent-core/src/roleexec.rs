//! The HA loop's executors (promotion-authority §10, step 7).
//!
//! [`RoleExecutor`] consumes [`HaDecision`]s and drives the local
//! PostgreSQL instance toward what the lease says. The split matters:
//! the loop stays a pure decision function (its tests stay
//! deterministic, its tick stays cheap), and **shadow mode is the
//! executor's absence** — the same structural guarantee shadow always
//! had, now expressed at the composition root instead of inside the
//! loop.
//!
//! # The contract is convergence
//!
//! The loop re-derives its decision every tick, so the executor will
//! see the same decision repeatedly and every action must be
//! ensure-shaped: promote-when-not-primary, stop-when-should-not-serve,
//! follow-when-not-following. There is no transition tracking to get
//! out of sync — a crashed executor resumes by doing whatever the next
//! tick's decision implies.
//!
//! # Decision → action
//!
//! | Decision | Action |
//! |---|---|
//! | `TookOver{already_primary: false}`, `AwaitingPromotion` | journaled [`PostgresInstance::promote_and_wait`] |
//! | `WouldDemote` | [`PostgresInstance::ensure_stopped`] — fencing |
//! | `Following`, `WouldFollowNewHolder` | converge onto the holder (see below) |
//! | everything else | none — they are watching states |
//!
//! The follow path is state-dependent, and one of its branches is the
//! most important line in this module: a node that is **running as
//! primary while someone else holds the lease** is fenced
//! (`ensure_stopped`), because that is the stale-primary half of §2.1
//! — the node that kept serving on the wrong side of a partition. The
//! decision layer reports `Following` for it (the holder looks healthy);
//! the executor is what knows the local instance's role contradicts the
//! lease.
//!
//! A `Down` instance under `Following` is deliberately left alone:
//! demote policy is stop-and-wait, and rejoining a demoted ex-primary
//! (`cluster recover`) is the operator's call. The executor never
//! performs a destructive rebuild.
//!
//! # pgpool self-attach (testing/README.md finding 16)
//!
//! In a pgpool-routed deployment, part of what "primary" means is that
//! the local pgpool routes to it. `failover_on_backend_error` can
//! degenerate the new primary's backend *on its own instance* (observed
//! ~20 s **after** a promotion, on a transient connection error during
//! the takeover churn), and the §4 contract makes that permanent:
//! `auto_failback off`, and pgpool never health-checks a down backend.
//! That instance then blackholes writes, and later pcp attaches wedge
//! in `find_primary_node_repeatedly` because its map holds no primary.
//!
//! So the holder converges this too: on primary-holder ticks (and after
//! a promotion completes) the executor probes the local pgpool's view
//! of its own backend and re-attaches it when marked down. The probe is
//! **spawned off the tick** — a wedged pgpool must never stall the HA
//! loop past `retry_timeout`/`leader_ttl` (the finding 11/12/14 class:
//! rivals would depose a healthy holder) — single-flight, and
//! rate-limited to `SELF_ATTACH_PROBE_INTERVAL`. Probe failures are
//! debug-level (pgpool legitimately down is a normal state); a backend
//! found down is warn-level and acted on.
//!
//! # What journaling must never do
//!
//! Fencing is not journaled and must never be gated on journaling: a
//! full `inflight_ops` directory or a wedged disk cannot be allowed to
//! stand between the loop and stopping a primary that has lost its
//! lease. Promotion *is* journaled (§5: "the CAS is the gate;
//! `inflight_ops` remains the record"), but a `begin` failure downgrades
//! to a warning — the promotion proceeds, because the alternative is a
//! cluster that cannot fail over while a journal is broken.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pgman::instance::{InstanceState, PostgresInstance, UpstreamSpec};
use tracing::{debug, error, info, warn};

use crate::config::{NodePool, PostgresRuntime};
use crate::ha::HaDecision;
use crate::inflight_ops::{InflightOpStore, InflightPayload};
use crate::pcp::Pcp;
use crate::peers::PeerRegistry;

/// How often a primary holder re-probes the local pgpool's view of its
/// own backend (module docs, "pgpool self-attach"). The observed
/// degeneration hit ~20 s after a promotion, so a 10 s cadence keeps
/// the blackhole window to one probe interval while staying far below
/// the healthz snapshotter's ~1 s `pcp_node_info` cadence in cost.
const SELF_ATTACH_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// How often a primary holder re-checks quorum-commit convergence
/// (docs/quorum-commit.md §5-6). Same reasoning as the self-attach
/// cadence; a promotion forces an immediate check regardless.
const SYNC_ARM_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Executes [`HaDecision`]s against the local instance.
pub struct RoleExecutor {
    instance: Arc<dyn PostgresInstance>,
    peers: Arc<dyn PeerRegistry>,
    pool: NodePool,
    inflight: Arc<dyn InflightOpStore>,
    /// Local pgpool control plane, for the self-attach convergence
    /// (module docs, "pgpool self-attach").
    pcp: Arc<dyn Pcp>,
    /// Budget for [`PostgresInstance::promote_and_wait`]. Set to
    /// `leader_ttl` deliberately: that is exactly how long rivals must
    /// watch the new holder before they may depose it (the finding-13
    /// rule), so a promotion that cannot finish inside it has lost the
    /// race by the lease's own clock — give up and let the next tick
    /// decide with fresh state.
    promote_deadline: Duration,
    pg_port: u16,
    repl_user: String,
    /// The holder this node last successfully configured its standby to
    /// follow. Executor-side memory, not cluster state: it exists so
    /// `Following` ticks are free once converged, and so a failed
    /// follow retries next tick. Reset on any role change and lost on
    /// restart — the cost of a restart is one redundant (idempotent)
    /// follow.
    confirmed_upstream: Mutex<Option<i32>>,
    /// Single-flight guard for the spawned self-attach probe. `Arc` so
    /// the spawned task can clear it without holding the executor.
    self_attach_in_flight: Arc<AtomicBool>,
    /// When the last self-attach probe was *spawned* (not when it
    /// finished) — the rate limit is on spawn cadence.
    self_attach_last_probe: Mutex<Option<Instant>>,
    /// Rate limit for the quorum-commit convergence check.
    sync_arm_last_probe: Mutex<Option<Instant>>,
    /// When a CONFIRMED follow was first observed not streaming — the
    /// finding-15 wedge clock. `None` while streaming (or not
    /// following).
    follow_stalled_since: Mutex<Option<Instant>>,
    /// Surfaced in `/healthz` as `follow_wedged`: a follow this
    /// executor confirmed has not streamed for over the grace window.
    /// Cleared the moment streaming is observed (or the role changes).
    follow_wedged: Arc<AtomicBool>,
}

impl RoleExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance: Arc<dyn PostgresInstance>,
        peers: Arc<dyn PeerRegistry>,
        pool: NodePool,
        inflight: Arc<dyn InflightOpStore>,
        pcp: Arc<dyn Pcp>,
        promote_deadline: Duration,
        pg: &PostgresRuntime,
        follow_wedged: Arc<AtomicBool>,
    ) -> Self {
        Self {
            instance,
            peers,
            pool,
            inflight,
            pcp,
            promote_deadline,
            pg_port: pg.port,
            repl_user: pg.repl_user.clone(),
            confirmed_upstream: Mutex::new(None),
            self_attach_in_flight: Arc::new(AtomicBool::new(false)),
            self_attach_last_probe: Mutex::new(None),
            sync_arm_last_probe: Mutex::new(None),
            follow_stalled_since: Mutex::new(None),
            follow_wedged,
        }
    }

    /// Execute one decision. Never returns an error: failures are
    /// logged and the next tick retries — an executor error must not
    /// kill the loop that would have corrected it.
    pub async fn apply(&self, decision: &HaDecision) {
        match decision {
            HaDecision::TookOver {
                term,
                already_primary: false,
            } => self.ensure_primary(*term).await,
            // A promote that outlived one deadline window: re-issue.
            // promote_and_wait short-circuits once recovery ends, and
            // re-signalling pg_promote mid-promotion is harmless.
            HaDecision::AwaitingPromotion { term, .. } => self.ensure_primary(*term).await,

            HaDecision::WouldDemote { reason } => {
                self.fence(&format!("demote decision: {reason}")).await;
            }

            HaDecision::Following { holder } => self.converge_follow(*holder).await,
            HaDecision::WouldFollowNewHolder { holder, .. } => {
                self.converge_follow(*holder).await;
            }

            HaDecision::RetainedLease { .. }
            | HaDecision::TookOver {
                already_primary: true,
                ..
            } => {
                // Primary steady state; any remembered upstream is
                // stale the moment we hold the lease as primary — and
                // so is any follower-era wedge state.
                *self.confirmed_upstream.lock().unwrap() = None;
                *self.follow_stalled_since.lock().unwrap() = None;
                self.follow_wedged.store(false, Ordering::SeqCst);
                self.ensure_quorum_commit(false).await;
                self.ensure_self_attached();
            }

            // Watching states — acting on any of them would be acting
            // without evidence.
            HaDecision::Paused
            | HaDecision::StoreUnknown { .. }
            | HaDecision::HolderUnhealthy { .. }
            | HaDecision::StoodDown { .. }
            | HaDecision::LostTakeover { .. }
            | HaDecision::AdoptedObservedPrimary { .. } => {}
        }
    }

    /// Journaled promotion. Convergent: already-primary is a cheap
    /// no-op inside `promote_and_wait`.
    async fn ensure_primary(&self, term: u64) {
        if matches!(self.instance.state().await, InstanceState::Primary) {
            return;
        }
        let local_id = self.pool.local_node_id;

        // Journal is the record, not the gate — see module docs.
        let op = match self
            .inflight
            .begin(
                InflightPayload::Promote {
                    node_id: local_id,
                    term,
                },
                "promoting",
                true,
            )
            .await
        {
            Ok(op) => Some(op.id),
            Err(e) => {
                warn!(
                    ?e,
                    term, "roleexec: promote journal begin failed; proceeding unjournaled"
                );
                None
            }
        };

        info!(term, deadline = ?self.promote_deadline, "roleexec: promoting local PostgreSQL");
        match self.instance.promote_and_wait(self.promote_deadline).await {
            Ok(()) => {
                info!(term, "roleexec: promotion complete");
                *self.confirmed_upstream.lock().unwrap() = None;
                if let Some(id) = op {
                    if let Err(e) = self.inflight.complete(&id).await {
                        warn!(?e, id, "roleexec: promote journal complete failed");
                    }
                }
                // In a pgpool-routed deployment, self-attach is part of
                // what "promote" means (finding 16) — and the holder's
                // steady-state ticks keep converging it afterwards.
                // Likewise quorum commit: arm as soon as evidence
                // allows, forced past the rate limit.
                self.ensure_quorum_commit(true).await;
                self.ensure_self_attached();
            }
            Err(e) => {
                // Deadline or hard failure. The lease's own clock has
                // been running (rivals may depose after leader_ttl);
                // next tick decides with fresh state.
                error!(term, err = %e, "roleexec: promotion did not complete");
                if let Some(id) = op {
                    if let Err(e2) = self.inflight.abandon(&id, &e.to_string()).await {
                        warn!(?e2, id, "roleexec: promote journal abandon failed");
                    }
                }
            }
        }
    }

    /// Stop the local instance. The one action that must never be
    /// gated, delayed, or made clever.
    async fn fence(&self, why: &str) {
        error!(why, "roleexec: FENCING — stopping local PostgreSQL");
        *self.confirmed_upstream.lock().unwrap() = None;
        *self.follow_stalled_since.lock().unwrap() = None;
        self.follow_wedged.store(false, Ordering::SeqCst);
        if let Err(e) = self.instance.ensure_stopped().await {
            // The next tick re-decides and re-tries. This is the one
            // failure worth shouting about at every occurrence.
            error!(err = %e, "roleexec: FENCE FAILED — local PostgreSQL may still be serving");
        }
    }

    /// Converge a follower onto `holder`. State-dependent — see the
    /// module docs' decision table.
    async fn converge_follow(&self, holder: i32) {
        match self.instance.state().await {
            // The stale-primary half of §2.1: running as primary while
            // the lease belongs to someone else. Fence.
            InstanceState::Primary => {
                self.fence(&format!(
                    "local PostgreSQL runs as primary but node {holder} holds the lease"
                ))
                .await;
            }
            InstanceState::Standby { streaming } => {
                let confirmed = *self.confirmed_upstream.lock().unwrap();
                if confirmed == Some(holder) {
                    // Converged per the conf — but VERIFY the stream
                    // (finding 15): a standby past the holder's fork
                    // point takes the conf rewrite and reload without
                    // complaint while PostgreSQL loops "new timeline
                    // forked off before current recovery point"
                    // underneath, never streaming. Strict flush-max
                    // candidacy makes that unreachable in the designed
                    // flows; this detection is the defense in depth
                    // that says so LOUDLY if some other path gets here.
                    if streaming {
                        *self.follow_stalled_since.lock().unwrap() = None;
                        self.follow_wedged.store(false, Ordering::SeqCst);
                        return;
                    }
                    let stalled_for = {
                        let mut since = self.follow_stalled_since.lock().unwrap();
                        since.get_or_insert_with(Instant::now).elapsed()
                    };
                    if stalled_for >= self.promote_deadline {
                        // Grace = leader_ttl, same clock the rest of
                        // the design breathes in.
                        error!(
                            holder,
                            stalled_for_s = stalled_for.as_secs(),
                            "roleexec: follow WEDGED — confirmed onto the holder but not \
                             streaming past the grace (likely diverged past the holder's \
                             fork point; finding 15). Redundancy is degraded until \
                             `cluster recover` rebuilds this node. Re-attempting the \
                             follow; /healthz shows follow_wedged=true"
                        );
                        self.follow_wedged.store(true, Ordering::SeqCst);
                        // Clear the confirmation so the follow re-runs:
                        // idempotent and harmless, and it self-heals
                        // the conf-drift flavors of "not streaming".
                        *self.confirmed_upstream.lock().unwrap() = None;
                        *self.follow_stalled_since.lock().unwrap() = None;
                    }
                    return;
                }
                match self.follow(holder).await {
                    Ok(()) => {
                        info!(holder, "roleexec: now following lease holder");
                        *self.confirmed_upstream.lock().unwrap() = Some(holder);
                        *self.follow_stalled_since.lock().unwrap() = None;
                    }
                    Err(e) => {
                        // Unconfirmed → next tick retries.
                        warn!(holder, err = %e, "roleexec: follow failed; will retry");
                    }
                }
            }
            // Demote policy: a stopped instance stays stopped until the
            // operator rejoins it (`cluster recover`). The executor
            // never runs a destructive rebuild.
            InstanceState::Down => {}
            // No evidence, no action.
            InstanceState::Unknown => {}
        }
    }

    /// Converge quorum commit on this primary (docs/quorum-commit.md
    /// §5-6): arm `synchronous_standby_names = ANY 1 (members minus
    /// self)` at the first-standby-attached event, and repair
    /// membership drift in an already-armed value. NEVER writes the
    /// empty string — disarming is the operator's `allow-async` escape
    /// hatch alone, and the next standby attach re-arms over it.
    ///
    /// Inline on the tick like `observe_local`'s queries (bounded
    /// local SQL), rate-limited to [`SYNC_ARM_PROBE_INTERVAL`];
    /// `force` (post-promotion) skips the rate limit so a fresh
    /// primary arms as soon as evidence allows.
    async fn ensure_quorum_commit(&self, force: bool) {
        {
            let mut last = self.sync_arm_last_probe.lock().unwrap();
            if !force {
                if let Some(t) = *last {
                    if t.elapsed() < SYNC_ARM_PROBE_INTERVAL {
                        return;
                    }
                }
            }
            *last = Some(Instant::now());
        }
        let mut others: Vec<String> = self
            .pool
            .members
            .iter()
            .filter(|n| n.id != self.pool.local_node_id)
            .map(|n| n.slot_name())
            .collect();
        if others.is_empty() {
            return; // single-node pool: quorum commit has no quorum
        }
        others.sort();
        let desired = format!("ANY 1 ({})", others.join(", "));
        let current = match self.instance.sync_standby_names().await {
            Ok(v) => v,
            Err(e) => {
                debug!(err = %e, "roleexec: sync_standby_names read failed; retrying next probe");
                return;
            }
        };
        if current == desired {
            return; // armed and converged
        }
        if current.is_empty() {
            // Disarmed (bootstrap, or operator allow-async). Arm only
            // on the first-standby-attached EVENT: arming with nobody
            // connected would hang every commit before a follower can
            // possibly exist (e.g. mid cluster-init, before the first
            // basebackup child comes up).
            match self.instance.connected_member_standbys().await {
                Ok(names) if !names.is_empty() => {}
                Ok(_) => return, // nobody attached yet — stay disarmed
                Err(e) => {
                    debug!(err = %e, "roleexec: connected_member_standbys failed; retrying");
                    return;
                }
            }
        }
        // Arm, or repair membership drift in an armed value.
        match self.instance.set_sync_standby_names(&desired).await {
            Ok(()) => info!(
                value = %desired,
                was = %current,
                "roleexec: quorum commit ARMED — acknowledged commits now require a \
                 lease-following standby (docs/quorum-commit.md)"
            ),
            Err(e) => warn!(err = %e, "roleexec: arming quorum commit failed; retrying next probe"),
        }
    }

    /// Converge the local pgpool onto this primary: spawn a probe that
    /// re-attaches our own backend if the local instance marks it down
    /// (module docs, "pgpool self-attach"). Never blocks the tick;
    /// single-flight; rate-limited to [`SELF_ATTACH_PROBE_INTERVAL`].
    fn ensure_self_attached(&self) {
        {
            let mut last = self.self_attach_last_probe.lock().unwrap();
            if let Some(t) = *last {
                if t.elapsed() < SELF_ATTACH_PROBE_INTERVAL {
                    return;
                }
            }
            if self.self_attach_in_flight.swap(true, Ordering::SeqCst) {
                return; // previous probe (or its attach) still running
            }
            *last = Some(Instant::now());
        }
        let pcp = self.pcp.clone();
        let local_id = self.pool.local_node_id;
        let in_flight = self.self_attach_in_flight.clone();
        tokio::spawn(async move {
            match self_attach_probe(pcp.as_ref(), local_id).await {
                Ok(true) => info!(
                    local_id,
                    "roleexec: self-attach complete — local pgpool routes to this primary again"
                ),
                Ok(false) => {}
                // pgpool being down/unreachable is a legitimate state
                // (masked pre-bootstrap, operator maintenance) — noise
                // at warn level would page on every quiet minute of it.
                Err(e) => debug!(local_id, err = %e, "roleexec: pgpool self-attach probe failed"),
            }
            in_flight.store(false, Ordering::SeqCst);
        });
    }

    /// The follow itself: prepare the upstream (slot on the holder,
    /// via peer RPC — the cross-node half stays agent-side), then
    /// re-point the local standby.
    async fn follow(&self, holder: i32) -> anyhow::Result<()> {
        let local = self.pool.local_node()?;
        let holder_node = self.pool.node_by_id(holder)?;
        let slot_name = local.slot_name();

        let client = self.peers.client(holder_node).await?;
        client
            .create_slot(&slot_name)
            .await
            .map_err(|e| anyhow::anyhow!("create slot {slot_name} on holder: {e}"))?;

        self.instance
            .follow(&UpstreamSpec {
                host: holder_node.hostname.clone(),
                port: self.pg_port,
                repl_user: self.repl_user.clone(),
                slot_name,
            })
            .await
    }
}

/// One self-attach probe: read the local pgpool's backend map and
/// re-attach `local_id` if it is marked down. Returns whether an attach
/// was issued. Split from the spawn wrapper so the decision logic is
/// unit-testable without a runtime-spawned task.
async fn self_attach_probe(pcp: &dyn Pcp, local_id: i32) -> anyhow::Result<bool> {
    let nodes = pcp.node_info_all().await?;
    let Some(me) = nodes.iter().find(|n| n.id == local_id) else {
        anyhow::bail!(
            "local backend {local_id} missing from pcp_node_info output ({} rows)",
            nodes.len()
        );
    };
    if me.is_up() {
        return Ok(false);
    }
    warn!(
        local_id,
        status = %me.status_name,
        "roleexec: local pgpool marks this primary's own backend down \
         (finding 16 — failover_on_backend_error + auto_failback off \
         makes that permanent); self-attaching"
    );
    if let Err(e) = pcp.attach_node(local_id).await {
        // The one probe failure that is NOT routine: we saw the backend
        // down and could not fix it. Warn here; the wrapper's debug is
        // for the routine pgpool-not-running case.
        warn!(local_id, err = %e, "roleexec: self-attach failed; retrying next probe interval");
        return Err(anyhow::anyhow!("self-attach of backend {local_id}: {e}"));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::inflight_ops::InMemoryInflightOpStore;
    use crate::peers::{PeerClient, PeerRegistry};
    use async_trait::async_trait;
    use pg_agent_proto::pgagentpb as pb;
    use std::sync::Mutex as StdMutex;

    // ----- scripted PostgresInstance ---------------------------------------

    struct ScriptedInstance {
        state: StdMutex<InstanceState>,
        calls: StdMutex<Vec<String>>,
        promote_fails: bool,
        /// Scripted `synchronous_standby_names` GUC; set_sync writes it.
        sync_names: StdMutex<String>,
        /// Scripted `pg_stat_replication` member application_names.
        connected: StdMutex<Vec<String>>,
    }

    impl ScriptedInstance {
        fn new(state: InstanceState) -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(state),
                calls: StdMutex::new(Vec::new()),
                promote_fails: false,
                sync_names: StdMutex::new(String::new()),
                connected: StdMutex::new(Vec::new()),
            })
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PostgresInstance for ScriptedInstance {
        async fn state(&self) -> InstanceState {
            self.state.lock().unwrap().clone()
        }
        async fn promote_and_wait(&self, _: Duration) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push("promote".into());
            if self.promote_fails {
                anyhow::bail!("scripted: promote failed")
            }
            *self.state.lock().unwrap() = InstanceState::Primary;
            Ok(())
        }
        async fn ensure_stopped(&self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push("stop".into());
            *self.state.lock().unwrap() = InstanceState::Down;
            Ok(())
        }
        async fn follow(&self, upstream: &UpstreamSpec) -> anyhow::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("follow:{}", upstream.host));
            Ok(())
        }
        async fn rebuild_as_standby(&self, _: &UpstreamSpec) -> anyhow::Result<()> {
            panic!("v1 executor must never rebuild — demote policy is operator rejoin");
        }
        async fn sync_standby_names(&self) -> anyhow::Result<String> {
            Ok(self.sync_names.lock().unwrap().clone())
        }
        async fn set_sync_standby_names(&self, value: &str) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!("set_sync:{value}"));
            *self.sync_names.lock().unwrap() = value.to_string();
            Ok(())
        }
        async fn connected_member_standbys(&self) -> anyhow::Result<Vec<String>> {
            Ok(self.connected.lock().unwrap().clone())
        }
    }

    // ----- stub peers -------------------------------------------------------

    #[derive(Default)]
    struct StubPeers {
        slot_calls: StdMutex<Vec<(String, String)>>, // (host, slot)
        create_slot_fails: StdMutex<bool>,
    }

    struct StubPeerClient {
        host: String,
        peers: Arc<StubPeers>,
    }

    #[async_trait]
    impl PeerClient for StubPeerClient {
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, slot: &str) -> anyhow::Result<()> {
            if *self.peers.create_slot_fails.lock().unwrap() {
                anyhow::bail!("stub: slot create refused")
            }
            self.peers
                .slot_calls
                .lock()
                .unwrap()
                .push((self.host.clone(), slot.to_string()));
            Ok(())
        }
        async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
            unreachable!()
        }
        async fn start(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn fetch_wal(
            &self,
            _: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
            unreachable!()
        }
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
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
    }

    // ----- stub pcp ---------------------------------------------------------

    /// Scripted local-pgpool view. `backend_up` drives what
    /// `node_info_all` reports for every backend; `node_info_fails`
    /// scripts the pgpool-not-running case. Attaches are recorded and
    /// flip `backend_up` back to true (a real attach does exactly that
    /// to pgpool's map).
    #[derive(Default)]
    struct StubPcp {
        backend_up: StdMutex<bool>,
        node_info_fails: StdMutex<bool>,
        attach_calls: StdMutex<Vec<i32>>,
    }

    impl StubPcp {
        fn all_up() -> Arc<Self> {
            Arc::new(Self {
                backend_up: StdMutex::new(true),
                ..Default::default()
            })
        }
        fn row(&self, id: i32) -> crate::pcp::NodeInfo {
            let up = *self.backend_up.lock().unwrap();
            crate::pcp::NodeInfo {
                id,
                hostname: format!("db{id}"),
                port: 5432,
                status_code: if up { 2 } else { 3 },
                lb_weight: 0.33,
                status_name: if up { "up" } else { "down" }.into(),
                actual_status: "up".into(),
                role: "standby".into(),
                actual_role: "standby".into(),
                replication_delay: "0".into(),
                replication_state: "none".into(),
                sync_state: "none".into(),
            }
        }
    }

    #[async_trait]
    impl crate::pcp::Pcp for StubPcp {
        async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
            self.attach_calls.lock().unwrap().push(node_id);
            *self.backend_up.lock().unwrap() = true;
            Ok(())
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<crate::pcp::NodeInfo>> {
            if *self.node_info_fails.lock().unwrap() {
                anyhow::bail!("scripted: pgpool not running")
            }
            Ok((0..3).map(|id| self.row(id)).collect())
        }
    }

    /// Registry wrapper so the fixture can hand `Arc<dyn PeerRegistry>`
    /// while tests keep a typed handle to the shared `StubPeers`.
    struct StubRegistry(Arc<StubPeers>);

    #[async_trait]
    impl PeerRegistry for StubRegistry {
        async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            Ok(Arc::new(StubPeerClient {
                host: node.hostname.clone(),
                peers: self.0.clone(),
            }))
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // ----- fixture ----------------------------------------------------------

    fn pool3(local: i32) -> NodePool {
        NodePool {
            members: (0..3)
                .map(|id| NodeConfig {
                    id,
                    hostname: format!("db{id}"),
                })
                .collect(),
            local_node_id: local,
        }
    }

    struct Fixture {
        exec: RoleExecutor,
        instance: Arc<ScriptedInstance>,
        peers: Arc<StubPeers>,
        inflight: Arc<InMemoryInflightOpStore>,
        pcp: Arc<StubPcp>,
        wedged: Arc<AtomicBool>,
    }

    fn fixture(local: i32, state: InstanceState) -> Fixture {
        let instance = ScriptedInstance::new(state);
        let peers = Arc::new(StubPeers::default());
        let inflight = Arc::new(InMemoryInflightOpStore::new());
        let pcp = StubPcp::all_up();
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: std::path::PathBuf::from("/nonexistent"),
            repl_user: "repl".into(),
        };
        let wedged = Arc::new(AtomicBool::new(false));
        let exec = RoleExecutor::new(
            instance.clone(),
            Arc::new(StubRegistry(peers.clone())),
            pool3(local),
            inflight.clone(),
            pcp.clone(),
            Duration::from_secs(30),
            &pg,
            wedged.clone(),
        );
        Fixture {
            exec,
            instance,
            peers,
            inflight,
            pcp,
            wedged,
        }
    }

    /// Wait (bounded) for the spawned self-attach task to finish — its
    /// completion is observable as the single-flight flag clearing.
    async fn drain_self_attach(f: &Fixture) {
        for _ in 0..200 {
            if !f.exec.self_attach_in_flight.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("self-attach task did not finish");
    }

    // ----- scenarios --------------------------------------------------------

    #[tokio::test]
    async fn takeover_promotes_and_journals() {
        let f = fixture(1, InstanceState::Standby { streaming: true });
        f.exec
            .apply(&HaDecision::TookOver {
                term: 7,
                already_primary: false,
            })
            .await;
        assert_eq!(f.instance.calls(), vec!["promote"]);

        // Journaled and completed.
        let (ops, _) = f
            .inflight
            .list(&[crate::inflight_ops::InflightStatus::Done])
            .await
            .unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            ops[0].payload,
            InflightPayload::Promote {
                node_id: 1,
                term: 7
            }
        ));
    }

    #[tokio::test]
    async fn takeover_of_an_existing_primary_touches_nothing() {
        let f = fixture(1, InstanceState::Primary);
        f.exec
            .apply(&HaDecision::TookOver {
                term: 7,
                already_primary: true,
            })
            .await;
        f.exec.apply(&HaDecision::RetainedLease { term: 7 }).await;
        assert!(f.instance.calls().is_empty());
    }

    #[tokio::test]
    async fn demote_decision_stops_postgres() {
        let f = fixture(0, InstanceState::Primary);
        f.exec
            .apply(&HaDecision::WouldDemote {
                reason: "store unknown past retry_timeout".into(),
            })
            .await;
        assert_eq!(f.instance.calls(), vec!["stop"]);
    }

    /// The stale-primary half of §2.1: the decision layer says
    /// `Following` (the holder looks healthy), and only the executor
    /// can see that the local role contradicts the lease. It must
    /// fence, not follow.
    #[tokio::test]
    async fn a_primary_that_does_not_hold_the_lease_is_fenced() {
        let f = fixture(0, InstanceState::Primary);
        f.exec.apply(&HaDecision::Following { holder: 1 }).await;
        assert_eq!(f.instance.calls(), vec!["stop"]);
        assert!(
            f.peers.slot_calls.lock().unwrap().is_empty(),
            "must not have tried to follow"
        );
    }

    #[tokio::test]
    async fn following_preps_the_upstream_then_follows_once() {
        let f = fixture(1, InstanceState::Standby { streaming: true });
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;

        // Slot prepped on the holder for OUR slot, then one follow;
        // converged ticks are free.
        assert_eq!(
            *f.peers.slot_calls.lock().unwrap(),
            vec![("db0".to_string(), "node1".to_string())]
        );
        assert_eq!(f.instance.calls(), vec!["follow:db0"]);
    }

    #[tokio::test]
    async fn holder_change_refollows_onto_the_new_holder() {
        let f = fixture(2, InstanceState::Standby { streaming: true });
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec
            .apply(&HaDecision::WouldFollowNewHolder { prev: 0, holder: 1 })
            .await;
        assert_eq!(f.instance.calls(), vec!["follow:db0", "follow:db1"]);
    }

    /// A failed follow stays unconfirmed and retries on the next
    /// `Following` tick — the convergence contract.
    #[tokio::test]
    async fn failed_follow_retries_next_tick() {
        let f = fixture(1, InstanceState::Standby { streaming: false });
        *f.peers.create_slot_fails.lock().unwrap() = true;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        assert!(f.instance.calls().is_empty(), "follow aborted at slot prep");

        *f.peers.create_slot_fails.lock().unwrap() = false;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        assert_eq!(f.instance.calls(), vec!["follow:db0"]);
    }

    /// Demote policy: a stopped instance stays stopped. The executor
    /// must not restart (via follow's reload_or_restart) or rebuild a
    /// node the operator has not rejoined.
    #[tokio::test]
    async fn a_down_instance_is_left_for_the_operator() {
        let f = fixture(1, InstanceState::Down);
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec
            .apply(&HaDecision::WouldFollowNewHolder { prev: 2, holder: 0 })
            .await;
        assert!(f.instance.calls().is_empty());
        assert!(f.peers.slot_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn watching_states_do_nothing() {
        let f = fixture(1, InstanceState::Standby { streaming: true });
        for d in [
            HaDecision::Paused,
            HaDecision::StoreUnknown {
                unknown_for: Duration::from_secs(1),
            },
            HaDecision::HolderUnhealthy {
                holder: 0,
                unhealthy_for: Duration::from_secs(1),
            },
            HaDecision::StoodDown {
                reason: "tiebreak".into(),
            },
            HaDecision::LostTakeover {
                current_holder: Some(0),
            },
        ] {
            f.exec.apply(&d).await;
        }
        assert!(f.instance.calls().is_empty());
    }

    // ----- pgpool self-attach (finding 16) ----------------------------------

    #[tokio::test]
    async fn probe_attaches_when_local_backend_marked_down() {
        let pcp = StubPcp::all_up();
        *pcp.backend_up.lock().unwrap() = false;
        let attached = self_attach_probe(pcp.as_ref(), 1).await.unwrap();
        assert!(attached);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn probe_leaves_a_healthy_backend_alone() {
        let pcp = StubPcp::all_up();
        let attached = self_attach_probe(pcp.as_ref(), 1).await.unwrap();
        assert!(!attached);
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn probe_errors_when_local_backend_missing_from_map() {
        // Rows come back but none carries our id — pgpool.conf drift.
        // Refusing beats attaching a backend number that means another
        // node.
        let pcp = StubPcp::all_up();
        let err = self_attach_probe(pcp.as_ref(), 7).await.unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn holder_steady_state_self_attaches_a_degenerated_backend() {
        // The finding-16 shape: the winner is primary and holding, and
        // ~20 s later its own pgpool has degenerated its backend. The
        // steady-state tick must converge it back.
        let f = fixture(1, InstanceState::Primary);
        *f.pcp.backend_up.lock().unwrap() = false;
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        drain_self_attach(&f).await;
        assert_eq!(*f.pcp.attach_calls.lock().unwrap(), vec![1]);
        // The probe is pcp-only — it must not touch PostgreSQL.
        assert!(f.instance.calls().is_empty());
    }

    #[tokio::test]
    async fn promotion_completion_triggers_a_self_attach_probe() {
        let f = fixture(1, InstanceState::Standby { streaming: true });
        *f.pcp.backend_up.lock().unwrap() = false;
        f.exec
            .apply(&HaDecision::TookOver {
                term: 2,
                already_primary: false,
            })
            .await;
        drain_self_attach(&f).await;
        assert_eq!(f.instance.calls(), vec!["promote"]);
        assert_eq!(*f.pcp.attach_calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn self_attach_probes_are_rate_limited() {
        let f = fixture(1, InstanceState::Primary);
        *f.pcp.backend_up.lock().unwrap() = false;
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        drain_self_attach(&f).await;
        // Stub attach flipped the backend up; break it again and tick
        // immediately — inside the probe interval, nothing may fire.
        *f.pcp.backend_up.lock().unwrap() = false;
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        drain_self_attach(&f).await;
        assert_eq!(
            *f.pcp.attach_calls.lock().unwrap(),
            vec![1],
            "second tick inside the probe interval must not re-probe"
        );
    }

    #[tokio::test]
    async fn self_attach_survives_pgpool_being_down() {
        // pgpool not running is a routine state: the probe fails, the
        // executor keeps ticking, PostgreSQL is untouched.
        let f = fixture(1, InstanceState::Primary);
        *f.pcp.node_info_fails.lock().unwrap() = true;
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        drain_self_attach(&f).await;
        assert!(f.pcp.attach_calls.lock().unwrap().is_empty());
        assert!(f.instance.calls().is_empty());
    }

    // ----- wedged-follow detection (finding 15) ----------------------------

    #[tokio::test]
    async fn streaming_confirmed_follow_is_healthy() {
        let f = fixture(2, InstanceState::Standby { streaming: true });
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        assert_eq!(
            f.instance
                .calls()
                .iter()
                .filter(|c| c.starts_with("follow"))
                .count(),
            1,
            "streaming confirmed follow must stay converged"
        );
        assert!(!f.wedged.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn confirmed_but_never_streaming_is_detected_as_wedged() {
        // The finding-15 shape: the conf rewrite + reload succeeded
        // (follow Ok → confirmed) while PostgreSQL loops "forked off"
        // underneath and never streams. Grace zeroed so the very next
        // tick trips detection.
        let mut f = fixture(2, InstanceState::Standby { streaming: false });
        f.exec.promote_deadline = Duration::ZERO;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await; // follow → confirmed
        f.exec.apply(&HaDecision::Following { holder: 0 }).await; // stalled past grace → wedged
        assert!(
            f.wedged.load(Ordering::SeqCst),
            "confirmed-but-not-streaming past the grace must set the wedge flag"
        );
        // Confirmation was cleared, so the follow re-runs next tick.
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        assert_eq!(
            f.instance
                .calls()
                .iter()
                .filter(|c| c.starts_with("follow"))
                .count(),
            2,
            "a wedged follow is re-attempted, not silently trusted"
        );
    }

    #[tokio::test]
    async fn wedge_flag_clears_when_streaming_resumes() {
        let mut f = fixture(2, InstanceState::Standby { streaming: false });
        f.exec.promote_deadline = Duration::ZERO;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        assert!(f.wedged.load(Ordering::SeqCst));
        // The node starts streaming (e.g. after `cluster recover`).
        *f.instance.state.lock().unwrap() = InstanceState::Standby { streaming: true };
        f.exec.apply(&HaDecision::Following { holder: 0 }).await; // re-follow → confirmed
        f.exec.apply(&HaDecision::Following { holder: 0 }).await; // streaming observed
        assert!(
            !f.wedged.load(Ordering::SeqCst),
            "streaming clears the wedge flag"
        );
    }

    // ----- quorum commit arming (docs/quorum-commit.md §5-6) ---------------

    #[tokio::test]
    async fn holder_arms_quorum_commit_when_first_standby_attaches() {
        let f = fixture(0, InstanceState::Primary);
        f.instance.connected.lock().unwrap().push("node1".into());
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        assert!(
            f.instance
                .calls()
                .contains(&"set_sync:ANY 1 (node1, node2)".to_string()),
            "first attach must arm ANY 1 over the other members: {:?}",
            f.instance.calls()
        );
    }

    #[tokio::test]
    async fn holder_stays_disarmed_until_a_standby_attaches() {
        // Bootstrap shape: no follower exists yet — arming now would
        // hang every commit before a follower could possibly attach.
        let f = fixture(0, InstanceState::Primary);
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        assert!(
            !f.instance.calls().iter().any(|c| c.starts_with("set_sync")),
            "must not arm with nobody attached: {:?}",
            f.instance.calls()
        );
    }

    #[tokio::test]
    async fn holder_repairs_membership_drift_in_an_armed_value() {
        // Already armed (e.g. inherited via basebackup from the old
        // primary — names include self, exclude the old primary):
        // rewrite to the correct set WITHOUT requiring a fresh attach.
        let f = fixture(0, InstanceState::Primary);
        *f.instance.sync_names.lock().unwrap() = "ANY 1 (node0, node2)".into();
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        assert!(
            f.instance
                .calls()
                .contains(&"set_sync:ANY 1 (node1, node2)".to_string()),
            "armed-but-drifted names must be repaired: {:?}",
            f.instance.calls()
        );
    }

    #[tokio::test]
    async fn armed_and_converged_is_a_no_op() {
        let f = fixture(0, InstanceState::Primary);
        *f.instance.sync_names.lock().unwrap() = "ANY 1 (node1, node2)".into();
        f.exec.apply(&HaDecision::RetainedLease { term: 3 }).await;
        assert!(
            !f.instance.calls().iter().any(|c| c.starts_with("set_sync")),
            "converged value must not be rewritten: {:?}",
            f.instance.calls()
        );
    }

    #[tokio::test]
    async fn promotion_forces_an_immediate_arming_check() {
        let f = fixture(1, InstanceState::Standby { streaming: true });
        f.instance.connected.lock().unwrap().push("node2".into());
        f.exec
            .apply(&HaDecision::TookOver {
                term: 2,
                already_primary: false,
            })
            .await;
        let calls = f.instance.calls();
        assert!(calls.contains(&"promote".to_string()));
        assert!(
            calls.contains(&"set_sync:ANY 1 (node0, node2)".to_string()),
            "promotion must arm as soon as evidence allows: {calls:?}"
        );
    }

    #[tokio::test]
    async fn followers_never_probe_pgpool() {
        let f = fixture(2, InstanceState::Standby { streaming: true });
        *f.pcp.backend_up.lock().unwrap() = false;
        f.exec.apply(&HaDecision::Following { holder: 0 }).await;
        drain_self_attach(&f).await;
        assert!(
            f.pcp.attach_calls.lock().unwrap().is_empty(),
            "self-attach is the holder's convergence, not a follower's"
        );
    }
}
