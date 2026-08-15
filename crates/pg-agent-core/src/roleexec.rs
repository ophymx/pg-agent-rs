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
//! # What journaling must never do
//!
//! Fencing is not journaled and must never be gated on journaling: a
//! full `inflight_ops` directory or a wedged disk cannot be allowed to
//! stand between the loop and stopping a primary that has lost its
//! lease. Promotion *is* journaled (§5: "the CAS is the gate;
//! `inflight_ops` remains the record"), but a `begin` failure downgrades
//! to a warning — the promotion proceeds, because the alternative is a
//! cluster that cannot fail over while a journal is broken.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgman::instance::{InstanceState, PostgresInstance, UpstreamSpec};
use tracing::{error, info, warn};

use crate::config::{NodePool, PostgresRuntime};
use crate::ha::HaDecision;
use crate::inflight_ops::{InflightOpStore, InflightPayload};
use crate::peers::PeerRegistry;

/// Executes [`HaDecision`]s against the local instance.
pub struct RoleExecutor {
    instance: Arc<dyn PostgresInstance>,
    peers: Arc<dyn PeerRegistry>,
    pool: NodePool,
    inflight: Arc<dyn InflightOpStore>,
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
}

impl RoleExecutor {
    pub fn new(
        instance: Arc<dyn PostgresInstance>,
        peers: Arc<dyn PeerRegistry>,
        pool: NodePool,
        inflight: Arc<dyn InflightOpStore>,
        promote_deadline: Duration,
        pg: &PostgresRuntime,
    ) -> Self {
        Self {
            instance,
            peers,
            pool,
            inflight,
            promote_deadline,
            pg_port: pg.port,
            repl_user: pg.repl_user.clone(),
            confirmed_upstream: Mutex::new(None),
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
                // stale the moment we hold the lease as primary.
                *self.confirmed_upstream.lock().unwrap() = None;
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
            InstanceState::Standby { .. } => {
                let confirmed = *self.confirmed_upstream.lock().unwrap();
                if confirmed == Some(holder) {
                    return; // converged; Following ticks are free
                }
                match self.follow(holder).await {
                    Ok(()) => {
                        info!(holder, "roleexec: now following lease holder");
                        *self.confirmed_upstream.lock().unwrap() = Some(holder);
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
    }

    impl ScriptedInstance {
        fn new(state: InstanceState) -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(state),
                calls: StdMutex::new(Vec::new()),
                promote_fails: false,
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
    }

    fn fixture(local: i32, state: InstanceState) -> Fixture {
        let instance = ScriptedInstance::new(state);
        let peers = Arc::new(StubPeers::default());
        let inflight = Arc::new(InMemoryInflightOpStore::new());
        let pg = PostgresRuntime {
            port: 5432,
            data_dir: std::path::PathBuf::from("/nonexistent"),
            repl_user: "repl".into(),
        };
        let exec = RoleExecutor::new(
            instance.clone(),
            Arc::new(StubRegistry(peers.clone())),
            pool3(local),
            inflight.clone(),
            Duration::from_secs(30),
            &pg,
        );
        Fixture {
            exec,
            instance,
            peers,
            inflight,
        }
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
}
