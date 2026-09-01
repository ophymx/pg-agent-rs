//! [`ConsensusStore`] over a running Raft
//! (docs/promotion-authority.md §5, sequencing step 6).
//!
//! This is the swap the whole seam exists for: the HA loop's contract
//! does not change, and neither does the loop. What changes is that
//! answers now come from a quorum instead of a process-local mutex.
//!
//! # Every operation is a leader operation
//!
//! Worth stating plainly, because the design doc does not: **both a
//! linearizable read and a state-machine write can only be performed by
//! the Raft leader.** `ensure_linearizable` confirms leadership against
//! a quorum and fails on a follower; `client_write` returns
//! `ForwardToLeader`. Meanwhile the design's central invariant is that
//! the Raft leader is *not* the PostgreSQL primary and has no
//! relationship to it. Put together: on most nodes, most of the time,
//! retain and takeover are RPCs to another node.
//!
//! That is not a compromise of the model, but it is a cost the model
//! implies and did not price. Each tick's read costs a round trip to
//! the leader, which then costs a ReadIndex round trip to its quorum.
//! The `retry_timeout > election_timeout` invariant already covers the
//! case that matters — a read cannot complete while an election is in
//! flight — and this adds one hop inside the same budget.
//!
//! Forwarding is one hop, never two: the leader-side handlers refuse
//! rather than re-forward. A chain would make the latency unbounded in
//! exactly the churny conditions where the budget is tightest.
//!
//! # `Err` means unknown
//!
//! The trait's hardest rule survives contact with Raft unchanged, and
//! this module is where it would be easiest to break. Every failure
//! path here — no leader known, leader unreachable, `ensure_linearizable`
//! refused, RPC timed out — returns `Err`, never a defaulted or empty
//! [`ClusterState`]. A caller that sees `Err` must treat the lease as
//! *unknown*; treating it as vacant is the §3 hole reintroduced through
//! the front door, and an empty `ClusterState` is exactly what a
//! well-meaning `unwrap_or_default` would produce.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use openraft::error::{InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft};
use tracing::info;

use crate::config::{NodePool, RaftConfig};
use crate::consensus::{
    ClusterState, ConsensusStore, Paused, ReleaseOutcome, Switchover, TakeoverOutcome,
};
use crate::raftnet::{LeaderClient, PgAgentRaftHandle, RaftChannelFactory};
use crate::raftstore::{
    database_path, membership_map, open_database, to_raft_node_id, ClusterStateReader,
    CommandResponse, ConsensusCommand, RaftNodeId, RedbLogStore, RedbStateMachine,
};

/// A [`ConsensusStore`] backed by this node's Raft.
pub struct RaftConsensusStore {
    raft: PgAgentRaftHandle,
    reader: ClusterStateReader,
    leader: LeaderClient,
}

impl RaftConsensusStore {
    pub fn new(raft: PgAgentRaftHandle, reader: ClusterStateReader, leader: LeaderClient) -> Self {
        Self {
            raft,
            reader,
            leader,
        }
    }

    /// Address of the node openraft currently believes leads, if any.
    ///
    /// Read from metrics rather than remembered: leadership is not this
    /// type's state to track, and a cached leader is precisely the kind
    /// of stale belief that turns a re-election into a stuck loop.
    fn leader_addr(&self) -> Option<String> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader?;
        let node: &BasicNode = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)?;
        if node.addr.is_empty() {
            None
        } else {
            Some(node.addr.clone())
        }
    }

    /// Run a command through the local Raft if this node leads, else
    /// through the leader.
    async fn propose(&self, cmd: ConsensusCommand) -> anyhow::Result<CommandResponse> {
        match self.raft.client_write(cmd.clone()).await {
            Ok(written) => Ok(written.data),
            Err(local_err) => {
                // Not the leader (or no longer). Try the node that is.
                let addr = self
                    .leader_addr()
                    .ok_or_else(|| anyhow::anyhow!("consensus: no leader known ({local_err})"))?;
                self.leader.propose(&addr, &cmd).await
            }
        }
    }
}

#[async_trait]
impl ConsensusStore for RaftConsensusStore {
    async fn read_state(&self) -> anyhow::Result<ClusterState> {
        // Leader path: confirm leadership against a quorum, wait for the
        // state machine to reach the read index, then read locally.
        match self.raft.ensure_linearizable().await {
            Ok(_) => self.reader.read(),
            Err(local_err) => {
                let addr = self
                    .leader_addr()
                    .ok_or_else(|| anyhow::anyhow!("consensus: no leader known ({local_err})"))?;
                self.leader.read_state(&addr).await
            }
        }
    }

    async fn try_takeover(
        &self,
        candidate: i32,
        expected: Option<(i32, u64)>,
    ) -> anyhow::Result<TakeoverOutcome> {
        // The timestamp is minted here, by the proposer, and applied
        // verbatim on every replica — a clock read inside `apply` would
        // make replicas diverge. See `raftstore`'s determinism rule.
        let cmd = ConsensusCommand::Takeover {
            candidate,
            expected,
            at: Utc::now(),
        };
        match self.propose(cmd).await? {
            CommandResponse::Takeover(outcome) => Ok(outcome),
            other => Err(anyhow::anyhow!(
                "consensus: takeover got the wrong response variant: {other:?}"
            )),
        }
    }

    async fn release(&self, holder: i32, term: u64) -> anyhow::Result<ReleaseOutcome> {
        match self
            .propose(ConsensusCommand::Release { holder, term })
            .await?
        {
            CommandResponse::Release(outcome) => Ok(outcome),
            other => Err(anyhow::anyhow!(
                "consensus: release got the wrong response variant: {other:?}"
            )),
        }
    }

    async fn set_paused(&self, paused: Option<Paused>) -> anyhow::Result<()> {
        match self.propose(ConsensusCommand::SetPaused(paused)).await? {
            CommandResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "consensus: set_paused got the wrong response variant: {other:?}"
            )),
        }
    }

    async fn set_switchover(&self, switchover: Option<Switchover>) -> anyhow::Result<()> {
        match self
            .propose(ConsensusCommand::SetSwitchover(switchover))
            .await?
        {
            CommandResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "consensus: set_switchover got the wrong response variant: {other:?}"
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime — construction and membership bootstrap
// ---------------------------------------------------------------------------

/// Everything a node needs to participate in consensus, built once at
/// daemon startup.
///
/// Construction is deliberately *not* in `Agent::new` (documented as
/// cheap, no I/O): this opens a redb file and starts openraft's core
/// task. It belongs where the daemon can fail loudly and exit.
pub struct RaftRuntime {
    pub raft: PgAgentRaftHandle,
    pub reader: ClusterStateReader,
    pub store: Arc<RaftConsensusStore>,
    /// This node, in Raft's id space.
    pub local_id: RaftNodeId,
    /// The pool as membership, resolved once at startup. Membership
    /// changes at runtime are a step-7 concern; today the pool is
    /// snapshotted at startup everywhere else too.
    members: BTreeMap<RaftNodeId, BasicNode>,
}

/// How long [`RaftRuntime::bootstrap_membership`] waits for a first
/// leader before returning. Comfortably above any healthy election
/// (sub-second at the defaults); callers proposing immediately after
/// bootstrap depend on it.
const LEADER_WAIT_AFTER_BOOTSTRAP: Duration = Duration::from_secs(10);

/// Fraction of the election window used as the heartbeat interval.
///
/// openraft wants heartbeats comfortably inside the election window, or
/// a leader that is merely slow gets replaced. A fifth of the lower
/// bound leaves room for four missed heartbeats before any follower
/// starts campaigning — deliberately generous, because this design pays
/// for a spurious election in database availability.
const HEARTBEAT_DIVISOR: u64 = 5;

impl RaftRuntime {
    /// Open the store, start Raft, and build the [`ConsensusStore`].
    ///
    /// Does not bootstrap membership — see
    /// [`bootstrap_membership`](Self::bootstrap_membership). A restarting
    /// node must *not* re-initialize; it recovers its membership from
    /// its own log.
    pub async fn start(
        state_dir: &Path,
        node_pool: &NodePool,
        agent_port: u16,
        tls: Option<Arc<rustls::ClientConfig>>,
        cfg: &RaftConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let local = node_pool
            .local_node()
            .map_err(|e| anyhow::anyhow!("raft: resolve local node: {e}"))?;
        let local_id = to_raft_node_id(local.id)?;

        let members = membership_map(
            node_pool
                .members
                .iter()
                .map(|n| (n.id, n.peer_addr(agent_port))),
        )?;

        let db = open_database(state_dir)?;
        let reader = ClusterStateReader::new(db.clone());
        let log = RedbLogStore::new(db.clone());
        let sm = RedbStateMachine::new(db)?;

        // The `[raft]` block carries one election knob, an upper bound.
        // openraft wants a randomized range: a single value would have
        // every node time out together and split the vote repeatedly.
        let election_max = cfg.effective_election_timeout().as_millis() as u64;
        let election_min = (election_max / 2).max(1);
        let raft_config = Arc::new(
            Config {
                heartbeat_interval: (election_min / HEARTBEAT_DIVISOR).max(1),
                election_timeout_min: election_min,
                election_timeout_max: election_max,
                ..Default::default()
            }
            .validate()
            .map_err(|e| anyhow::anyhow!("raft: invalid openraft config: {e}"))?,
        );

        let raft = Raft::new(
            local_id,
            raft_config,
            RaftChannelFactory::new(tls.clone()),
            log,
            sm,
        )
        .await
        .map_err(|e| anyhow::anyhow!("raft: start: {e}"))?;

        let store = Arc::new(RaftConsensusStore::new(
            raft.clone(),
            reader.clone(),
            LeaderClient::new(tls),
        ));

        info!(
            node = local_id,
            members = members.len(),
            election_ms = election_max,
            db = %database_path(state_dir).display(),
            "raft: started"
        );

        Ok(Arc::new(Self {
            raft,
            reader,
            store,
            local_id,
            members,
        }))
    }

    /// Form the cluster from the configured node pool.
    ///
    /// Idempotent by openraft's own contract: `NotAllowed` means the
    /// cluster is already formed, which is the goal of calling this, so
    /// it is reported as "already formed" rather than an error. That
    /// matters because this runs from `ClusterInit`, which operators
    /// re-run — and because a second `initialize` on a live cluster
    /// would otherwise look like something to force past.
    pub async fn bootstrap_membership(&self) -> anyhow::Result<MembershipBootstrap> {
        let outcome = if self.raft.is_initialized().await.unwrap_or(false) {
            MembershipBootstrap::AlreadyFormed
        } else {
            match self.raft.initialize(self.members.clone()).await {
                Ok(()) => {
                    info!(
                        members = self.members.len(),
                        "raft: cluster membership initialized"
                    );
                    MembershipBootstrap::Formed {
                        members: self.members.len(),
                    }
                }
                Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                    MembershipBootstrap::AlreadyFormed
                }
                Err(e) => return Err(anyhow::anyhow!("raft: initialize membership: {e}")),
            }
        };

        // Don't return into a leaderless gap. `initialize()` commits the
        // membership and only then does the first election run; a caller
        // that immediately proposes (ClusterInit seeds the lease on the
        // next line) loses that race with "no leader known" — the
        // greenfield acceptance suite hit exactly this on its first run,
        // where the same code had won the race in every staged-migration
        // run before it. Bounded: elections are sub-second here, and a
        // cluster that cannot elect within this window has a problem the
        // caller should hear about from its own next step.
        let deadline = tokio::time::Instant::now() + LEADER_WAIT_AFTER_BOOTSTRAP;
        loop {
            if self.raft.metrics().borrow().current_leader.is_some() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                info!(
                    "raft: no leader within {:?} after membership bootstrap; proceeding",
                    LEADER_WAIT_AFTER_BOOTSTRAP
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipBootstrap {
    Formed { members: usize },
    AlreadyFormed,
}

impl MembershipBootstrap {
    /// One line for `ClusterInit`'s response message.
    pub fn describe(&self) -> String {
        match self {
            Self::Formed { members } => {
                format!("raft membership initialized ({members} nodes)")
            }
            Self::AlreadyFormed => "raft membership already formed".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raftnet::{RaftChannelFactory, RaftGrpcService};
    use crate::raftstore::{open_database, RedbLogStore, RedbStateMachine};
    use openraft::{Config, Raft};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;

    /// Retry `op` while Raft is between leaders.
    ///
    /// Leadership can move at any instant, and these tests assert on
    /// consensus SEMANTICS — who wins a CAS, whether a term advances —
    /// not on one node staying leader for the length of a test. A
    /// production caller treats "not the leader" as a transient and
    /// retries; so does this. `propose` itself forwards exactly one hop,
    /// so a leader that moves twice lands here rather than being handled
    /// underneath.
    ///
    /// The predicate is deliberately narrow, and that narrowness is the
    /// whole point. `try_takeover` is a CAS whose `expected: None` arm
    /// means "win only if the lease is VACANT" (`raftstore`'s
    /// `observed_holds`), so re-issuing a command that DID commit would
    /// observe its own lease and return `Lost` — a flake turned into a
    /// false assertion, which is worse than the flake. Both tolerated
    /// errors are pre-proposal rejections: openraft's own leader check
    /// and the remote's `FailedPrecondition` both fire before any entry
    /// is appended, so the command provably did not apply. Anything
    /// ambiguous — a transport failure, a timeout — is NOT retried and
    /// fails the test, because there it may well have committed.
    async fn settled<T, F, Fut>(what: &str, mut op: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            match op().await {
                Ok(v) => return v,
                Err(e) => {
                    let msg = format!("{e:#}");
                    let between_leaders =
                        msg.contains("no leader known") || msg.contains("not the leader");
                    if !between_leaders {
                        panic!("{what}: not a leadership transient: {e:#}");
                    }
                    if tokio::time::Instant::now() >= deadline {
                        panic!("{what}: still between leaders after 20s: {e:#}");
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Backstop for [`cluster`]'s readiness gate. Generous because it
    /// guards against a genuinely broken cluster rather than tuning
    /// anything — a healthy one clears it in a fraction of it, and the
    /// suite's runtime is unchanged.
    const CLUSTER_READY: Duration = Duration::from_secs(15);

    struct Node {
        store: RaftConsensusStore,
        raft: PgAgentRaftHandle,
        addr: String,
        _dir: TempDir,
    }

    async fn spawn(id: u64, shutdown: CancellationToken) -> Node {
        let dir = TempDir::new().unwrap();
        let db = open_database(dir.path()).unwrap();
        let reader = ClusterStateReader::new(db.clone());
        let log = RedbLogStore::new(db.clone());
        let sm = RedbStateMachine::new(db).unwrap();

        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft = Raft::new(id, config, RaftChannelFactory::new_dev(), log, sm)
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let service = RaftGrpcService::new(raft.clone(), reader.clone()).into_server();
        let s = shutdown.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    s.cancelled().await
                })
                .await
        });

        Node {
            store: RaftConsensusStore::new(raft.clone(), reader, LeaderClient::new_dev()),
            raft,
            addr,
            _dir: dir,
        }
    }

    async fn cluster(shutdown: &CancellationToken) -> Vec<Node> {
        let mut nodes = Vec::new();
        for id in 0..3u64 {
            nodes.push(spawn(id, shutdown.clone()).await);
        }
        let mut members = BTreeMap::new();
        for (i, n) in nodes.iter().enumerate() {
            members.insert(i as u64, BasicNode::new(n.addr.clone()));
        }
        nodes[0].raft.initialize(members).await.unwrap();

        // Readiness is NOT "node 0 believes someone leads". That belief
        // arrives the moment a vote is won, which is before the winner has
        // committed anything — and a node that has not yet established
        // itself can revert to candidate, at which point `current_leader`
        // goes back to `None`. A proposal issued in that window fails with
        // `ForwardToLeader { leader_id: None }`, and `propose`'s fallback
        // has nobody to forward to either, which is the observed
        // `consensus: no leader known (has to forward request to: None,
        // None)`.
        //
        // Wait for the `initialize` entries to be APPLIED instead. That is
        // strictly stronger and it is what the callers actually need:
        // entries only commit through a leader that holds a quorum, so an
        // applied entry 1 is proof the election really finished rather
        // than proof that somebody briefly thought so.
        //
        // All three nodes, not just the one we hold: several callers write
        // through a FOLLOWER, so a gate covering only node 0 would leave
        // exactly those tests racing.
        for (i, n) in nodes.iter().enumerate() {
            n.raft
                .wait(Some(CLUSTER_READY))
                .metrics(
                    |m| {
                        m.current_leader.is_some()
                            && m.last_applied.map(|l| l.index).unwrap_or(0) >= 1
                    },
                    "leader elected and the initialize entries applied",
                )
                .await
                .unwrap_or_else(|e| panic!("node {i} never became ready: {e}"));
        }
        nodes
    }

    async fn leader_index(nodes: &[Node]) -> usize {
        nodes[0].raft.current_leader().await.unwrap() as usize
    }

    /// A follower must be able to take the lease. This is the whole
    /// point of the leader/primary separation: if only the Raft leader
    /// could acquire, every Raft re-election would move the database
    /// primary, which is a worse version of the bug being fixed.
    #[tokio::test]
    async fn a_follower_can_take_the_lease_through_the_leader() {
        let shutdown = CancellationToken::new();
        let nodes = cluster(&shutdown).await;
        let leader = leader_index(&nodes).await;
        let follower = (0..3).find(|i| *i != leader).unwrap();

        let outcome = settled("takeover by follower", || {
            nodes[follower].store.try_takeover(follower as i32, None)
        })
        .await;
        match outcome {
            TakeoverOutcome::Won { lease } => assert_eq!(lease.holder, follower as i32),
            other => panic!("follower should have won an uncontested lease, got {other:?}"),
        }

        // And every node — leader included — can read it back.
        for (i, n) in nodes.iter().enumerate() {
            let state = settled("read_state", || n.store.read_state()).await;
            assert_eq!(
                state.lease.as_ref().map(|l| l.holder),
                Some(follower as i32),
                "node {i} disagrees about the lease holder"
            );
        }

        shutdown.cancel();
    }

    /// Two candidates racing: exactly one wins, and the loser is told
    /// who holds it rather than getting an error.
    #[tokio::test]
    async fn concurrent_takeovers_serialize() {
        let shutdown = CancellationToken::new();
        let nodes = cluster(&shutdown).await;

        let a = settled("takeover 0", || nodes[0].store.try_takeover(0, None)).await;
        // Node 1 still believes the lease is vacant — a stale
        // observation, and its CAS must lose on that basis.
        let b = settled("takeover 1", || nodes[1].store.try_takeover(1, None)).await;

        let won = [&a, &b]
            .iter()
            .filter(|o| matches!(o, TakeoverOutcome::Won { .. }))
            .count();
        assert_eq!(won, 1, "exactly one candidate may win: {a:?} / {b:?}");

        match (&a, &b) {
            (TakeoverOutcome::Won { lease }, TakeoverOutcome::Lost { current })
            | (TakeoverOutcome::Lost { current }, TakeoverOutcome::Won { lease }) => {
                assert_eq!(
                    current.as_ref().map(|l| l.holder),
                    Some(lease.holder),
                    "the loser must be told who actually holds it"
                );
            }
            other => panic!("expected one win and one loss, got {other:?}"),
        }

        shutdown.cancel();
    }

    /// Release requires being the current holder at the current term,
    /// and a reacquire afterwards must not reuse the term — fencing
    /// tokens are only worth anything if they never repeat.
    #[tokio::test]
    async fn release_then_reacquire_advances_the_term() {
        let shutdown = CancellationToken::new();
        let nodes = cluster(&shutdown).await;

        let TakeoverOutcome::Won { lease } =
            settled("takeover 0", || nodes[0].store.try_takeover(0, None)).await
        else {
            panic!("uncontested takeover should win");
        };

        // A node that is not the holder cannot release it.
        let out = settled("release by non-holder", || {
            nodes[1].store.release(1, lease.term)
        })
        .await;
        assert!(matches!(out, ReleaseOutcome::NotHolder { .. }));

        let out = settled("release by holder", || {
            nodes[0].store.release(0, lease.term)
        })
        .await;
        assert!(matches!(out, ReleaseOutcome::Released));
        assert!(settled("read_state", || nodes[2].store.read_state())
            .await
            .lease
            .is_none());

        let TakeoverOutcome::Won { lease: again } =
            settled("takeover 2", || nodes[2].store.try_takeover(2, None)).await
        else {
            panic!("vacant lease should be takeable");
        };
        assert!(
            again.term > lease.term,
            "term must advance across release ({} -> {})",
            lease.term,
            again.term
        );

        shutdown.cancel();
    }

    /// With no quorum and no leader, a read must fail — not return an
    /// empty state. A caller that reads `Err` treats the lease as
    /// unknown; one that reads a defaulted `ClusterState` would treat
    /// it as vacant and promote into a live primary.
    #[tokio::test]
    async fn a_lone_uninitialized_node_errors_rather_than_reporting_vacant() {
        let shutdown = CancellationToken::new();
        // Never initialized: no membership, no leader, no quorum.
        let node = spawn(0, shutdown.clone()).await;

        let err = node
            .store
            .read_state()
            .await
            .expect_err("a node with no quorum cannot know the lease");
        let msg = err.to_string();
        assert!(
            msg.contains("no leader known"),
            "unexpected error text: {msg}"
        );

        shutdown.cancel();
    }

    // ----- RaftRuntime ------------------------------------------------

    fn pool(n: i32, local: i32) -> NodePool {
        NodePool {
            members: (0..n)
                .map(|id| crate::config::NodeConfig {
                    id,
                    hostname: format!("127.0.0.{}", id + 1),
                })
                .collect(),
            local_node_id: local,
        }
    }

    /// Bootstrap is idempotent by openraft's own contract, and it has
    /// to be: `ClusterInit` is an operator command people re-run, and a
    /// second `initialize` against a live cluster must read as "already
    /// formed" rather than as something to force past.
    #[tokio::test]
    async fn membership_bootstrap_is_idempotent() {
        let dir = TempDir::new().unwrap();
        // Single-node membership: the post-bootstrap leader wait needs
        // an electable cluster, and the two peers of a 3-pool are never
        // spawned in this test.
        let rt = RaftRuntime::start(
            dir.path(),
            &pool(1, 0),
            9701,
            None,
            &crate::config::RaftConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            rt.bootstrap_membership().await.unwrap(),
            MembershipBootstrap::Formed { members: 1 }
        );
        assert_eq!(
            rt.bootstrap_membership().await.unwrap(),
            MembershipBootstrap::AlreadyFormed,
            "re-running ClusterInit must not be an error"
        );
    }

    /// A restarted node recovers membership from its own log. It must
    /// not re-initialize — that is what `start` deliberately does not
    /// do, and getting it wrong would let a restart redefine who the
    /// cluster's members are.
    #[tokio::test]
    async fn a_restarted_node_does_not_reform_the_cluster() {
        let dir = TempDir::new().unwrap();
        let cfg = crate::config::RaftConfig::default();
        {
            let rt = RaftRuntime::start(dir.path(), &pool(1, 0), 9701, None, &cfg)
                .await
                .unwrap();
            rt.bootstrap_membership().await.unwrap();
            rt.raft.shutdown().await.unwrap();
        }

        let rt = RaftRuntime::start(dir.path(), &pool(1, 0), 9701, None, &cfg)
            .await
            .unwrap();
        assert_eq!(
            rt.bootstrap_membership().await.unwrap(),
            MembershipBootstrap::AlreadyFormed
        );
    }

    /// The `[raft]` block carries one election value, an upper bound.
    /// openraft needs a randomized range — a single value has every
    /// node time out together and split the vote, repeatedly.
    #[tokio::test]
    async fn election_window_is_a_range_not_a_point() {
        let dir = TempDir::new().unwrap();
        let cfg = crate::config::RaftConfig {
            election_timeout_ms: Some(4_000),
            // The config invariants still have to hold at these values.
            retry_timeout_secs: Some(5),
            leader_ttl_secs: Some(30),
            ..Default::default()
        };
        cfg.validate().unwrap();

        let rt = RaftRuntime::start(dir.path(), &pool(3, 1), 9701, None, &cfg)
            .await
            .unwrap();
        assert_eq!(rt.local_id, 1);
        let c = rt.raft.config();
        assert_eq!(c.election_timeout_max, 4_000);
        assert_eq!(c.election_timeout_min, 2_000);
        assert!(
            c.heartbeat_interval < c.election_timeout_min,
            "heartbeats must fit comfortably inside the election window: {} vs {}",
            c.heartbeat_interval,
            c.election_timeout_min
        );
    }

    /// Pause and switchover ride the same log, so they are subject to
    /// the same quorum — and are readable from any node afterwards.
    #[tokio::test]
    async fn pause_and_switchover_replicate() {
        let shutdown = CancellationToken::new();
        let nodes = cluster(&shutdown).await;

        settled("set_paused", || {
            nodes[1].store.set_paused(Some(Paused {
                reason: "maintenance".into(),
                set_by: "operator".into(),
                at: Utc::now(),
            }))
        })
        .await;
        settled("set_switchover", || {
            nodes[2].store.set_switchover(Some(Switchover {
                target: 1,
                not_before: None,
            }))
        })
        .await;

        let state = settled("read_state", || nodes[0].store.read_state()).await;
        assert_eq!(state.paused.unwrap().reason, "maintenance");
        assert_eq!(state.switchover.unwrap().target, 1);

        shutdown.cancel();
    }
}
