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

use async_trait::async_trait;
use chrono::Utc;
use openraft::BasicNode;

use crate::consensus::{
    ClusterState, ConsensusStore, Paused, ReleaseOutcome, Switchover, TakeoverOutcome,
};
use crate::raftnet::{LeaderClient, PgAgentRaftHandle};
use crate::raftstore::{ClusterStateReader, CommandResponse, ConsensusCommand};

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

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if nodes[0].raft.current_leader().await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no leader elected");
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

        let outcome = nodes[follower]
            .store
            .try_takeover(follower as i32, None)
            .await
            .unwrap();
        match outcome {
            TakeoverOutcome::Won { lease } => assert_eq!(lease.holder, follower as i32),
            other => panic!("follower should have won an uncontested lease, got {other:?}"),
        }

        // And every node — leader included — can read it back.
        for (i, n) in nodes.iter().enumerate() {
            let state = n.store.read_state().await.unwrap();
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

        let a = nodes[0].store.try_takeover(0, None).await.unwrap();
        // Node 1 still believes the lease is vacant — a stale
        // observation, and its CAS must lose on that basis.
        let b = nodes[1].store.try_takeover(1, None).await.unwrap();

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

        let TakeoverOutcome::Won { lease } = nodes[0].store.try_takeover(0, None).await.unwrap()
        else {
            panic!("uncontested takeover should win");
        };

        // A node that is not the holder cannot release it.
        let out = nodes[1].store.release(1, lease.term).await.unwrap();
        assert!(matches!(out, ReleaseOutcome::NotHolder { .. }));

        let out = nodes[0].store.release(0, lease.term).await.unwrap();
        assert!(matches!(out, ReleaseOutcome::Released));
        assert!(nodes[2].store.read_state().await.unwrap().lease.is_none());

        let TakeoverOutcome::Won { lease: again } =
            nodes[2].store.try_takeover(2, None).await.unwrap()
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

    /// Pause and switchover ride the same log, so they are subject to
    /// the same quorum — and are readable from any node afterwards.
    #[tokio::test]
    async fn pause_and_switchover_replicate() {
        let shutdown = CancellationToken::new();
        let nodes = cluster(&shutdown).await;

        nodes[1]
            .store
            .set_paused(Some(Paused {
                reason: "maintenance".into(),
                set_by: "operator".into(),
                at: Utc::now(),
            }))
            .await
            .unwrap();
        nodes[2]
            .store
            .set_switchover(Some(Switchover {
                target: 1,
                not_before: None,
            }))
            .await
            .unwrap();

        let state = nodes[0].store.read_state().await.unwrap();
        assert_eq!(state.paused.unwrap().reason, "maintenance");
        assert_eq!(state.switchover.unwrap().target, 1);

        shutdown.cancel();
    }
}
