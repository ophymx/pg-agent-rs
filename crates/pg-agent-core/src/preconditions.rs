//! Pre-execution cluster-state validation.
//!
//! **Defense in depth, not the fix.** Every check here narrows the
//! window for acting on a wrong failure report; none closes it. Under a
//! real partition, "dead" and "unreachable" are indistinguishable by
//! asking around, and both answers to that dilemma are wrong — refusing
//! makes the cluster unavailable during exactly the partition it exists
//! to survive, proceeding is the original split-brain. Closing the
//! class requires a quorum-backed serialization point
//! (docs/promotion-authority.md §3). Do not read this module as having
//! closed the issue.
//!
//! What it *does* buy: when the announced-dead node is reachable and
//! demonstrably healthy, the failure report is provably wrong and the
//! destructive action is refused on positive evidence. (The original
//! motivating case — refusing to promote over a healthy primary, the
//! 2026-06-11 split-brain — is gone along with the pgpool-led promote
//! path itself: promotion is the lease's decision now, and the hook's
//! only destructive action left is the standby-down slot drop.)
//!
//! Callers map [`PreconditionOutcome::Unverifiable`] to "log and
//! proceed" — the evidence-gathering failed, and refusing on absent
//! evidence is the unavailability branch of the dilemma above.

use crate::config::NodeConfig;
use crate::peers::PeerRegistry;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Budget for gathering the evidence. Deliberately much tighter than
/// the peer mesh's general request timeout: this check sits on the
/// **critical path of every failover**, and the case it must answer
/// fastest — the announced-dead node really is dead — is exactly the
/// case where the RPC never returns.
///
/// Measured in the docker acceptance suite before this bound existed:
/// a partition-triggered failover stalled 30 s inside the precondition
/// check (an already-established channel to the isolated peer hung
/// until the full request timeout) before proceeding, adding 30 s to
/// the outage it was reacting to. Evidence we cannot get in five
/// seconds is evidence we do not get.
pub const PRECONDITION_TIMEOUT: Duration = Duration::from_secs(5);

/// A cluster-state-changing action about to be taken, described with
/// enough context to check it against live cluster state. Handlers add
/// variants as they adopt the validator (TODO.md: failover today;
/// follow_primary / recover / handoff preflights are candidates to
/// converge here).
pub enum ClusterIntent<'a> {
    /// Reactive failover, standby-down branch: drop `detached`'s
    /// replication slot because the standby is presumed dead. Wrong
    /// when `detached` is alive and actively streaming — dropping the
    /// slot would break healthy replication.
    DropSlotBecauseStandbyDown { detached: &'a NodeConfig },
}

pub enum PreconditionOutcome {
    /// No contradiction found — proceed.
    Pass,
    /// Positive evidence the intent's premise is wrong. Refuse the
    /// operation and surface `message`; do not write a replay marker,
    /// so a retry after the condition clears is not suppressed.
    Refuse { message: String },
    /// Evidence could not be gathered (peer unreachable, RPC failed).
    /// Callers log and proceed — see module docs for why refusing here
    /// would be worse.
    Unverifiable { reason: String },
}

/// Check `intent` against the live status of the node it presumes dead.
pub async fn validate_cluster_preconditions(
    peers: Arc<dyn PeerRegistry>,
    intent: ClusterIntent<'_>,
) -> PreconditionOutcome {
    let detached = match &intent {
        ClusterIntent::DropSlotBecauseStandbyDown { detached } => *detached,
    };

    let probe = async {
        let client = peers
            .client(detached)
            .await
            .map_err(|e| format!("dial {}: {e}", detached.hostname))?;
        client
            .get_status()
            .await
            .map_err(|e| format!("get_status({}): {e}", detached.hostname))
    };
    let status = match tokio::time::timeout(PRECONDITION_TIMEOUT, probe).await {
        Ok(Ok(s)) => s,
        Ok(Err(reason)) => return PreconditionOutcome::Unverifiable { reason },
        Err(_) => {
            return PreconditionOutcome::Unverifiable {
                reason: format!(
                    "{} did not answer within {}s",
                    detached.hostname,
                    PRECONDITION_TIMEOUT.as_secs()
                ),
            };
        }
    };

    match intent {
        ClusterIntent::DropSlotBecauseStandbyDown { detached } => {
            if status.is_postgres_running
                && status.is_in_recovery
                && status.replication_state == "streaming"
            {
                return PreconditionOutcome::Refuse {
                    message: format!(
                        "failover: refusing to drop slot {}: announced-failed standby node {} \
                         ({}) is reachable, running, and streaming — the failure report is \
                         wrong, and dropping its slot would break healthy replication. No \
                         action taken.",
                        detached.slot_name(),
                        detached.id,
                        detached.hostname
                    ),
                };
            }
        }
    }
    PreconditionOutcome::Pass
}

/// Convenience for the log-and-proceed contract on `Unverifiable`.
pub fn log_unverifiable(check: &str, reason: &str) {
    warn!(
        check,
        reason,
        "precondition unverifiable; proceeding (refusing on absent evidence \
         would trade split-brain risk for guaranteed unavailability — see \
         docs/promotion-authority.md §3)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peers::{PeerClient, PeerRegistry};
    use async_trait::async_trait;
    use pg_agent_proto::pgagentpb as pb;

    /// Peer whose `get_status` never returns — an isolated node with an
    /// already-established channel, which is what a partition looks
    /// like from here.
    struct HangingPeer;

    #[async_trait]
    impl PeerClient for HangingPeer {
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
            std::future::pending().await
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
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
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
            unreachable!()
        }
    }

    struct HangingRegistry;

    #[async_trait]
    impl PeerRegistry for HangingRegistry {
        async fn client(&self, _: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            Ok(Arc::new(HangingPeer))
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unreachable_peer_yields_unverifiable_within_the_budget() {
        let node = NodeConfig {
            id: 0,
            hostname: "db0".into(),
        };
        let started = tokio::time::Instant::now();
        let outcome = validate_cluster_preconditions(
            Arc::new(HangingRegistry),
            ClusterIntent::DropSlotBecauseStandbyDown { detached: &node },
        )
        .await;
        // Auto-advanced virtual clock: assert the bound, not wall time.
        assert!(
            started.elapsed() <= PRECONDITION_TIMEOUT,
            "precondition must not outlive its budget"
        );
        match outcome {
            PreconditionOutcome::Unverifiable { reason } => {
                assert!(reason.contains("did not answer"), "{reason}");
            }
            _ => panic!("expected Unverifiable"),
        }
    }
}
