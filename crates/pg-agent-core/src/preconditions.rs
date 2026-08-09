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
//! destructive action is refused on positive evidence. The 2026-06-11
//! split-brain (pgpool announced a healthy primary as failed during a
//! brief agent restart; the handler promoted a second primary) is
//! exactly this case, and this check would have prevented it.
//!
//! Callers map [`PreconditionOutcome::Unverifiable`] to "log and
//! proceed" — the evidence-gathering failed, and refusing on absent
//! evidence is the unavailability branch of the dilemma above.

use crate::config::NodeConfig;
use crate::peers::PeerRegistry;
use std::sync::Arc;
use tracing::warn;

/// A cluster-state-changing action about to be taken, described with
/// enough context to check it against live cluster state. Handlers add
/// variants as they adopt the validator (TODO.md: failover today;
/// follow_primary / recover / handoff preflights are candidates to
/// converge here).
pub enum ClusterIntent<'a> {
    /// Reactive failover, primary-down branch: promote a successor
    /// because `detached` — the announced-failed primary — is presumed
    /// dead. Wrong when `detached` is alive and still primary.
    PromoteBecausePrimaryDown { detached: &'a NodeConfig },
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
        ClusterIntent::PromoteBecausePrimaryDown { detached }
        | ClusterIntent::DropSlotBecauseStandbyDown { detached } => *detached,
    };

    let status = match peers.client(detached).await {
        Ok(client) => match client.get_status().await {
            Ok(s) => s,
            Err(e) => {
                return PreconditionOutcome::Unverifiable {
                    reason: format!("get_status({}): {e}", detached.hostname),
                };
            }
        },
        Err(e) => {
            return PreconditionOutcome::Unverifiable {
                reason: format!("dial {}: {e}", detached.hostname),
            };
        }
    };

    match intent {
        ClusterIntent::PromoteBecausePrimaryDown { detached } => {
            if status.is_postgres_running && !status.is_in_recovery {
                return PreconditionOutcome::Refuse {
                    message: format!(
                        "failover: refusing to promote: announced-failed primary node {} ({}) \
                         is reachable and running as primary — the failure report is wrong \
                         (likely a health-check false positive, e.g. a brief agent restart), \
                         and promoting a second primary would create split-brain. No action \
                         taken; if node {} really must be replaced, stop it first.",
                        detached.id, detached.hostname, detached.id
                    ),
                };
            }
        }
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
