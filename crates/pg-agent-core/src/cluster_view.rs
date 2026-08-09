//! Cross-node status collection and WAL-position comparison.
//!
//! Two pieces that every "who is most advanced?" question needs:
//!
//! - [`collect_statuses`] — fan out `GetStatus` to a set of peers in
//!   parallel under one wall-clock budget, returning per-peer results
//!   (reachable-with-status vs error) instead of silently dropping
//!   failures. Callers classify; this module only collects.
//! - [`WalPosition`] — a `(timeline, lsn)` pair ordered
//!   lexicographically: a higher timeline is *always* ahead of a lower
//!   one (it has survived a promotion the other node hasn't seen), and
//!   within a timeline the higher LSN has more WAL.
//!
//! Consumers today: the reactive-failover lag gate in
//! `localserver::failover` and the phantom-primary startup check in
//! `agent`. The HA loop's candidate selection (docs/promotion-authority.md
//! §"The HA loop") is the intended third consumer — "am I the
//! most-advanced reachable candidate?" is this comparator over this
//! collector's output.

use crate::config::NodeConfig;
use crate::peers::PeerRegistry;
use pg_agent_proto::pgagentpb as pb;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

/// Default wall-clock budget for one status fan-out. Matches the
/// per-peer dial timeout in [`crate::peers`] — long enough for a TLS
/// handshake to every reachable peer in parallel, tight enough that a
/// partitioned peer can't stall the caller.
pub const STATUS_FANOUT_BUDGET: Duration = Duration::from_secs(5);

/// One peer's answer from a [`collect_statuses`] fan-out. `status` is
/// `Err` for both "dial failed" and "RPC failed" — the distinction is
/// preserved in the error text, and no caller branches on it.
pub struct PeerStatusView {
    pub node: NodeConfig,
    pub status: anyhow::Result<pb::NodeStatus>,
}

/// Fan out `GetStatus` to every node in `nodes`, all in parallel, under
/// one `budget`. Returns one [`PeerStatusView`] per node (order not
/// guaranteed). `Err(Elapsed)` means the budget expired before every
/// peer answered — the caller decides whether partial evidence would
/// have been acceptable; none is returned.
pub async fn collect_statuses(
    registry: Arc<dyn PeerRegistry>,
    nodes: &[NodeConfig],
    budget: Duration,
) -> Result<Vec<PeerStatusView>, tokio::time::error::Elapsed> {
    let fanout = async {
        let mut js: JoinSet<PeerStatusView> = JoinSet::new();
        for node in nodes.iter().cloned() {
            let registry = registry.clone();
            js.spawn(async move {
                let status = match registry.client(&node).await {
                    Ok(client) => client.get_status().await,
                    Err(e) => Err(e),
                };
                PeerStatusView { node, status }
            });
        }
        let mut out = Vec::new();
        while let Some(res) = js.join_next().await {
            if let Ok(view) = res {
                out.push(view);
            }
        }
        out
    };
    tokio::time::timeout(budget, fanout).await
}

/// A node's WAL position: `(timeline, lsn)`, ordered lexicographically.
/// The derived `Ord` is the comparison rule — timeline dominates, LSN
/// breaks ties within a timeline. Comparing LSNs *across* timelines as
/// raw numbers would be meaningless; the lexicographic order encodes
/// "a promotion outranks any amount of unpromoted WAL".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalPosition {
    pub timeline: i32,
    pub lsn: u64,
}

impl WalPosition {
    /// Extract a position from a `NodeStatus`, requiring **both** fields
    /// to be known. `GetStatus` reports `timeline_id = 0` /
    /// `current_wal_lsn = 0` when its probes fail (0 = unknown by
    /// convention, see `Agent::get_status`); a half-known position must
    /// not participate in an ordering decision.
    pub fn from_status(s: &pb::NodeStatus) -> Option<Self> {
        (s.timeline_id > 0 && s.current_wal_lsn > 0).then_some(Self {
            timeline: s.timeline_id,
            lsn: s.current_wal_lsn,
        })
    }

    /// Bytes this position trails `ahead` by, when both are on the same
    /// timeline. `None` for cross-timeline comparisons — there is no
    /// meaningful byte distance across a promotion.
    pub fn lag_behind(&self, ahead: &Self) -> Option<u64> {
        (self.timeline == ahead.timeline).then(|| ahead.lsn.saturating_sub(self.lsn))
    }
}

impl fmt::Display for WalPosition {
    /// PostgreSQL-style rendering: `TL3@1A/2B00C000`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TL{}@{:X}/{:X}",
            self.timeline,
            self.lsn >> 32,
            self.lsn & 0xFFFF_FFFF
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(timeline: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            timeline_id: timeline,
            current_wal_lsn: lsn,
            ..Default::default()
        }
    }

    #[test]
    fn ordering_is_lexicographic_timeline_first() {
        let old_tl_far_ahead = WalPosition {
            timeline: 1,
            lsn: u64::MAX,
        };
        let new_tl_at_start = WalPosition {
            timeline: 2,
            lsn: 1,
        };
        assert!(new_tl_at_start > old_tl_far_ahead);

        let a = WalPosition {
            timeline: 3,
            lsn: 100,
        };
        let b = WalPosition {
            timeline: 3,
            lsn: 200,
        };
        assert!(b > a);
        assert_eq!(a, a);
    }

    #[test]
    fn from_status_requires_both_fields() {
        assert!(WalPosition::from_status(&status(0, 100)).is_none());
        assert!(WalPosition::from_status(&status(2, 0)).is_none());
        assert!(WalPosition::from_status(&status(0, 0)).is_none());
        assert_eq!(
            WalPosition::from_status(&status(2, 100)),
            Some(WalPosition {
                timeline: 2,
                lsn: 100
            })
        );
    }

    #[test]
    fn lag_behind_same_timeline_only() {
        let behind = WalPosition {
            timeline: 2,
            lsn: 1000,
        };
        let ahead = WalPosition {
            timeline: 2,
            lsn: 5000,
        };
        assert_eq!(behind.lag_behind(&ahead), Some(4000));
        // Not actually behind — saturates to zero rather than wrapping.
        assert_eq!(ahead.lag_behind(&behind), Some(0));
        let other_tl = WalPosition {
            timeline: 3,
            lsn: 1,
        };
        assert_eq!(behind.lag_behind(&other_tl), None);
    }

    #[test]
    fn display_matches_pg_lsn_rendering() {
        let p = WalPosition {
            timeline: 3,
            lsn: (0x1A << 32) | 0x2B00_C000,
        };
        assert_eq!(p.to_string(), "TL3@1A/2B00C000");
    }
}
