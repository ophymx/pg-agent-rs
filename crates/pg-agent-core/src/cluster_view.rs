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
use std::time::{Duration, Instant};
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

/// When this node last observed each peer **running as a primary**.
///
/// Written by the HA loop's per-tick fan-out and read by
/// `Agent::get_status`, which ships the ages to peers as
/// `NodeStatus.peer_primary_seen_age_ms`. That is what lets a
/// candidate ask "does anyone else still watch the holder serve?"
/// before deposing it — the second-opinion gate (finding 25). Ages,
/// never timestamps: the cluster assumes no clock synchronization.
///
/// SERVING, not reachable. A holder whose PostgreSQL died still
/// answers `GetStatus` from its healthy agent, so recording mere
/// contact here would make every witness vouch for a dead primary and
/// block the most ordinary failover there is — which is exactly what
/// it did the first time this was built.
#[derive(Default)]
pub struct PeerSeen {
    inner: std::sync::Mutex<std::collections::HashMap<i32, Instant>>,
}

impl PeerSeen {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `node_id` was observed running as a primary, now.
    pub fn record_primary(&self, node_id: i32) {
        self.inner.lock().unwrap().insert(node_id, Instant::now());
    }

    /// Age in milliseconds of the last primary sighting per peer.
    /// Peers never seen serving are absent — "no evidence", which a
    /// consumer must not read as "seen long ago" or "seen recently".
    pub fn ages_ms(&self) -> std::collections::HashMap<i32, u64> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, at)| (*id, at.elapsed().as_millis() as u64))
            .collect()
    }
}

/// Fan out `GetStatus` to every node in `nodes`, all in parallel, under
/// one `budget`. ALWAYS returns one [`PeerStatusView`] per node (order
/// not guaranteed): peers that answered carry their status, peers that
/// did not answer before the budget expired carry an `Err`.
///
/// Partial evidence is the whole point. The previous contract returned
/// `Err(Elapsed)` with NOTHING when any peer outlived the budget — and
/// the HA loop mapped that to an empty view, so one unreachable peer
/// (a partition — exactly when the view matters) blinded the caller to
/// every peer that DID answer. A rival then read the missing holder as
/// "unhealthy", ran its deposal clock on absence of evidence, and
/// deposed a healthy serving primary (acceptance G5, caught by the
/// audit's dual-serving invariant; the fence contained it). One slow
/// peer must degrade exactly one peer's evidence.
///
/// `seen`, when supplied, is updated for every peer observed SERVING as
/// a primary — the recording lives here, in the one place peer statuses
/// arrive, rather than in whichever caller happened to need it. It was
/// caller-side once, attached to the HA loop, and the second fan-out
/// path (`cluster_status`) silently never recorded: a fact about the
/// mechanism belongs to the mechanism.
pub async fn collect_statuses(
    registry: Arc<dyn PeerRegistry>,
    nodes: &[NodeConfig],
    budget: Duration,
    seen: Option<&PeerSeen>,
) -> Vec<PeerStatusView> {
    let deadline = tokio::time::Instant::now() + budget;
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
    while !js.is_empty() {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        match tokio::time::timeout(deadline - now, js.join_next()).await {
            Ok(Some(Ok(view))) => out.push(view),
            Ok(Some(Err(_join_err))) => {} // task panicked; its Err entry is added below
            Ok(None) => break,
            Err(_) => break, // budget expired; stragglers get Err entries below
        }
    }
    js.abort_all();
    // One definition of "I saw that node serving", applied wherever
    // statuses arrive. SERVING, not merely answering: a node whose
    // PostgreSQL has died keeps answering GetStatus from a healthy
    // agent, and a witness must vouch for the role (finding 25).
    if let Some(seen) = seen {
        for v in &out {
            if let Ok(s) = &v.status {
                if s.is_postgres_running && !s.is_in_recovery {
                    seen.record_primary(v.node.id);
                }
            }
        }
    }
    for node in nodes {
        if !out.iter().any(|v| v.node.id == node.id) {
            out.push(PeerStatusView {
                node: node.clone(),
                status: Err(anyhow::anyhow!(
                    "did not answer within the {budget:?} status fan-out budget"
                )),
            });
        }
    }
    out
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
    /// to be known (0 = unknown by convention, see `Agent::get_status`);
    /// a half-known position must not participate in an ordering
    /// decision.
    ///
    /// The LSN is `last_flush_lsn`, not `current_wal_lsn`: candidate
    /// selection must compare what a node has durably FLUSHED, because
    /// that is what it owns and will replay before promoting — and
    /// under quorum commit, what the acknowledged-write guarantee
    /// attaches to (docs/quorum-commit.md §4). Replay-based comparison
    /// misclassified a flush-complete standby as lagging (finding 19).
    pub fn from_status(s: &pb::NodeStatus) -> Option<Self> {
        (s.timeline_id > 0 && s.last_flush_lsn > 0).then_some(Self {
            timeline: s.timeline_id,
            lsn: s.last_flush_lsn,
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
    use crate::peers::{PeerClient, PeerRegistry};
    use async_trait::async_trait;

    fn status(timeline: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            timeline_id: timeline,
            last_flush_lsn: lsn,
            ..Default::default()
        }
    }

    /// Peer whose `get_status` hangs forever (node 1) or answers as a
    /// healthy primary (everyone else) — the G5 shape: one partitioned
    /// peer, the rest fine.
    struct SplitClient {
        hang: bool,
    }

    #[async_trait]
    impl PeerClient for SplitClient {
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
            if self.hang {
                std::future::pending().await
            } else {
                Ok(pb::NodeStatus {
                    is_postgres_running: true,
                    ..Default::default()
                })
            }
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
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>> {
            unreachable!()
        }
    }

    struct SplitRegistry;

    #[async_trait]
    impl PeerRegistry for SplitRegistry {
        async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            Ok(Arc::new(SplitClient { hang: node.id == 1 }))
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// The G5 regression: one hanging peer must cost exactly ONE
    /// peer's evidence, never the whole view. (The old contract
    /// returned Err with nothing, the HA loop mapped that to an empty
    /// view, and a rival deposed a healthy serving holder because the
    /// holder was simply missing from it.)
    #[tokio::test(start_paused = true)]
    async fn one_hanging_peer_degrades_only_its_own_evidence() {
        let nodes: Vec<NodeConfig> = (0..2)
            .map(|id| NodeConfig {
                id,
                hostname: format!("db{id}"),
            })
            .collect();
        let views = collect_statuses(
            Arc::new(SplitRegistry),
            &nodes,
            Duration::from_secs(5),
            None,
        )
        .await;
        assert_eq!(views.len(), 2, "one view per node, always");
        let healthy = views.iter().find(|v| v.node.id == 0).unwrap();
        assert!(
            healthy.status.as_ref().is_ok_and(|s| s.is_postgres_running),
            "the answering peer's evidence must survive the straggler"
        );
        let hung = views.iter().find(|v| v.node.id == 1).unwrap();
        assert!(
            hung.status.is_err(),
            "the straggler degrades to an explicit Err entry"
        );
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
