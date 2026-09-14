//! The consensus-store seam (docs/promotion-authority.md §5).
//!
//! [`ConsensusStore`] is the HA loop's entire view of the replicated
//! state machine: the lease, the pause flag, a scheduled switchover,
//! and a generation counter. It is deliberately *not* openraft's
//! storage interface — it is the decision layer's contract, and what
//! sits behind it is swappable: [`crate::raftstore`]'s openraft-backed
//! implementation in every deployment, [`InMemoryConsensusStore`] in
//! unit tests, an external store if embedded Raft ever proves
//! untrustable. The HA loop is identical under any of them, which is
//! what keeps the storage decision cheap to revisit.
//!
//! # Semantics the trait promises (and impls must honor)
//!
//! - **`read_state` is linearizable.** In the real implementation it is
//!   a ReadIndex quorum round-trip (openraft `ensure_linearizable`); it
//!   fails when a quorum is unreachable. **`Err` means UNKNOWN, never
//!   "vacant"** — a caller that treats a failed read as an empty lease
//!   reintroduces the §3 hole through the front door. The retain path
//!   maps `Err` past `retry_timeout` to "demote local PostgreSQL".
//! - **`try_takeover` is a CAS** on the exact `(holder, term)` the
//!   candidate observed (or on observed vacancy). Concurrent candidates
//!   serialize; one wins, the rest see their precondition fail.
//! - **Terms are fencing tokens**: strictly monotonic across holder
//!   changes, never reused, so a node that was partitioned away can
//!   prove to itself that it lost.
//! - **Reads dominate writes.** Steady state is zero writes — retain is
//!   a read; the log grows only on holder change, pause/resume,
//!   switchover, and membership change.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// State machine types
// ---------------------------------------------------------------------------

/// The committed lease: `holder` is the node id entitled to run
/// PostgreSQL as primary. **This is unrelated to Raft leadership** —
/// the lease is an entry *in* the state machine; Raft leadership is
/// merely how entries commit (promotion-authority: "the separation
/// that must not collapse").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub holder: i32,
    /// Fencing token — strictly monotonic across holder changes.
    pub term: u64,
    pub since: DateTime<Utc>,
}

/// Maintenance pause. `None` in [`ClusterState::paused`] = not paused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Paused {
    pub reason: String,
    /// Issuing identity (operator cert CN once pause ships as a command).
    pub set_by: String,
    pub at: DateTime<Utc>,
}

/// A scheduled planned promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Switchover {
    pub target: i32,
    pub not_before: Option<DateTime<Utc>>,
}

/// The whole replicated state document. Small and deliberately bounded
/// — the log holds decisions, not progress; `inflight_ops` stays local.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterState {
    pub lease: Option<Lease>,
    pub paused: Option<Paused>,
    pub switchover: Option<Switchover>,
    /// Bumped on every committed mutation. New lease terms are minted
    /// from it, which is what makes terms monotonic even across
    /// vacate-then-reacquire sequences.
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TakeoverOutcome {
    /// The CAS committed; the candidate now holds `lease`.
    Won { lease: Lease },
    /// The precondition failed — someone else moved first. `current` is
    /// the lease as committed at decision time (`None` = now vacant).
    Lost { current: Option<Lease> },
}

/// Serde-carried because it is a Raft state-machine response
/// ([`crate::raftstore::CommandResponse`]) once the store is
/// openraft-backed, not only an in-process return value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReleaseOutcome {
    Released,
    /// The `(holder, term)` presented is not the committed lease — the
    /// caller's view was stale. Nothing was changed.
    NotHolder {
        current: Option<Lease>,
    },
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ConsensusStore: Send + Sync {
    /// Linearizable read of the full state document. `Err` means the
    /// state is **unknown** (no quorum), not vacant — see module docs.
    async fn read_state(&self) -> anyhow::Result<ClusterState>;

    /// Compare-and-swap the lease to `candidate`. `expected` is what
    /// the candidate observed: `Some((holder, term))` to take over from
    /// a holder it watched go unhealthy past `leader_ttl`, `None` if it
    /// observed vacancy. Commits only if the observation still holds.
    async fn try_takeover(
        &self,
        candidate: i32,
        expected: Option<(i32, u64)>,
    ) -> anyhow::Result<TakeoverOutcome>;

    /// Voluntarily vacate the lease (the demote path). Succeeds only if
    /// `(holder, term)` is still the committed lease — a stale holder
    /// cannot clobber its successor's lease.
    async fn release(&self, holder: i32, term: u64) -> anyhow::Result<ReleaseOutcome>;

    /// Set or clear the pause flag.
    async fn set_paused(&self, paused: Option<Paused>) -> anyhow::Result<()>;

    /// Set or clear the scheduled switchover.
    async fn set_switchover(&self, switchover: Option<Switchover>) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// InMemoryConsensusStore — tests
// ---------------------------------------------------------------------------

/// Deterministic in-memory [`ConsensusStore`], for **unit tests only**.
///
/// The fault-injection switches turn partition and failure scenarios
/// into ordinary assertions: "read fails mid-retain", "CAS races
/// another candidate", "store goes dark past retry_timeout".
///
/// No daemon has ever been able to run on this — the agent used to be
/// able to, back when consensus was optional and a node could fall back
/// to a process-local store authoritative for nothing. That fallback is
/// gone: `pg_agentd` builds a raft-backed store or fails to start.
///
/// Persistence is deliberately absent: durability only matters when a
/// store's answers are authoritative promises (a vote, a committed
/// entry must survive a crash). That belongs to the openraft
/// implementation, not here.
#[derive(Default)]
pub struct InMemoryConsensusStore {
    state: Mutex<ClusterState>,
    /// Fail the next N reads (then recover). Models a transient loss of
    /// quorum contact.
    fail_reads: AtomicU32,
    /// Fail the next N writes (then recover).
    fail_writes: AtomicU32,
    /// Persistent unavailability switch — models a full partition from
    /// the quorum until cleared. Trumps the counters.
    unavailable: std::sync::atomic::AtomicBool,
}

impl InMemoryConsensusStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the store with a starting state (test arrangement).
    pub fn install(&self, state: ClusterState) {
        *self.state.lock().unwrap() = state;
    }

    /// Non-linearizable peek for test assertions. Bypasses fault
    /// injection on purpose — assertions about state must not be
    /// confused by an injected outage.
    pub fn snapshot(&self) -> ClusterState {
        self.state.lock().unwrap().clone()
    }

    pub fn fail_next_reads(&self, n: u32) {
        self.fail_reads.store(n, Ordering::SeqCst);
    }

    pub fn fail_next_writes(&self, n: u32) {
        self.fail_writes.store(n, Ordering::SeqCst);
    }

    pub fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn check_gate(&self, counter: &AtomicU32, what: &str) -> anyhow::Result<()> {
        if self.unavailable.load(Ordering::SeqCst) {
            anyhow::bail!("consensus store: unavailable (injected partition)");
        }
        // Decrement-if-positive without underflow.
        let mut cur = counter.load(Ordering::SeqCst);
        while cur > 0 {
            match counter.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => anyhow::bail!("consensus store: {what} failed (injected)"),
                Err(actual) => cur = actual,
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ConsensusStore for InMemoryConsensusStore {
    async fn read_state(&self) -> anyhow::Result<ClusterState> {
        self.check_gate(&self.fail_reads, "read")?;
        Ok(self.state.lock().unwrap().clone())
    }

    async fn try_takeover(
        &self,
        candidate: i32,
        expected: Option<(i32, u64)>,
    ) -> anyhow::Result<TakeoverOutcome> {
        self.check_gate(&self.fail_writes, "takeover")?;
        let mut state = self.state.lock().unwrap();
        let observed_holds = match (&state.lease, expected) {
            (None, None) => true,
            (Some(cur), Some((h, t))) => cur.holder == h && cur.term == t,
            _ => false,
        };
        if !observed_holds {
            return Ok(TakeoverOutcome::Lost {
                current: state.lease.clone(),
            });
        }
        state.generation += 1;
        let lease = Lease {
            holder: candidate,
            // Minted from the post-bump generation: strictly greater
            // than every term ever issued, including across
            // vacate-then-reacquire (a plain `old term + 1` would reuse
            // terms after a release).
            term: state.generation,
            since: Utc::now(),
        };
        state.lease = Some(lease.clone());
        Ok(TakeoverOutcome::Won { lease })
    }

    async fn release(&self, holder: i32, term: u64) -> anyhow::Result<ReleaseOutcome> {
        self.check_gate(&self.fail_writes, "release")?;
        let mut state = self.state.lock().unwrap();
        match &state.lease {
            Some(cur) if cur.holder == holder && cur.term == term => {
                state.generation += 1;
                state.lease = None;
                Ok(ReleaseOutcome::Released)
            }
            other => Ok(ReleaseOutcome::NotHolder {
                current: other.clone(),
            }),
        }
    }

    async fn set_paused(&self, paused: Option<Paused>) -> anyhow::Result<()> {
        self.check_gate(&self.fail_writes, "set_paused")?;
        let mut state = self.state.lock().unwrap();
        state.generation += 1;
        state.paused = paused;
        Ok(())
    }

    async fn set_switchover(&self, switchover: Option<Switchover>) -> anyhow::Result<()> {
        self.check_gate(&self.fail_writes, "set_switchover")?;
        let mut state = self.state.lock().unwrap();
        state.generation += 1;
        state.switchover = switchover;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> InMemoryConsensusStore {
        InMemoryConsensusStore::new()
    }

    #[tokio::test]
    async fn vacant_takeover_wins_and_mints_term() {
        let s = store();
        let out = s.try_takeover(1, None).await.unwrap();
        match out {
            TakeoverOutcome::Won { lease } => {
                assert_eq!(lease.holder, 1);
                assert!(lease.term > 0);
            }
            other => panic!("expected Won, got {other:?}"),
        }
        assert_eq!(s.snapshot().lease.unwrap().holder, 1);
    }

    #[tokio::test]
    async fn takeover_on_stale_observation_loses() {
        let s = store();
        s.try_takeover(1, None).await.unwrap();
        // Node 2 observed vacancy before node 1 won — its CAS must lose.
        let out = s.try_takeover(2, None).await.unwrap();
        match out {
            TakeoverOutcome::Lost { current } => {
                assert_eq!(current.unwrap().holder, 1);
            }
            other => panic!("expected Lost, got {other:?}"),
        }
        // Wrong term loses too.
        let cur = s.snapshot().lease.unwrap();
        let out = s
            .try_takeover(2, Some((cur.holder, cur.term + 99)))
            .await
            .unwrap();
        assert!(matches!(out, TakeoverOutcome::Lost { .. }));
    }

    #[tokio::test]
    async fn takeover_with_correct_observation_wins_and_bumps_term() {
        let s = store();
        s.try_takeover(1, None).await.unwrap();
        let cur = s.snapshot().lease.unwrap();
        let out = s
            .try_takeover(2, Some((cur.holder, cur.term)))
            .await
            .unwrap();
        match out {
            TakeoverOutcome::Won { lease } => {
                assert_eq!(lease.holder, 2);
                assert!(lease.term > cur.term, "fencing: term must increase");
            }
            other => panic!("expected Won, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn terms_stay_monotonic_across_vacate_and_reacquire() {
        let s = store();
        s.try_takeover(1, None).await.unwrap();
        let t1 = s.snapshot().lease.unwrap().term;
        s.release(1, t1).await.unwrap();
        let out = s.try_takeover(2, None).await.unwrap();
        match out {
            TakeoverOutcome::Won { lease } => {
                assert!(lease.term > t1, "term reuse would break fencing");
            }
            other => panic!("expected Won, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn release_requires_exact_lease() {
        let s = store();
        s.try_takeover(1, None).await.unwrap();
        let term = s.snapshot().lease.unwrap().term;
        // Wrong holder / wrong term: no change.
        assert!(matches!(
            s.release(2, term).await.unwrap(),
            ReleaseOutcome::NotHolder { .. }
        ));
        assert!(matches!(
            s.release(1, term + 1).await.unwrap(),
            ReleaseOutcome::NotHolder { .. }
        ));
        assert!(s.snapshot().lease.is_some());
        // Exact match releases.
        assert_eq!(s.release(1, term).await.unwrap(), ReleaseOutcome::Released);
        assert!(s.snapshot().lease.is_none());
    }

    #[tokio::test]
    async fn injected_read_failures_are_transient() {
        let s = store();
        s.fail_next_reads(2);
        assert!(s.read_state().await.is_err());
        assert!(s.read_state().await.is_err());
        assert!(s.read_state().await.is_ok());
    }

    #[tokio::test]
    async fn unavailable_blocks_reads_and_writes_until_cleared() {
        let s = store();
        s.set_unavailable(true);
        assert!(s.read_state().await.is_err());
        assert!(s.try_takeover(1, None).await.is_err());
        // The partition heals; the store answers again, state unchanged.
        s.set_unavailable(false);
        assert!(s.read_state().await.unwrap().lease.is_none());
    }

    #[tokio::test]
    async fn paused_and_switchover_round_trip() {
        let s = store();
        let g0 = s.snapshot().generation;
        s.set_paused(Some(Paused {
            reason: "kernel upgrade".into(),
            set_by: "operator".into(),
            at: Utc::now(),
        }))
        .await
        .unwrap();
        s.set_switchover(Some(Switchover {
            target: 2,
            not_before: None,
        }))
        .await
        .unwrap();
        let state = s.read_state().await.unwrap();
        assert_eq!(state.paused.as_ref().unwrap().reason, "kernel upgrade");
        assert_eq!(state.switchover.as_ref().unwrap().target, 2);
        assert!(state.generation > g0, "mutations must bump generation");
        s.set_paused(None).await.unwrap();
        assert!(s.read_state().await.unwrap().paused.is_none());
    }
}
