//! Hook-idempotency markers. Failover / FollowPrimary / RecoveryFirstStage
//! each compute a stable key from their request, check
//! [`ReplayMarkerStore::has`], short-circuit on hit, otherwise execute and
//! [`ReplayMarkerStore::mark_done`] on success.
//!
//! Marker filename: `.pg_agent_idem_<op>_<sha256(op|key) hex>.done`,
//! contents = RFC3339Nano UTC completion timestamp. Sweep prunes markers
//! older than 24 h.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub const DEFAULT_RETENTION: chrono::Duration = chrono::Duration::hours(24);

#[async_trait]
pub trait ReplayMarkerStore: Send + Sync {
    async fn has(&self, op: &str, key: &str) -> anyhow::Result<bool>;

    /// Call only on the success path — writing a marker on a failure return
    /// would let the next retry short-circuit a half-done operation.
    async fn mark_done(&self, op: &str, key: &str) -> anyhow::Result<()>;

    /// Best-effort prune of markers older than retention. Per-marker errors
    /// are logged and skipped — a single broken marker must not block the
    /// sweep.
    async fn sweep(&self, now: DateTime<Utc>);
}

// TODO(v1): FileReplayMarkerStore rooted at $PGDATA, retention configurable.
