//! Durable retry queue for failed cleanup operations (today: peer DropSlot
//! that failed after a successful failover / follow_primary /
//! recovery_1st_stage). The hook RPC returns ok and the cleanup goes here
//! — surfacing it to pgpool would cause loops.
//!
//! Storage: one JSON file per intent under `<agent_dir>/maintenance/`, atomic
//! temp+rename writes. See SPEC §5.13 for sweep cadence, retry budget,
//! backoff.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
pub const MAX_ATTEMPTS: u32 = 5;
pub const BASE_BACKOFF: Duration = Duration::from_secs(30);
pub const MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);
pub const PER_OP_TIMEOUT: Duration = Duration::from_secs(30);

pub const OP_DROP_SLOT_CLEANUP: &str = "drop_slot_cleanup";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaintenanceStatus {
    Pending,
    Done,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceIntent {
    pub id: String,
    pub op: String,
    pub status: MaintenanceStatus,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
}

/// One intent file that could not be loaded. Surfaced through `list`
/// (operator-facing) so corruption doesn't disappear into the log; the
/// background sweep silently ignores these in `list_pending` so a single
/// bad file doesn't block retries of the rest.
#[derive(Debug, Clone)]
pub struct SkippedIntent {
    pub path: String,
    pub error: String,
}

/// Payload shape for `OP_DROP_SLOT_CLEANUP`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropSlotCleanupPayload {
    pub slot_name: String,
    pub target_hostname: String,
    pub cause: String,
    pub last_error: String,
}

#[async_trait]
pub trait MaintenanceStore: Send + Sync {
    async fn append(&self, op: &str, payload: Vec<u8>) -> anyhow::Result<MaintenanceIntent>;

    async fn list_pending(&self) -> anyhow::Result<Vec<MaintenanceIntent>>;

    async fn list(
        &self,
        statuses: &[MaintenanceStatus],
    ) -> anyhow::Result<(Vec<MaintenanceIntent>, Vec<SkippedIntent>)>;

    async fn get(&self, id: &str) -> anyhow::Result<MaintenanceIntent>;

    async fn mark_attempt(
        &self,
        id: &str,
        err_msg: &str,
        next_retry_at: DateTime<Utc>,
    ) -> anyhow::Result<()>;

    async fn mark_done(&self, id: &str) -> anyhow::Result<()>;
    async fn mark_abandoned(&self, id: &str, err_msg: &str) -> anyhow::Result<()>;

    /// Operator-forced retry — sets NextRetryAt without consuming an
    /// attempt-budget slot. Refuses non-pending intents.
    async fn reschedule(&self, id: &str, when: DateTime<Utc>) -> anyhow::Result<()>;
}

pub struct FileMaintenanceStore {
    pub dir: PathBuf,
    pub retention: chrono::Duration,
}

// TODO(v1):
//   - FileMaintenanceStore atomic temp+rename writes; deterministic id
//     `<unix_nano>-<sanitised_op>-<seq>.json`; oldest-first sort by
//     CreatedAt; best-effort prune of terminal intents > retention in
//     list_pending.
//   - MaintenanceWorker: 30 s ticker, per-op 30 s timeout, exponential
//     backoff (30 s base, ×2 capped at 10 min), 5 attempts → MarkAbandoned.
//     Obsolete ops `rewind_restore_replslot` /
//     `rewind_delete_quarantine_slots` → silently MarkDone (legacy intents
//     from Go versions).
//   - Worker also calls ReplayMarkerStore::sweep on the same cadence.
