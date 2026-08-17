//! Durable retry queue for failed cleanup operations.
//!
//! **Scope today: failed `DropSlot` retry only.** A peer (or local)
//! `pg_drop_replication_slot` call that fails *after* the primary
//! success of `Failover` / `FollowPrimary` / `RecoveryFirstStage` leaves
//! an orphan replication slot. Orphan slots pin WAL on the primary and
//! eventually fill the disk — a slow-motion ticking time bomb. The hook
//! RPC must still return `ok=true` to pgpool (the cluster's
//! authoritative side already succeeded); the cleanup is deferred here.
//!
//! The design accommodates additional intent types but adding one is a
//! **deliberate choice**, not a default. See SPEC §5.13.
//!
//! # Typed payload (vs. Go's raw JSON)
//!
//! The payload is a Rust enum with `#[serde(tag = "op")]`. Adding a new
//! variant forces the worker's `match` to handle it (compile error
//! otherwise) — the polymorphism stays type-safe end to end. No runtime
//! "unsupported op" branch, no `serde_json::from_slice` dispatch.
//!
//! # On-disk format
//!
//! One JSON file per intent under `<state_dir>/maintenance/`, named
//! `<unix_nano>-<sanitised_op>-<seq>.json`. Writes are atomic via
//! temp + rename in the same directory.

use crate::config::NodePool;
use crate::localdb::LocalDb;
use crate::peers::PeerRegistry;
use crate::replay_markers::ReplayMarkerStore;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
pub const MAX_ATTEMPTS: u32 = 5;
pub const BASE_BACKOFF: Duration = Duration::from_secs(30);
pub const MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);
pub const PER_OP_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaintenanceStatus {
    Pending,
    Done,
    Abandoned,
}

impl MaintenanceStatus {
    pub fn as_wire(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Abandoned => "abandoned",
        }
    }
}

/// What the queue is for. Tag `"op"` lets the on-disk JSON discriminate
/// between variants; the enum closes the universe so the worker's
/// `match` can't miss a case.
///
/// **New variants are a deliberate choice.** Most "I tried something
/// and it failed" cases should stay as bubble-up errors. See SPEC §5.13.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MaintenancePayload {
    /// Failed `DropSlot` after a primary-success hook (Failover,
    /// FollowPrimary, RecoveryFirstStage).
    DropSlotCleanup {
        /// Name of the replication slot to drop (`node{id}` convention).
        slot_name: String,
        /// Hostname of the node hosting the slot. The worker dispatches
        /// `LocalDb::drop_slot` if it's the local node, else
        /// `PeerClient::drop_slot` against the resolved peer.
        target_hostname: String,
        /// Code-level reason we enqueued. Free-form; consumed by
        /// operator review only.
        cause: String,
        /// Error message at enqueue time. Preserved across retries; the
        /// intent's own `last_error` field gets overwritten by each
        /// failed retry.
        initial_error: String,
    },
}

impl MaintenancePayload {
    /// String tag for the on-disk filename + the wire `MaintenanceIntent.op`
    /// field. Derived from the variant — single source of truth.
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::DropSlotCleanup { .. } => "drop_slot_cleanup",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaintenanceIntent {
    pub id: String,
    pub status: MaintenanceStatus,
    pub payload: MaintenancePayload,
    #[serde(default)]
    pub attempts: u32,
    /// Most-recent retry's failure message. Empty until the first retry.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
}

/// An on-disk intent file that couldn't be loaded (malformed JSON, read
/// error). Surfaced through `list()` so corruption doesn't disappear
/// into a warn log; `list_pending()` silently skips them so the worker
/// keeps making progress on the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedIntent {
    pub path: String,
    pub error: String,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

#[async_trait]
pub trait MaintenanceStore: Send + Sync {
    /// Create + persist a new pending intent. Returns the intent (with
    /// generated id) so the caller can log it for correlation.
    async fn append(&self, payload: MaintenancePayload) -> anyhow::Result<MaintenanceIntent>;

    /// Pending intents only, oldest-first by `created_at`. Best-effort
    /// prunes terminal (done/abandoned) intents past retention as a
    /// side effect.
    async fn list_pending(&self) -> anyhow::Result<Vec<MaintenanceIntent>>;

    /// All intents matching `statuses` (empty = every status), oldest
    /// first. Second return is the corruption list — operator-facing
    /// surfaces use this to flag bad files instead of swallowing them.
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

    /// Operator-forced retry — sets `NextRetryAt` without bumping
    /// `attempts`. Refuses non-pending intents (re-running a done /
    /// abandoned intent would be the operator's mistake).
    async fn reschedule(&self, id: &str, when: DateTime<Utc>) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// FileMaintenanceStore
// ---------------------------------------------------------------------------

pub struct FileMaintenanceStore {
    dir: PathBuf,
    retention: chrono::Duration,
    seq: AtomicU64,
}

impl FileMaintenanceStore {
    /// Build a store rooted at `dir` (typically
    /// `<state_dir>/maintenance/`). The daemon creates the directory at
    /// startup — we don't here, matching the replay-marker store.
    pub fn new(dir: PathBuf, retention: chrono::Duration) -> Self {
        Self {
            dir,
            retention,
            seq: AtomicU64::new(0),
        }
    }

    fn intent_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn make_id(&self, payload: &MaintenancePayload, now: DateTime<Utc>) -> String {
        // `<unix_nano>-<op>-<seq>` — seq disambiguates within the same
        // nanosecond (rare but possible during sweep storms). Matches
        // the Go id shape; operators can scan-sort this lexicographically
        // and get oldest-first.
        let nanos = now.timestamp_nanos_opt().unwrap_or(0);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{nanos}-{}-{seq}", payload.op_name())
    }

    async fn write_atomic(&self, intent: &MaintenanceIntent) -> anyhow::Result<()> {
        let final_path = self.intent_path(&intent.id);
        let tmp_path = final_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(intent)
            .map_err(|e| anyhow::anyhow!("maintenance: marshal {}: {e}", intent.id))?;

        tokio::fs::write(&tmp_path, &bytes)
            .await
            .map_err(|e| anyhow::anyhow!("maintenance: write tmp {}: {e}", tmp_path.display()))?;
        if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(anyhow::anyhow!(
                "maintenance: rename {} -> {}: {e}",
                tmp_path.display(),
                final_path.display()
            ));
        }
        Ok(())
    }

    async fn read_one(&self, path: &Path) -> anyhow::Result<MaintenanceIntent> {
        let raw = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("maintenance: read {}: {e}", path.display()))?;
        serde_json::from_slice(&raw)
            .map_err(|e| anyhow::anyhow!("maintenance: parse {}: {e}", path.display()))
    }

    /// Read every `*.json` file in `dir` — separate good intents from
    /// corrupt ones. Used by both `list` and `list_pending`.
    async fn read_all(&self) -> anyhow::Result<(Vec<MaintenanceIntent>, Vec<SkippedIntent>)> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((vec![], vec![])),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "maintenance: read_dir {}: {e}",
                    self.dir.display()
                ));
            }
        };

        let mut intents = Vec::new();
        let mut skipped = Vec::new();
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    return Err(anyhow::anyhow!("maintenance: next_entry: {e}"));
                }
            };
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let name = path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            match self.read_one(&path).await {
                Ok(intent) => intents.push(intent),
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        ?e,
                        "maintenance: skipping unreadable intent"
                    );
                    skipped.push(SkippedIntent {
                        path: name,
                        error: e.to_string(),
                    });
                }
            }
        }
        Ok((intents, skipped))
    }
}

#[async_trait]
impl MaintenanceStore for FileMaintenanceStore {
    async fn append(&self, payload: MaintenancePayload) -> anyhow::Result<MaintenanceIntent> {
        let now = Utc::now();
        let id = self.make_id(&payload, now);
        let intent = MaintenanceIntent {
            id,
            status: MaintenanceStatus::Pending,
            payload,
            attempts: 0,
            last_error: String::new(),
            created_at: now,
            updated_at: now,
            next_retry_at: None,
        };
        self.write_atomic(&intent).await?;
        Ok(intent)
    }

    async fn list_pending(&self) -> anyhow::Result<Vec<MaintenanceIntent>> {
        let (intents, _skipped) = self.read_all().await?;
        let now = Utc::now();
        let mut pending = Vec::new();
        for intent in intents {
            match intent.status {
                MaintenanceStatus::Pending => pending.push(intent),
                MaintenanceStatus::Done | MaintenanceStatus::Abandoned => {
                    // Best-effort prune of terminal intents past
                    // retention. A failure here just means we'll try
                    // again next sweep — never a blocker.
                    if now.signed_duration_since(intent.updated_at) > self.retention {
                        let p = self.intent_path(&intent.id);
                        if let Err(e) = tokio::fs::remove_file(&p).await {
                            warn!(
                                id = %intent.id,
                                status = ?intent.status,
                                ?e,
                                "maintenance: prune terminal intent failed"
                            );
                        }
                    }
                }
            }
        }
        pending.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(pending)
    }

    async fn list(
        &self,
        statuses: &[MaintenanceStatus],
    ) -> anyhow::Result<(Vec<MaintenanceIntent>, Vec<SkippedIntent>)> {
        let (mut intents, mut skipped) = self.read_all().await?;
        if !statuses.is_empty() {
            intents.retain(|i| statuses.contains(&i.status));
        }
        intents.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        skipped.sort_by(|a, b| a.path.cmp(&b.path));
        Ok((intents, skipped))
    }

    async fn get(&self, id: &str) -> anyhow::Result<MaintenanceIntent> {
        if id.is_empty() {
            anyhow::bail!("maintenance: id is required");
        }
        self.read_one(&self.intent_path(id)).await
    }

    async fn mark_attempt(
        &self,
        id: &str,
        err_msg: &str,
        next_retry_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let mut intent = self.get(id).await?;
        intent.attempts += 1;
        intent.last_error = err_msg.to_string();
        intent.updated_at = Utc::now();
        intent.next_retry_at = Some(next_retry_at);
        intent.status = MaintenanceStatus::Pending;
        self.write_atomic(&intent).await
    }

    async fn mark_done(&self, id: &str) -> anyhow::Result<()> {
        let mut intent = self.get(id).await?;
        intent.status = MaintenanceStatus::Done;
        intent.updated_at = Utc::now();
        intent.next_retry_at = None;
        intent.last_error.clear();
        self.write_atomic(&intent).await
    }

    async fn mark_abandoned(&self, id: &str, err_msg: &str) -> anyhow::Result<()> {
        let mut intent = self.get(id).await?;
        intent.status = MaintenanceStatus::Abandoned;
        intent.updated_at = Utc::now();
        intent.last_error = err_msg.to_string();
        intent.next_retry_at = None;
        self.write_atomic(&intent).await
    }

    async fn reschedule(&self, id: &str, when: DateTime<Utc>) -> anyhow::Result<()> {
        let mut intent = self.get(id).await?;
        if intent.status != MaintenanceStatus::Pending {
            anyhow::bail!(
                "maintenance: cannot reschedule intent {id} in status {:?}",
                intent.status
            );
        }
        intent.updated_at = Utc::now();
        intent.next_retry_at = Some(when);
        self.write_atomic(&intent).await
    }
}

// ---------------------------------------------------------------------------
// MaintenanceWorker
// ---------------------------------------------------------------------------

pub struct MaintenanceWorker {
    store: Arc<dyn MaintenanceStore>,
    peers: Arc<dyn PeerRegistry>,
    node_pool: NodePool,
    db: Arc<dyn LocalDb>,
    replay: Arc<dyn ReplayMarkerStore>,
    /// Consulted before executing a `drop_slot_cleanup` — see
    /// [`Self::process_drop_slot`].
    inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
    sweep_every: Duration,
}

impl MaintenanceWorker {
    pub fn new(
        store: Arc<dyn MaintenanceStore>,
        peers: Arc<dyn PeerRegistry>,
        node_pool: NodePool,
        db: Arc<dyn LocalDb>,
        replay: Arc<dyn ReplayMarkerStore>,
        inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
        sweep_every: Duration,
    ) -> Self {
        Self {
            store,
            peers,
            node_pool,
            db,
            replay,
            inflight,
            sweep_every,
        }
    }

    /// Sweep loop. Runs an initial sweep immediately, then ticks on
    /// `sweep_every`. The replay-marker sweep piggybacks on the same
    /// cadence — one timer, two janitor jobs. Returns on cancellation.
    pub async fn run(&self, shutdown: CancellationToken) {
        loop {
            self.process_sweep().await;
            self.replay.sweep(Utc::now()).await;

            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(self.sweep_every) => {}
            }
        }
    }

    async fn process_sweep(&self) {
        let intents = match self.store.list_pending().await {
            Ok(i) => i,
            Err(e) => {
                warn!(?e, "maintenance: list_pending failed");
                return;
            }
        };
        let now = Utc::now();
        for intent in intents {
            if matches!(intent.next_retry_at, Some(t) if t > now) {
                continue;
            }
            if let Err(e) = self.process_intent(&intent).await {
                warn!(
                    intent_id = %intent.id,
                    op = %intent.payload.op_name(),
                    ?e,
                    "maintenance: process intent failed"
                );
            }
        }
    }

    async fn process_intent(&self, intent: &MaintenanceIntent) -> anyhow::Result<()> {
        // Closed match — adding a MaintenancePayload variant forces a
        // new arm here at compile time. The compile-time enforcement is
        // exactly why we use a typed enum instead of an op string +
        // raw JSON payload.
        match &intent.payload {
            MaintenancePayload::DropSlotCleanup {
                slot_name,
                target_hostname,
                ..
            } => {
                self.process_drop_slot(intent, slot_name, target_hostname)
                    .await
            }
        }
    }

    async fn process_drop_slot(
        &self,
        intent: &MaintenanceIntent,
        slot_name: &str,
        target_hostname: &str,
    ) -> anyhow::Result<()> {
        let node = match self.node_pool.node_by_hostname(target_hostname) {
            Ok(n) => n.clone(),
            Err(e) => {
                return self
                    .fail(
                        intent,
                        format!("resolve target host {target_hostname:?}: {e}"),
                    )
                    .await;
            }
        };

        // A queued drop is a *stale* instruction: it was recorded when
        // the slot looked abandoned, and it retries with exponential
        // backoff. By the time it runs, an orchestration may have
        // re-created that slot and be streaming through it — the
        // acceptance suite caught this loop deleting a recovery's slot
        // once per backoff step. The peer branch below is guarded
        // server-side too, but the local branch calls `db.drop_slot`
        // directly, so the check has to happen here as well.
        if let Some(owner) = {
            // Live evidence keys the discharge: the slot being active
            // means the rebuilt node came up, ending the op's
            // ownership at that event (grace is only the never-came-up
            // backstop). Only the LOCAL slot state is authoritative
            // here — for a peer-held slot the server-side guard in
            // `PeerServer::drop_slot` re-checks with its own DB.
            let db = self.db.clone();
            let s = slot_name.to_string();
            let local = self.node_pool.is_local(&node);
            crate::inflight_ops::owner_of_slot_observing(
                self.inflight.as_ref(),
                slot_name,
                crate::localserver::CROSS_OP_GRACE,
                move || async move {
                    if local {
                        db.slot_active(&s).await
                    } else {
                        // No local evidence about a remote slot: never
                        // discharge from here.
                        Ok(false)
                    }
                },
            )
            .await
        } {
            info!(
                intent_id = %intent.id,
                slot = slot_name,
                op = %owner.payload.op_name(),
                id = %owner.id,
                phase = %owner.phase,
                "maintenance: drop_slot dropped from the queue — an orchestration owns this slot"
            );
            if let Err(e) = self.store.mark_done(&intent.id).await {
                return Err(anyhow::anyhow!("mark maintenance intent done: {e}"));
            }
            return Ok(());
        }

        // Per-op timeout keeps a wedged peer from stalling the rest of
        // the sweep iteration. The sweep ctx is the long-lived agent
        // lifetime so the only deadline that fires inside drop_slot is
        // this one.
        let op = async {
            if self.node_pool.is_local(&node) {
                self.db.drop_slot(slot_name).await
            } else {
                let client = self
                    .peers
                    .client(&node)
                    .await
                    .map_err(|e| anyhow::anyhow!("peer client for {target_hostname:?}: {e}"))?;
                client.drop_slot(slot_name).await.map_err(|e| {
                    anyhow::anyhow!("drop slot rpc {slot_name:?} on {target_hostname:?}: {e}")
                })
            }
        };

        let result = tokio::time::timeout(PER_OP_TIMEOUT, op).await;
        match result {
            Ok(Ok(())) => {
                if let Err(e) = self.store.mark_done(&intent.id).await {
                    return Err(anyhow::anyhow!("mark maintenance intent done: {e}"));
                }
                info!(
                    intent_id = %intent.id,
                    slot = slot_name,
                    target = target_hostname,
                    "maintenance: drop slot cleanup completed"
                );
                Ok(())
            }
            Ok(Err(e)) => self.fail(intent, e.to_string()).await,
            Err(_) => {
                self.fail(
                    intent,
                    format!(
                        "drop slot {slot_name:?} on {target_hostname:?}: per-op timeout {PER_OP_TIMEOUT:?}"
                    ),
                )
                .await
            }
        }
    }

    /// Record a failure: either schedule a backoff retry or mark
    /// abandoned if the budget is exhausted.
    async fn fail(&self, intent: &MaintenanceIntent, err_msg: String) -> anyhow::Result<()> {
        let next_attempt = intent.attempts + 1;
        if next_attempt >= MAX_ATTEMPTS {
            self.store
                .mark_abandoned(&intent.id, &err_msg)
                .await
                .map_err(|e| anyhow::anyhow!("mark abandoned: {e}"))?;
        } else {
            let next_retry_at = Utc::now()
                + chrono::Duration::from_std(backoff(next_attempt))
                    .unwrap_or(chrono::Duration::zero());
            self.store
                .mark_attempt(&intent.id, &err_msg, next_retry_at)
                .await
                .map_err(|e| anyhow::anyhow!("mark attempt: {e}"))?;
        }
        debug!(
            intent_id = %intent.id,
            attempts = intent.attempts,
            "maintenance: recorded failure"
        );
        Ok(())
    }
}

/// Exponential backoff for retry N (1-indexed). 30 s base × 2 per attempt,
/// capped at 10 min. Sequence is 30 s → 60 s → 2 min → 4 min → 8 min →
/// 10 min (cap) → 10 min → …
pub fn backoff(next_attempt: u32) -> Duration {
    if next_attempt <= 1 {
        return BASE_BACKOFF;
    }
    let mut d = BASE_BACKOFF;
    // Double for each (next_attempt - 1) step, but bail at the cap to
    // avoid u64 overflow shenanigans on absurd inputs.
    for _ in 1..next_attempt {
        if d >= MAX_BACKOFF / 2 {
            return MAX_BACKOFF;
        }
        d *= 2;
    }
    d.min(MAX_BACKOFF)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::localdb::ReplicationLag;
    use std::sync::Mutex;
    use tempfile::TempDir;

    fn payload(slot: &str, host: &str) -> MaintenancePayload {
        MaintenancePayload::DropSlotCleanup {
            slot_name: slot.to_string(),
            target_hostname: host.to_string(),
            cause: "rpc_error".to_string(),
            initial_error: "boom".to_string(),
        }
    }

    fn store_fixture() -> (TempDir, Arc<FileMaintenanceStore>) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("maintenance");
        std::fs::create_dir(&dir).unwrap();
        let store = Arc::new(FileMaintenanceStore::new(dir, chrono::Duration::hours(24)));
        (tmp, store)
    }

    // ----- backoff() --------------------------------------------------------

    #[test]
    fn backoff_doubles_then_caps() {
        assert_eq!(backoff(1), Duration::from_secs(30));
        assert_eq!(backoff(2), Duration::from_secs(60));
        assert_eq!(backoff(3), Duration::from_secs(120));
        assert_eq!(backoff(4), Duration::from_secs(240));
        assert_eq!(backoff(5), Duration::from_secs(480));
        assert_eq!(backoff(6), Duration::from_secs(600)); // cap
        assert_eq!(backoff(10), Duration::from_secs(600)); // still cap, no overflow
        assert_eq!(backoff(u32::MAX), Duration::from_secs(600));
    }

    #[test]
    fn backoff_zero_or_one_is_base() {
        assert_eq!(backoff(0), Duration::from_secs(30));
        assert_eq!(backoff(1), Duration::from_secs(30));
    }

    // ----- payload + status -------------------------------------------------

    #[test]
    fn payload_op_name_round_trips_through_serde() {
        let p = payload("node1", "server1");
        assert_eq!(p.op_name(), "drop_slot_cleanup");
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains(r#""op":"drop_slot_cleanup""#));
        let parsed: MaintenancePayload = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn maintenance_status_serde() {
        assert_eq!(
            serde_json::to_string(&MaintenanceStatus::Pending).unwrap(),
            "\"pending\""
        );
        assert_eq!(
            serde_json::from_str::<MaintenanceStatus>("\"abandoned\"").unwrap(),
            MaintenanceStatus::Abandoned
        );
    }

    // ----- FileMaintenanceStore: append + get -------------------------------

    #[tokio::test]
    async fn append_then_get_round_trips() {
        let (_tmp, store) = store_fixture();
        let appended = store.append(payload("node2", "server3")).await.unwrap();
        let fetched = store.get(&appended.id).await.unwrap();
        assert_eq!(appended, fetched);
        assert_eq!(fetched.status, MaintenanceStatus::Pending);
        assert_eq!(fetched.attempts, 0);
        assert!(fetched.last_error.is_empty());
        assert!(fetched.next_retry_at.is_none());
    }

    #[tokio::test]
    async fn get_rejects_empty_id() {
        let (_tmp, store) = store_fixture();
        assert!(store.get("").await.is_err());
    }

    // ----- FileMaintenanceStore: list + list_pending ------------------------

    #[tokio::test]
    async fn list_pending_returns_only_pending_oldest_first() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("node0", "server1")).await.unwrap();
        let b = store.append(payload("node1", "server2")).await.unwrap();
        let c = store.append(payload("node2", "server3")).await.unwrap();
        store.mark_done(&b.id).await.unwrap();

        let pending = store.list_pending().await.unwrap();
        let ids: Vec<_> = pending.iter().map(|i| i.id.clone()).collect();
        assert_eq!(ids, vec![a.id, c.id]);
    }

    #[tokio::test]
    async fn list_filters_by_status() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("n", "h")).await.unwrap();
        let b = store.append(payload("n", "h")).await.unwrap();
        let c = store.append(payload("n", "h")).await.unwrap();
        store.mark_done(&b.id).await.unwrap();
        store.mark_abandoned(&c.id, "gave up").await.unwrap();

        let (only_done, _) = store.list(&[MaintenanceStatus::Done]).await.unwrap();
        assert_eq!(only_done.len(), 1);
        assert_eq!(only_done[0].id, b.id);

        let (all, _) = store.list(&[]).await.unwrap();
        assert_eq!(all.len(), 3);
        let (multi, _) = store
            .list(&[MaintenanceStatus::Pending, MaintenanceStatus::Abandoned])
            .await
            .unwrap();
        assert_eq!(multi.len(), 2);
        assert!(multi.iter().any(|i| i.id == a.id));
        assert!(multi.iter().any(|i| i.id == c.id));
    }

    #[tokio::test]
    async fn list_surfaces_corrupt_files() {
        let (_tmp, store) = store_fixture();
        let good = store.append(payload("n", "h")).await.unwrap();
        tokio::fs::write(store.dir.join("bad.json"), b"{not valid json")
            .await
            .unwrap();

        let (intents, skipped) = store.list(&[]).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].id, good.id);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].path, "bad.json");
    }

    #[tokio::test]
    async fn list_pending_silently_skips_corrupt() {
        let (_tmp, store) = store_fixture();
        let good = store.append(payload("n", "h")).await.unwrap();
        tokio::fs::write(store.dir.join("bad.json"), b"{")
            .await
            .unwrap();
        let pending = store.list_pending().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, good.id);
    }

    #[tokio::test]
    async fn list_pending_prunes_terminal_past_retention() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("maintenance");
        std::fs::create_dir(&dir).unwrap();
        // Retention of 0 → every terminal intent is pruned immediately.
        let store = FileMaintenanceStore::new(dir, chrono::Duration::zero());
        let a = store.append(payload("n", "h")).await.unwrap();
        let b = store.append(payload("n", "h")).await.unwrap();
        store.mark_done(&a.id).await.unwrap();
        store.mark_abandoned(&b.id, "x").await.unwrap();

        // list_pending should prune both terminal intents.
        let pending = store.list_pending().await.unwrap();
        assert!(pending.is_empty());
        assert!(store.get(&a.id).await.is_err(), "done should be gone");
        assert!(store.get(&b.id).await.is_err(), "abandoned should be gone");
    }

    // ----- mark_* + reschedule ---------------------------------------------

    #[tokio::test]
    async fn mark_attempt_bumps_counter_and_sets_next_retry() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("n", "h")).await.unwrap();
        let when = Utc::now() + chrono::Duration::seconds(60);
        store.mark_attempt(&a.id, "boom", when).await.unwrap();
        let after = store.get(&a.id).await.unwrap();
        assert_eq!(after.attempts, 1);
        assert_eq!(after.last_error, "boom");
        assert_eq!(after.status, MaintenanceStatus::Pending);
        assert_eq!(after.next_retry_at, Some(when));
    }

    #[tokio::test]
    async fn mark_done_clears_retry_and_error() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("n", "h")).await.unwrap();
        store.mark_attempt(&a.id, "boom", Utc::now()).await.unwrap();
        store.mark_done(&a.id).await.unwrap();
        let after = store.get(&a.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Done);
        assert!(after.last_error.is_empty());
        assert!(after.next_retry_at.is_none());
    }

    #[tokio::test]
    async fn reschedule_refuses_non_pending() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("n", "h")).await.unwrap();
        store.mark_done(&a.id).await.unwrap();
        let err = store
            .reschedule(&a.id, Utc::now())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot reschedule"));
    }

    #[tokio::test]
    async fn reschedule_does_not_consume_attempt() {
        let (_tmp, store) = store_fixture();
        let a = store.append(payload("n", "h")).await.unwrap();
        store
            .mark_attempt(&a.id, "boom", Utc::now() + chrono::Duration::hours(1))
            .await
            .unwrap();
        let now = Utc::now();
        store.reschedule(&a.id, now).await.unwrap();
        let after = store.get(&a.id).await.unwrap();
        // Attempts unchanged, retry pulled forward.
        assert_eq!(after.attempts, 1);
        assert_eq!(after.next_retry_at, Some(now));
    }

    // ----- MaintenanceWorker: dispatch + budget -----------------------------

    /// In-memory stub of LocalDb whose only meaningful method is
    /// `drop_slot`. Records calls; can be configured per-call via a
    /// scripted response list, or to a fixed default once the script
    /// runs out (e.g. "always succeed" or "always fail with X").
    struct StubDb {
        calls: Mutex<Vec<String>>,
        // Pre-loaded list, consumed left-to-right (Vec::pop pulls from
        // the end so we reverse on construction). Some(msg) = fail,
        // None = succeed.
        responses: Mutex<Vec<Option<String>>>,
        // What to do once the scripted list is exhausted.
        fallback: Option<String>,
    }
    impl StubDb {
        fn always_ok() -> Self {
            Self {
                calls: Mutex::new(vec![]),
                responses: Mutex::new(vec![]),
                fallback: None,
            }
        }
        fn always_fail(msg: &str) -> Self {
            Self {
                calls: Mutex::new(vec![]),
                responses: Mutex::new(vec![]),
                fallback: Some(msg.to_string()),
            }
        }
        fn scripted(responses: Vec<Option<String>>) -> Self {
            Self {
                calls: Mutex::new(vec![]),
                responses: Mutex::new(responses.into_iter().rev().collect()),
                fallback: None,
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl LocalDb for StubDb {
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_conf(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(name.to_string());
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| self.fallback.clone());
            match response {
                Some(msg) => anyhow::bail!(msg),
                None => Ok(()),
            }
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("worker tests don't use promote")
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            unreachable!()
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            unreachable!()
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    /// PeerRegistry/ReplayMarkerStore stubs — worker tests target the
    /// local-DB dispatch path so these can be inert. `client()` panics
    /// because we shouldn't reach it; `sweep` is a no-op.
    struct PanicPeers;
    #[async_trait]
    impl PeerRegistry for PanicPeers {
        async fn client(
            &self,
            _: &NodeConfig,
        ) -> anyhow::Result<Arc<dyn crate::peers::PeerClient>> {
            panic!("local-target test should not dispatch via peers")
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct NoopReplay;
    #[async_trait]
    impl ReplayMarkerStore for NoopReplay {
        async fn has(&self, _: &str, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn mark_done(&self, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn sweep(&self, _: DateTime<Utc>) {}
    }

    fn local_node_pool() -> NodePool {
        NodePool {
            members: vec![
                NodeConfig {
                    id: 0,
                    hostname: "local".to_string(),
                },
                NodeConfig {
                    id: 1,
                    hostname: "peer".to_string(),
                },
            ],
            local_node_id: 0,
        }
    }

    fn make_worker(store: Arc<FileMaintenanceStore>, db: Arc<dyn LocalDb>) -> MaintenanceWorker {
        MaintenanceWorker::new(
            store as Arc<dyn MaintenanceStore>,
            Arc::new(PanicPeers) as Arc<dyn PeerRegistry>,
            local_node_pool(),
            db,
            Arc::new(NoopReplay) as Arc<dyn ReplayMarkerStore>,
            Arc::new(crate::inflight_ops::InMemoryInflightOpStore::new()),
            DEFAULT_SWEEP_INTERVAL,
        )
    }

    #[tokio::test]
    async fn worker_success_path_marks_done_in_one_sweep() {
        let (_tmp, store) = store_fixture();
        let db = Arc::new(StubDb::always_ok());
        let worker = make_worker(store.clone(), db.clone());

        let intent = store.append(payload("node0", "local")).await.unwrap();

        worker.process_sweep().await;

        assert_eq!(db.calls(), vec!["node0".to_string()]);
        let after = store.get(&intent.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Done);
    }

    #[tokio::test]
    async fn worker_transient_failure_marks_attempt_with_backoff() {
        let (_tmp, store) = store_fixture();
        let db = Arc::new(StubDb::scripted(vec![Some("slot busy".into())]));
        let worker = make_worker(store.clone(), db.clone());

        let intent = store.append(payload("node0", "local")).await.unwrap();

        worker.process_sweep().await;

        let after = store.get(&intent.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Pending);
        assert_eq!(after.attempts, 1);
        assert!(after.last_error.contains("slot busy"));
        let next = after.next_retry_at.expect("retry scheduled");
        let dt = next - Utc::now();
        // backoff(1) = 30s, allow a few seconds slack for sleep precision
        assert!(dt >= chrono::Duration::seconds(25));
        assert!(dt <= chrono::Duration::seconds(35));
    }

    #[tokio::test]
    async fn worker_exhausts_budget_then_abandons() {
        let (_tmp, store) = store_fixture();
        // Always fail. Each sweep produces one attempt → abandon on 5th.
        let db = Arc::new(StubDb::always_fail("slot busy"));
        let worker = make_worker(store.clone(), db.clone());

        let intent = store.append(payload("node0", "local")).await.unwrap();
        // Pre-fast-forward by clearing next_retry_at between sweeps.
        for expected_attempts in 1..=4 {
            worker.process_sweep().await;
            let after = store.get(&intent.id).await.unwrap();
            assert_eq!(after.status, MaintenanceStatus::Pending);
            assert_eq!(after.attempts, expected_attempts);
            // Pull retry forward so the next sweep picks it up.
            store
                .reschedule(&intent.id, Utc::now() - chrono::Duration::seconds(1))
                .await
                .unwrap();
        }
        // 5th attempt → abandon
        worker.process_sweep().await;
        let after = store.get(&intent.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Abandoned);
        // The drop_slot was attempted on each of the 5 sweeps.
        assert_eq!(db.calls().len(), 5);
    }

    #[tokio::test]
    async fn worker_honours_next_retry_at_in_future() {
        let (_tmp, store) = store_fixture();
        let db = Arc::new(StubDb::always_ok());
        let worker = make_worker(store.clone(), db.clone());

        let intent = store.append(payload("node0", "local")).await.unwrap();
        store
            .reschedule(&intent.id, Utc::now() + chrono::Duration::hours(1))
            .await
            .unwrap();
        worker.process_sweep().await;

        // Nothing was attempted — retry is still in the future.
        assert!(db.calls().is_empty());
        let after = store.get(&intent.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Pending);
        assert_eq!(after.attempts, 0);
    }

    #[tokio::test]
    async fn worker_unknown_target_fails_intent() {
        let (_tmp, store) = store_fixture();
        let db = Arc::new(StubDb::always_ok());
        let worker = make_worker(store.clone(), db.clone());

        let intent = store.append(payload("nodeX", "not-in-pool")).await.unwrap();
        worker.process_sweep().await;

        let after = store.get(&intent.id).await.unwrap();
        assert_eq!(after.status, MaintenanceStatus::Pending);
        assert_eq!(after.attempts, 1);
        assert!(after.last_error.contains("resolve target host"));
        // db.drop_slot should NOT have been called — we couldn't even
        // resolve who to call.
        assert!(db.calls().is_empty());
    }
}
