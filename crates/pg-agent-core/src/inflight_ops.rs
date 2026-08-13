//! Durable journal for multi-phase state-change orchestrations.
//!
//! # Scope
//!
//! Tracks operator-initiated cluster-state-changing operations whose
//! execution spans multiple steps that must survive a daemon crash —
//! today [`InflightPayload::Handoff`] and the rebase-a-standby
//! orchestration [`InflightPayload::FollowPrimary`], with future
//! operations (switchover, cluster pause/resume) slotted in as new
//! enum variants. The contract is distinct from the other two durable
//! stores in `<state_dir>/`:
//!
//! - [`crate::replay_markers`] — after-success dedup of pgpool-driven
//!   hooks. Binary: file exists = done. 24 h TTL.
//! - [`crate::maintenance`] — retryable single-step cleanup queue
//!   driven by a background worker (`MaintenanceWorker`).
//! - **This module** — multi-phase orchestration journal. Driven inline
//!   by the RPC handler; resume is operator-driven via
//!   `pg_agentctl ops resume <id>` rather than a background loop.
//!
//! Each variant of [`InflightPayload`] picks its own phase string
//! ladder. For handoff: `"preflight_done"` → `"target_promoted"` →
//! `"slot_created"` → `"local_stopped"` → `"data_copied"` →
//! `"recovery_conf_written"` → `"local_started"` → `"attached"` →
//! `"done"`. For follow_primary: `"queued"` → `"dialing"` →
//! `"detached_stopped"` → `"slot_created"` → `"data_copied"` →
//! `"recovery_conf_written"` → `"detached_started"` → `"attached"` →
//! `"done"`.
//!
//! # On-disk format
//!
//! One JSON file per op under `<state_dir>/inflight_ops/`, named
//! `<unix_nanos>-<op_name>-<seq>.json`. Atomic temp + rename so a
//! crash mid-write never leaves a half-written file the resume scan
//! would fail to parse.
//!
//! # Single-flight
//!
//! [`InflightOpStore::begin`] is the single creation entry point. With
//! `exclusive=true` it fails if any `InProgress` op exists at all
//! (cluster-state ops cannot run concurrently). Always fails if an
//! `InProgress` op exists with the same `(op_name, key)` regardless of
//! `exclusive` (per-op idempotency). The gate is held by an
//! in-process `tokio::sync::Mutex` because the store is meant for use
//! by a single daemon process — cross-process writers aren't
//! supported.
//!
//! # Sweep policy
//!
//! `Done` / `Abandoned` ops past `retention` (default 7 days) are
//! pruned. **`InProgress` ops are NEVER auto-swept** — a stuck
//! orchestration must surface to `pg_agentctl ops list` so an operator
//! can resume or abandon it.

use crate::config::NodePool;
use crate::peers::PeerRegistry;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tracing::{debug, warn};

#[allow(unused)]
// currently unused; reserved for future variants that consult cluster topology.
use NodePool as _;
#[allow(unused)] // ditto for cross-peer probes (e.g. switchover verifying remote state).
use PeerRegistry as _;

/// 7 days. Longer than [`crate::replay_markers::DEFAULT_RETENTION`]
/// because in-flight ops are rare, and operators often want incident
/// review windows that span a working week.
pub const DEFAULT_RETENTION: chrono::Duration = chrono::Duration::days(7);

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InflightStatus {
    InProgress,
    Done,
    Abandoned,
}

impl InflightStatus {
    pub fn as_wire(&self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Done => "done",
            Self::Abandoned => "abandoned",
        }
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Abandoned)
    }
}

/// What's being orchestrated. New variants are deliberate; each carries
/// the data needed to resume the orchestration from any recorded phase.
///
/// Tag `"op"` lets on-disk JSON discriminate variants; the enum closes
/// the universe so future resume dispatch can't miss a case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum InflightPayload {
    /// Operator-initiated planned primary handoff. See
    /// `localserver::cluster_handoff`.
    Handoff {
        from_node_id: i32,
        to_node_id: i32,
        /// Resolved at preflight so resume doesn't need to re-resolve
        /// (the recorded hostname is the authoritative target for this
        /// orchestration, even if config reload changed the pool).
        to_hostname: String,
        /// `<from-node>` slot name as it was created on the new
        /// primary. Resume needs this to drive rewind/basebackup.
        slot_name: String,
        /// `--allow-lag` was passed; preserved for resume.
        allow_lag: bool,
    },
    /// Rebase a standby onto a (possibly new) primary. Same logical
    /// operation as the existing `PgAgentLocal::FollowPrimary` RPC
    /// handler that pgpool's `follow_primary_command` invokes; this
    /// variant tracks instances triggered by something other than
    /// pgpool — currently `cluster_handoff`'s post-completion fan-out
    /// (one variant per remaining standby).
    ///
    /// Long-term, the existing `FollowPrimary` RPC should converge on
    /// this same driver: orchestration shouldn't care who pulled the
    /// trigger, only that the right peers get the right instructions
    /// and the end state is correct. See ROADMAP item
    /// "follow_primary unification".
    FollowPrimary {
        /// The standby being rebased.
        detached_node_id: i32,
        /// Resolved at orchestration start so resume doesn't depend on
        /// the pool still containing this node.
        detached_hostname: String,
        /// The primary the detached should follow.
        new_primary_node_id: i32,
        new_primary_hostname: String,
    },
    /// `recovery_1st_stage` — rebuild a standby's data directory from
    /// this primary (checkpoint → create slot → basebackup → configure).
    /// Driven by `pg_agentctl cluster recover` and by pgpool's
    /// `pcp_recovery_node`.
    ///
    /// Journaling this is not only for operator visibility: it is what
    /// lets `failover` know a recovery is in flight for a node. Stopping
    /// the target's PostgreSQL (which recovery does deliberately) makes
    /// pgpool fire `failover_command` with that node as `detached`, and
    /// the standby-down branch's job is to drop that node's replication
    /// slot — the slot this orchestration just created. Before this
    /// variant existed the two raced and recovery silently produced a
    /// standby that could never stream.
    Recovery {
        /// The primary running the orchestration (always the local node).
        primary_node_id: i32,
        /// The standby being rebuilt.
        standby_node_id: i32,
        /// Resolved at orchestration start, like the other variants.
        standby_hostname: String,
        /// Slot created on the primary for the standby to stream through.
        slot_name: String,
    },
}

impl InflightPayload {
    /// Stable string tag used in filenames + the wire `InflightOp.op`
    /// field. Derived from the variant; single source of truth.
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Handoff { .. } => "handoff",
            Self::FollowPrimary { .. } => "follow_primary",
            Self::Recovery { .. } => "recovery",
        }
    }

    /// Op-specific idempotency key. Two ops with the same `(op_name,
    /// key)` are the same logical orchestration — a re-invocation
    /// should resume or detect the existing one, not start a fresh
    /// parallel one.
    pub fn key(&self) -> String {
        match self {
            Self::Handoff {
                from_node_id,
                to_node_id,
                ..
            } => format!("from={from_node_id},to={to_node_id}"),
            Self::FollowPrimary {
                detached_node_id,
                new_primary_node_id,
                ..
            } => format!("detached={detached_node_id},new_primary={new_primary_node_id}"),
            Self::Recovery {
                primary_node_id,
                standby_node_id,
                ..
            } => format!("primary={primary_node_id},standby={standby_node_id}"),
        }
    }

    /// The node whose data directory / replication slot this
    /// orchestration owns, when it owns one. `failover` consults this
    /// before acting destructively on a node that another op is
    /// already mid-way through rebuilding.
    pub fn target_node_id(&self) -> Option<i32> {
        match self {
            Self::Handoff { to_node_id, .. } => Some(*to_node_id),
            Self::FollowPrimary {
                detached_node_id, ..
            } => Some(*detached_node_id),
            Self::Recovery {
                standby_node_id, ..
            } => Some(*standby_node_id),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InflightOp {
    pub id: String,
    pub status: InflightStatus,
    pub payload: InflightPayload,
    /// Op-specific phase name. See module docs for the handoff ladder.
    pub phase: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Last error captured at any phase transition that surfaced one.
    /// Cleared when the op moves to `Done`; preserved on `Abandoned`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// On-disk file that couldn't be loaded (malformed JSON, read error).
/// Surfaced through [`InflightOpStore::list`] so corruption stays
/// visible to operators rather than disappearing into a warn log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedInflightOp {
    pub path: String,
    pub error: String,
}

// ---------------------------------------------------------------------------
// Store trait
// ---------------------------------------------------------------------------

/// Reason `begin` was rejected. Lets the caller construct an actionable
/// error message that names the conflicting op without re-querying.
#[derive(Debug, Clone)]
pub enum BeginRejected {
    /// Same `(op_name, key)` already in flight. Caller's option:
    /// resume the existing op (`ResumeInflightOp` RPC) or abandon it.
    DuplicateInProgress { id: String, phase: String },
    /// `exclusive=true` and a different op is in flight. The caller is
    /// blocked until the other op completes or is abandoned.
    ExclusiveBlocked {
        id: String,
        op_name: String,
        phase: String,
    },
}

impl std::fmt::Display for BeginRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateInProgress { id, phase } => write!(
                f,
                "an instance of this op is already in flight (id={id}, phase={phase}); \
                 resume with `pg_agentctl ops resume {id}` or abandon with \
                 `pg_agentctl ops abandon {id}`"
            ),
            Self::ExclusiveBlocked { id, op_name, phase } => write!(
                f,
                "blocked by in-flight `{op_name}` op (id={id}, phase={phase}); \
                 wait for it to finish or abandon it with \
                 `pg_agentctl ops abandon {id}`"
            ),
        }
    }
}

#[async_trait]
pub trait InflightOpStore: Send + Sync {
    /// Begin a new op. Returns `Err(BeginRejected)` packaged into the
    /// `anyhow::Error` chain if the gate refuses. Atomic with respect
    /// to other `begin` calls on the same store instance (in-process
    /// mutex inside `FileInflightOpStore`).
    async fn begin(
        &self,
        payload: InflightPayload,
        phase: &str,
        exclusive: bool,
    ) -> anyhow::Result<InflightOp>;

    /// Transition an in-flight op to a new phase. `last_error` is
    /// `Some` only when the transition is recording a recoverable
    /// problem; resume can inspect it. Fails if `id` isn't found or
    /// the op is already terminal.
    async fn update_phase(
        &self,
        id: &str,
        phase: &str,
        last_error: Option<String>,
    ) -> anyhow::Result<()>;

    /// Mark an op terminal-Done. Final phase is recorded as `"done"`.
    async fn complete(&self, id: &str) -> anyhow::Result<()>;

    /// Mark an op terminal-Abandoned with the operator-provided
    /// reason. The recorded phase is preserved so an operator
    /// inspecting later can see where the op was when abandoned.
    async fn abandon(&self, id: &str, reason: &str) -> anyhow::Result<()>;

    /// Look up the most-recent op (by `started_at`) matching the given
    /// `op_name` + `key`. Used by handlers for idempotency / resume
    /// decisions and by the failover handler for cross-op consult.
    /// Returns `None` if no such op exists.
    async fn find(&self, op_name: &str, key: &str) -> anyhow::Result<Option<InflightOp>>;

    async fn get(&self, id: &str) -> anyhow::Result<InflightOp>;

    /// All ops, optionally filtered by status (empty = every status).
    /// Second return is the corruption list — see `SkippedInflightOp`.
    async fn list(
        &self,
        statuses: &[InflightStatus],
    ) -> anyhow::Result<(Vec<InflightOp>, Vec<SkippedInflightOp>)>;

    /// Prune `Done`/`Abandoned` past retention. `InProgress` is NEVER
    /// auto-swept — those must surface to operators.
    async fn sweep(&self, now: DateTime<Utc>);
}

// ---------------------------------------------------------------------------
// FileInflightOpStore
// ---------------------------------------------------------------------------

pub struct FileInflightOpStore {
    dir: PathBuf,
    retention: chrono::Duration,
    seq: AtomicU64,
    /// In-process gate around `begin`. The store is single-writer per
    /// daemon process; this mutex makes the read-then-write
    /// "is anything in flight" check atomic. Cross-process writers
    /// aren't supported (would need flock); the daemon is the sole
    /// writer.
    begin_gate: Mutex<()>,
}

impl FileInflightOpStore {
    /// Build a store rooted at `dir` (typically
    /// `<state_dir>/inflight_ops/`). The daemon creates the directory
    /// at startup — we don't here, matching the replay and maintenance
    /// stores.
    pub fn new(dir: PathBuf, retention: chrono::Duration) -> Self {
        Self {
            dir,
            retention,
            seq: AtomicU64::new(0),
            begin_gate: Mutex::new(()),
        }
    }

    fn op_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn make_id(&self, payload: &InflightPayload, now: DateTime<Utc>) -> String {
        let nanos = now.timestamp_nanos_opt().unwrap_or(0);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{nanos}-{}-{seq}", payload.op_name())
    }

    async fn write_atomic(&self, op: &InflightOp) -> anyhow::Result<()> {
        let final_path = self.op_path(&op.id);
        let tmp_path = final_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(op)
            .map_err(|e| anyhow::anyhow!("inflight_ops: marshal {}: {e}", op.id))?;

        tokio::fs::write(&tmp_path, &bytes)
            .await
            .map_err(|e| anyhow::anyhow!("inflight_ops: write tmp {}: {e}", tmp_path.display()))?;
        if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(anyhow::anyhow!(
                "inflight_ops: rename {} -> {}: {e}",
                tmp_path.display(),
                final_path.display()
            ));
        }
        Ok(())
    }

    async fn read_one(&self, path: &Path) -> anyhow::Result<InflightOp> {
        let raw = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("inflight_ops: read {}: {e}", path.display()))?;
        serde_json::from_slice(&raw)
            .map_err(|e| anyhow::anyhow!("inflight_ops: parse {}: {e}", path.display()))
    }

    async fn read_all(&self) -> anyhow::Result<(Vec<InflightOp>, Vec<SkippedInflightOp>)> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((vec![], vec![])),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "inflight_ops: read_dir {}: {e}",
                    self.dir.display()
                ));
            }
        };
        let mut ops = Vec::new();
        let mut skipped = Vec::new();
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => return Err(anyhow::anyhow!("inflight_ops: next_entry: {e}")),
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
                Ok(op) => ops.push(op),
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        ?e,
                        "inflight_ops: skipping unreadable file"
                    );
                    skipped.push(SkippedInflightOp {
                        path: name,
                        error: e.to_string(),
                    });
                }
            }
        }
        Ok((ops, skipped))
    }
}

#[async_trait]
impl InflightOpStore for FileInflightOpStore {
    async fn begin(
        &self,
        payload: InflightPayload,
        phase: &str,
        exclusive: bool,
    ) -> anyhow::Result<InflightOp> {
        let _gate = self.begin_gate.lock().await;
        let (existing, _skipped) = self.read_all().await?;
        let op_name = payload.op_name();
        let key = payload.key();
        for op in &existing {
            if op.status != InflightStatus::InProgress {
                continue;
            }
            if op.payload.op_name() == op_name && op.payload.key() == key {
                return Err(anyhow::anyhow!(BeginRejected::DuplicateInProgress {
                    id: op.id.clone(),
                    phase: op.phase.clone(),
                }
                .to_string()));
            }
            if exclusive {
                return Err(anyhow::anyhow!(BeginRejected::ExclusiveBlocked {
                    id: op.id.clone(),
                    op_name: op.payload.op_name().to_string(),
                    phase: op.phase.clone(),
                }
                .to_string()));
            }
        }
        let now = Utc::now();
        let id = self.make_id(&payload, now);
        let op = InflightOp {
            id,
            status: InflightStatus::InProgress,
            payload,
            phase: phase.to_string(),
            started_at: now,
            updated_at: now,
            completed_at: None,
            last_error: None,
        };
        self.write_atomic(&op).await?;
        debug!(
            id = %op.id,
            op = %op.payload.op_name(),
            phase = %op.phase,
            "inflight_ops: begin"
        );
        Ok(op)
    }

    async fn update_phase(
        &self,
        id: &str,
        phase: &str,
        last_error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut op = self.get(id).await?;
        if op.status.is_terminal() {
            anyhow::bail!(
                "inflight_ops: update_phase on terminal op {id} (status={})",
                op.status.as_wire()
            );
        }
        op.phase = phase.to_string();
        op.updated_at = Utc::now();
        op.last_error = last_error;
        self.write_atomic(&op).await?;
        debug!(id, phase, "inflight_ops: update_phase");
        Ok(())
    }

    async fn complete(&self, id: &str) -> anyhow::Result<()> {
        let mut op = self.get(id).await?;
        let now = Utc::now();
        op.status = InflightStatus::Done;
        op.phase = "done".to_string();
        op.updated_at = now;
        op.completed_at = Some(now);
        op.last_error = None;
        self.write_atomic(&op).await
    }

    async fn abandon(&self, id: &str, reason: &str) -> anyhow::Result<()> {
        let mut op = self.get(id).await?;
        let now = Utc::now();
        op.status = InflightStatus::Abandoned;
        op.updated_at = now;
        op.completed_at = Some(now);
        op.last_error = Some(reason.to_string());
        self.write_atomic(&op).await
    }

    async fn find(&self, op_name: &str, key: &str) -> anyhow::Result<Option<InflightOp>> {
        let (ops, _skipped) = self.read_all().await?;
        Ok(ops
            .into_iter()
            .filter(|o| o.payload.op_name() == op_name && o.payload.key() == key)
            .max_by_key(|o| o.started_at))
    }

    async fn get(&self, id: &str) -> anyhow::Result<InflightOp> {
        if id.is_empty() {
            anyhow::bail!("inflight_ops: id is required");
        }
        self.read_one(&self.op_path(id)).await
    }

    async fn list(
        &self,
        statuses: &[InflightStatus],
    ) -> anyhow::Result<(Vec<InflightOp>, Vec<SkippedInflightOp>)> {
        let (mut ops, mut skipped) = self.read_all().await?;
        if !statuses.is_empty() {
            ops.retain(|o| statuses.contains(&o.status));
        }
        ops.sort_by(|a, b| {
            a.started_at
                .cmp(&b.started_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        skipped.sort_by(|a, b| a.path.cmp(&b.path));
        Ok((ops, skipped))
    }

    async fn sweep(&self, now: DateTime<Utc>) {
        let ops = match self.read_all().await {
            Ok((o, _)) => o,
            Err(e) => {
                warn!(?e, "inflight_ops: read_all failed during sweep");
                return;
            }
        };
        for op in ops {
            if !op.status.is_terminal() {
                continue;
            }
            let when = op.completed_at.unwrap_or(op.updated_at);
            if now.signed_duration_since(when) <= self.retention {
                continue;
            }
            let p = self.op_path(&op.id);
            if let Err(e) = tokio::fs::remove_file(&p).await {
                warn!(id = %op.id, ?e, "inflight_ops: prune failed");
            } else {
                debug!(id = %op.id, "inflight_ops: pruned terminal op");
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, FileInflightOpStore) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("inflight_ops");
        std::fs::create_dir(&dir).unwrap();
        let store = FileInflightOpStore::new(dir, DEFAULT_RETENTION);
        (tmp, store)
    }

    fn handoff_payload(from: i32, to: i32) -> InflightPayload {
        InflightPayload::Handoff {
            from_node_id: from,
            to_node_id: to,
            to_hostname: format!("db{to}"),
            slot_name: format!("node{from}"),
            allow_lag: false,
        }
    }

    #[tokio::test]
    async fn begin_persists_and_assigns_id() {
        let (_tmp, store) = fixture();
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        assert_eq!(op.status, InflightStatus::InProgress);
        assert_eq!(op.phase, "preflight_done");
        assert_eq!(op.payload.op_name(), "handoff");
        assert_eq!(op.payload.key(), "from=1,to=2");
        assert!(!op.id.is_empty());
        // Persists on disk and can be re-read.
        let got = store.get(&op.id).await.unwrap();
        assert_eq!(got, op);
    }

    #[tokio::test]
    async fn begin_fails_when_in_progress_duplicate_key() {
        let (_tmp, store) = fixture();
        let _first = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        let err = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("already in flight"), "got: {err}");
        assert!(err.contains("resume with"));
    }

    #[tokio::test]
    async fn begin_fails_when_exclusive_and_any_in_progress() {
        let (_tmp, store) = fixture();
        // (1->2) in flight.
        let _first = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        // (1->3) is a different key but exclusive=true blocks anyway.
        let err = store
            .begin(handoff_payload(1, 3), "preflight_done", true)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("blocked by in-flight"), "got: {err}");
    }

    #[tokio::test]
    async fn begin_succeeds_when_exclusive_and_only_done_exists() {
        let (_tmp, store) = fixture();
        let first = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store.complete(&first.id).await.unwrap();
        // (1->3) now allowed — exclusive gate considers only InProgress ops.
        let second = store
            .begin(handoff_payload(1, 3), "preflight_done", true)
            .await
            .unwrap();
        assert_eq!(second.status, InflightStatus::InProgress);
    }

    #[tokio::test]
    async fn update_phase_bumps_updated_at_and_records_error() {
        let (_tmp, store) = fixture();
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        store
            .update_phase(&op.id, "target_promoted", Some("flaky network".into()))
            .await
            .unwrap();
        let got = store.get(&op.id).await.unwrap();
        assert_eq!(got.phase, "target_promoted");
        assert_eq!(got.last_error.as_deref(), Some("flaky network"));
        assert!(got.updated_at > op.updated_at);
    }

    #[tokio::test]
    async fn update_phase_rejects_terminal_op() {
        let (_tmp, store) = fixture();
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store.complete(&op.id).await.unwrap();
        let err = store
            .update_phase(&op.id, "something", None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("terminal"), "got: {err}");
    }

    #[tokio::test]
    async fn complete_marks_terminal_and_sets_completed_at() {
        let (_tmp, store) = fixture();
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store.complete(&op.id).await.unwrap();
        let got = store.get(&op.id).await.unwrap();
        assert_eq!(got.status, InflightStatus::Done);
        assert_eq!(got.phase, "done");
        assert!(got.completed_at.is_some());
        assert!(got.last_error.is_none());
    }

    #[tokio::test]
    async fn abandon_marks_terminal_with_reason() {
        let (_tmp, store) = fixture();
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store
            .update_phase(&op.id, "target_promoted", None)
            .await
            .unwrap();
        store
            .abandon(&op.id, "operator-initiated rollback")
            .await
            .unwrap();
        let got = store.get(&op.id).await.unwrap();
        assert_eq!(got.status, InflightStatus::Abandoned);
        // Phase is preserved (the operator wants to see where we were).
        assert_eq!(got.phase, "target_promoted");
        assert_eq!(
            got.last_error.as_deref(),
            Some("operator-initiated rollback")
        );
        assert!(got.completed_at.is_some());
    }

    #[tokio::test]
    async fn find_returns_most_recent_for_op_key() {
        let (_tmp, store) = fixture();
        let first = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store.complete(&first.id).await.unwrap();
        // begin a fresh op with the same (op, key) — allowed because
        // the first is now terminal.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let second = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        let got = store.find("handoff", "from=1,to=2").await.unwrap().unwrap();
        assert_eq!(got.id, second.id, "find returned older op");
    }

    #[tokio::test]
    async fn find_returns_none_for_unknown() {
        let (_tmp, store) = fixture();
        assert!(store
            .find("handoff", "from=1,to=2")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn list_filters_by_status() {
        let (_tmp, store) = fixture();
        let a = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        store.complete(&a.id).await.unwrap();
        let b = store
            .begin(handoff_payload(1, 3), "preflight_done", true)
            .await
            .unwrap();
        let (in_progress, _) = store.list(&[InflightStatus::InProgress]).await.unwrap();
        assert_eq!(in_progress.len(), 1);
        assert_eq!(in_progress[0].id, b.id);
        let (done, _) = store.list(&[InflightStatus::Done]).await.unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].id, a.id);
        // No filter = all.
        let (all, _) = store.list(&[]).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn sweep_prunes_terminal_past_retention_keeps_inflight_and_fresh() {
        let (_tmp, store) = fixture();
        // 3 ops: an in-flight, a fresh done, an old done.
        let inflight = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        let fresh_done = store
            .begin(handoff_payload(1, 3), "preflight_done", false)
            .await
            .unwrap();
        store.complete(&fresh_done.id).await.unwrap();
        let old_done = store
            .begin(handoff_payload(1, 4), "preflight_done", false)
            .await
            .unwrap();
        store.complete(&old_done.id).await.unwrap();
        // Backdate the "old done" by editing the on-disk file.
        let path = store.op_path(&old_done.id);
        let mut op: InflightOp =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        op.completed_at = Some(Utc::now() - chrono::Duration::days(8));
        op.updated_at = Utc::now() - chrono::Duration::days(8);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&op).unwrap())
            .await
            .unwrap();

        store.sweep(Utc::now()).await;

        assert!(store.get(&inflight.id).await.is_ok(), "in-flight must stay");
        assert!(
            store.get(&fresh_done.id).await.is_ok(),
            "fresh done must stay"
        );
        assert!(
            store.get(&old_done.id).await.is_err(),
            "old done must be pruned"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_corrupt_files_for_operator_review() {
        let (_tmp, store) = fixture();
        let bad = store.dir.join("garbled.json");
        tokio::fs::write(&bad, b"{not even close to json")
            .await
            .unwrap();
        store.sweep(Utc::now()).await;
        assert!(bad.exists(), "corrupt file must be kept");
    }

    #[tokio::test]
    async fn read_all_skips_tmp_leftovers_and_non_json() {
        let (_tmp, store) = fixture();
        // Write a real op so the directory isn't empty.
        let op = store
            .begin(handoff_payload(1, 2), "preflight_done", true)
            .await
            .unwrap();
        // Plant a leftover temp file (simulating a crash mid-write) and
        // a stray non-json file. read_all + list must ignore both.
        tokio::fs::write(store.dir.join("leftover.json.tmp"), b"{}")
            .await
            .unwrap();
        tokio::fs::write(store.dir.join("README"), b"hands off")
            .await
            .unwrap();
        let (ops, skipped) = store.list(&[]).await.unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, op.id);
        assert!(skipped.is_empty(), "skipped should be empty: {skipped:?}");
    }

    fn follow_primary_payload(detached: i32, new_primary: i32) -> InflightPayload {
        InflightPayload::FollowPrimary {
            detached_node_id: detached,
            detached_hostname: format!("db{detached}"),
            new_primary_node_id: new_primary,
            new_primary_hostname: format!("db{new_primary}"),
        }
    }

    /// FollowPrimary payload round-trips through the journal: op_name,
    /// key, and a re-read produce the same payload. The journal does
    /// not collide with a Handoff op for the same nodes — different
    /// op_name, different namespace.
    #[tokio::test]
    async fn follow_primary_payload_persists_and_keys_distinctly_from_handoff() {
        let (_tmp, store) = fixture();
        // A handoff and a follow_primary for the same (1, 2) pair
        // coexist because they have different op_names.
        let h = store
            .begin(handoff_payload(1, 2), "preflight_done", false)
            .await
            .unwrap();
        let fp = store
            .begin(follow_primary_payload(2, 1), "queued", false)
            .await
            .unwrap();
        assert_eq!(h.payload.op_name(), "handoff");
        assert_eq!(fp.payload.op_name(), "follow_primary");
        assert_eq!(fp.payload.key(), "detached=2,new_primary=1");
        // Round-trip from disk.
        let got = store.get(&fp.id).await.unwrap();
        assert_eq!(got.payload, fp.payload);
        // find() scopes by op_name.
        let found = store
            .find("follow_primary", "detached=2,new_primary=1")
            .await
            .unwrap();
        assert_eq!(found.as_ref().map(|o| &o.id), Some(&fp.id));
        // Same key as a handoff would NOT collide.
        let no_match = store.find("follow_primary", "from=1,to=2").await.unwrap();
        assert!(no_match.is_none());
    }

    #[tokio::test]
    async fn corrupt_file_surfaces_via_list_skipped() {
        let (_tmp, store) = fixture();
        tokio::fs::write(store.dir.join("garbled.json"), b"{not json")
            .await
            .unwrap();
        let (ops, skipped) = store.list(&[]).await.unwrap();
        assert!(ops.is_empty());
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].path, "garbled.json");
    }
}

// ---------------------------------------------------------------------------
// InMemoryInflightOpStore — for tests in other modules
// ---------------------------------------------------------------------------

/// Non-durable [`InflightOpStore`] for tests that need the journal
/// present but not persistent — mirrors [`crate::peers::NoOpPeerRegistry`]'s
/// role. Public so `peerserver` / `peers` tests can wire a `PeerServer`
/// without each inventing its own stub.
#[derive(Default)]
pub struct InMemoryInflightOpStore {
    ops: std::sync::Mutex<Vec<InflightOp>>,
    seq: std::sync::atomic::AtomicU64,
}

impl InMemoryInflightOpStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an op directly, bypassing `begin` — lets a test stage a
    /// specific status/phase.
    pub fn seed(&self, op: InflightOp) {
        self.ops.lock().unwrap().push(op);
    }

    /// Build an `InProgress` op at `phase` and stage it.
    pub fn seed_in_progress(&self, payload: InflightPayload, phase: &str) -> InflightOp {
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let now = Utc::now();
        let op = InflightOp {
            id: format!("mem-{}-{n}", payload.op_name()),
            status: InflightStatus::InProgress,
            payload,
            phase: phase.to_string(),
            started_at: now,
            updated_at: now,
            completed_at: None,
            last_error: None,
        };
        self.seed(op.clone());
        op
    }
}

#[async_trait]
impl InflightOpStore for InMemoryInflightOpStore {
    async fn begin(
        &self,
        payload: InflightPayload,
        phase: &str,
        _exclusive: bool,
    ) -> anyhow::Result<InflightOp> {
        Ok(self.seed_in_progress(payload, phase))
    }
    async fn update_phase(
        &self,
        id: &str,
        phase: &str,
        last_error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut ops = self.ops.lock().unwrap();
        if let Some(op) = ops.iter_mut().find(|o| o.id == id) {
            op.phase = phase.to_string();
            op.updated_at = Utc::now();
            op.last_error = last_error;
        }
        Ok(())
    }
    async fn complete(&self, id: &str) -> anyhow::Result<()> {
        let mut ops = self.ops.lock().unwrap();
        if let Some(op) = ops.iter_mut().find(|o| o.id == id) {
            op.status = InflightStatus::Done;
            op.completed_at = Some(Utc::now());
        }
        Ok(())
    }
    async fn abandon(&self, id: &str, reason: &str) -> anyhow::Result<()> {
        let mut ops = self.ops.lock().unwrap();
        if let Some(op) = ops.iter_mut().find(|o| o.id == id) {
            op.status = InflightStatus::Abandoned;
            op.completed_at = Some(Utc::now());
            op.last_error = Some(reason.to_string());
        }
        Ok(())
    }
    async fn find(&self, op_name: &str, key: &str) -> anyhow::Result<Option<InflightOp>> {
        let ops = self.ops.lock().unwrap();
        Ok(ops
            .iter()
            .filter(|o| o.payload.op_name() == op_name && o.payload.key() == key)
            .max_by_key(|o| o.started_at)
            .cloned())
    }
    async fn get(&self, id: &str) -> anyhow::Result<InflightOp> {
        let ops = self.ops.lock().unwrap();
        ops.iter()
            .find(|o| o.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no such op: {id}"))
    }
    async fn list(
        &self,
        statuses: &[InflightStatus],
    ) -> anyhow::Result<(Vec<InflightOp>, Vec<SkippedInflightOp>)> {
        let ops = self.ops.lock().unwrap();
        let out: Vec<InflightOp> = ops
            .iter()
            .filter(|o| statuses.is_empty() || statuses.contains(&o.status))
            .cloned()
            .collect();
        Ok((out, vec![]))
    }
    async fn sweep(&self, _now: DateTime<Utc>) {}
}

// ---------------------------------------------------------------------------
// Ownership queries
// ---------------------------------------------------------------------------

/// Node id encoded in a replication-slot name. Slots are `node{id}`
/// (SPEC §5.1); anything else has no owning node.
pub fn node_id_from_slot(slot_name: &str) -> Option<i32> {
    slot_name.strip_prefix("node")?.parse().ok()
}

/// The orchestration that owns `node_id` — `InProgress`, or terminal-
/// `Done` within `grace`.
///
/// The grace exists because destructive requests aimed at a node reach
/// us *late*: pgpool's `failover_command` lags its health check, and a
/// queued `drop_slot_cleanup` intent retries with exponential backoff.
/// Both routinely arrive after an orchestration finished but before the
/// node it rebuilt is streaming — a window in which the node looks
/// legitimately dead to every other check.
///
/// `Abandoned` ops deliberately do **not** own anything: abandonment
/// runs the cleanup path, so the slot is meant to go.
///
/// A journal read failure yields `None` — guards fail open, matching
/// the "proceed on absent evidence" posture used elsewhere.
pub async fn owner_of_node(
    store: &dyn InflightOpStore,
    node_id: i32,
    grace: chrono::Duration,
) -> Option<InflightOp> {
    let (ops, _) = store
        .list(&[InflightStatus::InProgress, InflightStatus::Done])
        .await
        .map_err(|e| warn!(?e, "inflight ownership check failed; proceeding unguarded"))
        .ok()?;
    ops.into_iter()
        .filter(|o| o.payload.target_node_id() == Some(node_id))
        .filter(|o| match o.status {
            InflightStatus::InProgress => true,
            _ => o.completed_at.is_some_and(|t| Utc::now() - t < grace),
        })
        .max_by_key(|o| o.started_at)
}

/// [`owner_of_node`] keyed by slot name.
pub async fn owner_of_slot(
    store: &dyn InflightOpStore,
    slot_name: &str,
    grace: chrono::Duration,
) -> Option<InflightOp> {
    owner_of_node(store, node_id_from_slot(slot_name)?, grace).await
}
