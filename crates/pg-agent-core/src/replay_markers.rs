//! Hook-idempotency markers. **Scope (narrowed from the Go version): only
//! `FollowPrimary` and `RecoveryFirstStage` carry markers.** Both flows
//! run `pg_basebackup`, which wipes `$PGDATA` before streaming the
//! primary's data — re-running a fully-completed flow would clobber the
//! healthy standby's data dir. `Failover` was dropped from the protected
//! set because its operations are naturally near-idempotent (see SPEC
//! §5.12 for the full reasoning).
//!
//! # Storage
//!
//! Files live under `<state_dir>/replay/` (NOT under `$PGDATA` — agent
//! bookkeeping doesn't belong mixed in with PostgreSQL's own files).
//! Filename: `<sanitised_op>_<sha256(op|key) hex>.json`. Content is
//! `{"op", "key", "completed_at"}` so `cat <state_dir>/replay/*.json | jq`
//! is a working incident-review tool without any other infrastructure.
//!
//! The sha256 keeps the name a fixed length regardless of how long the
//! caller's key is; the op prefix lets an operator spot a marker's hook
//! at a glance from `ls`.
//!
//! # Lifecycle
//!
//! The handler calls [`ReplayMarkerStore::has`] before doing any work;
//! if true it returns the "already processed; skipping" result. On
//! success it calls [`ReplayMarkerStore::mark_done`]. The maintenance
//! worker runs [`ReplayMarkerStore::sweep`] on each tick (~30s) to prune
//! markers whose `completed_at` is older than retention (24h default).
//!
//! # What's NOT covered
//!
//! Markers protect against re-fires AFTER success, not in-progress
//! re-entry. A retry that races with a still-running flow is left to
//! the natural failure modes of each step (basebackup refuses a
//! non-empty target, Stop on a stopped target is a no-op, etc).
//! Distributed locking would close that gap; the mark-after-success
//! model is deliberately simpler since after-success re-fires are the
//! common case.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

pub const DEFAULT_RETENTION: chrono::Duration = chrono::Duration::hours(24);

#[async_trait]
pub trait ReplayMarkerStore: Send + Sync {
    async fn has(&self, op: &str, key: &str) -> anyhow::Result<bool>;

    /// Call only on the success path — writing a marker on a failure
    /// return would let the next retry short-circuit a half-done op.
    async fn mark_done(&self, op: &str, key: &str) -> anyhow::Result<()>;

    /// Best-effort prune of markers older than retention. Per-marker
    /// errors are logged and skipped — a single broken marker must not
    /// block the sweep. Bad parses are KEPT (not deleted) so an operator
    /// can investigate corruption rather than have it disappear silently.
    async fn sweep(&self, now: DateTime<Utc>);
}

// ---------------------------------------------------------------------------
// File format
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarkerFile {
    op: String,
    key: String,
    completed_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// FileReplayMarkerStore — production impl
// ---------------------------------------------------------------------------

pub struct FileReplayMarkerStore {
    dir: PathBuf,
    retention: chrono::Duration,
}

impl FileReplayMarkerStore {
    /// Build a store rooted at `dir` (typically `<state_dir>/replay/`).
    /// Does NOT create the directory — the daemon's startup path is
    /// responsible for that (matches the maintenance store; centralises
    /// directory creation).
    pub fn new(dir: PathBuf, retention: chrono::Duration) -> Self {
        Self { dir, retention }
    }

    fn marker_path(&self, op: &str, key: &str) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(op.as_bytes());
        hasher.update(b"|");
        hasher.update(key.as_bytes());
        let digest = hex::encode(hasher.finalize());
        self.dir.join(format!("{}_{digest}.json", sanitize_op(op)))
    }
}

#[async_trait]
impl ReplayMarkerStore for FileReplayMarkerStore {
    async fn has(&self, op: &str, key: &str) -> anyhow::Result<bool> {
        let path = self.marker_path(op, key);
        match tokio::fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(anyhow::anyhow!(
                "replay: stat {path}: {e}",
                path = path.display()
            )),
        }
    }

    async fn mark_done(&self, op: &str, key: &str) -> anyhow::Result<()> {
        let path = self.marker_path(op, key);
        let marker = MarkerFile {
            op: op.to_string(),
            key: key.to_string(),
            completed_at: Utc::now(),
        };
        let payload = serde_json::to_vec(&marker)
            .map_err(|e| anyhow::anyhow!("replay: marshal marker: {e}"))?;

        // Atomic temp + rename so a crash mid-write never leaves a
        // half-written marker that subsequent sweeps would fail to parse.
        // Temp shares the same directory so rename(2) stays atomic
        // (same filesystem guarantee).
        let tmp = path.with_extension("json.tmp");
        tokio::fs::write(&tmp, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("replay: write tmp {tmp}: {e}", tmp = tmp.display()))?;
        if let Err(e) = tokio::fs::rename(&tmp, &path).await {
            // Best-effort cleanup so a leaked tmp doesn't sit forever.
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(anyhow::anyhow!(
                "replay: rename {tmp} -> {path}: {e}",
                tmp = tmp.display(),
                path = path.display(),
            ));
        }
        debug!(op, "replay: marked done");
        Ok(())
    }

    async fn sweep(&self, now: DateTime<Utc>) {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(?e, dir = %self.dir.display(), "replay: read_dir failed");
                return;
            }
        };

        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => return,
                Err(e) => {
                    warn!(?e, "replay: next_entry failed; sweep aborted");
                    return;
                }
            };
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue; // ignore .tmp leftovers, foreign files
            }
            self.sweep_one(&path, now).await;
        }
    }
}

impl FileReplayMarkerStore {
    async fn sweep_one(&self, path: &Path, now: DateTime<Utc>) {
        let raw = match tokio::fs::read(path).await {
            Ok(v) => v,
            Err(e) => {
                warn!(path = %path.display(), ?e, "replay: read marker failed");
                return;
            }
        };
        let marker: MarkerFile = match serde_json::from_slice(&raw) {
            Ok(m) => m,
            Err(e) => {
                // Don't delete — an operator should see corruption rather
                // than have it disappear into the sweep.
                warn!(
                    path = %path.display(),
                    ?e,
                    "replay: marker parse failed; keeping for operator review"
                );
                return;
            }
        };
        if now.signed_duration_since(marker.completed_at) <= self.retention {
            return; // still fresh
        }
        if let Err(e) = tokio::fs::remove_file(path).await {
            warn!(path = %path.display(), ?e, "replay: prune failed");
        } else {
            debug!(path = %path.display(), "replay: pruned stale marker");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Restrict the op-name prefix to `[a-z0-9_-]` so it's safe to drop into
/// a filename. Empty input → `"op"` (so we never produce a filename that
/// starts with `_`).
fn sanitize_op(op: &str) -> String {
    let mut out = String::with_capacity(op.len());
    for c in op.trim().chars() {
        let lower = c.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() || lower == '_' || lower == '-' {
            out.push(lower);
        } else if c == ' ' {
            out.push('-');
        }
    }
    if out.is_empty() {
        "op".to_string()
    } else {
        out
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, FileReplayMarkerStore) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("replay");
        std::fs::create_dir(&dir).unwrap();
        let store = FileReplayMarkerStore::new(dir, DEFAULT_RETENTION);
        (tmp, store)
    }

    // ----- sanitize_op -------------------------------------------------------

    #[test]
    fn sanitize_op_accepts_safe_chars() {
        assert_eq!(sanitize_op("follow_primary"), "follow_primary");
        assert_eq!(sanitize_op("recovery-1st-stage"), "recovery-1st-stage");
    }

    #[test]
    fn sanitize_op_lowercases() {
        assert_eq!(sanitize_op("FollowPrimary"), "followprimary");
    }

    #[test]
    fn sanitize_op_drops_unsafe_chars() {
        assert_eq!(sanitize_op("../etc/passwd"), "etcpasswd");
        assert_eq!(sanitize_op("op\u{0}name"), "opname");
        assert_eq!(sanitize_op("op with spaces"), "op-with-spaces");
    }

    #[test]
    fn sanitize_op_empty_input_yields_placeholder() {
        assert_eq!(sanitize_op(""), "op");
        assert_eq!(sanitize_op("/"), "op");
        assert_eq!(sanitize_op("   "), "op");
    }

    // ----- marker_path -------------------------------------------------------

    #[test]
    fn marker_path_is_deterministic() {
        let (_tmp, store) = fixture();
        let p1 = store.marker_path("follow_primary", "detached=1,new_primary=0");
        let p2 = store.marker_path("follow_primary", "detached=1,new_primary=0");
        assert_eq!(p1, p2);
    }

    #[test]
    fn marker_path_differs_per_op_or_key() {
        let (_tmp, store) = fixture();
        let a = store.marker_path("follow_primary", "k");
        let b = store.marker_path("follow_primary", "k2");
        let c = store.marker_path("recovery_first_stage", "k");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn marker_path_op_collision_resistance() {
        // op="ab", key="cd" must collide neither with op="a", key="bcd"
        // nor op="abc", key="d" — that's why marker_path uses a
        // separator byte between op and key inside the hash.
        let (_tmp, store) = fixture();
        assert_ne!(store.marker_path("ab", "cd"), store.marker_path("a", "bcd"));
        assert_ne!(store.marker_path("ab", "cd"), store.marker_path("abc", "d"));
    }

    // ----- has + mark_done round-trip ---------------------------------------

    #[tokio::test]
    async fn has_returns_false_for_missing() {
        let (_tmp, store) = fixture();
        assert!(!store.has("follow_primary", "x").await.unwrap());
    }

    #[tokio::test]
    async fn mark_done_then_has() {
        let (_tmp, store) = fixture();
        store.mark_done("follow_primary", "k1").await.unwrap();
        assert!(store.has("follow_primary", "k1").await.unwrap());
        // Different key isn't marked.
        assert!(!store.has("follow_primary", "k2").await.unwrap());
        // Different op isn't marked.
        assert!(!store.has("recovery_first_stage", "k1").await.unwrap());
    }

    #[tokio::test]
    async fn mark_done_writes_parseable_json() {
        let (_tmp, store) = fixture();
        store
            .mark_done("recovery_first_stage", "primary=0,standby=1")
            .await
            .unwrap();
        let path = store.marker_path("recovery_first_stage", "primary=0,standby=1");
        let raw = tokio::fs::read_to_string(&path).await.unwrap();
        let m: MarkerFile = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.op, "recovery_first_stage");
        assert_eq!(m.key, "primary=0,standby=1");
        // completed_at is recent — within a few seconds of now.
        let age = Utc::now().signed_duration_since(m.completed_at);
        assert!(age >= chrono::Duration::zero());
        assert!(age < chrono::Duration::seconds(10));
    }

    #[tokio::test]
    async fn mark_done_overwrites_existing() {
        let (_tmp, store) = fixture();
        store.mark_done("follow_primary", "k").await.unwrap();
        // A second mark for the same (op, key) succeeds (rename replaces).
        store.mark_done("follow_primary", "k").await.unwrap();
        assert!(store.has("follow_primary", "k").await.unwrap());
    }

    // ----- sweep ------------------------------------------------------------

    /// Build a marker file with an explicit `completed_at` so we can test
    /// sweep behaviour without waiting 24h.
    async fn plant_marker(
        store: &FileReplayMarkerStore,
        op: &str,
        key: &str,
        completed_at: DateTime<Utc>,
    ) {
        let m = MarkerFile {
            op: op.to_string(),
            key: key.to_string(),
            completed_at,
        };
        let path = store.marker_path(op, key);
        tokio::fs::write(&path, serde_json::to_vec(&m).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sweep_prunes_old_markers_keeps_fresh() {
        let (_tmp, store) = fixture();
        let now = Utc::now();
        plant_marker(
            &store,
            "follow_primary",
            "old",
            now - chrono::Duration::days(2),
        )
        .await;
        plant_marker(
            &store,
            "follow_primary",
            "fresh",
            now - chrono::Duration::minutes(5),
        )
        .await;

        store.sweep(now).await;

        assert!(
            !store.has("follow_primary", "old").await.unwrap(),
            "old marker should have been pruned"
        );
        assert!(
            store.has("follow_primary", "fresh").await.unwrap(),
            "fresh marker should have been kept"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_malformed_markers() {
        let (_tmp, store) = fixture();
        // Drop garbage in the replay dir — sweep must warn + keep, not
        // delete (operator needs to see corruption).
        let bad = store.dir.join("follow_primary_corrupt.json");
        tokio::fs::write(&bad, b"{this is not valid json")
            .await
            .unwrap();

        store.sweep(Utc::now()).await;

        assert!(bad.exists(), "malformed marker must be kept");
    }

    #[tokio::test]
    async fn sweep_ignores_non_json_files() {
        let (_tmp, store) = fixture();
        let stray = store.dir.join("README");
        tokio::fs::write(&stray, b"don't sweep me").await.unwrap();
        let tmp_leftover = store.dir.join("follow_primary_abc.json.tmp");
        tokio::fs::write(&tmp_leftover, b"").await.unwrap();

        store.sweep(Utc::now()).await;

        assert!(stray.exists(), "non-.json file untouched");
        assert!(tmp_leftover.exists(), ".json.tmp leftover untouched");
    }

    #[tokio::test]
    async fn sweep_tolerates_missing_dir() {
        let tmp = TempDir::new().unwrap();
        let store =
            FileReplayMarkerStore::new(tmp.path().join("does_not_exist"), DEFAULT_RETENTION);
        // Must not panic, must not error, must not create the dir.
        store.sweep(Utc::now()).await;
    }
}
