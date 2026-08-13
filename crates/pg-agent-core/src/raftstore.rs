//! openraft storage for the consensus state machine
//! (docs/promotion-authority.md §5, sequencing step 6).
//!
//! Two halves of openraft's `storage-v2` split, both backed by one redb
//! file under `<state_dir>/raft/`:
//!
//! - [`RedbLogStore`] — the Raft log and vote. Persistence here is a
//!   protocol obligation: a vote that does not survive a crash lets a
//!   node vote twice in one term, which is how you get two leaders.
//! - [`RedbStateMachine`] — the applied [`ClusterState`] (lease, pause,
//!   switchover, generation) plus snapshots.
//!
//! # Why this module is the risky one
//!
//! Raft itself is not being written here — openraft is. What is being
//! written is storage, and promotion-authority is explicit that this is
//! where our bugs will live, which is why passing openraft's own
//! conformance suite (`openraft::testing::Suite`) is a hard gate rather
//! than a nice-to-have: it is what makes "redb instead of RocksDB"
//! a cheap choice, because the tested part is the part we are not
//! writing. See the `conformance` test at the bottom of this file.
//!
//! # The determinism rule
//!
//! **Nothing in [`RedbStateMachine::apply`] may consult the clock, the
//! filesystem, or any other node.** Every replica applies the same
//! entries in the same order and must reach byte-identical state, so
//! wall-clock timestamps are *proposed* — minted once by whichever node
//! submits the command, carried in [`ConsensusCommand`], and applied
//! verbatim everywhere. This is the one place the openraft-backed store
//! genuinely differs from
//! [`InMemoryConsensusStore`](crate::consensus::InMemoryConsensusStore),
//! where a `Utc::now()` at apply time was harmless because there was
//! exactly one replica.
//!
//! # Node ids
//!
//! Raft's `NodeId` is `u64` here while the rest of the agent uses `i32`
//! (`NodePool`, `ClusterState::lease.holder`). That is not an oversight:
//! `openraft::testing::Suite` requires `NodeId: From<u64>`, which `i32`
//! cannot implement. The two id spaces hold the same small non-negative
//! integers and convert at the seam — Raft membership is `u64`, the
//! lease *inside* the state machine stays `i32` so [`ClusterState`] is
//! unchanged by which store is behind it.

// `StorageError<NodeId>` is ~224 bytes and openraft's, not ours: every
// signature in this file is dictated by the traits being implemented, so
// boxing it here would only add a conversion at each trait boundary.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::consensus::{ClusterState, Lease, Paused, ReleaseOutcome, Switchover, TakeoverOutcome};

// ---------------------------------------------------------------------------
// Type config
// ---------------------------------------------------------------------------

/// A mutation of [`ClusterState`], proposed through Raft.
///
/// One variant per [`ConsensusStore`](crate::consensus::ConsensusStore)
/// write. `read_state` has no variant on purpose — it is a linearizable
/// read (`ensure_linearizable`), not a log entry, which is what keeps
/// steady state at zero writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusCommand {
    /// CAS the lease onto `candidate`. `expected` is the `(holder,
    /// term)` the candidate observed, or `None` for observed vacancy.
    Takeover {
        candidate: i32,
        expected: Option<(i32, u64)>,
        /// Proposer-minted. Applied verbatim on every replica — see the
        /// determinism rule in the module docs.
        at: DateTime<Utc>,
    },
    /// Voluntarily vacate; succeeds only if `(holder, term)` is still
    /// the committed lease.
    Release {
        holder: i32,
        term: u64,
    },
    SetPaused(Option<Paused>),
    SetSwitchover(Option<Switchover>),
}

/// State-machine reply. `Noop` covers the entries Raft writes for its
/// own purposes (blank leader-init entries, membership changes), which
/// still consume a log index and so still need a response slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResponse {
    Takeover(TakeoverOutcome),
    Release(ReleaseOutcome),
    Ack,
    Noop,
}

openraft::declare_raft_types!(
    /// Type config for the agent's Raft. Unlisted associated types take
    /// the macro's defaults: `Node = BasicNode`, `Entry = Entry<Self>`,
    /// `SnapshotData = Cursor<Vec<u8>>`, `Responder = OneshotResponder`,
    /// `AsyncRuntime = TokioRuntime`.
    pub PgAgentTypeConfig:
        D = ConsensusCommand,
        R = CommandResponse,
        NodeId = u64,
);

/// Raft-side node id. See the module docs on the `i32`/`u64` seam.
pub type RaftNodeId = u64;

type StorageResult<T> = Result<T, StorageError<RaftNodeId>>;

// ---------------------------------------------------------------------------
// redb layout
// ---------------------------------------------------------------------------

/// Subdirectory of `state_dir` holding the Raft database — sibling of
/// the existing `replay/` and `maintenance/` directories.
pub const RAFT_SUBDIR: &str = "raft";
/// The single redb file. One file, not one per concern: redb gives
/// cross-table atomicity within a write transaction, which is what lets
/// `apply` advance `last_applied` and the cluster state together.
pub const RAFT_DB_FILE: &str = "raft.redb";

/// `log index -> serialized Entry`.
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
/// Everything that is not a log entry, keyed by the constants below.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const META_VOTE: &str = "vote";
const META_COMMITTED: &str = "committed";
const META_LAST_PURGED: &str = "last_purged";
const META_STATE_MACHINE: &str = "state_machine";
const META_SNAPSHOT: &str = "snapshot";

/// Open (creating if absent) the Raft database under `state_dir`.
///
/// Both stores share one handle: redb allows a single writer process,
/// and a second `Database::create` on the same path in the same process
/// would deadlock on the file lock rather than fail cleanly.
pub fn open_database(state_dir: &Path) -> anyhow::Result<Arc<Database>> {
    let dir = state_dir.join(RAFT_SUBDIR);
    std::fs::create_dir_all(&dir).map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    open_database_at(&dir.join(RAFT_DB_FILE))
}

/// Open a Raft database at an exact path (tests, and the recovery path
/// where an operator points at a relocated file).
pub fn open_database_at(path: &Path) -> anyhow::Result<Arc<Database>> {
    let db = Database::create(path)
        .map_err(|e| anyhow::anyhow!("open raft db {}: {e}", path.display()))?;
    // Materialize both tables so read transactions on a fresh database
    // see them instead of erroring with TableDoesNotExist.
    let txn = db.begin_write()?;
    {
        txn.open_table(LOGS)?;
        txn.open_table(META)?;
    }
    txn.commit()?;
    Ok(Arc::new(db))
}

/// Where [`open_database`] would put the file, for diagnostics and for
/// the "delete it and let Raft re-replicate" recovery in the docs.
pub fn database_path(state_dir: &Path) -> PathBuf {
    state_dir.join(RAFT_SUBDIR).join(RAFT_DB_FILE)
}

fn read_meta(db: &Database, key: &str) -> StorageResult<Option<Vec<u8>>> {
    let txn = db.begin_read().map_err(io_read)?;
    let table = txn.open_table(META).map_err(io_read)?;
    let got = table.get(key).map_err(io_read)?;
    Ok(got.map(|v| v.value().to_vec()))
}

fn write_meta(db: &Database, key: &str, value: &[u8]) -> StorageResult<()> {
    let txn = db.begin_write().map_err(io_write)?;
    {
        let mut table = txn.open_table(META).map_err(io_write)?;
        table.insert(key, value).map_err(io_write)?;
    }
    // redb's default durability is Immediate: the commit fsyncs before
    // returning, which is exactly the promise `save_vote` must keep.
    txn.commit().map_err(io_write)?;
    Ok(())
}

fn io_read<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<RaftNodeId> {
    StorageIOError::read(&e).into()
}

fn io_write<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<RaftNodeId> {
    StorageIOError::write(&e).into()
}

fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(io_write)
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(io_read)
}

// ---------------------------------------------------------------------------
// Log store
// ---------------------------------------------------------------------------

/// redb-backed [`RaftLogStorage`]: the log, the vote, and the committed
/// pointer.
#[derive(Clone)]
pub struct RedbLogStore {
    db: Arc<Database>,
}

impl RedbLogStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn last_purged(&self) -> StorageResult<Option<LogId<RaftNodeId>>> {
        match read_meta(&self.db, META_LAST_PURGED)? {
            Some(bytes) => decode(&bytes),
            None => Ok(None),
        }
    }
}

impl RaftLogReader<PgAgentTypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<PgAgentTypeConfig>>> {
        let txn = self.db.begin_read().map_err(io_read)?;
        let table = txn.open_table(LOGS).map_err(io_read)?;
        let mut out = Vec::new();
        for row in table.range(range).map_err(io_read)? {
            let (_, value) = row.map_err(io_read)?;
            out.push(decode(value.value())?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<PgAgentTypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<PgAgentTypeConfig>> {
        let last_purged = self.last_purged()?;

        let txn = self.db.begin_read().map_err(io_read)?;
        let table = txn.open_table(LOGS).map_err(io_read)?;
        let last = table.last().map_err(io_read)?;
        // No entry present means everything up to `last_purged` is gone
        // and nothing has replaced it — the trait wants the purge point
        // reported as the last log id in that case, not `None`.
        let last_log_id = match last {
            Some((_, value)) => {
                let entry: Entry<PgAgentTypeConfig> = decode(value.value())?;
                Some(entry.log_id)
            }
            None => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> StorageResult<()> {
        // Durable before returning. A lost vote is a double vote, which
        // is a second leader in the same term.
        write_meta(&self.db, META_VOTE, &encode(vote)?)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<RaftNodeId>>> {
        match read_meta(&self.db, META_VOTE)? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(&mut self, committed: Option<LogId<RaftNodeId>>) -> StorageResult<()> {
        write_meta(&self.db, META_COMMITTED, &encode(&committed)?)
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<RaftNodeId>>> {
        match read_meta(&self.db, META_COMMITTED)? {
            Some(bytes) => decode(&bytes),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<PgAgentTypeConfig>,
    ) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<PgAgentTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let txn = self.db.begin_write().map_err(io_write)?;
        {
            let mut table = txn.open_table(LOGS).map_err(io_write)?;
            for entry in entries {
                let bytes = encode(&entry)?;
                table
                    .insert(entry.log_id.index, bytes.as_slice())
                    .map_err(io_write)?;
            }
        }
        txn.commit().map_err(io_write)?;

        // The commit above already fsynced, so the entries are on disk
        // by the time we signal. The trait permits the callback before
        // or after this method returns; reporting completion only after
        // a durable commit is the conservative reading.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<RaftNodeId>) -> StorageResult<()> {
        let txn = self.db.begin_write().map_err(io_write)?;
        {
            let mut table = txn.open_table(LOGS).map_err(io_write)?;
            // Inclusive of `log_id` — conflicting suffix, drop it whole
            // so no hole is left behind.
            let mut victims = Vec::new();
            for row in table.range(log_id.index..).map_err(io_write)? {
                let (k, _) = row.map_err(io_write)?;
                victims.push(k.value());
            }
            for k in victims {
                table.remove(k).map_err(io_write)?;
            }
        }
        txn.commit().map_err(io_write)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> StorageResult<()> {
        let bytes = encode(&Some(log_id))?;
        let txn = self.db.begin_write().map_err(io_write)?;
        {
            // Purge point and entry removal commit together: a crash
            // between them would otherwise leave a purge point claiming
            // entries that are still present, or entries gone with no
            // record of where the log now starts.
            let mut meta = txn.open_table(META).map_err(io_write)?;
            meta.insert(META_LAST_PURGED, bytes.as_slice())
                .map_err(io_write)?;

            let mut table = txn.open_table(LOGS).map_err(io_write)?;
            let mut victims = Vec::new();
            for row in table.range(..=log_id.index).map_err(io_write)? {
                let (k, _) = row.map_err(io_write)?;
                victims.push(k.value());
            }
            for k in victims {
                table.remove(k).map_err(io_write)?;
            }
        }
        txn.commit().map_err(io_write)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Everything the state machine holds, as one serializable document.
/// A snapshot is exactly this, serialized — which is why `apply` can
/// persist state directly and snapshots stay a derived artifact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied: Option<LogId<RaftNodeId>>,
    pub last_membership: StoredMembership<RaftNodeId, BasicNode>,
    /// The replicated document the HA loop actually reads.
    pub cluster: ClusterState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<RaftNodeId, BasicNode>,
    data: Vec<u8>,
}

/// Reads the applied [`ClusterState`] back out of the state machine's
/// redb file.
///
/// This exists because openraft takes ownership of the
/// [`RedbStateMachine`] when the [`Raft`](openraft::Raft) is built, and
/// cloning it would not help: the clone's `data` is a point-in-time copy
/// that stops tracking `apply`. Going back to the file is not a
/// workaround but the direct consequence of choosing a *persistent*
/// state machine — `apply` commits durably before it returns, so any
/// read that happens after it observes it.
///
/// The linearizability of a read is the caller's business, not this
/// type's: this returns whatever has been applied locally. Pair it with
/// `ensure_linearizable` (see [`crate::raftconsensus`]) before treating
/// the answer as authoritative.
#[derive(Clone)]
pub struct ClusterStateReader {
    db: Arc<Database>,
}

impl ClusterStateReader {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// The applied cluster state; `Default` when nothing has been
    /// applied yet, which is a genuinely empty state machine rather
    /// than an unknown one.
    pub fn read(&self) -> anyhow::Result<ClusterState> {
        let bytes = read_meta(&self.db, META_STATE_MACHINE)
            .map_err(|e| anyhow::anyhow!("read state machine: {e}"))?;
        match bytes {
            Some(bytes) => {
                let data: StateMachineData = serde_json::from_slice(&bytes)?;
                Ok(data.cluster)
            }
            None => Ok(ClusterState::default()),
        }
    }
}

/// redb-backed [`RaftStateMachine`] over [`ClusterState`].
#[derive(Clone)]
pub struct RedbStateMachine {
    db: Arc<Database>,
    data: StateMachineData,
    /// Disambiguates snapshots built at the same `last_applied`. Not a
    /// random id: `apply` and everything reachable from it must stay
    /// deterministic, and a counter is reproducible where a UUID is not.
    snapshot_idx: u64,
}

impl RedbStateMachine {
    /// Load the persisted state machine, or start empty.
    pub fn new(db: Arc<Database>) -> anyhow::Result<Self> {
        let data = match read_meta(&db, META_STATE_MACHINE)
            .map_err(|e| anyhow::anyhow!("read state machine: {e}"))?
        {
            Some(bytes) => serde_json::from_slice(&bytes)?,
            None => StateMachineData::default(),
        };
        Ok(Self {
            db,
            data,
            snapshot_idx: 0,
        })
    }

    /// The applied cluster state. This is what
    /// `ConsensusStore::read_state` returns once the linearizable read
    /// has confirmed this node may answer.
    pub fn cluster_state(&self) -> &ClusterState {
        &self.data.cluster
    }

    fn persist(&self) -> StorageResult<()> {
        write_meta(&self.db, META_STATE_MACHINE, &encode(&self.data)?)
    }

    /// The whole of the business logic, and deliberately a mirror of
    /// [`InMemoryConsensusStore`](crate::consensus::InMemoryConsensusStore)'s
    /// — same CAS, same generation bump, same term minting. The only
    /// difference is that time arrives in the command instead of being
    /// read here (see the module docs).
    fn apply_command(&mut self, cmd: ConsensusCommand) -> CommandResponse {
        let state = &mut self.data.cluster;
        match cmd {
            ConsensusCommand::Takeover {
                candidate,
                expected,
                at,
            } => {
                let observed_holds = match (&state.lease, expected) {
                    (None, None) => true,
                    (Some(cur), Some((h, t))) => cur.holder == h && cur.term == t,
                    _ => false,
                };
                if !observed_holds {
                    return CommandResponse::Takeover(TakeoverOutcome::Lost {
                        current: state.lease.clone(),
                    });
                }
                state.generation += 1;
                let lease = Lease {
                    holder: candidate,
                    // Minted from the post-bump generation, so terms are
                    // strictly monotonic even across vacate-then-
                    // reacquire. Fencing depends on it.
                    term: state.generation,
                    since: at,
                };
                state.lease = Some(lease.clone());
                CommandResponse::Takeover(TakeoverOutcome::Won { lease })
            }
            ConsensusCommand::Release { holder, term } => match &state.lease {
                Some(cur) if cur.holder == holder && cur.term == term => {
                    state.generation += 1;
                    state.lease = None;
                    CommandResponse::Release(ReleaseOutcome::Released)
                }
                other => CommandResponse::Release(ReleaseOutcome::NotHolder {
                    current: other.clone(),
                }),
            },
            ConsensusCommand::SetPaused(paused) => {
                state.generation += 1;
                state.paused = paused;
                CommandResponse::Ack
            }
            ConsensusCommand::SetSwitchover(switchover) => {
                state.generation += 1;
                state.switchover = switchover;
                CommandResponse::Ack
            }
        }
    }
}

impl RaftStateMachine<PgAgentTypeConfig> for RedbStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(
        Option<LogId<RaftNodeId>>,
        StoredMembership<RaftNodeId, BasicNode>,
    )> {
        Ok((self.data.last_applied, self.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<CommandResponse>>
    where
        I: IntoIterator<Item = Entry<PgAgentTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            self.data.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => CommandResponse::Noop,
                EntryPayload::Normal(cmd) => self.apply_command(cmd),
                EntryPayload::Membership(membership) => {
                    self.data.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership);
                    CommandResponse::Noop
                }
            };
            responses.push(response);
        }
        // One durable write per batch, not per entry: the batch is
        // applied atomically or not at all, and `last_applied` moving
        // without the state that came with it is the corruption this
        // ordering exists to prevent.
        self.persist()?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        let mut builder = self.clone();
        builder.snapshot_idx = self.snapshot_idx;
        builder
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StorageResult<()> {
        let bytes = snapshot.into_inner();
        let data: StateMachineData = decode(&bytes)?;
        self.data = data;
        self.persist()?;

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        };
        write_meta(&self.db, META_SNAPSHOT, &encode(&stored)?)
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<PgAgentTypeConfig>>> {
        let Some(bytes) = read_meta(&self.db, META_SNAPSHOT)? else {
            return Ok(None);
        };
        let stored: StoredSnapshot = decode(&bytes)?;
        Ok(Some(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

impl RaftSnapshotBuilder<PgAgentTypeConfig> for RedbStateMachine {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<PgAgentTypeConfig>> {
        let data = encode(&self.data)?;

        let snapshot_id = match self.data.last_applied {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx),
            None => format!("--{}", self.snapshot_idx),
        };

        let meta = SnapshotMeta {
            last_log_id: self.data.last_applied,
            last_membership: self.data.last_membership.clone(),
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        write_meta(&self.db, META_SNAPSHOT, &encode(&stored)?)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Open both stores over one database under `state_dir`.
pub fn open_stores(state_dir: &Path) -> anyhow::Result<(RedbLogStore, RedbStateMachine)> {
    let db = open_database(state_dir)?;
    Ok((RedbLogStore::new(db.clone()), RedbStateMachine::new(db)?))
}

/// Membership map for openraft's `initialize`, converting the agent's
/// `i32` node ids to Raft's `u64` and pairing each with its peer
/// address. Rejects negative ids rather than wrapping them.
pub fn membership_map(
    nodes: impl IntoIterator<Item = (i32, String)>,
) -> anyhow::Result<BTreeMap<RaftNodeId, BasicNode>> {
    let mut out = BTreeMap::new();
    for (id, addr) in nodes {
        let raft_id = to_raft_node_id(id)?;
        out.insert(raft_id, BasicNode::new(addr));
    }
    Ok(out)
}

/// `i32` (agent) -> `u64` (Raft). See the module docs on the seam.
pub fn to_raft_node_id(id: i32) -> anyhow::Result<RaftNodeId> {
    u64::try_from(id).map_err(|_| anyhow::anyhow!("node id {id} is negative; Raft ids are u64"))
}

/// `u64` (Raft) -> `i32` (agent).
pub fn from_raft_node_id(id: RaftNodeId) -> anyhow::Result<i32> {
    i32::try_from(id).map_err(|_| anyhow::anyhow!("raft node id {id} does not fit an i32 node id"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::StoreBuilder;
    use openraft::testing::Suite;
    use tempfile::TempDir;

    struct TempDbBuilder;

    impl StoreBuilder<PgAgentTypeConfig, RedbLogStore, RedbStateMachine, TempDir> for TempDbBuilder {
        async fn build(&self) -> StorageResult<(TempDir, RedbLogStore, RedbStateMachine)> {
            let dir = TempDir::new().map_err(io_write)?;
            let db = open_database(dir.path())
                .map_err(|e| StorageIOError::write(&std::io::Error::other(e.to_string())))?;
            let sm = RedbStateMachine::new(db.clone()).map_err(|e| {
                StorageIOError::read_state_machine(&std::io::Error::other(e.to_string()))
            })?;
            Ok((dir, RedbLogStore::new(db), sm))
        }
    }

    /// openraft's own storage conformance suite. promotion-authority
    /// §5 makes this a hard gate: it is what makes the engine choice
    /// low-stakes, because the part being tested is the part we did not
    /// write.
    #[test]
    fn conformance() {
        Suite::test_all(TempDbBuilder).unwrap();
    }

    fn temp_sm() -> (TempDir, RedbStateMachine) {
        let dir = TempDir::new().unwrap();
        let db = open_database(dir.path()).unwrap();
        let sm = RedbStateMachine::new(db).unwrap();
        (dir, sm)
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    #[test]
    fn takeover_cas_mirrors_the_in_memory_store() {
        let (_dir, mut sm) = temp_sm();

        // Vacant + observed vacancy -> wins, term minted from generation.
        let r = sm.apply_command(ConsensusCommand::Takeover {
            candidate: 1,
            expected: None,
            at: at(100),
        });
        let CommandResponse::Takeover(TakeoverOutcome::Won { lease }) = r else {
            panic!("expected Won, got {r:?}");
        };
        assert_eq!(lease.holder, 1);
        assert_eq!(lease.since, at(100), "timestamp comes from the command");
        let first_term = lease.term;
        assert!(first_term > 0);

        // A candidate that still believes the lease is vacant loses.
        let r = sm.apply_command(ConsensusCommand::Takeover {
            candidate: 2,
            expected: None,
            at: at(200),
        });
        assert!(matches!(
            r,
            CommandResponse::Takeover(TakeoverOutcome::Lost { .. })
        ));

        // Wrong term loses too.
        let r = sm.apply_command(ConsensusCommand::Takeover {
            candidate: 2,
            expected: Some((1, first_term + 99)),
            at: at(300),
        });
        assert!(matches!(
            r,
            CommandResponse::Takeover(TakeoverOutcome::Lost { .. })
        ));

        // Correct observation wins and the term strictly advances.
        let r = sm.apply_command(ConsensusCommand::Takeover {
            candidate: 2,
            expected: Some((1, first_term)),
            at: at(400),
        });
        let CommandResponse::Takeover(TakeoverOutcome::Won { lease }) = r else {
            panic!("expected Won, got {r:?}");
        };
        assert_eq!(lease.holder, 2);
        assert!(lease.term > first_term);
    }

    #[test]
    fn release_requires_the_current_holder_and_term_is_never_reused() {
        let (_dir, mut sm) = temp_sm();
        sm.apply_command(ConsensusCommand::Takeover {
            candidate: 1,
            expected: None,
            at: at(100),
        });
        let held = sm.cluster_state().lease.clone().unwrap();

        // Stale holder cannot clobber.
        let r = sm.apply_command(ConsensusCommand::Release {
            holder: 2,
            term: held.term,
        });
        assert!(matches!(
            r,
            CommandResponse::Release(ReleaseOutcome::NotHolder { .. })
        ));

        let r = sm.apply_command(ConsensusCommand::Release {
            holder: 1,
            term: held.term,
        });
        assert!(matches!(
            r,
            CommandResponse::Release(ReleaseOutcome::Released)
        ));
        assert!(sm.cluster_state().lease.is_none());

        // Reacquiring after a release must not reuse the old term —
        // fencing tokens are only useful if they never repeat.
        let r = sm.apply_command(ConsensusCommand::Takeover {
            candidate: 1,
            expected: None,
            at: at(500),
        });
        let CommandResponse::Takeover(TakeoverOutcome::Won { lease }) = r else {
            panic!("expected Won");
        };
        assert!(lease.term > held.term);
    }

    #[tokio::test]
    async fn state_machine_survives_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let db = open_database(dir.path()).unwrap();
            let mut sm = RedbStateMachine::new(db).unwrap();
            sm.apply(vec![Entry {
                log_id: openraft::testing::log_id(1, 0, 7),
                payload: EntryPayload::Normal(ConsensusCommand::Takeover {
                    candidate: 3,
                    expected: None,
                    at: at(100),
                }),
            }])
            .await
            .unwrap();
        }

        let db = open_database(dir.path()).unwrap();
        let mut sm = RedbStateMachine::new(db).unwrap();
        let (last_applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied.unwrap().index, 7);
        assert_eq!(sm.cluster_state().lease.as_ref().unwrap().holder, 3);
    }

    #[tokio::test]
    async fn apply_is_deterministic_across_replicas() {
        // Same entries, two independent state machines, byte-identical
        // result — the property the whole design rests on, and the
        // reason timestamps are proposed rather than read at apply.
        let entries = || {
            vec![
                Entry {
                    log_id: openraft::testing::log_id(1, 0, 1),
                    payload: EntryPayload::Normal(ConsensusCommand::Takeover {
                        candidate: 1,
                        expected: None,
                        at: at(1_000),
                    }),
                },
                Entry {
                    log_id: openraft::testing::log_id(1, 0, 2),
                    payload: EntryPayload::Normal(ConsensusCommand::SetPaused(Some(Paused {
                        reason: "maintenance".into(),
                        set_by: "operator".into(),
                        at: at(1_100),
                    }))),
                },
            ]
        };

        let (_d1, mut a) = temp_sm();
        let (_d2, mut b) = temp_sm();
        a.apply(entries()).await.unwrap();
        b.apply(entries()).await.unwrap();

        assert_eq!(a.data, b.data);
    }

    #[tokio::test]
    async fn snapshot_round_trips_through_install() {
        let (_dir, mut sm) = temp_sm();
        sm.apply(vec![Entry {
            log_id: openraft::testing::log_id(1, 0, 4),
            payload: EntryPayload::Normal(ConsensusCommand::Takeover {
                candidate: 2,
                expected: None,
                at: at(100),
            }),
        }])
        .await
        .unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 4);

        // A fresh machine installs it and ends up at the same state.
        let (_dir2, mut fresh) = temp_sm();
        fresh
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert_eq!(fresh.cluster_state().lease.as_ref().unwrap().holder, 2);
        assert_eq!(fresh.data.last_applied.unwrap().index, 4);

        let current = fresh.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);
    }

    #[test]
    fn node_id_seam_rejects_ids_it_cannot_represent() {
        assert_eq!(to_raft_node_id(3).unwrap(), 3u64);
        assert_eq!(from_raft_node_id(3).unwrap(), 3i32);
        assert!(to_raft_node_id(-1).is_err());
        assert!(from_raft_node_id(u64::MAX).is_err());

        let m = membership_map([(0, "db0:9000".to_string()), (1, "db1:9000".to_string())]).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m[&0].addr, "db0:9000");
    }
}
