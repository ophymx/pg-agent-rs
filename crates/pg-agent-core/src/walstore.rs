//! WAL segment movement between this node and its peers:
//!
//! - `open_archive` serves a segment from the local archive_dir for the
//!   peer-side `FetchWal` handler.
//! - `write_restore` receives bytes streamed from a peer and writes them
//!   atomically (temp + rename) into `$PGDATA` at the path PostgreSQL's
//!   `restore_command` supplied. Caller-supplied path is authoritative
//!   (SPEC §17 invariant 9) but must resolve inside `$PGDATA`.

use crate::errors::AgentError;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;

#[async_trait]
pub trait WalStore: Send + Sync {
    /// Open a WAL segment from the local archive directory for streaming.
    ///
    /// `fs::ErrorKind::NotFound` when the segment is absent — the FetchWal
    /// handler maps this to gRPC `NotFound` so PostgreSQL pauses and retries.
    async fn open_archive(
        &self,
        wal_file: &str,
    ) -> anyhow::Result<Box<dyn AsyncRead + Send + Unpin>>;

    /// Atomically write `src` to `dest_path`. Returns
    /// [`AgentError::DestOutsidePgData`] if `dest_path` resolves outside
    /// the configured PGDATA root.
    async fn write_restore(
        &self,
        dest_path: &Path,
        src: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), AgentError>;
}

pub struct FileWalStore {
    pub pg_data_dir: PathBuf,
    pub archive_dir: PathBuf,
}

// TODO(v1): impl FileWalStore — `filepath::Localize`-equivalent rejection of
// `..` / absolute paths in open_archive; temp + rename atomic write inside
// dest_path's parent dir; reject dest_path outside pg_data_dir.
