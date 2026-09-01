//! WAL segment movement between this node and its peers.
//!
//! - [`WalStore::open_archive`] serves a segment from the local
//!   `archive_dir` for the peer-side `FetchWal` handler to stream.
//! - [`WalStore::write_restore`] receives bytes streamed from a peer and
//!   writes them atomically into `$PGDATA` at the path PostgreSQL's
//!   `restore_command` supplied. The caller-supplied path is authoritative
//!   (SPEC §17 invariant 9) but MUST resolve inside `pg_data_dir`.
//!
//! # Security model
//!
//! Both methods sit at a trust boundary — the inputs come from peer
//! agents over gRPC and from PostgreSQL itself respectively. They are
//! defended in depth:
//!
//! * **`open_archive`** validates the WAL filename against the same regex
//!   the proto declares (`^([0-9A-F]{24}|[0-9A-F]{8}\.history)$`). The
//!   regex precludes `/`, `..`, lowercase, and any string that isn't a
//!   PostgreSQL-shaped WAL or timeline-history filename — so the only
//!   path that `archive_dir.join(name)` can produce is `archive_dir/<name>`
//!   with no escape, even if a peer crafts a hostile request.
//!
//! * **`write_restore`** canonicalizes the destination's parent directory
//!   (resolves symlinks)
//!   and verify the result is a descendant of the canonicalized
//!   `pg_data_dir`. A purely syntactic check would miss the
//!   "`$PGDATA/pg_wal` is a symlink to `/tmp`" attack; canonicalize-parent
//!   catches it.
//!
//! # Atomic write
//!
//! Writes go to a hidden temp file (`.<base>-<8 hex>`) in the destination's
//! parent directory, opened with `O_CREAT | O_EXCL | mode 0600`. After a
//! successful copy + flush, [`tokio::fs::rename`] swaps it onto the final
//! path — POSIX `rename(2)` is atomic on the same filesystem. On any
//! failure path the temp file is removed; the hidden + random prefix means
//! even a leaked temp can never be mistaken for a valid WAL segment.

use async_trait::async_trait;
use rand::rngs::OsRng;
use rand::RngCore;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io::{AsyncBufRead, AsyncRead};
use tracing::debug;

/// WAL-store failures, typed so transport handlers can map each to a
/// distinct wire status (absent segment → retryable NotFound, bad name
/// or escape attempt → InvalidArgument).
#[derive(Debug, thiserror::Error)]
pub enum WalStoreError {
    #[error("dest_path is outside pgdata root")]
    DestOutsidePgData,
    #[error("WAL segment not found: {0}")]
    WalNotFound(String),
    #[error("invalid wal_file {wal_file:?}: {reason}")]
    WalInvalid { wal_file: String, reason: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// `^([0-9A-F]{24}|[0-9A-F]{8}\.history)$` — accept exactly what
/// PostgreSQL's `restore_command` legitimately asks for: a 24-char
/// uppercase-hex WAL segment, or an 8-char uppercase-hex timeline
/// history file. Anything else (including any name containing `/` or
/// `..` or `archive_status/foo.ready`) is rejected so peers can't use
/// FetchWal to exfiltrate arbitrary files from `archive_dir`.
fn wal_filename_re() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^([0-9A-F]{24}|[0-9A-F]{8}\.history)$").unwrap())
}

#[async_trait]
pub trait WalStore: Send + Sync {
    /// Open a WAL segment from the local archive directory for streaming.
    /// Returns [`WalStoreError::WalNotFound`] when the segment is absent —
    /// the FetchWal handler maps this to gRPC `NotFound` so PostgreSQL
    /// pauses and retries. Returns [`WalStoreError::WalInvalid`] for bad
    /// filenames (handler maps to gRPC `InvalidArgument`).
    async fn open_archive(
        &self,
        wal_file: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, WalStoreError>;

    /// Atomically write `src` to `dest_path`. Returns
    /// [`WalStoreError::DestOutsidePgData`] when `dest_path` doesn't resolve
    /// inside the configured `pg_data_dir` (handler maps to gRPC
    /// `InvalidArgument`).
    ///
    /// `src` is [`AsyncBufRead`], not [`AsyncRead`], so the copy can hand
    /// the reader's own buffer straight to the file. The production `src`
    /// is a `StreamReader` over `FetchWal`'s 1 MiB chunks; behind a bare
    /// `AsyncRead` the copy would drag all 16 MiB through
    /// `tokio::io::copy`'s 8 KiB scratch buffer, turning ~16 writes into
    /// ~2048 — and every `tokio::fs` write is a blocking-pool dispatch.
    async fn write_restore(
        &self,
        dest_path: &Path,
        src: Box<dyn AsyncBufRead + Send + Unpin>,
    ) -> Result<(), WalStoreError>;
}

// ---------------------------------------------------------------------------
// FileWalStore — production impl
// ---------------------------------------------------------------------------

pub struct FileWalStore {
    pub pg_data_dir: PathBuf,
    pub archive_dir: PathBuf,
}

impl FileWalStore {
    pub fn new(pg_data_dir: PathBuf, archive_dir: PathBuf) -> Self {
        Self {
            pg_data_dir,
            archive_dir,
        }
    }

    /// Resolve `dest_path` against the canonicalized `pg_data_dir` and
    /// return the canonical destination path. Errors with
    /// [`WalStoreError::DestOutsidePgData`] for any of:
    ///
    /// - parent directory doesn't exist or can't be canonicalized
    /// - canonicalized parent isn't a descendant of canonicalized
    ///   `pg_data_dir` (catches symlink escapes)
    ///
    /// PostgreSQL's `restore_command` substitutes `%p` with a path
    /// **relative to PGDATA** (PG `chdir`'s there before invoking the
    /// command). The restore wrapper passes that value verbatim as `dest_path`,
    /// so a relative path here is the normal case — we resolve it
    /// against `pg_data_dir`. Absolute paths are also accepted (a
    /// future restore_command might pre-resolve, or an operator might
    /// hit this RPC directly) and validated the same way.
    ///
    /// We canonicalize the *parent* rather than the destination itself
    /// because the destination may not yet exist (we're about to create
    /// it). The parent always exists during a real `restore_command`
    /// invocation — PostgreSQL writes into `pg_wal/`, which initdb
    /// creates.
    async fn resolve_dest_path(&self, dest_path: &Path) -> Result<PathBuf, WalStoreError> {
        let absolute = if dest_path.is_absolute() {
            dest_path.to_path_buf()
        } else {
            self.pg_data_dir.join(dest_path)
        };
        let parent = absolute.parent().ok_or(WalStoreError::DestOutsidePgData)?;
        let base = absolute
            .file_name()
            .ok_or(WalStoreError::DestOutsidePgData)?;

        let parent_canonical = tokio::fs::canonicalize(parent)
            .await
            .map_err(|_| WalStoreError::DestOutsidePgData)?;
        let pg_root_canonical = tokio::fs::canonicalize(&self.pg_data_dir)
            .await
            .map_err(|_| WalStoreError::DestOutsidePgData)?;
        if !parent_canonical.starts_with(&pg_root_canonical) {
            return Err(WalStoreError::DestOutsidePgData);
        }
        Ok(parent_canonical.join(base))
    }
}

#[async_trait]
impl WalStore for FileWalStore {
    async fn open_archive(
        &self,
        wal_file: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, WalStoreError> {
        validate_wal_filename(wal_file)?;
        let path = self.archive_dir.join(wal_file);
        match tokio::fs::File::open(&path).await {
            Ok(f) => {
                debug!(wal_file, path = %path.display(), "walstore: opened archive");
                Ok(Box::new(f))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(WalStoreError::WalNotFound(wal_file.to_string()))
            }
            Err(e) => Err(WalStoreError::Io(e)),
        }
    }

    async fn write_restore(
        &self,
        dest_path: &Path,
        mut src: Box<dyn AsyncBufRead + Send + Unpin>,
    ) -> Result<(), WalStoreError> {
        let resolved = self.resolve_dest_path(dest_path).await?;

        let dir = resolved.parent().ok_or(WalStoreError::DestOutsidePgData)?;
        let base = resolved
            .file_name()
            .ok_or(WalStoreError::DestOutsidePgData)?
            .to_string_lossy()
            .into_owned();
        let suffix = random_hex_suffix()?;
        let tmp_path = dir.join(format!(".{base}-{suffix}"));

        // O_CREAT | O_EXCL on a hidden+random name prevents an attacker
        // from pre-creating the temp path and winning the race against
        // our open. Mode 0600 keeps the temp readable only by us.
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)
            .await
            .map_err(WalStoreError::Io)?;

        // From here on, any failure must remove the temp file. Body is in
        // an async block so we can capture the Result and dispatch
        // cleanup uniformly without futures::TryFutureExt gymnastics.
        let body = async {
            // copy_buf, not copy: `src` is already buffered, so each 1 MiB
            // chunk goes to the file whole instead of via an 8 KiB scratch
            // buffer. See the trait doc.
            tokio::io::copy_buf(&mut src, &mut tmp_file)
                .await
                .map_err(WalStoreError::Io)?;
            // `sync_all`, not `flush`: flush only pushes the bytes to the
            // kernel, which is not enough here. Rename the temp file while
            // its contents are still dirty page cache and a host crash can
            // leave the directory entry durable but the data not — a
            // correctly-named WAL segment full of zeros. PostgreSQL would
            // fail that segment's CRC and read it as end-of-WAL, quietly
            // ending recovery early rather than erroring. Syncing before
            // the rename closes the window: the data is on disk before the
            // name that promises it exists.
            //
            // `sync_all` subsumes the old `flush` — it completes the
            // in-flight write before issuing the fsync.
            //
            // The parent directory is deliberately *not* fsynced after the
            // rename. Losing the rename is safe: the segment simply isn't
            // there, and PostgreSQL's restore_command asks for it again.
            // Only the data-without-name ordering above is dangerous.
            tmp_file.sync_all().await.map_err(WalStoreError::Io)?;
            // Drop closes the file before the rename — explicit so the
            // ordering reads obvious. POSIX doesn't strictly require
            // close-before-rename but it makes the lifecycle explicit.
            drop(tmp_file);
            tokio::fs::rename(&tmp_path, &resolved)
                .await
                .map_err(WalStoreError::Io)?;
            Ok(())
        }
        .await;

        if body.is_err() {
            // Best-effort cleanup — if removal also fails (e.g. tmpfile
            // was already unlinked) we don't bury the real error.
            let _ = tokio::fs::remove_file(&tmp_path).await;
        } else {
            debug!(
                dest = %resolved.display(),
                "walstore: write_restore complete"
            );
        }
        body
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn validate_wal_filename(name: &str) -> Result<(), WalStoreError> {
    // Defense in depth — the regex below also rejects these, but explicit
    // failure modes are self-documenting and survive a regex change.
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(WalStoreError::WalInvalid {
            wal_file: name.to_string(),
            reason: "contains path component".to_string(),
        });
    }
    if !wal_filename_re().is_match(name) {
        return Err(WalStoreError::WalInvalid {
            wal_file: name.to_string(),
            reason: "must match ^([0-9A-F]{24}|[0-9A-F]{8}\\.history)$".to_string(),
        });
    }
    Ok(())
}

/// 8 hex chars from the OS RNG — used to make the temp file's name
/// unguessable so an attacker can't pre-create it and race the EXCL open.
/// Failure here means the OS RNG is unavailable; bubble up rather than
/// fall back to a predictable value.
fn random_hex_suffix() -> Result<String, WalStoreError> {
    let mut bytes = [0u8; 4];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| WalStoreError::Other(anyhow::anyhow!("walstore: OS RNG: {e}")))?;
    Ok(hex::encode(bytes))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    /// Set up a (pg_data_dir, archive_dir, FileWalStore) triple in a fresh
    /// tempdir. Both dirs are real directories so canonicalize() works.
    fn fixture() -> (TempDir, FileWalStore) {
        let tmp = TempDir::new().unwrap();
        let pg = tmp.path().join("pg_data");
        let archive = tmp.path().join("archive");
        std::fs::create_dir(&pg).unwrap();
        std::fs::create_dir(&archive).unwrap();
        let store = FileWalStore::new(pg.clone(), archive.clone());
        (tmp, store)
    }

    // ----- validate_wal_filename -------------------------------------------

    #[test]
    fn wal_filename_accepts_valid_segment_and_history() {
        validate_wal_filename("000000010000000000000001").unwrap();
        validate_wal_filename("ABCDEF0123456789FEDCBA98").unwrap();
        validate_wal_filename("00000002.history").unwrap();
    }

    #[test]
    fn wal_filename_rejects_lowercase() {
        // Postgres's restore_command always uses uppercase hex; lowercase
        // is a sign of tampering.
        assert!(validate_wal_filename("000000010000000000000abc").is_err());
    }

    #[test]
    fn wal_filename_rejects_wrong_length() {
        assert!(validate_wal_filename("00000001").is_err());
        assert!(validate_wal_filename("00000001000000000000000100").is_err());
    }

    #[test]
    fn wal_filename_rejects_path_components() {
        for n in [
            "",
            ".",
            "..",
            "../etc/passwd",
            "foo/bar",
            "archive_status/x",
        ] {
            let e = validate_wal_filename(n).unwrap_err();
            assert!(
                matches!(e, WalStoreError::WalInvalid { .. }),
                "{n:?} should be WalInvalid, got {e:?}"
            );
        }
    }

    #[test]
    fn wal_filename_rejects_non_hex_chars() {
        assert!(validate_wal_filename("0000000G0000000000000001").is_err()); // G
        assert!(validate_wal_filename("000000010000000000000001\0").is_err()); // NUL
    }

    // ----- open_archive ----------------------------------------------------

    /// `Result<Box<dyn AsyncRead + ...>, _>::unwrap_err()` needs `T: Debug`
    /// which the trait object isn't, so route via `.err()`.
    fn expect_err<T>(r: Result<T, WalStoreError>) -> WalStoreError {
        match r {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e,
        }
    }

    #[tokio::test]
    async fn open_archive_rejects_invalid_filename() {
        let (_tmp, store) = fixture();
        let e = expect_err(store.open_archive("../etc/passwd").await);
        assert!(matches!(e, WalStoreError::WalInvalid { .. }));
    }

    #[tokio::test]
    async fn open_archive_returns_not_found_for_missing() {
        let (_tmp, store) = fixture();
        let e = expect_err(store.open_archive("000000010000000000000001").await);
        assert!(matches!(e, WalStoreError::WalNotFound(ref n) if n == "000000010000000000000001"));
    }

    #[tokio::test]
    async fn open_archive_reads_file_contents() {
        let (_tmp, store) = fixture();
        let name = "000000010000000000000002";
        std::fs::write(store.archive_dir.join(name), b"wal bytes here").unwrap();

        let mut reader = store.open_archive(name).await.unwrap();
        let mut got = Vec::new();
        use tokio::io::AsyncReadExt as _;
        reader.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"wal bytes here");
    }

    // ----- write_restore ---------------------------------------------------

    /// PostgreSQL's `restore_command` substitutes `%p` as a path
    /// relative to PGDATA (PG `chdir`'s to PGDATA before exec). When
    /// the parent of the resolved path exists under pg_data_dir, the
    /// write should succeed — that's the live shape on every restore.
    #[tokio::test]
    async fn write_restore_resolves_relative_path_against_pg_data_dir() {
        let (_tmp, store) = fixture();
        // pg_basebackup creates pg_wal/; mirror that so the parent canonicalises.
        std::fs::create_dir(store.pg_data_dir.join("pg_wal")).unwrap();
        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(&b"wal bytes"[..]);
        store
            .write_restore(Path::new("pg_wal/000000010000000000000001"), src)
            .await
            .expect("relative path under pg_data_dir should resolve and write");
        let written =
            std::fs::read(store.pg_data_dir.join("pg_wal/000000010000000000000001")).unwrap();
        assert_eq!(written, b"wal bytes");
    }

    /// A relative path with a parent that doesn't exist under pg_data_dir
    /// still fails — the parent canonicalize step catches it.
    #[tokio::test]
    async fn write_restore_rejects_relative_path_with_missing_parent() {
        let (_tmp, store) = fixture();
        // pg_wal/ deliberately NOT created.
        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(&b"x"[..]);
        let e = store
            .write_restore(Path::new("pg_wal/000000010000000000000001"), src)
            .await
            .unwrap_err();
        assert!(matches!(e, WalStoreError::DestOutsidePgData));
    }

    #[tokio::test]
    async fn write_restore_rejects_outside_pgdata() {
        let (tmp, store) = fixture();
        // /tmp exists, is absolute, but is not inside pg_data_dir.
        let outside = tmp.path().join("not_pg_data");
        std::fs::create_dir(&outside).unwrap();
        let dest = outside.join("foo");

        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(&b"x"[..]);
        let e = store.write_restore(&dest, src).await.unwrap_err();
        assert!(matches!(e, WalStoreError::DestOutsidePgData));
    }

    #[tokio::test]
    async fn write_restore_rejects_symlink_escape() {
        // The interesting case: $PGDATA/escape -> /tmp. A purely
        // syntactic check would let `$PGDATA/escape/foo` through;
        // canonicalize-parent catches it.
        let (tmp, store) = fixture();
        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, store.pg_data_dir.join("escape")).unwrap();

        let dest = store.pg_data_dir.join("escape").join("evil");
        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(&b"x"[..]);
        let e = store.write_restore(&dest, src).await.unwrap_err();
        assert!(
            matches!(e, WalStoreError::DestOutsidePgData),
            "symlink escape should be rejected, got {e:?}"
        );
    }

    #[tokio::test]
    async fn write_restore_happy_path() {
        let (_tmp, store) = fixture();
        // Mimic a real layout — restore_command writes into pg_wal/.
        let pg_wal = store.pg_data_dir.join("pg_wal");
        std::fs::create_dir(&pg_wal).unwrap();
        let dest = pg_wal.join("000000010000000000000003");

        let payload: &[u8] = b"the brown fox jumps over the WAL";
        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(payload);
        store.write_restore(&dest, src).await.unwrap();

        // File exists with the right contents.
        let read_back = std::fs::read(&dest).unwrap();
        assert_eq!(read_back, payload);

        // No stray temp file beside it (hidden prefix `.dest-...`).
        let leftover_temps: Vec<_> = std::fs::read_dir(&pg_wal)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| {
                let s = n.to_string_lossy();
                s.starts_with(".000000010000000000000003-")
            })
            .collect();
        assert!(
            leftover_temps.is_empty(),
            "no temp files should remain after a successful write, found {leftover_temps:?}"
        );
    }

    #[tokio::test]
    async fn write_restore_cleans_temp_on_copy_failure() {
        let (_tmp, store) = fixture();
        let dest = store.pg_data_dir.join("000000010000000000000004");

        // Reader that errors after the first read — exercises the
        // tmp-file cleanup path. `copy_buf` only ever drives
        // `poll_fill_buf`/`consume`, but `AsyncBufRead`'s supertrait
        // still demands a `poll_read`, so both are supplied.
        struct Failing {
            yielded: bool,
        }
        impl AsyncRead for Failing {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if !self.yielded {
                    self.yielded = true;
                    std::task::Poll::Ready(Err(std::io::Error::other("simulated read failure")))
                } else {
                    std::task::Poll::Ready(Ok(()))
                }
            }
        }
        impl AsyncBufRead for Failing {
            fn poll_fill_buf(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<&[u8]>> {
                if !self.yielded {
                    self.yielded = true;
                    std::task::Poll::Ready(Err(std::io::Error::other("simulated read failure")))
                } else {
                    std::task::Poll::Ready(Ok(&[]))
                }
            }
            fn consume(self: std::pin::Pin<&mut Self>, _: usize) {}
        }
        let src: Box<dyn AsyncBufRead + Send + Unpin> = Box::new(Failing { yielded: false });

        let err = store.write_restore(&dest, src).await.unwrap_err();
        assert!(matches!(err, WalStoreError::Io(_)));

        // Final destination must NOT exist.
        assert!(!dest.exists(), "dest must not exist after a failed write");
        // No leftover hidden temp file in pg_data_dir.
        let leftover_temps: Vec<_> = std::fs::read_dir(&store.pg_data_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| {
                n.to_string_lossy()
                    .starts_with(".000000010000000000000004-")
            })
            .collect();
        assert!(
            leftover_temps.is_empty(),
            "temp file should be cleaned up after failure, found {leftover_temps:?}"
        );
    }

    #[test]
    fn random_hex_suffix_is_8_chars() {
        let s = random_hex_suffix().unwrap();
        assert_eq!(s.len(), 8);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
