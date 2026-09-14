//! `$PGDATA` hook symlinks — creation, repair, and discovery of `pg_agentc`.
//!
//! pgpool's `pgpool_recovery` C extension invokes hooks by exec'ing
//! `$PGDATA/recovery_1st_stage` and `$PGDATA/pgpool_remote_start` as
//! subprocesses of the primary's PostgreSQL backend. Both names must
//! exist as symlinks pointing at `pg_agentc` for the hook surface to
//! work.
//!
//! Two call sites need this logic:
//!
//! 1. **Daemon startup** — `pg_agentd` creates / repairs the symlinks
//!    once at boot, refusing if a foreign symlink or regular file is in
//!    the way (operator-owned state must not be silently clobbered).
//!
//! 2. **After every `pg_basebackup`** — pg_basebackup replicates only
//!    files and tablespace symlinks; per upstream docs ("Other symbolic
//!    links and special device files are skipped"), the hook symlinks
//!    are dropped on the floor. Without repair, a freshly-rebuilt
//!    standby promoted via `Failover` would fire hooks at non-existent
//!    paths. `StandbyExec::basebackup` (trait method on
//!    [`crate::pgstandby::StandbyOps`]) calls [`ensure_hook_symlinks`]
//!    at the tail of its success path so the repair window is "during
//!    the same handler that did the wipe" — no orchestrator can forget.
//!
//! `pg_rewind` does NOT wipe `$PGDATA` (it modifies in place), so the
//! existing symlinks survive a rewind. Only basebackup needs the
//! repair.

use pg_agent_hookspec::PGDATA_SYMLINK_HOOKS;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum SymlinkError {
    #[error("hook symlink {name}: create at {path}: {source}")]
    Create {
        name: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("hook symlink {name}: path exists at {path} and is not a symlink")]
    NotASymlink { name: &'static str, path: PathBuf },
    #[error("hook symlink {name}: readlink {path}: {source}")]
    Readlink {
        name: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "hook symlink {name}: existing symlink at {path} points to {existing:?} \
         which is not pg_agentc; refusing to overwrite"
    )]
    ForeignSymlink {
        name: &'static str,
        path: PathBuf,
        existing: PathBuf,
    },
    #[error("hook symlink {name}: remove stale {path}: {source}")]
    Remove {
        name: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("find pg_agentc: {0}")]
    FindPgAgentc(String),
}

/// Locate the `pg_agentc` binary that pgpool will exec via the `$PGDATA`
/// symlinks. Prefers a sibling of the running executable (Debian package
/// layout: `/usr/bin/pg_agentd` next to `/usr/bin/pg_agentc`), then
/// falls back to `PATH`.
pub fn find_pg_agentc() -> Result<PathBuf, SymlinkError> {
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.to_path_buf()));
    let path_var = std::env::var_os("PATH");
    let path_dirs: Vec<PathBuf> = path_var
        .as_ref()
        .map(|p| std::env::split_paths(p).collect())
        .unwrap_or_default();
    find_pg_agentc_in(sibling.as_deref(), &path_dirs).ok_or_else(|| {
        SymlinkError::FindPgAgentc("pg_agentc not found alongside pg_agentd or in PATH".to_string())
    })
}

/// Pure lookup helper — pulled out so tests can drive it with synthetic
/// search dirs without mutating process env. Returns the first
/// `pg_agentc` regular file found: sibling first (mirrors the Debian
/// package layout), then each `PATH` entry in order.
pub(crate) fn find_pg_agentc_in(
    sibling_dir: Option<&Path>,
    path_dirs: &[PathBuf],
) -> Option<PathBuf> {
    if let Some(dir) = sibling_dir {
        let candidate = dir.join("pg_agentc");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    for dir in path_dirs {
        let candidate = dir.join("pg_agentc");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// The pgpool hook symlinks as pgman restore entries: `(name relative
/// to $PGDATA, target)`. Handed to
/// [`pgman::pgstandby::StandbyExec::new`] so a basebackup-rebuilt
/// standby gets its hooks back without pgman knowing what a pgpool
/// hook is.
pub fn hook_restore_symlinks(pg_agentc_bin: &Path) -> Vec<(String, PathBuf)> {
    PGDATA_SYMLINK_HOOKS
        .iter()
        .map(|name| (name.to_string(), pg_agentc_bin.to_path_buf()))
        .collect()
}

/// Create or repair every symlink in [`PGDATA_SYMLINK_HOOKS`] under
/// `pg_data_dir`, pointing at `pg_agentc_bin`. Per-symlink rules:
///
/// 1. Path does not exist → create.
/// 2. Path is a symlink whose target basename is `pg_agentc` → replace
///    (the target's directory may have changed across reinstalls;
///    forcing a refresh keeps the symlink pointing at the binary we
///    actually want pgpool to exec).
/// 3. Path is a symlink pointing elsewhere → return `ForeignSymlink`.
///    Operator-owned state must not be silently clobbered.
/// 4. Path exists but is not a symlink → return `NotASymlink`.
pub fn ensure_hook_symlinks(pg_data_dir: &Path, pg_agentc_bin: &Path) -> Result<(), SymlinkError> {
    for name in PGDATA_SYMLINK_HOOKS {
        ensure_one(pg_data_dir, pg_agentc_bin, name)?;
    }
    Ok(())
}

fn ensure_one(
    pg_data_dir: &Path,
    pg_agentc_bin: &Path,
    name: &'static str,
) -> Result<(), SymlinkError> {
    let full = pg_data_dir.join(name);

    match std::fs::read_link(&full) {
        Ok(existing) => {
            // Symlink — accept only if it points to a path whose
            // basename is `pg_agentc`. Anything else is foreign.
            if existing.file_name().and_then(|s| s.to_str()) != Some("pg_agentc") {
                return Err(SymlinkError::ForeignSymlink {
                    name,
                    path: full,
                    existing,
                });
            }
            // Same shape, possibly different target path — remove + recreate.
            std::fs::remove_file(&full).map_err(|source| SymlinkError::Remove {
                name,
                path: full.clone(),
                source,
            })?;
            std::os::unix::fs::symlink(pg_agentc_bin, &full).map_err(|source| {
                SymlinkError::Create {
                    name,
                    path: full.clone(),
                    source,
                }
            })?;
            info!(
                link = name,
                old = %existing.display(),
                new = %pg_agentc_bin.display(),
                "hook symlink updated"
            );
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Doesn't exist yet — create.
            std::os::unix::fs::symlink(pg_agentc_bin, &full).map_err(|source| {
                SymlinkError::Create {
                    name,
                    path: full.clone(),
                    source,
                }
            })?;
            info!(
                link = name,
                target = %pg_agentc_bin.display(),
                "hook symlink created"
            );
            Ok(())
        }
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
            // read_link on a regular file or directory returns EINVAL.
            // Surface a clear "not a symlink" error rather than the raw
            // EINVAL the user can't act on.
            Err(SymlinkError::NotASymlink { name, path: full })
        }
        Err(source) => Err(SymlinkError::Readlink {
            name,
            path: full,
            source,
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn fake_bin(dir: &TempDir, name: &str) -> PathBuf {
        let p = dir.path().join(name);
        fs::write(&p, b"#!/bin/sh\n").unwrap();
        p
    }

    #[test]
    fn ensure_creates_when_missing() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir = TempDir::new().unwrap();
        let bin = fake_bin(&bin_dir, "pg_agentc");

        ensure_hook_symlinks(pgdata.path(), &bin).unwrap();

        for name in PGDATA_SYMLINK_HOOKS {
            let path = pgdata.path().join(name);
            let target = fs::read_link(&path).unwrap();
            assert_eq!(target, bin);
        }
    }

    #[test]
    fn ensure_replaces_existing_pg_agentc_symlink() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir_old = TempDir::new().unwrap();
        let bin_dir_new = TempDir::new().unwrap();
        let old_bin = fake_bin(&bin_dir_old, "pg_agentc");
        let new_bin = fake_bin(&bin_dir_new, "pg_agentc");

        ensure_hook_symlinks(pgdata.path(), &old_bin).unwrap();
        ensure_hook_symlinks(pgdata.path(), &new_bin).unwrap();

        for name in PGDATA_SYMLINK_HOOKS {
            let target = fs::read_link(pgdata.path().join(name)).unwrap();
            assert_eq!(target, new_bin);
        }
    }

    #[test]
    fn ensure_rejects_foreign_symlink() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir = TempDir::new().unwrap();
        let bin = fake_bin(&bin_dir, "pg_agentc");
        let other = fake_bin(&bin_dir, "not_pg_agentc");

        // Pre-place a foreign symlink at one of the hook paths.
        let target_name = PGDATA_SYMLINK_HOOKS[0];
        symlink(&other, pgdata.path().join(target_name)).unwrap();

        let err = ensure_hook_symlinks(pgdata.path(), &bin).unwrap_err();
        match err {
            SymlinkError::ForeignSymlink { name, existing, .. } => {
                assert_eq!(name, target_name);
                assert_eq!(existing, other);
            }
            other => panic!("expected ForeignSymlink, got {other:?}"),
        }
    }

    #[test]
    fn ensure_rejects_regular_file_in_the_way() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir = TempDir::new().unwrap();
        let bin = fake_bin(&bin_dir, "pg_agentc");

        let target_name = PGDATA_SYMLINK_HOOKS[0];
        fs::write(pgdata.path().join(target_name), b"i'm a regular file").unwrap();

        let err = ensure_hook_symlinks(pgdata.path(), &bin).unwrap_err();
        match err {
            SymlinkError::NotASymlink { name, .. } => assert_eq!(name, target_name),
            other => panic!("expected NotASymlink, got {other:?}"),
        }
    }

    #[test]
    fn ensure_rejects_directory_in_the_way() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir = TempDir::new().unwrap();
        let bin = fake_bin(&bin_dir, "pg_agentc");

        let target_name = PGDATA_SYMLINK_HOOKS[0];
        fs::create_dir(pgdata.path().join(target_name)).unwrap();

        let err = ensure_hook_symlinks(pgdata.path(), &bin).unwrap_err();
        assert!(matches!(err, SymlinkError::NotASymlink { .. }));
    }

    #[test]
    fn ensure_is_idempotent() {
        let pgdata = TempDir::new().unwrap();
        let bin_dir = TempDir::new().unwrap();
        let bin = fake_bin(&bin_dir, "pg_agentc");

        ensure_hook_symlinks(pgdata.path(), &bin).unwrap();
        ensure_hook_symlinks(pgdata.path(), &bin).unwrap();
        ensure_hook_symlinks(pgdata.path(), &bin).unwrap();

        for name in PGDATA_SYMLINK_HOOKS {
            let target = fs::read_link(pgdata.path().join(name)).unwrap();
            assert_eq!(target, bin);
        }
    }

    #[test]
    fn find_pg_agentc_in_prefers_sibling() {
        let sibling_dir = TempDir::new().unwrap();
        let path_dir = TempDir::new().unwrap();
        let sibling_bin = fake_bin(&sibling_dir, "pg_agentc");
        let _path_bin = fake_bin(&path_dir, "pg_agentc");
        let found =
            find_pg_agentc_in(Some(sibling_dir.path()), &[path_dir.path().to_path_buf()]).unwrap();
        assert_eq!(found, sibling_bin, "sibling should win over PATH");
    }

    #[test]
    fn find_pg_agentc_in_falls_back_to_path() {
        let sibling_dir = TempDir::new().unwrap();
        // sibling_dir has no pg_agentc.
        let path_dir = TempDir::new().unwrap();
        let path_bin = fake_bin(&path_dir, "pg_agentc");
        let found =
            find_pg_agentc_in(Some(sibling_dir.path()), &[path_dir.path().to_path_buf()]).unwrap();
        assert_eq!(found, path_bin);
    }

    #[test]
    fn find_pg_agentc_in_returns_none_when_missing() {
        let sibling_dir = TempDir::new().unwrap();
        let path_dir = TempDir::new().unwrap();
        // No pg_agentc anywhere.
        let found = find_pg_agentc_in(Some(sibling_dir.path()), &[path_dir.path().to_path_buf()]);
        assert!(found.is_none());
    }

    #[test]
    fn find_pg_agentc_in_tolerates_no_sibling() {
        let path_dir = TempDir::new().unwrap();
        let path_bin = fake_bin(&path_dir, "pg_agentc");
        let found = find_pg_agentc_in(None, &[path_dir.path().to_path_buf()]).unwrap();
        assert_eq!(found, path_bin);
    }
}
