//! "Make this node a standby of some primary" operations:
//! [`StandbyOps::basebackup`], [`StandbyOps::rewind`], and
//! [`StandbyOps::write_recovery_conf`]. The only production impl
//! is [`StandbyExec`], which shells out to `pg_basebackup` and
//! `pg_rewind` and writes the recovery config directly to `$PGDATA`.
//!
//! # Subprocess story
//!
//! Both `pg_basebackup` and `pg_rewind` print progress to *stderr* using
//! `\r` for in-place updates (e.g.
//! `"55564/1261024 kB (4%), 1/1 tablespace\r…"`). The stderr drain reads
//! the pipe byte-by-byte (through a BufReader), splits on `\r` OR `\n`,
//! dispatches matching progress lines to the caller's callback, logs
//! other lines at debug, and captures the *first* 4 KiB of non-progress
//! output into a tail buffer.
//!
//! On a non-zero exit the tail buffer is folded into the returned
//! error — so callers see `"basebackup: exit 1; stderr: FATAL:
//! could not connect to server: …"` instead of an unhelpful `exit 1`.
//!
//! # Cancellation
//!
//! `Command::kill_on_drop(true)` plus `tokio::join!(drain, child.wait())`
//! (not `tokio::spawn`) means that dropping the outer future — e.g.
//! when a `tokio::time::timeout` hook deadline fires — SIGKILLs the
//! subprocess and drops the drainer in the same step. No spawned tasks
//! left behind, no zombies.
//!
//! # Replslot cleanup
//!
//! `pg_rewind` clears `$PGDATA/pg_replslot/*` **both before and after**
//! its run (SPEC §17 invariant 5). Before: stale slot dirs from this
//! node's pre-rewind role would otherwise survive (rewind copies only
//! changed blocks). After: rewind may have copied slot dirs from the
//! source's role that would crash PG recovery if left in place.

use crate::config::PgReplicationTlsConfig;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

/// `(bytes_done, bytes_total)`. `bytes_total = 0` means unknown.
pub type ProgressCb = Box<dyn Fn(i64, i64) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct BasebackupOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
    pub slot_name: String,
}

#[derive(Debug, Clone)]
pub struct RewindOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
}

/// Wire-shaped input — local concerns (TLS material, `$PGDATA`) live on
/// the impl, not on this struct. Don't extend with fields the caller
/// can't supply.
#[derive(Debug, Clone)]
pub struct WriteRecoveryConfOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
    pub slot_name: String,
}

impl WriteRecoveryConfOpts {
    /// Input validation that the gRPC `ConfigureStandby` handler runs
    /// before dispatching. Repeated inside the impl as defense in depth.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.primary_host.is_empty() {
            anyhow::bail!("primary_host is required");
        }
        if !allowed_primary_host().is_match(&self.primary_host) {
            anyhow::bail!("primary_host contains invalid characters");
        }
        if self.primary_port == 0 {
            anyhow::bail!("primary_port must be greater than zero");
        }
        if self.slot_name.is_empty() {
            anyhow::bail!("slot_name is required");
        }
        if !allowed_slot_name().is_match(&self.slot_name) {
            anyhow::bail!("slot_name contains invalid characters");
        }
        if self.repl_user.is_empty() {
            anyhow::bail!("repl_user is required");
        }
        if !allowed_repl_user().is_match(&self.repl_user) {
            anyhow::bail!("repl_user contains invalid characters");
        }
        Ok(())
    }
}

#[async_trait]
pub trait StandbyOps: Send + Sync {
    /// Clears `$PGDATA` contents first, then exec's `pg_basebackup`.
    /// **Caller must have verified PostgreSQL isn't running on this
    /// node** — otherwise we'd wipe a live datadir.
    async fn basebackup(
        &self,
        opts: BasebackupOpts,
        progress: Option<ProgressCb>,
    ) -> anyhow::Result<()>;

    /// Clears `pg_replslot/*` before *and* after (see SPEC §17 invariant 5).
    async fn rewind(&self, opts: RewindOpts, progress: Option<ProgressCb>) -> anyhow::Result<()>;

    /// Writes `$PGDATA/myrecovery.conf` + creates `$PGDATA/standby.signal`.
    async fn write_recovery_conf(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// StandbyExec — production impl
// ---------------------------------------------------------------------------

/// Production [`StandbyOps`] — drives `pg_basebackup` / `pg_rewind` as
/// subprocesses and writes recovery config directly into `pg_data_dir`.
/// Construction is cheap; clone-via-Arc at the daemon's composition root.
pub struct StandbyExec {
    pub pg_home: PathBuf,
    pub pg_data_dir: PathBuf,
    pub replication_tls: PgReplicationTlsConfig,
}

impl StandbyExec {
    pub fn new(
        pg_home: PathBuf,
        pg_data_dir: PathBuf,
        replication_tls: PgReplicationTlsConfig,
    ) -> Self {
        Self {
            pg_home,
            pg_data_dir,
            replication_tls,
        }
    }

    fn basebackup_bin(&self) -> PathBuf {
        self.pg_home.join("bin").join("pg_basebackup")
    }

    fn rewind_bin(&self) -> PathBuf {
        self.pg_home.join("bin").join("pg_rewind")
    }
}

#[async_trait]
impl StandbyOps for StandbyExec {
    async fn basebackup(
        &self,
        opts: BasebackupOpts,
        progress: Option<ProgressCb>,
    ) -> anyhow::Result<()> {
        // pg_basebackup refuses a non-empty target dir (no override
        // flag exists). The caller must have verified PG isn't running;
        // we wipe anything left over so a half-finished prior attempt
        // doesn't block us. The directory itself stays.
        clear_pgdata_contents(&self.pg_data_dir)
            .await
            .map_err(|e| anyhow::anyhow!("basebackup: clear pgdata: {e}"))?;

        // dbname is empty — pg_basebackup speaks the replication protocol
        // and doesn't take one.
        let conninfo = self.replication_tls.conninfo(
            &opts.primary_host,
            opts.primary_port,
            &opts.repl_user,
            "",
        );

        let bin = self.basebackup_bin();
        let mut args = vec![
            "--pgdata".to_string(),
            self.pg_data_dir.display().to_string(),
            "--dbname".to_string(),
            conninfo,
            "--wal-method=stream".to_string(),
            "--checkpoint=fast".to_string(),
            // --write-recovery-conf intentionally omitted: write_recovery_conf
            // is the single source of recovery config. Letting pg_basebackup
            // append to postgresql.auto.conf would split the recovery
            // config across two files and conflict with ALTER SYSTEM.
            "--no-password".to_string(),
        ];
        if !opts.slot_name.is_empty() {
            args.push("--slot".to_string());
            args.push(opts.slot_name.clone());
        }
        if progress.is_some() {
            args.push("--progress".to_string());
        }

        info!(
            bin = %bin.display(),
            primary = %opts.primary_host,
            datadir = %self.pg_data_dir.display(),
            "basebackup: starting"
        );
        run_pg_binary(&bin, &args, progress)
            .await
            .map_err(|e| anyhow::anyhow!("basebackup: {e}"))?;
        info!(datadir = %self.pg_data_dir.display(), "basebackup: completed");
        Ok(())
    }

    async fn rewind(&self, opts: RewindOpts, progress: Option<ProgressCb>) -> anyhow::Result<()> {
        // Pre-clean: stale slot dirs from this node's pre-rewind role
        // would otherwise survive (rewind copies only changed blocks).
        clean_replslots(&self.pg_data_dir)
            .await
            .map_err(|e| anyhow::anyhow!("rewind: pre-clean replslots: {e}"))?;

        // pg_rewind needs a regular DB connection (dbname=postgres),
        // unlike pg_basebackup which uses the replication protocol.
        let conninfo = self.replication_tls.conninfo(
            &opts.primary_host,
            opts.primary_port,
            &opts.repl_user,
            "postgres",
        );

        let bin = self.rewind_bin();
        let args = vec![
            "--target-pgdata".to_string(),
            self.pg_data_dir.display().to_string(),
            "--source-server".to_string(),
            conninfo,
            "--no-password".to_string(),
            "--progress".to_string(),
        ];

        info!(
            bin = %bin.display(),
            primary = %opts.primary_host,
            datadir = %self.pg_data_dir.display(),
            "rewind: starting"
        );
        run_pg_binary(&bin, &args, progress)
            .await
            .map_err(|e| anyhow::anyhow!("rewind: {e}"))?;

        // Post-clean: rewind may have copied slot dirs from the source's
        // role that would crash PG recovery if left in place.
        clean_replslots(&self.pg_data_dir)
            .await
            .map_err(|e| anyhow::anyhow!("rewind: post-clean replslots: {e}"))?;

        info!(datadir = %self.pg_data_dir.display(), "rewind: completed");
        Ok(())
    }

    async fn write_recovery_conf(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()> {
        opts.validate()
            .map_err(|e| anyhow::anyhow!("write_recovery_conf: validate: {e}"))?;

        let conninfo = self.replication_tls.conninfo(
            &opts.primary_host,
            opts.primary_port,
            &opts.repl_user,
            "",
        );

        let content = render_recovery_conf(&conninfo, &opts.slot_name)?;

        atomic_write(
            &self.pg_data_dir.join("myrecovery.conf"),
            content.as_bytes(),
            0o640,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write_recovery_conf: write myrecovery.conf: {e}"))?;

        atomic_write(&self.pg_data_dir.join("standby.signal"), b"", 0o640)
            .await
            .map_err(|e| anyhow::anyhow!("write_recovery_conf: write standby.signal: {e}"))?;

        info!(datadir = %self.pg_data_dir.display(), "write_recovery_conf: completed");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Subprocess driver
// ---------------------------------------------------------------------------

/// Spawn `bin`, drain its stderr concurrently, and surface a useful
/// error message on non-zero exit. `kill_on_drop(true)` + `join!` means
/// a cancelled outer future SIGKILLs the subprocess in the same drop.
async fn run_pg_binary(
    bin: &Path,
    args: &[String],
    progress: Option<ProgressCb>,
) -> anyhow::Result<()> {
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null()) // pg_basebackup/pg_rewind put nothing here in our flags
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            anyhow::anyhow!(
                "{}: binary not found (install the matching PostgreSQL client \
                 package or check that pghome={} is correct)",
                bin.display(),
                bin.parent()
                    .and_then(|p| p.parent())
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "?".to_string())
            )
        } else {
            anyhow::anyhow!("spawn {}: {e}", bin.display())
        }
    })?;

    let stderr = child
        .stderr
        .take()
        .expect("stderr piped — Command::spawn invariant");
    let bin_name = bin
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("pg_binary")
        .to_string();

    let tail: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));

    // tokio::join! runs both concurrently in the SAME task. Dropping the
    // outer future drops both, which kills the subprocess (kill_on_drop)
    // and abandons the drain. tokio::spawn would have detached the
    // drain task — that's the wrong behaviour for cancellation.
    let drain = drain_stderr(stderr, progress.as_ref(), tail.clone(), &bin_name);
    let wait = child.wait();
    let (drain_result, wait_result) = tokio::join!(drain, wait);

    if let Err(e) = drain_result {
        // Drain failure isn't fatal — wait_result is the authority on
        // whether the subprocess succeeded.
        warn!(?e, bin = %bin_name, "pg subprocess: stderr drain error");
    }

    let status = wait_result.map_err(|e| anyhow::anyhow!("wait {bin_name}: {e}"))?;
    if !status.success() {
        let tail_str = tail.lock().unwrap().clone();
        let suffix = if tail_str.is_empty() {
            String::new()
        } else {
            format!("; stderr: {tail_str}")
        };
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "?".to_string());
        anyhow::bail!("{bin_name}: exit {code}{suffix}");
    }
    Ok(())
}

const STDERR_TAIL_MAX: usize = 4096;

/// Read `\r`- or `\n`-delimited lines from `stderr`; dispatch progress
/// updates via `progress`, log everything else at debug, accumulate the
/// first `STDERR_TAIL_MAX` bytes of non-progress output into `tail`.
async fn drain_stderr<R: tokio::io::AsyncRead + Unpin>(
    stderr: R,
    progress: Option<&ProgressCb>,
    tail: Arc<Mutex<String>>,
    bin_name: &str,
) -> std::io::Result<()> {
    let mut reader = BufReader::with_capacity(4096, stderr);
    let mut chunk = [0u8; 256];
    let mut line = Vec::with_capacity(256);

    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            // EOF — flush any pending line that didn't end with a delim.
            if !line.is_empty() {
                process_line(&line, progress, &tail, bin_name);
            }
            return Ok(());
        }
        for &b in &chunk[..n] {
            if b == b'\r' || b == b'\n' {
                if !line.is_empty() {
                    process_line(&line, progress, &tail, bin_name);
                    line.clear();
                }
            } else {
                line.push(b);
            }
        }
    }
}

fn process_line(bytes: &[u8], progress: Option<&ProgressCb>, tail: &Mutex<String>, bin_name: &str) {
    let line = String::from_utf8_lossy(bytes);
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }
    if let Some((done_kb, total_kb)) = parse_kb_fraction(trimmed) {
        if let Some(cb) = progress {
            cb(done_kb * 1024, total_kb * 1024);
        }
    } else {
        debug!(bin = %bin_name, line = %trimmed);
        append_tail(&mut tail.lock().unwrap(), trimmed);
    }
}

/// Parse `"55564/1261024 kB ..."`-shaped progress lines (works for both
/// `pg_basebackup` and `pg_rewind`). Returns kB values; the caller
/// multiplies by 1024 for the bytes-going-on-the-wire that
/// `OpProgress` carries.
fn parse_kb_fraction(s: &str) -> Option<(i64, i64)> {
    let idx = s.find(" kB").or_else(|| s.find(" KB"))?;
    let frac = s[..idx].trim();
    let (a, b) = frac.split_once('/')?;
    let done: i64 = a.trim().parse().ok()?;
    let total: i64 = b.trim().parse().ok()?;
    Some((done, total))
}

/// Append `line` to `tail` if doing so wouldn't push past
/// `STDERR_TAIL_MAX`. Keeps the *first* output (typical FATAL is at the
/// top); later overflow lines are silently dropped — matching the Go
/// impl. Useful for stuffing into error messages.
fn append_tail(tail: &mut String, line: &str) {
    if tail.len() >= STDERR_TAIL_MAX {
        return;
    }
    // +1 for the join newline.
    let extra = line.len() + if tail.is_empty() { 0 } else { 1 };
    if tail.len() + extra > STDERR_TAIL_MAX {
        return;
    }
    if !tail.is_empty() {
        tail.push('\n');
    }
    tail.push_str(line);
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

/// Remove every entry directly under `pg_data_dir`. The directory
/// itself is kept. Caller MUST have verified PostgreSQL is not running.
async fn clear_pgdata_contents(pg_data_dir: &Path) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(pg_data_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let ft = entry.file_type().await?;
        if ft.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    Ok(())
}

/// Remove every entry under `pg_data_dir/pg_replslot/`. A missing
/// directory is OK (nothing to clean).
async fn clean_replslots(pg_data_dir: &Path) -> std::io::Result<()> {
    let dir = pg_data_dir.join("pg_replslot");
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    while let Some(entry) = entries.next_entry().await? {
        debug!(path = %entry.path().display(), "rewind: removing stale slot dir");
        let ft = entry.file_type().await?;
        if ft.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?;
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

/// Atomic temp+rename write into `path` with the given mode. Removes
/// the temp on any failure path.
async fn atomic_write(path: &Path, contents: &[u8], mode: u32) -> std::io::Result<()> {
    // tokio::fs::OpenOptions has its own `mode` method on Unix — no
    // need to bring in `std::os::unix::fs::OpenOptionsExt`.
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other(format!("no parent for {}", path.display())))?;
    let base = path
        .file_name()
        .ok_or_else(|| std::io::Error::other(format!("no basename for {}", path.display())))?
        .to_string_lossy()
        .into_owned();
    // Hidden temp so a leaked tmp (after a hard kill) is invisible to
    // operators glancing at $PGDATA. PID disambiguates between concurrent
    // writers in the unlikely case of two agents racing.
    let tmp = dir.join(format!(".{base}-{}.tmp", std::process::id()));

    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(&tmp)
        .await?;
    f.write_all(contents).await?;
    f.flush().await?;
    drop(f);

    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery conf renderer
// ---------------------------------------------------------------------------

/// Render the contents of `$PGDATA/myrecovery.conf`. The conninfo is
/// quoted with single quotes; this function refuses to render if the
/// conninfo contains `'`, `\r`, or `\n` — those would escape the
/// quoting. Inputs are validated upstream (regex on host/user/slot,
/// path regex on TLS cert paths) so reaching the refusal branch
/// indicates a bug in the conninfo helper, not bad user input.
fn render_recovery_conf(conninfo: &str, slot_name: &str) -> anyhow::Result<String> {
    if conninfo.is_empty() {
        anyhow::bail!("conninfo is required");
    }
    if conninfo.contains('\'') || conninfo.contains('\r') || conninfo.contains('\n') {
        anyhow::bail!("conninfo contains forbidden characters");
    }
    if slot_name.is_empty() {
        anyhow::bail!("slot_name is required");
    }
    if !allowed_slot_name().is_match(slot_name) {
        anyhow::bail!("slot_name contains invalid characters");
    }
    Ok(format!(
        "# managed by pg_agent\n\
         primary_conninfo = '{conninfo}'\n\
         primary_slot_name = '{slot_name}'\n\
         restore_command = 'pg_agentc restore-wal %f %p'\n"
    ))
}

// ---------------------------------------------------------------------------
// Validation regexes (shared with localdb / proto-handler validators)
// ---------------------------------------------------------------------------

/// Host can be DNS name, IPv4, or IPv6 literal (without brackets); kept
/// strict to prevent conninfo / template injection.
fn allowed_primary_host() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[A-Za-z0-9._:-]+$").unwrap())
}

/// Conservative ASCII identifier for PostgreSQL roles.
fn allowed_repl_user() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[A-Za-z0-9_.-]+$").unwrap())
}

/// Replication slot names use the same identifier alphabet.
fn allowed_slot_name() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[A-Za-z0-9_.-]+$").unwrap())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ----- parse_kb_fraction ------------------------------------------------

    #[test]
    fn parse_kb_fraction_basebackup_shape() {
        assert_eq!(
            parse_kb_fraction("55564/1261024 kB (4%), 1/1 tablespace"),
            Some((55564, 1261024))
        );
    }

    #[test]
    fn parse_kb_fraction_rewind_shape() {
        assert_eq!(
            parse_kb_fraction("55564/1261024 kB (4%) copied"),
            Some((55564, 1261024))
        );
    }

    #[test]
    fn parse_kb_fraction_uppercase_kb_unit() {
        assert_eq!(parse_kb_fraction("1/2 KB"), Some((1, 2)));
    }

    #[test]
    fn parse_kb_fraction_rejects_garbage() {
        for line in [
            "",
            "no kB here",
            "garbage/text kB ok",
            "1024 kB", // missing slash
            "FATAL: out of disk",
            "WARNING: pg_basebackup: aborted",
        ] {
            assert_eq!(parse_kb_fraction(line), None, "expected None for {line:?}");
        }
    }

    // ----- append_tail ------------------------------------------------------

    #[test]
    fn append_tail_joins_with_newline() {
        let mut t = String::new();
        append_tail(&mut t, "first");
        append_tail(&mut t, "second");
        assert_eq!(t, "first\nsecond");
    }

    #[test]
    fn append_tail_drops_overflow_lines() {
        let mut t = String::new();
        // Pad close to STDERR_TAIL_MAX with a single big line.
        let big = "x".repeat(STDERR_TAIL_MAX - 10);
        append_tail(&mut t, &big);
        let before = t.clone();
        // The following won't fit (would need 10+ chars including
        // the newline separator); drop it silently.
        append_tail(&mut t, "this line will not fit");
        assert_eq!(t, before, "overflow line should have been dropped");
    }

    // ----- drain_stderr -----------------------------------------------------

    type CapturedCalls = Arc<std::sync::Mutex<Vec<(i64, i64)>>>;

    fn capture_progress() -> (ProgressCb, CapturedCalls) {
        let calls: CapturedCalls = Arc::new(std::sync::Mutex::new(vec![]));
        let cb_calls = calls.clone();
        let cb: ProgressCb = Box::new(move |d, t| {
            cb_calls.lock().unwrap().push((d, t));
        });
        (cb, calls)
    }

    #[tokio::test]
    async fn drain_stderr_dispatches_progress_and_captures_tail() {
        // Two \r-delimited progress updates, then \n-delimited diagnostic
        // lines that should be captured into the tail. Mixed delimiters
        // is the realistic shape pg_basebackup produces.
        let bytes: &[u8] = b"55564/1261024 kB (4%), 1/1 tablespace\
            \r110000/1261024 kB (8%), 1/1 tablespace\
            \rFATAL: could not connect to server\n\
            pg_basebackup: error: aborted\n";

        let (cb, calls) = capture_progress();
        let tail = Arc::new(Mutex::new(String::new()));
        drain_stderr(bytes, Some(&cb), tail.clone(), "pg_basebackup")
            .await
            .unwrap();

        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                (55564 * 1024, 1261024 * 1024),
                (110000 * 1024, 1261024 * 1024),
            ]
        );
        let t = tail.lock().unwrap();
        assert!(t.contains("FATAL: could not connect to server"));
        assert!(t.contains("pg_basebackup: error: aborted"));
    }

    #[tokio::test]
    async fn drain_stderr_flushes_partial_final_line() {
        // EOF without trailing delimiter — partial line should still be
        // captured into the tail (rare but real for crashed subprocesses).
        let bytes: &[u8] = b"FATAL: incomplete";
        let tail = Arc::new(Mutex::new(String::new()));
        drain_stderr(bytes, None, tail.clone(), "pg_rewind")
            .await
            .unwrap();
        assert_eq!(tail.lock().unwrap().as_str(), "FATAL: incomplete");
    }

    #[tokio::test]
    async fn drain_stderr_handles_no_progress_callback() {
        // Progress lines without a callback should NOT land in the tail
        // (they're not diagnostic info) — they're silently consumed.
        let bytes: &[u8] = b"1/2 kB\nFATAL: x\n";
        let tail = Arc::new(Mutex::new(String::new()));
        drain_stderr(bytes, None, tail.clone(), "pg_basebackup")
            .await
            .unwrap();
        let t = tail.lock().unwrap();
        assert!(!t.contains("1/2 kB"));
        assert!(t.contains("FATAL: x"));
    }

    // ----- render_recovery_conf --------------------------------------------

    #[test]
    fn render_recovery_conf_no_tls_matches_template() {
        let got = render_recovery_conf("host=server1 port=5432 user=repl", "node0").unwrap();
        assert_eq!(
            got,
            "# managed by pg_agent\n\
             primary_conninfo = 'host=server1 port=5432 user=repl'\n\
             primary_slot_name = 'node0'\n\
             restore_command = 'pg_agentc restore-wal %f %p'\n"
        );
    }

    #[test]
    fn render_recovery_conf_rejects_quoting_break_chars() {
        // Each of these would break the single-quote wrapping if rendered.
        for bad in [
            "host=server'1 port=5432 user=repl",
            "host=server\n port=5432",
            "host=server\rport=5432",
        ] {
            let err = render_recovery_conf(bad, "node0").unwrap_err().to_string();
            assert!(err.contains("forbidden characters"), "got {err:?}");
        }
    }

    #[test]
    fn render_recovery_conf_rejects_empty_inputs() {
        assert!(render_recovery_conf("", "node0").is_err());
        assert!(render_recovery_conf("host=x", "").is_err());
    }

    #[test]
    fn render_recovery_conf_rejects_bad_slot_name() {
        assert!(render_recovery_conf("host=x", "bad slot name").is_err());
        assert!(render_recovery_conf("host=x", "drop;").is_err());
    }

    // ----- WriteRecoveryConfOpts::validate ---------------------------------

    fn valid_opts() -> WriteRecoveryConfOpts {
        WriteRecoveryConfOpts {
            primary_host: "server1".into(),
            primary_port: 5432,
            repl_user: "repl".into(),
            slot_name: "node0".into(),
        }
    }

    #[test]
    fn validate_accepts_well_formed_input() {
        valid_opts().validate().unwrap();
    }

    #[test]
    fn validate_rejects_each_bad_field() {
        type Mutator = fn(&mut WriteRecoveryConfOpts);
        let cases: &[(&str, Mutator)] = &[
            ("primary_host", |o| o.primary_host.clear()),
            ("primary_host", |o| o.primary_host = "bad host".into()), // space rejected
            ("primary_port", |o| o.primary_port = 0),
            ("slot_name", |o| o.slot_name.clear()),
            ("slot_name", |o| o.slot_name = "drop;".into()),
            ("repl_user", |o| o.repl_user.clear()),
            ("repl_user", |o| o.repl_user = "with space".into()),
        ];
        for (label, mutate) in cases {
            let mut o = valid_opts();
            mutate(&mut o);
            assert!(
                o.validate().is_err(),
                "expected {label:?} mutation to fail validation"
            );
        }
    }

    // ----- clear_pgdata_contents -------------------------------------------

    #[tokio::test]
    async fn clear_pgdata_contents_empties_dir_but_keeps_root() {
        let tmp = TempDir::new().unwrap();
        let pg = tmp.path().join("pg");
        std::fs::create_dir(&pg).unwrap();
        std::fs::write(pg.join("file.txt"), b"hello").unwrap();
        std::fs::create_dir(pg.join("subdir")).unwrap();
        std::fs::write(pg.join("subdir/nested.txt"), b"world").unwrap();

        clear_pgdata_contents(&pg).await.unwrap();

        assert!(pg.exists(), "pgdata directory itself must remain");
        let remaining: Vec<_> = std::fs::read_dir(&pg)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(
            remaining.is_empty(),
            "pgdata contents should be gone: {remaining:?}"
        );
    }

    // ----- clean_replslots --------------------------------------------------

    #[tokio::test]
    async fn clean_replslots_tolerates_missing_dir() {
        let tmp = TempDir::new().unwrap();
        let pg = tmp.path().join("pg");
        std::fs::create_dir(&pg).unwrap();
        // pg_replslot doesn't exist — must not error.
        clean_replslots(&pg).await.unwrap();
    }

    #[tokio::test]
    async fn clean_replslots_removes_contents_not_dir() {
        let tmp = TempDir::new().unwrap();
        let pg = tmp.path().join("pg");
        let slot_dir = pg.join("pg_replslot");
        std::fs::create_dir_all(&slot_dir).unwrap();
        std::fs::create_dir(slot_dir.join("node1")).unwrap();
        std::fs::write(slot_dir.join("node1").join("state"), b"junk").unwrap();
        std::fs::write(slot_dir.join("loose.txt"), b"junk").unwrap();

        clean_replslots(&pg).await.unwrap();

        assert!(
            slot_dir.exists(),
            "pg_replslot directory itself must remain"
        );
        let remaining: Vec<_> = std::fs::read_dir(&slot_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(
            remaining.is_empty(),
            "pg_replslot should be empty: {remaining:?}"
        );
    }

    // ----- atomic_write -----------------------------------------------------

    #[tokio::test]
    async fn atomic_write_writes_with_mode_and_no_temp_left() {
        let tmp = TempDir::new().unwrap();
        let dest = tmp.path().join("myrecovery.conf");
        atomic_write(&dest, b"hello", 0o640).await.unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");

        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o640);

        // No leftover hidden temp.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| {
                let s = n.to_string_lossy();
                s.starts_with(".myrecovery.conf-")
            })
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp file should remain, found {leftovers:?}"
        );
    }

    #[tokio::test]
    async fn atomic_write_overwrites_existing() {
        let tmp = TempDir::new().unwrap();
        let dest = tmp.path().join("file");
        atomic_write(&dest, b"v1", 0o640).await.unwrap();
        atomic_write(&dest, b"v2", 0o640).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"v2");
    }
}
