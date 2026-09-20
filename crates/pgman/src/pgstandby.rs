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
//! its run. Before: stale slot dirs from this
//! node's pre-rewind role would otherwise survive (rewind copies only
//! changed blocks). After: rewind may have copied slot dirs from the
//! source's role that would crash PG recovery if left in place.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Replication connection config
// ---------------------------------------------------------------------------

/// `sslmode` values libpq recognises.
/// <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>
const ALLOWED_SSLMODES: &[&str] = &[
    "disable",
    "allow",
    "prefer",
    "require",
    "verify-ca",
    "verify-full",
];

/// Default replication `sslmode`. libpq itself defaults to `prefer`, which
/// does NOT verify the server cert — insecure. An explicit `sslmode=` is
/// always emitted in conninfo so the operator either gets verify-full
/// (secure-by-default) or has consciously chosen something else.
pub const DEFAULT_REPL_SSLMODE: &str = "verify-full";

#[derive(Debug, thiserror::Error)]
#[error(
    "replication sslmode must be one of disable, allow, prefer, require, verify-ca, verify-full"
)]
pub struct InvalidSslMode;

/// Connection knobs for the replication conninfo string (basebackup,
/// rewind, `myrecovery.conf`'s `primary_conninfo`). This crate does NOT
/// own the TLS material itself: libpq looks up cert paths from its own
/// defaults (`~postgres/.postgresql/{postgresql.crt,postgresql.key,root.crt}`)
/// or from `PGSSLCERT` / `PGSSLKEY` / `PGSSLROOTCERT` env vars on the
/// server unit. Only the `sslmode` is chosen here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PgReplicationConfig {
    #[serde(default)]
    pub sslmode: Option<String>,
}

impl PgReplicationConfig {
    /// Reject any `sslmode` libpq wouldn't recognise. Other validations
    /// belong elsewhere — there are no cert paths in this struct.
    pub fn validate(&self) -> Result<(), InvalidSslMode> {
        if let Some(mode) = self.sslmode.as_deref() {
            if !ALLOWED_SSLMODES.contains(&mode) {
                return Err(InvalidSslMode);
            }
        }
        Ok(())
    }

    /// `sslmode` value to write into `primary_conninfo`. Never empty —
    /// falls back to [`DEFAULT_REPL_SSLMODE`] when the operator hasn't
    /// set one.
    pub fn effective_sslmode(&self) -> &str {
        self.sslmode.as_deref().unwrap_or(DEFAULT_REPL_SSLMODE)
    }

    /// Build a libpq conninfo string. `dbname` is included only when
    /// non-empty (`pg_basebackup` + `primary_conninfo` speak the
    /// replication protocol and don't take a dbname; pass `"postgres"`
    /// for `pg_rewind`, which needs a regular DB connection).
    ///
    /// Inputs are trusted — the caller validates host/port/user at its
    /// wire boundary. The conninfo carries `sslmode=` only;
    /// `sslcert`/`sslkey`/`sslrootcert` are picked up from libpq
    /// defaults (`~postgres/.postgresql/…`) or env vars.
    pub fn conninfo(&self, host: &str, port: u16, user: &str, dbname: &str) -> String {
        use std::fmt::Write as _;
        let mut s = format!("host={host} port={port} user={user}");
        if !dbname.is_empty() {
            write!(s, " dbname={dbname}").unwrap();
        }
        write!(s, " sslmode={}", self.effective_sslmode()).unwrap();
        s
    }
}

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

impl BasebackupOpts {
    /// Input validation that the gRPC `Basebackup` handler runs before
    /// dispatching. Same connection-param rules as
    /// [`WriteRecoveryConfOpts`] plus slot_name.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_connection(&self.primary_host, self.primary_port, &self.repl_user)?;
        if self.slot_name.is_empty() {
            anyhow::bail!("slot_name is required");
        }
        if !allowed_slot_name().is_match(&self.slot_name) {
            anyhow::bail!("slot_name contains invalid characters");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RewindOpts {
    pub primary_host: String,
    pub primary_port: u16,
    pub repl_user: String,
}

impl RewindOpts {
    /// Input validation that the gRPC `Rewind` handler runs before
    /// dispatching. No slot_name (rewind talks libpq, not a slot).
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_connection(&self.primary_host, self.primary_port, &self.repl_user)
    }
}

/// Shared host/port/user check used by basebackup, rewind, and recovery-conf
/// validation. Strict alphabets defeat conninfo injection — a host like
/// `"primary host=attacker"` would otherwise add a second `host=` key=value
/// pair via libpq's whitespace tokeniser and redirect to an attacker host.
fn validate_connection(host: &str, port: u16, user: &str) -> anyhow::Result<()> {
    if host.is_empty() {
        anyhow::bail!("primary_host is required");
    }
    if !allowed_primary_host().is_match(host) {
        anyhow::bail!("primary_host contains invalid characters");
    }
    if port == 0 {
        anyhow::bail!("primary_port must be greater than zero");
    }
    if user.is_empty() {
        anyhow::bail!("repl_user is required");
    }
    if !allowed_repl_user().is_match(user) {
        anyhow::bail!("repl_user contains invalid characters");
    }
    Ok(())
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
        validate_connection(&self.primary_host, self.primary_port, &self.repl_user)?;
        if self.slot_name.is_empty() {
            anyhow::bail!("slot_name is required");
        }
        if !allowed_slot_name().is_match(&self.slot_name) {
            anyhow::bail!("slot_name contains invalid characters");
        }
        Ok(())
    }
}

#[async_trait]
pub trait StandbyOps: Send + Sync {
    /// Clears `$PGDATA` contents first, then exec's `pg_basebackup`,
    /// then recreates the implementation's configured symlinks under
    /// `$PGDATA` (see [`StandbyExec::restore_symlinks`]). Per upstream
    /// PostgreSQL docs, `pg_basebackup` silently skips non-tablespace
    /// symlinks — without the repair tail, whatever the deployment
    /// keeps as symlinks inside the data directory would silently
    /// vanish on every rebuild.
    ///
    /// The repair lives here (not in a separate step) because the wipe
    /// and repair are tightly coupled: they're the same code path on
    /// the same node. `rewind` modifies `$PGDATA` in place and doesn't
    /// affect symlinks; it doesn't need to repair them.
    ///
    /// **Caller must have verified PostgreSQL isn't running on this
    /// node** — otherwise we'd wipe a live datadir.
    async fn basebackup(
        &self,
        opts: BasebackupOpts,
        progress: Option<ProgressCb>,
    ) -> anyhow::Result<()>;

    /// Clears `pg_replslot/*` before *and* after — before, stale slot
    /// dirs from this node's pre-rewind role survive the block-level
    /// copy; after, dirs rewind copied from the source's role would
    /// crash PostgreSQL recovery.
    /// Does NOT touch symlinks — rewind modifies pgdata in place, and
    /// symlinks (being outside `pg_replslot/`) survive untouched.
    async fn rewind(&self, opts: RewindOpts, progress: Option<ProgressCb>) -> anyhow::Result<()>;

    /// Writes `$PGDATA/myrecovery.conf` + creates `$PGDATA/standby.signal`.
    async fn write_recovery_conf(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()>;

    /// Overwrites `$PGDATA/myrecovery.conf` with a conninfo-less stub:
    /// after a reload the walreceiver stops and STAYS stopped — the
    /// candidacy freeze (finding 23). `standby.signal` is untouched
    /// (still a standby, just detached); the next follow/recover
    /// rewrites the file.
    async fn detach_recovery_conf(&self) -> anyhow::Result<()>;

    /// What `$PGDATA/global/pg_control` records — timeline and latest
    /// checkpoint — readable with PostgreSQL stopped.
    ///
    /// Belongs with the other data-directory operations for the reason
    /// they are grouped at all: it reads `$PGDATA` directly instead of
    /// asking a running server. That is the whole point — a node whose
    /// PostgreSQL is down still *has* a timeline and a checkpoint, and
    /// callers comparing lineage across the pool need both exactly
    /// when the live probe cannot answer.
    ///
    /// Defaults to unknown so test doubles need not implement it;
    /// [`StandbyExec`] overrides it.
    async fn control_point(&self) -> anyhow::Result<crate::timeline::ControlPoint> {
        Ok(crate::timeline::ControlPoint::UNKNOWN)
    }

    /// Just the timeline from [`control_point`](Self::control_point).
    /// `0` means unknown. Provided, not implemented — the control file
    /// is one read, and splitting it would cost two subprocesses for
    /// callers that want both fields.
    async fn control_timeline(&self) -> anyhow::Result<i32> {
        Ok(self.control_point().await?.timeline_id)
    }
}

// ---------------------------------------------------------------------------
// StandbyExec — production impl
// ---------------------------------------------------------------------------

/// Production [`StandbyOps`] — drives `pg_basebackup` / `pg_rewind` as
/// subprocesses and writes recovery config directly into `pg_data_dir`.
/// Construction is cheap; clone-via-Arc at the daemon's composition root.
pub struct StandbyExec {
    pub pg_install_prefix: PathBuf,
    pub pg_data_dir: PathBuf,
    pub replication: PgReplicationConfig,
    /// Symlinks to recreate under `$PGDATA` after every basebackup, as
    /// `(name relative to $PGDATA, target)`. `pg_basebackup` silently
    /// skips every non-tablespace symlink, so anything the deployment
    /// expects inside the data directory has to be restored by whoever
    /// wiped it — and wipe + restore are one code path on one node,
    /// which is why this is instance-level configuration rather than a
    /// separate orchestration step. What the links *mean* is the
    /// caller's business; the agent passes its pgpool hook symlinks
    /// here without this crate knowing what pgpool is.
    pub restore_symlinks: Vec<(String, PathBuf)>,
    /// `restore_command` line for the generated recovery config, or
    /// `None` to omit it (streaming-only standby). Caller-provided for
    /// the same reason as [`restore_symlinks`](Self::restore_symlinks):
    /// which command fetches archived WAL is the deployment's knowledge
    /// — the agent passes its own wrapper here without this crate
    /// knowing the binary exists.
    pub restore_command: Option<String>,
}

impl StandbyExec {
    pub fn new(
        pg_install_prefix: PathBuf,
        pg_data_dir: PathBuf,
        replication: PgReplicationConfig,
        restore_symlinks: Vec<(String, PathBuf)>,
        restore_command: Option<String>,
    ) -> Self {
        Self {
            pg_install_prefix,
            pg_data_dir,
            replication,
            restore_symlinks,
            restore_command,
        }
    }

    fn basebackup_bin(&self) -> PathBuf {
        self.pg_install_prefix.join("bin").join("pg_basebackup")
    }

    fn rewind_bin(&self) -> PathBuf {
        self.pg_install_prefix.join("bin").join("pg_rewind")
    }

    fn controldata_bin(&self) -> PathBuf {
        self.pg_install_prefix.join("bin").join("pg_controldata")
    }
}

/// Pull the timeline and latest checkpoint location out of
/// `pg_controldata` output.
///
/// Parsed rather than read from `global/pg_control` directly because the
/// control file is a versioned binary struct; `pg_controldata` is the
/// supported reader and ships in the same directory as the other
/// binaries this module already depends on.
///
/// Each field falls back to `0` (unknown) independently when absent or
/// unparseable — a different PostgreSQL major, a localized build, a
/// truncated read. Unknown, never a guess: callers compare these
/// across nodes and against switchpoints, and a fabricated value would
/// be worse than no answer.
fn parse_control_point(out: &str) -> crate::timeline::ControlPoint {
    let field = |label: &str| {
        out.lines()
            .find_map(|line| line.strip_prefix(label))
            .map(str::trim)
    };
    crate::timeline::ControlPoint {
        timeline_id: field("Latest checkpoint's TimeLineID:")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0),
        checkpoint_lsn: field("Latest checkpoint location:")
            .and_then(crate::timeline::parse_lsn)
            .unwrap_or(0),
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
        let conninfo =
            self.replication
                .conninfo(&opts.primary_host, opts.primary_port, &opts.repl_user, "");

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

        // Post-basebackup pgdata repair. pg_basebackup silently skipped
        // every non-tablespace symlink — recreate the configured ones so
        // the rebuilt standby carries what the deployment expects inside
        // $PGDATA. Plain creation, no repair rules: the wipe above
        // emptied the directory and the clone could not have copied a
        // symlink, so the name is known-absent. Failure is fatal — the
        // caller told us the standby is not complete without these.
        for (name, target) in &self.restore_symlinks {
            let link = self.pg_data_dir.join(name);
            std::os::unix::fs::symlink(target, &link).map_err(|e| {
                anyhow::anyhow!(
                    "basebackup: restore symlink {} -> {}: {e}",
                    link.display(),
                    target.display()
                )
            })?;
        }

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
        let conninfo = self.replication.conninfo(
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

        let conninfo =
            self.replication
                .conninfo(&opts.primary_host, opts.primary_port, &opts.repl_user, "");

        let content =
            render_recovery_conf(&conninfo, &opts.slot_name, self.restore_command.as_deref())?;

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

    async fn detach_recovery_conf(&self) -> anyhow::Result<()> {
        let content = "# pg-agent: DETACHED — no primary_conninfo. Written by the\n\
                       # candidacy freeze (finding 23): a candidate must stop receiving\n\
                       # so flush positions are frozen before they are compared. The\n\
                       # next follow/recover rewrites this file.\n";
        atomic_write(
            &self.pg_data_dir.join("myrecovery.conf"),
            content.as_bytes(),
            0o640,
        )
        .await
        .map_err(|e| anyhow::anyhow!("detach_recovery_conf: write myrecovery.conf: {e}"))?;
        info!(datadir = %self.pg_data_dir.display(), "detach_recovery_conf: completed");
        Ok(())
    }

    async fn control_point(&self) -> anyhow::Result<crate::timeline::ControlPoint> {
        let bin = self.controldata_bin();
        let out = Command::new(&bin)
            .arg("-D")
            .arg(&self.pg_data_dir)
            // pg_controldata localizes its field labels. Pin the locale
            // so the parse above matches on any host.
            .env("LC_ALL", "C")
            .env("LANG", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("control_point: spawn {}: {e}", bin.display()))?;
        if !out.status.success() {
            return Err(anyhow::anyhow!(
                "control_point: {} exited {}; stderr: {}",
                bin.display(),
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(parse_control_point(&String::from_utf8_lossy(&out.stdout)))
    }
}

// ---------------------------------------------------------------------------
// Subprocess driver
// ---------------------------------------------------------------------------

/// Kill the subprocess if stderr goes completely silent for this long.
/// Both tools run with `--progress` in production (the peer-RPC callers
/// always pass a progress callback), so the copy phases chatter
/// continuously and *any* stderr byte counts as liveness — this is a
/// hung-transfer detector, not a duration cap. Five minutes of total
/// silence comfortably clears the quiet phases that legitimately exist
/// (fast-checkpoint wait at basebackup start, rewind's source scan)
/// while still catching a dead-peer TCP connection with no keepalive.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// How often the stall watchdog samples `last_activity`.
const STALL_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Spawn `bin`, drain its stderr concurrently, and surface a useful
/// error message on non-zero exit. `kill_on_drop(true)` + `join!` means
/// a cancelled outer future SIGKILLs the subprocess in the same drop —
/// which is also how the stall watchdog kills a silent transfer (drop
/// the joined future, not a signal race).
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
                 package or check that pg_install_prefix={} is correct)",
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
    let last_activity: Arc<Mutex<std::time::Instant>> =
        Arc::new(Mutex::new(std::time::Instant::now()));

    // tokio::join! runs both concurrently in the SAME task. Dropping the
    // outer future drops both, which kills the subprocess (kill_on_drop)
    // and abandons the drain. tokio::spawn would have detached the
    // drain task — that's the wrong behaviour for cancellation.
    let drain = drain_stderr(
        stderr,
        progress.as_ref(),
        tail.clone(),
        &bin_name,
        last_activity.clone(),
    );
    let wait = child.wait();
    let run = async { tokio::join!(drain, wait) };
    tokio::pin!(run);

    let (drain_result, wait_result) = loop {
        tokio::select! {
            results = &mut run => break results,
            _ = tokio::time::sleep(STALL_CHECK_INTERVAL) => {
                let silent_for = last_activity.lock().unwrap().elapsed();
                if silent_for >= STALL_TIMEOUT {
                    // Dropping `run` SIGKILLs the child (kill_on_drop)
                    // and abandons the drain in the same drop.
                    let tail_str = tail.lock().unwrap().clone();
                    let suffix = if tail_str.is_empty() {
                        String::new()
                    } else {
                        format!("; last output: {tail_str}")
                    };
                    anyhow::bail!(
                        "{bin_name}: no output for {}s — presumed hung; killed{suffix}",
                        silent_for.as_secs()
                    );
                }
            }
        }
    };

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
/// Every successful read stamps `last_activity` — the stall watchdog's
/// liveness signal.
async fn drain_stderr<R: tokio::io::AsyncRead + Unpin>(
    stderr: R,
    progress: Option<&ProgressCb>,
    tail: Arc<Mutex<String>>,
    bin_name: &str,
    last_activity: Arc<Mutex<std::time::Instant>>,
) -> std::io::Result<()> {
    let mut reader = BufReader::with_capacity(4096, stderr);
    let mut chunk = [0u8; 256];
    let mut line = Vec::with_capacity(256);

    loop {
        let n = reader.read(&mut chunk).await?;
        *last_activity.lock().unwrap() = std::time::Instant::now();
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
    // NotFound during the walk is tolerated everywhere: deletion is the
    // goal, so an entry vanishing between readdir and unlink means the
    // work is already done. (Defense in depth for anything else
    // deleting concurrently — the settling guard in the peer server
    // keeps the known case, a mid-shutdown postmaster, out entirely.)
    fn ignore_missing(r: std::io::Result<()>) -> std::io::Result<()> {
        match r {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }
    let mut entries = tokio::fs::read_dir(pg_data_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let ft = match entry.file_type().await {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            other => other?,
        };
        if ft.is_dir() {
            ignore_missing(tokio::fs::remove_dir_all(&path).await)?;
        } else {
            ignore_missing(tokio::fs::remove_file(&path).await)?;
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
fn render_recovery_conf(
    conninfo: &str,
    slot_name: &str,
    restore_command: Option<&str>,
) -> anyhow::Result<String> {
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
    // application_name = the slot name = this node's one identity
    // (`node{id}`). It is what the primary's
    // `synchronous_standby_names = ANY 1 (...)` will match walsenders
    // by (docs/quorum-commit.md §6) — without it, quorum commit has
    // nothing to name. Already validated by the slot-name regex above.
    let mut out = format!(
        "# managed by pgman\n\
         primary_conninfo = '{conninfo} application_name={slot_name}'\n\
         primary_slot_name = '{slot_name}'\n"
    );
    if let Some(cmd) = restore_command {
        // Same quoting rules as the conninfo: the value lands inside
        // single quotes, so anything that could escape them is refused
        // even though the source is deployment config, not user input.
        if cmd.contains('\'') || cmd.contains('\r') || cmd.contains('\n') {
            anyhow::bail!("restore_command contains forbidden characters");
        }
        out.push_str(&format!("restore_command = '{cmd}'\n"));
    }
    Ok(out)
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
pub fn allowed_slot_name() -> &'static regex::Regex {
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

    /// Fresh `last_activity` stamp for drain tests — the stall watchdog
    /// isn't under test here, the drain just requires the argument.
    fn test_activity() -> Arc<Mutex<std::time::Instant>> {
        Arc::new(Mutex::new(std::time::Instant::now()))
    }

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
        drain_stderr(
            bytes,
            Some(&cb),
            tail.clone(),
            "pg_basebackup",
            test_activity(),
        )
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
        drain_stderr(bytes, None, tail.clone(), "pg_rewind", test_activity())
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
        drain_stderr(bytes, None, tail.clone(), "pg_basebackup", test_activity())
            .await
            .unwrap();
        let t = tail.lock().unwrap();
        assert!(!t.contains("1/2 kB"));
        assert!(t.contains("FATAL: x"));
    }

    // ----- render_recovery_conf --------------------------------------------

    #[test]
    fn render_recovery_conf_no_tls_matches_template() {
        let got = render_recovery_conf(
            "host=server1 port=5432 user=repl",
            "node0",
            Some("restore-wrapper %f %p"),
        )
        .unwrap();
        assert_eq!(
            got,
            "# managed by pgman\n\
             primary_conninfo = 'host=server1 port=5432 user=repl application_name=node0'\n\
             primary_slot_name = 'node0'\n\
             restore_command = 'restore-wrapper %f %p'\n"
        );
    }

    #[test]
    fn render_recovery_conf_omits_restore_command_when_unset() {
        let got = render_recovery_conf("host=x port=5432 user=r", "node0", None).unwrap();
        assert!(
            !got.contains("restore_command"),
            "no restore_command line without one configured: {got}"
        );
    }

    #[test]
    fn render_recovery_conf_rejects_quote_breaking_restore_command() {
        for bad in ["cmd '; rm -rf /", "cmd\nx", "cmd\rx"] {
            assert!(
                render_recovery_conf("host=x", "node0", Some(bad)).is_err(),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn render_recovery_conf_rejects_quoting_break_chars() {
        // Each of these would break the single-quote wrapping if rendered.
        for bad in [
            "host=server'1 port=5432 user=repl",
            "host=server\n port=5432",
            "host=server\rport=5432",
        ] {
            let err = render_recovery_conf(bad, "node0", None)
                .unwrap_err()
                .to_string();
            assert!(err.contains("forbidden characters"), "got {err:?}");
        }
    }

    #[test]
    fn render_recovery_conf_rejects_empty_inputs() {
        assert!(render_recovery_conf("", "node0", None).is_err());
        assert!(render_recovery_conf("host=x", "", None).is_err());
    }

    #[test]
    fn render_recovery_conf_rejects_bad_slot_name() {
        assert!(render_recovery_conf("host=x", "bad slot name", None).is_err());
        assert!(render_recovery_conf("host=x", "drop;", None).is_err());
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

    // ----- basebackup post-step: hook symlink repair --------------------

    /// Write a shell stub at `<pg_install_prefix>/bin/pg_basebackup` that exits 0
    /// without actually replicating. Lets us drive `StandbyExec::basebackup`
    /// end-to-end and observe the post-step (hook-symlink repair) without
    /// a real PostgreSQL source.
    fn install_basebackup_stub(pg_install_prefix: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let bin_dir = pg_install_prefix.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let stub = bin_dir.join("pg_basebackup");
        // The real pg_basebackup writes into --pgdata; ours just exits 0.
        std::fs::write(&stub, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&stub).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&stub, perms).unwrap();
        stub
    }

    #[tokio::test]
    async fn basebackup_repairs_hook_symlinks_on_success() {
        let tmp = TempDir::new().unwrap();
        let pg_install_prefix = tmp.path().join("pg_install_prefix");
        install_basebackup_stub(&pg_install_prefix);

        let pgdata = tmp.path().join("pgdata");
        std::fs::create_dir_all(&pgdata).unwrap();

        // Fake hook binary. The repair tail creates the configured
        // symlinks under pgdata pointing at it.
        let hook_bin = tmp.path().join("hook-target");
        std::fs::write(&hook_bin, "#!/bin/sh\nexit 0\n").unwrap();

        let exec = StandbyExec::new(
            pg_install_prefix,
            pgdata.clone(),
            PgReplicationConfig::default(),
            vec![
                ("hook_a".to_string(), hook_bin.clone()),
                ("hook_b".to_string(), hook_bin.clone()),
            ],
            None,
        );

        exec.basebackup(
            BasebackupOpts {
                primary_host: "127.0.0.1".into(),
                primary_port: 5432,
                repl_user: "repl".into(),
                slot_name: "node1".into(),
            },
            None,
        )
        .await
        .expect("stub basebackup should succeed");

        // Both configured symlinks must now exist under pgdata,
        // pointing at the target — the whole restore contract.
        for name in ["hook_a", "hook_b"] {
            let target = std::fs::read_link(pgdata.join(name))
                .unwrap_or_else(|e| panic!("symlink {name} missing: {e}"));
            assert_eq!(target, hook_bin);
        }
    }
}

#[cfg(test)]
mod controldata_tests {
    use super::parse_control_point;
    use crate::timeline::ControlPoint;

    /// Real `pg_controldata` output, trimmed to the neighbourhood of
    /// the fields. The two adjacent `TimeLineID` lines are the reason
    /// this parses a prefix rather than searching for a substring.
    const SAMPLE: &str = "\
Database cluster state:               shut down
Latest checkpoint location:           0/25000028
Latest checkpoint's REDO location:    0/25000028
Latest checkpoint's TimeLineID:       5
Latest checkpoint's PrevTimeLineID:   5
Latest checkpoint's full_page_writes: on
";

    #[test]
    fn reads_the_checkpoint_timeline_and_location() {
        let cp = parse_control_point(SAMPLE);
        assert_eq!(cp.timeline_id, 5);
        assert_eq!(cp.checkpoint_lsn, 0x2500_0028);
        assert!(cp.is_known());
    }

    #[test]
    fn prev_timeline_does_not_win() {
        // PrevTimeLineID differs after a promotion; picking it up
        // would report the timeline this node LEFT.
        let promoted = SAMPLE.replace("PrevTimeLineID:   5", "PrevTimeLineID:   4");
        assert_eq!(parse_control_point(&promoted).timeline_id, 5);
    }

    #[test]
    fn redo_location_does_not_win() {
        // "Latest checkpoint's REDO location" sits adjacent to the
        // field we want and differs from it on a busy server; the
        // prefix match must not slide onto it.
        let busy = SAMPLE.replace(
            "REDO location:    0/25000028",
            "REDO location:    0/24000000",
        );
        assert_eq!(parse_control_point(&busy).checkpoint_lsn, 0x2500_0028);
    }

    #[test]
    fn unknown_rather_than_a_guess() {
        // A different major version, a localized build, or a truncated
        // read must not produce a value — callers compare these across
        // nodes and against switchpoints, and would act on a
        // fabricated one.
        assert_eq!(
            parse_control_point("Database cluster state: shut down\n"),
            ControlPoint::UNKNOWN
        );
        assert_eq!(parse_control_point(""), ControlPoint::UNKNOWN);
        let garbled = parse_control_point(
            "Latest checkpoint's TimeLineID:       not-a-number\n\
             Latest checkpoint location:           not-an-lsn\n",
        );
        assert_eq!(garbled, ControlPoint::UNKNOWN);
        // Each field falls back independently: a readable timeline
        // beside an unreadable location is still not comparable.
        let half = parse_control_point("Latest checkpoint's TimeLineID:       5\n");
        assert_eq!(half.timeline_id, 5);
        assert!(!half.is_known(), "half a control point cannot be compared");
    }
}
