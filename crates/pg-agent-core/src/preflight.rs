//! Runtime-prerequisite checks. Each is independent and idempotent;
//! failures are reported, not raised. Designed to be invoked from
//! `pg_agentctl preflight` and from Ansible (parseable output,
//! deterministic exit code).
//!
//! **Scope: localhost only.** Two buckets per SPEC §14:
//!
//! 1. **Filesystem** — TLS material, pgpool_node_id, libpq home defaults
//!    (`.pcppass`, `.postgresql/`), recovery tools. Always run.
//! 2. **DB-backed** — settings, roles, extension. Skipped (WARN) when the
//!    caller couldn't open a local DB connection.
//!
//! Peer-mesh validation (the network-level twin of "TLS material is
//! valid in our own eyes") lives in `pg_agentctl cluster status` instead.
//! Mixing it in here forced a chicken-and-egg in Ansible's per-host loop:
//! node A's preflight couldn't pass until node B's daemon was up, and
//! vice-versa. Splitting localhost-correctness from mesh-correctness
//! lets each node's preflight pass on its own merits.

use crate::config::{Config, DEFAULT_PGPOOL_NODE_ID_FILES};
use crate::localdb::LocalDb;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Recommended `wal_keep_size` floor, in MB. Covers the failover gap
/// that replication slots structurally cannot (see the check's docs):
/// enough WAL for a standby carrying normal replay debt to re-follow a
/// freshly promoted primary without a reclone. A deployment with heavy
/// write bursts should raise it — the check is a floor, not a target.
const WAL_KEEP_SIZE_FLOOR_MB: i64 = 512;

/// The standby recovery include the agent writes into `$PGDATA`
/// (`pgman::pgstandby`'s `write_recovery_conf`). PostgreSQL reads it
/// only if the effective `postgresql.conf` includes it, and nothing in
/// the write path can tell the difference: `ConfigureStandby` reports
/// success, the standby starts, and it simply never streams — no
/// `primary_conninfo`, no error (testing/FINDINGS.md finding 6). That is
/// the class of silent localhost misconfiguration `validate-env` exists
/// to catch, so the missing include is an ERR.
const RECOVERY_CONF_FILE: &str = "myrecovery.conf";

/// Ceiling on the number of files the include walk will read. A
/// `postgresql.conf` tree is a handful of files; this only bounds the
/// pathological case (an operator's `include_dir` pointing somewhere
/// enormous) so `validate-env` can't turn into a filesystem crawl.
const INCLUDE_SCAN_MAX_FILES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Ok,
    Warn,
    Err,
}

impl CheckStatus {
    pub fn tag(&self) -> &'static str {
        match self {
            CheckStatus::Ok => "OK  ",
            CheckStatus::Warn => "WARN",
            CheckStatus::Err => "ERR ",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

impl Check {
    fn ok(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Ok,
            detail: detail.into(),
        }
    }
    fn warn(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Warn,
            detail: detail.into(),
        }
    }
    fn err(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Err,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PreflightReport {
    pub checks: Vec<Check>,
}

impl PreflightReport {
    pub fn has_errors(&self) -> bool {
        self.checks.iter().any(|c| c.status == CheckStatus::Err)
    }

    pub fn print(&self, w: &mut dyn Write) -> std::io::Result<()> {
        let name_width = self.checks.iter().map(|c| c.name.len()).max().unwrap_or(0);
        let (mut errs, mut warns) = (0usize, 0usize);
        for c in &self.checks {
            if c.detail.is_empty() {
                writeln!(
                    w,
                    "{}  {:<width$}",
                    c.status.tag(),
                    c.name,
                    width = name_width
                )?;
            } else {
                writeln!(
                    w,
                    "{}  {:<width$}  {}",
                    c.status.tag(),
                    c.name,
                    c.detail,
                    width = name_width
                )?;
            }
            match c.status {
                CheckStatus::Err => errs += 1,
                CheckStatus::Warn => warns += 1,
                _ => {}
            }
        }
        writeln!(w)?;
        match (errs, warns) {
            (0, 0) => writeln!(w, "validate-env: all checks passed")?,
            (0, _) => writeln!(w, "validate-env: {warns} warning(s) — OK with warnings")?,
            _ => writeln!(
                w,
                "validate-env: {errs} error(s), {warns} warning(s) — FAIL"
            )?,
        }
        Ok(())
    }

    /// Machine-readable JSON dump suitable for `ansible.builtin.command` +
    /// `register:` + `from_json`. The field shape is stable.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "checks": self.checks.iter().map(|c| serde_json::json!({
                "name":   c.name,
                "status": c.status.tag().trim(),
                "detail": c.detail,
            })).collect::<Vec<_>>(),
            "has_errors": self.has_errors(),
        })
    }
}

/// Run every localhost check in a deterministic order. DB-backed checks
/// are skipped with a single WARN row if `db` is None — pass `Some(db)`
/// when the local PG socket is reachable.
///
/// For mesh-level validation (peer mTLS reachability), use
/// `pg_agentctl cluster status` after every daemon is up — see the
/// module-level doc for the rationale.
pub async fn preflight(cfg: &Config, db: Option<Arc<dyn LocalDb>>) -> PreflightReport {
    let mut r = PreflightReport::default();

    // ---- filesystem checks ---------------------------------------------
    fs_tls_material(cfg, &mut r);
    fs_pgpool_node_id(cfg, &mut r);
    fs_postgres_home_defaults(cfg, &mut r);
    fs_recovery_tools(cfg, &mut r);
    // Filesystem-shaped, but reads `config_file` / `data_directory`
    // from the server when there is one — the only source that is
    // right on both layouts and after any operator override.
    fs_recovery_conf_include(cfg, db.as_ref(), &mut r).await;
    unit_restart_policy(cfg, &mut r).await;
    raft_prerequisites(cfg, &mut r);

    // ---- db-backed checks ----------------------------------------------
    match db {
        Some(db) => {
            db_settings(&db, cfg, &mut r).await;
            db_extension(&db, &mut r).await;
            db_roles(&db, cfg, &mut r).await;
        }
        None => r.checks.push(Check::warn(
            "db connection",
            "skipped (no local DB connection — pass without --skip-db when PG is up)",
        )),
    }

    r
}

// ---------------------------------------------------------------------------
// Filesystem checkers
// ---------------------------------------------------------------------------

fn fs_tls_material(cfg: &Config, r: &mut PreflightReport) {
    if !cfg.tls.is_configured() {
        r.checks.push(Check::warn(
            "tls material",
            "no [tls] block configured (single-node / --dev only)",
        ));
        return;
    }
    let ca = cfg.tls.ca_cert.as_deref().expect("is_configured");
    let cert = cfg.tls.cert.as_deref().expect("is_configured");
    let key = cfg.tls.key.as_deref().expect("is_configured");
    check_readable(r, "tls material: ca_cert", ca);
    check_readable(r, "tls material: cert", cert);
    check_readable(r, "tls material: key", key);
    check_mode_at_most(r, "tls material: key mode", key, 0o600);
}

fn fs_pgpool_node_id(cfg: &Config, r: &mut PreflightReport) {
    // Same probe order the agent itself uses, so preflight reports on
    // the file the agent would actually read rather than on one
    // family's spelling of it.
    let path = DEFAULT_PGPOOL_NODE_ID_FILES
        .iter()
        .map(Path::new)
        .find(|p| p.exists())
        .unwrap_or_else(|| Path::new(DEFAULT_PGPOOL_NODE_ID_FILES[0]));
    let name = format!("pgpool_node_id: {}", path.display());
    match std::fs::read_to_string(path) {
        Ok(raw) => match raw.trim().parse::<i32>() {
            Ok(id) if id == cfg.local_node_id => {
                r.checks.push(Check::ok(
                    name,
                    format!("matches resolved local_node_id={id}"),
                ));
            }
            Ok(id) => r.checks.push(Check::err(
                name,
                format!("file says {id}, config resolved {}", cfg.local_node_id),
            )),
            Err(e) => r.checks.push(Check::err(name, format!("parse: {e}"))),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Absent is acceptable if the operator set node_id /
            // node_id_file or hostname matched. preflight doesn't try
            // to fix it; just notes the absence.
            r.checks.push(Check::warn(
                name,
                "file absent (relying on explicit node_id / node_id_file / hostname)",
            ));
        }
        Err(e) => r.checks.push(Check::err(name, format!("read: {e}"))),
    }
}

fn fs_postgres_home_defaults(cfg: &Config, r: &mut PreflightReport) {
    let home = match cfg.postgres.user_home.as_deref() {
        Some(h) => h,
        None => {
            r.checks.push(Check::warn(
                "postgres user_home",
                "unset; skipping .pcppass / .postgresql checks",
            ));
            return;
        }
    };

    // .pcppass — pg-agent reads this via libpq default lookup.
    let pcppass = home.join(".pcppass");
    if pcppass.exists() {
        check_mode_at_most(r, ".pcppass mode", &pcppass, 0o600);
    } else {
        r.checks.push(Check::warn(
            ".pcppass",
            format!("{} absent (pcp_attach_node will prompt)", pcppass.display()),
        ));
    }

    // .postgresql/postgresql.{crt,key} + root.crt — libpq's default
    // location for replication client material.
    let dotpg = home.join(".postgresql");
    for f in ["postgresql.crt", "postgresql.key", "root.crt"] {
        let path = dotpg.join(f);
        if path.exists() {
            r.checks
                .push(Check::ok(f.to_string(), path.display().to_string()));
            if f == "postgresql.key" {
                check_mode_at_most(r, "postgresql.key mode", &path, 0o600);
            }
        } else {
            r.checks.push(Check::warn(
                f.to_string(),
                format!(
                    "{} absent (replication uses libpq defaults; required for cert-auth)",
                    path.display()
                ),
            ));
        }
    }
}

fn fs_recovery_tools(cfg: &Config, r: &mut PreflightReport) {
    let prefix = match cfg.postgres.pg_install_prefix.as_deref() {
        Some(p) => p,
        None => {
            r.checks.push(Check::err(
                "pg_install_prefix",
                "unset; can't locate pg_basebackup / pg_rewind",
            ));
            return;
        }
    };
    for tool in ["pg_basebackup", "pg_rewind"] {
        let path = prefix.join("bin").join(tool);
        check_executable(r, &format!("recovery tool: {tool}"), &path);
    }
}

/// Assert that the effective `postgresql.conf` includes the
/// `myrecovery.conf` the agent writes into `$PGDATA`.
///
/// Without the include, a node can be reconfigured as a standby
/// successfully and still never stream: `ConfigureStandby` writes the
/// file and reports success, PostgreSQL starts, and nothing anywhere
/// reads `primary_conninfo`. There is no error to find — which is why
/// this belongs in the deploy gate rather than in the recovery path.
///
/// The check resolves the include the way PostgreSQL does, and that
/// resolution is the point: **a relative include is taken as relative
/// to the directory holding the file that references it, not to
/// `data_directory`.** On the Debian layout — config under
/// `/etc/postgresql/<ver>/<cluster>/`, `$PGDATA` under
/// `/var/lib/postgresql/<ver>/<cluster>/` — a bare
/// `include_if_exists = 'myrecovery.conf'` therefore points at a file
/// in `/etc` that nothing ever writes. It looks right, it parses, PG
/// starts clean, and the standby never streams. So "an include naming
/// `myrecovery.conf`" is not enough to pass: it has to resolve to the
/// path `pgman` actually writes.
async fn fs_recovery_conf_include(
    cfg: &Config,
    db: Option<&Arc<dyn LocalDb>>,
    r: &mut PreflightReport,
) {
    const NAME: &str = "recovery conf include";

    // Prefer the running server's own answer: it is right on both
    // layouts and after any operator override of either path. Fall
    // back to config + layout probing when PG is down — the normal
    // case for the `ExecStartPre=` invocation.
    let mut data_dir = cfg.postgres.data_dir.clone();
    let mut config_file: Option<PathBuf> = None;
    if let Some(db) = db {
        if let Ok(v) = db.setting("data_directory").await {
            if !v.trim().is_empty() {
                data_dir = Some(PathBuf::from(v.trim()));
            }
        }
        if let Ok(v) = db.setting("config_file").await {
            if !v.trim().is_empty() {
                config_file = Some(PathBuf::from(v.trim()));
            }
        }
    }

    let Some(data_dir) = data_dir else {
        r.checks.push(Check::warn(
            NAME,
            format!("postgres.data_dir unset; cannot locate {RECOVERY_CONF_FILE}"),
        ));
        return;
    };
    let want = data_dir.join(RECOVERY_CONF_FILE);

    let (config_file, tried) = match config_file {
        Some(p) => (Some(p), Vec::new()),
        None => {
            let candidates = config_file_candidates(&data_dir);
            (candidates.iter().find(|p| p.is_file()).cloned(), candidates)
        }
    };
    let Some(config_file) = config_file else {
        r.checks.push(Check::warn(
            NAME,
            format!(
                "could not locate postgresql.conf (tried {}); cannot confirm {} is included",
                tried
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                want.display()
            ),
        ));
        return;
    };

    match scan_for_recovery_include(&config_file, &want) {
        Some(IncludeHit::Optional { in_file }) => r.checks.push(Check::ok(
            NAME,
            format!("{} includes {}", in_file.display(), want.display()),
        )),
        Some(IncludeHit::Required { in_file }) => r.checks.push(Check::warn(
            NAME,
            format!(
                "{} includes {} with `include` rather than `include_if_exists` — \
                 PostgreSQL refuses to start whenever that file is absent, which is a \
                 primary's normal state",
                in_file.display(),
                want.display()
            ),
        )),
        Some(IncludeHit::Elsewhere {
            in_file,
            raw,
            resolved,
        }) => r.checks.push(Check::err(
            NAME,
            format!(
                "{} includes '{raw}', which resolves to {} — but the agent writes {}. \
                 A relative include resolves against the directory of the file that \
                 references it, not the data directory; spell it absolutely",
                in_file.display(),
                resolved.display(),
                want.display()
            ),
        )),
        None => r.checks.push(Check::err(
            NAME,
            format!(
                "no include in {} names {} — a standby configured on this node would \
                 start with no primary_conninfo and silently never stream. Add \
                 `include_if_exists = '{}'`",
                config_file.display(),
                want.display(),
                want.display()
            ),
        )),
    }
}

/// Where `postgresql.conf` might live when the server is down and
/// can't be asked, most specific first.
fn config_file_candidates(data_dir: &Path) -> Vec<PathBuf> {
    // RHEL family (and any initdb-default layout): inside PGDATA.
    let mut out = vec![data_dir.join("postgresql.conf")];
    // Debian family: PGDATA is /var/lib/postgresql/<ver>/<cluster>,
    // and the config is the same tail under /etc/postgresql.
    if let (Some(cluster), Some(ver)) = (
        data_dir.file_name(),
        data_dir.parent().and_then(|p| p.file_name()),
    ) {
        out.push(
            Path::new("/etc/postgresql")
                .join(ver)
                .join(cluster)
                .join("postgresql.conf"),
        );
    }
    out
}

/// What the include walk found, best outcome wins.
#[derive(Debug)]
enum IncludeHit {
    /// `include_if_exists` naming the file the agent writes. Correct.
    Optional { in_file: PathBuf },
    /// Plain `include` naming it — works, but couples PostgreSQL's
    /// ability to start to a file that only exists on standbys.
    Required { in_file: PathBuf },
    /// An include names `myrecovery.conf`, but not the one that gets
    /// written. The silent case this check exists for.
    Elsewhere {
        in_file: PathBuf,
        raw: String,
        resolved: PathBuf,
    },
}

impl IncludeHit {
    fn rank(&self) -> u8 {
        match self {
            IncludeHit::Optional { .. } => 3,
            IncludeHit::Required { .. } => 2,
            IncludeHit::Elsewhere { .. } => 1,
        }
    }
}

/// Walk `root` and everything it includes, looking for the directive
/// that pulls in the agent's recovery file. Follows `include`,
/// `include_if_exists` and `include_dir` the way PostgreSQL does;
/// missing include targets are skipped rather than reported (PG's own
/// `include_if_exists` semantics, and a plain missing `include` is the
/// server's complaint to make, not ours).
fn scan_for_recovery_include(root: &Path, want: &Path) -> Option<IncludeHit> {
    let mut best: Option<IncludeHit> = None;
    let mut queue: Vec<PathBuf> = vec![root.to_path_buf()];
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut budget = INCLUDE_SCAN_MAX_FILES;

    while let Some(file) = queue.pop() {
        if !seen.insert(file.clone()) {
            continue; // include cycles are the operator's problem, not a hang
        }
        if budget == 0 {
            break;
        }
        budget -= 1;

        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let dir = file.parent().unwrap_or(Path::new("."));

        for line in text.lines() {
            let Some((key, value)) = parse_include_directive(line) else {
                continue;
            };
            let target = if Path::new(&value).is_absolute() {
                PathBuf::from(&value)
            } else {
                dir.join(&value)
            };

            if key == "include_dir" {
                let Ok(entries) = std::fs::read_dir(&target) else {
                    continue;
                };
                let mut confs: Vec<PathBuf> = entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "conf"))
                    .collect();
                confs.sort();
                queue.extend(confs);
                continue;
            }

            if target.file_name().is_some_and(|n| n == RECOVERY_CONF_FILE) {
                let hit = if same_target(&target, want) {
                    if key == "include_if_exists" {
                        IncludeHit::Optional {
                            in_file: file.clone(),
                        }
                    } else {
                        IncludeHit::Required {
                            in_file: file.clone(),
                        }
                    }
                } else {
                    IncludeHit::Elsewhere {
                        in_file: file.clone(),
                        raw: value.clone(),
                        resolved: target.clone(),
                    }
                };
                if best.as_ref().is_none_or(|b| hit.rank() > b.rank()) {
                    best = Some(hit);
                }
            } else {
                queue.push(target);
            }
        }
    }

    best
}

/// Parse one `postgresql.conf` line into an include directive.
/// Accepts both spellings PostgreSQL does (`include 'x'` and
/// `include = 'x'`), quoted or bare, and ignores everything else.
fn parse_include_directive(line: &str) -> Option<(String, String)> {
    let line = strip_comment(line).trim();
    let key_end = line.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))?;
    let (key, rest) = line.split_at(key_end);
    let key = key.to_ascii_lowercase();
    if !matches!(
        key.as_str(),
        "include" | "include_if_exists" | "include_dir"
    ) {
        return None;
    }
    let rest = rest.trim_start();
    let rest = rest.strip_prefix('=').unwrap_or(rest).trim_start();
    let value = match rest.strip_prefix('\'') {
        Some(quoted) => quoted[..quoted.find('\'')?].to_string(),
        None => rest.split_whitespace().next()?.to_string(),
    };
    if value.is_empty() {
        return None;
    }
    Some((key, value))
}

/// Everything before the first unquoted `#`.
fn strip_comment(line: &str) -> &str {
    let mut in_quote = false;
    for (i, c) in line.char_indices() {
        match c {
            '\'' => in_quote = !in_quote,
            '#' if !in_quote => return &line[..i],
            _ => {}
        }
    }
    line
}

/// Do two paths name the same recovery file? Compared through the
/// parent directory so a symlinked `$PGDATA` (Debian's
/// `/var/lib/postgresql/<ver>/<cluster>` on some deployments) doesn't
/// read as a mismatch. The file itself usually does not exist — a
/// primary never has one — so it can't be canonicalized directly.
fn same_target(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    let norm = |p: &Path| -> PathBuf {
        let parent = p.parent().unwrap_or(Path::new("/"));
        let parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
        match p.file_name() {
            Some(n) => parent.join(n),
            None => parent,
        }
    };
    norm(a) == norm(b)
}

fn check_readable(r: &mut PreflightReport, name: &str, path: &Path) {
    match std::fs::metadata(path) {
        Ok(_) => r.checks.push(Check::ok(name, path.display().to_string())),
        Err(e) => r
            .checks
            .push(Check::err(name, format!("{}: {e}", path.display()))),
    }
}

fn check_executable(r: &mut PreflightReport, name: &str, path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mode = meta.permissions().mode();
            if mode & 0o111 == 0 {
                r.checks.push(Check::err(
                    name,
                    format!(
                        "{} not executable (mode {:o})",
                        path.display(),
                        mode & 0o777
                    ),
                ));
            } else {
                r.checks.push(Check::ok(name, path.display().to_string()));
            }
        }
        Err(e) => r
            .checks
            .push(Check::err(name, format!("{}: {e}", path.display()))),
    }
}

fn check_mode_at_most(r: &mut PreflightReport, name: &str, path: &Path, max: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mode = meta.permissions().mode() & 0o777;
            if mode & !max != 0 {
                r.checks.push(Check::err(
                    name,
                    format!("mode {:o} exceeds {:o}", mode, max),
                ));
            } else {
                r.checks.push(Check::ok(name, format!("mode {:o}", mode)));
            }
        }
        Err(e) => r
            .checks
            .push(Check::err(name, format!("{}: {e}", path.display()))),
    }
}

// ---------------------------------------------------------------------------
// DB-backed checkers
// ---------------------------------------------------------------------------

async fn db_settings(db: &Arc<dyn LocalDb>, cfg: &Config, r: &mut PreflightReport) {
    // wal_level must be >= replica for streaming replication.
    let pool_size = cfg.pool.len() as i32;
    let want_slots = (pool_size + 2).max(3);

    for (name, want, ord) in [
        (
            "wal_level",
            "replica",
            SettingOrd::AtLeast(&["replica", "logical"]),
        ),
        ("hot_standby", "on", SettingOrd::Equal("on")),
    ] {
        match db.setting(name).await {
            Ok(got) => match &ord {
                SettingOrd::Equal(target) => {
                    if got.as_str() == *target {
                        r.checks
                            .push(Check::ok(format!("setting: {name}"), got.to_string()));
                    } else {
                        r.checks.push(Check::err(
                            format!("setting: {name}"),
                            format!("got {got:?}, want {want:?}"),
                        ));
                    }
                }
                SettingOrd::AtLeast(allowed) => {
                    if allowed.contains(&got.as_str()) {
                        r.checks
                            .push(Check::ok(format!("setting: {name}"), got.to_string()));
                    } else {
                        r.checks.push(Check::err(
                            format!("setting: {name}"),
                            format!("got {got:?}, want one of {:?}", allowed),
                        ));
                    }
                }
            },
            Err(e) => r.checks.push(Check::err(
                format!("setting: {name}"),
                format!("query failed: {e}"),
            )),
        }
    }

    // wal_keep_size: the floor that survives a failover's slot gap.
    // Slots reserve WAL from the moment they exist (finding 22's fix),
    // but a slot created at promotion cannot retroactively protect
    // segments written BEFORE it — and a standby whose replay trails
    // inside one of those segments needs exactly those. wal_keep_size
    // is the only thing that covers that gap, so this is a WARN and
    // not an error: the agent does not manage this GUC (it is static
    // deployment config, unlike synchronous_standby_names), but it
    // must say plainly when the deployment has left the gap open.
    match db.setting("wal_keep_size").await {
        Ok(got) => {
            // `current_setting` renders memory GUCs WITH their unit
            // ("512MB", "1GB", "0") — unlike the unitless integer
            // settings below, so a bare parse::<i64>() reads every
            // configured value as 0 and warns at a correctly-tuned
            // deployment. (It did exactly that on first run.)
            let mb = parse_size_mb(&got).unwrap_or(0);
            if mb >= WAL_KEEP_SIZE_FLOOR_MB {
                r.checks.push(Check::ok(
                    "setting: wal_keep_size",
                    format!("{mb}MB (want ≥{WAL_KEEP_SIZE_FLOOR_MB}MB)"),
                ));
            } else {
                r.checks.push(Check::warn(
                    "setting: wal_keep_size",
                    format!(
                        "{mb}MB (want ≥{WAL_KEEP_SIZE_FLOOR_MB}MB) — a standby whose \
                         replay trails through a failover can lose its WAL window to \
                         the new primary's first checkpoint and need a full reclone \
                         (finding 22)"
                    ),
                ));
            }
        }
        Err(e) => r.checks.push(Check::warn(
            "setting: wal_keep_size",
            format!("query failed: {e}"),
        )),
    }

    // Numeric settings: ≥ pool size + slack.
    for name in ["max_replication_slots", "max_wal_senders"] {
        match db.setting(name).await {
            Ok(got) => match got.parse::<i32>() {
                Ok(n) if n >= want_slots => {
                    r.checks.push(Check::ok(
                        format!("setting: {name}"),
                        format!("{n} (pool={pool_size}, want ≥{want_slots})"),
                    ));
                }
                Ok(n) => r.checks.push(Check::err(
                    format!("setting: {name}"),
                    format!("{n} (pool={pool_size}, want ≥{want_slots})"),
                )),
                Err(e) => r.checks.push(Check::err(
                    format!("setting: {name}"),
                    format!("parse {got:?}: {e}"),
                )),
            },
            Err(e) => r.checks.push(Check::err(
                format!("setting: {name}"),
                format!("query failed: {e}"),
            )),
        }
    }
}

/// Parse a PostgreSQL memory setting ("0", "8kB", "512MB", "1GB") into
/// whole MB, rounding down. `None` if the shape is unrecognized — the
/// caller treats that as "cannot confirm" rather than silently 0.
fn parse_size_mb(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let digits_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let n: i64 = s[..digits_end].parse().ok()?;
    match s[digits_end..].trim() {
        "" | "MB" => Some(n),
        "B" => Some(n / (1024 * 1024)),
        "kB" => Some(n / 1024),
        "GB" => Some(n * 1024),
        "TB" => Some(n * 1024 * 1024),
        _ => None,
    }
}

enum SettingOrd {
    Equal(&'static str),
    AtLeast(&'static [&'static str]),
}

async fn db_extension(db: &Arc<dyn LocalDb>, r: &mut PreflightReport) {
    match db.extension_exists("pgpool_recovery").await {
        Ok(true) => r
            .checks
            .push(Check::ok("extension: pgpool_recovery", "installed")),
        Ok(false) => r.checks.push(Check::err(
            "extension: pgpool_recovery",
            "missing — CREATE EXTENSION pgpool_recovery (recovery_1st_stage needs it)",
        )),
        Err(e) => r.checks.push(Check::err(
            "extension: pgpool_recovery",
            format!("query failed: {e}"),
        )),
    }
}

async fn db_roles(db: &Arc<dyn LocalDb>, cfg: &Config, r: &mut PreflightReport) {
    let repl_user = cfg
        .postgres
        .repl_user
        .as_deref()
        .unwrap_or("repl")
        .to_string();
    for role in [repl_user.as_str(), "pgpool"] {
        match db.role_exists(role).await {
            Ok(true) => r.checks.push(Check::ok(format!("role: {role}"), "exists")),
            Ok(false) => r.checks.push(Check::err(
                format!("role: {role}"),
                if role == "pgpool" {
                    "missing — needed for PCP + health-check user"
                } else {
                    "missing — needed for replication (ClusterInit creates it)"
                }
                .to_string(),
            )),
            Err(e) => r.checks.push(Check::err(
                format!("role: {role}"),
                format!("query failed: {e}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL unit restart policy
// ---------------------------------------------------------------------------

/// The drop-in that fixes a resurrecting unit. Same filename the
/// acceptance suite writes (`testing/docker/provision.sh`) and the same
/// one BOOTSTRAP §1.1 hands the operator, so a node, a fixture and a
/// playbook all name the same file.
const RESTART_DROPIN: &str = "10-agent-managed.conf";

/// Is systemd allowed to restart PostgreSQL behind the agent's back?
///
/// PGDG's `postgresql-<ver>.service` ships `Restart=on-failure`
/// **active**; Debian's `postgresql@.service` ships the same line
/// commented out. Nothing in BOOTSTRAP closes that: `systemctl disable`
/// stops boot-time autostart, not `Restart=`.
///
/// **The hazard is not the agent's own fence.** systemd never restarts
/// a unit it stopped by an explicit stop job, so `ensure_stopped` is
/// safe on either family. The hazard is a postmaster that dies on its
/// own terms — crash, OOM, an operator's `kill -9` — on a node the
/// cluster has since moved past. systemd hands it straight back with no
/// agent involvement, and if the node's agent died with it (the G8/G9
/// shapes) nothing is left to fence it. Quorum commit means it cannot
/// acknowledge a write (docs/quorum-commit.md §3), so this is not an
/// acknowledged-write hole — it is a node answering reads as a primary
/// after the cluster deposed it, which is what the fence exists to
/// prevent.
///
/// Found by the acceptance suite rather than by reading unit files
/// (testing/FINDINGS.md finding 29): G9 SIGKILLs the primary's postmaster
/// and waits for the lease to depose it. On Rocky systemd returned the
/// primary inside a second, nothing was ever deposed, and the thirteen
/// scenarios that followed ran against a cluster no assertion expected.
///
/// ERR, not WARN, and with no opt-out — the same standing as the pool
/// size and mTLS refusals. A deployment where PostgreSQL's lifecycle is
/// half systemd's and half the lease's has no coherent answer to "who
/// decides whether this node serves", and the failure only ever shows
/// up during an outage.
async fn unit_restart_policy(cfg: &Config, r: &mut PreflightReport) {
    let Some(unit) = cfg.postgres.service.as_deref() else {
        // `apply_defaults` fills this, so reaching here means a caller
        // skipped it. The unit-name check belongs to config validation;
        // say why this check could not run and move on.
        r.checks.push(Check::warn(
            "postgres unit: restart policy",
            "no [postgres] service configured — cannot ask systemd about it",
        ));
        return;
    };
    let raw = tokio::process::Command::new("systemctl")
        .args(["show", unit, "--property=Restart", "--property=LoadState"])
        .output()
        .await;
    let verdict = match raw {
        Ok(out) if out.status.success() => {
            restart_policy_verdict(unit, &String::from_utf8_lossy(&out.stdout))
        }
        Ok(out) => Check::warn(
            "postgres unit: restart policy",
            format!(
                "systemctl show {unit} exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        ),
        Err(e) => Check::warn(
            "postgres unit: restart policy",
            format!("could not run systemctl: {e}"),
        ),
    };
    r.checks.push(verdict);
}

/// The verdict, split out from the subprocess so the interesting part
/// is testable without a systemd to ask.
///
/// Parsed as `KEY=VALUE` lines rather than with `--value`, because two
/// `--value` properties come back as bare lines in an order the manual
/// does not promise. `LoadState` is read alongside for a reason:
/// `systemctl show` answers for a unit that does not exist by printing
/// defaults, and `Restart=no` is that default — so without it, a typo'd
/// unit name reports a clean bill of health.
fn restart_policy_verdict(unit: &str, stdout: &str) -> Check {
    let name = "postgres unit: restart policy";
    let mut restart = None;
    let mut load_state = None;
    for line in stdout.lines() {
        match line.split_once('=') {
            Some(("Restart", v)) => restart = Some(v.trim()),
            Some(("LoadState", v)) => load_state = Some(v.trim()),
            _ => {}
        }
    }
    match load_state {
        Some("loaded") => {}
        Some(other) => {
            return Check::warn(
                name,
                format!(
                    "{unit} is {other}, not loaded — cannot assess its restart policy \
                     (and the agent will not be able to manage it either)"
                ),
            )
        }
        None => return Check::warn(name, format!("systemctl said nothing about {unit}")),
    }
    match restart {
        Some("no") => Check::ok(name, format!("{unit}: Restart=no")),
        Some(policy) => Check::err(
            name,
            format!(
                "{unit}: Restart={policy} — systemd will restart a postmaster that dies \
                 badly, with no agent involvement, on a node the lease may have already \
                 moved past. Fix: printf '[Service]\\nRestart=no\\n' > \
                 /etc/systemd/system/{unit}.d/{RESTART_DROPIN} (mkdir -p first), then \
                 systemctl daemon-reload"
            ),
        ),
        None => Check::warn(name, format!("systemctl reported no Restart= for {unit}")),
    }
}

// ---------------------------------------------------------------------------
// Consensus prerequisites
// ---------------------------------------------------------------------------

/// Preconditions for the consensus plane
/// (docs/promotion-authority.md §5).
///
/// These run on every node, every time. They used to be skipped unless
/// `[raft] enabled = true`, on the reasoning that a checklist should
/// not report on things the operator has not turned on — sound while
/// there was something to turn on, and obsolete the moment consensus
/// became the only way this daemon decides anything. Now the four are
/// unconditional preconditions for the daemon working at all.
///
/// The deployment-shape two (pool size, transport auth) are refusals to
/// start rather than warnings, because each one's failure mode only
/// becomes visible during an outage, which is the worst possible time
/// to learn about it — **except under `--dev`**, where they drop to
/// WARN. A single-node dev instance genuinely is not an HA cluster and
/// genuinely has no mTLS, and erroring on both would make `--dev`
/// unstartable under the `ExecStartPre` gate. `--dev` already carries
/// exactly this meaning everywhere else (`reject_insecure_remote_peer`,
/// `PeerPool::new_dev`); it is the flag that says "I know".
fn raft_prerequisites(cfg: &Config, r: &mut PreflightReport) {
    // Severity for the two checks a dev instance legitimately fails.
    let shape = |name: &str, detail: String| {
        if cfg.dev_mode {
            Check::warn(name, detail)
        } else {
            Check::err(name, detail)
        }
    };
    // 0. A config that still carries the obsolete switch. `false` never
    //    reaches preflight (config load refuses it outright); `true`
    //    lands here so the operator gets the same "delete this line"
    //    from `validate-env` that the daemon's journal gives them.
    if let Some(msg) = cfg.raft.obsolete_enabled_warning() {
        r.checks.push(Check::warn("raft: obsolete config key", msg));
    }

    // 1. Three nodes is a hard minimum. A 2-node Raft cluster tolerates
    //    zero failures — losing either node loses quorum, so the
    //    survivor cannot even confirm it still holds the lease and must
    //    demote itself. A 2-node pool is therefore not a degraded HA
    //    cluster; it is a cluster that stops serving when either half
    //    of it goes away. This is the one place to catch that before an
    //    outage demonstrates it.
    let n = cfg.pool.len();
    if n < 3 {
        r.checks.push(shape(
            "raft: pool size",
            format!(
                "{n} node(s); Raft needs at least 3 — a 2-node Raft cluster \
                 tolerates zero failures, and the survivor demotes itself \
                 rather than serving without quorum"
            ),
        ));
    } else {
        r.checks
            .push(Check::ok("raft: pool size", format!("{n} nodes")));
    }

    // 2. The local node must be in the pool it is joining. `local_node_id
    //    == -1` is config.rs's unresolved sentinel; a node that cannot
    //    identify itself cannot pick a Raft node id, and everything
    //    downstream would be guessing.
    let pool = cfg.to_node_pool();
    if pool.local_node_id >= 0 {
        r.checks.push(Check::ok(
            "raft: local node id",
            format!("node {}", pool.local_node_id),
        ));
    } else {
        r.checks.push(Check::err(
            "raft: local node id",
            "unresolved — local hostname matches no [[pool]] entry, so this \
             node cannot know which Raft member it is",
        ));
    }

    // 3. mTLS. The consensus plane rides the peer listener, so an
    //    unauthenticated Raft port is an unauthenticated *promotion
    //    authority*: anyone who can reach it can propose a lease
    //    takeover. Dev mode's plain TCP is fine for a single node and
    //    indefensible for a real pool.
    if cfg.tls.is_configured() {
        r.checks
            .push(Check::ok("raft: transport auth", "mTLS (peer listener)"));
    } else {
        r.checks.push(shape(
            "raft: transport auth",
            "no TLS configured — the consensus plane shares the peer \
             listener, so this would expose lease takeover to anyone who \
             can reach the port"
                .to_string(),
        ));
    }

    // 4. The state directory has to be writable *now*, not at the first
    //    election. A vote that cannot be persisted is a vote that can be
    //    cast twice after a crash, which is two leaders in one term.
    match cfg.state_dir.as_deref() {
        Some(dir) => {
            let raft_dir = dir.join(crate::raftstore::RAFT_SUBDIR);
            match std::fs::create_dir_all(&raft_dir) {
                Ok(()) => r
                    .checks
                    .push(Check::ok("raft: state dir", raft_dir.display().to_string())),
                Err(e) => r.checks.push(Check::err(
                    "raft: state dir",
                    format!("{}: {e}", raft_dir.display()),
                )),
            }
        }
        None => r.checks.push(Check::err(
            "raft: state dir",
            "state_dir is unset — Raft needs somewhere durable for its log and vote",
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NodeConfig, PostgresConfig, TlsConfig};
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use tempfile::TempDir;

    /// `current_setting` renders memory GUCs with their unit, so the
    /// wal_keep_size floor check must read units — a bare integer
    /// parse reported a correctly-configured 512MB deployment as 0MB
    /// and warned at it (caught on the live cluster, not by a test).
    #[test]
    fn memory_settings_parse_with_their_units() {
        assert_eq!(parse_size_mb("512MB"), Some(512));
        assert_eq!(parse_size_mb("1GB"), Some(1024));
        assert_eq!(parse_size_mb("0"), Some(0));
        assert_eq!(parse_size_mb(" 2048 "), Some(2048)); // unitless = MB
        assert_eq!(parse_size_mb("16384kB"), Some(16));
        assert_eq!(parse_size_mb("1TB"), Some(1024 * 1024));
        // Unrecognized shapes are "cannot confirm", never a silent 0.
        assert_eq!(parse_size_mb("lots"), None);
        assert_eq!(parse_size_mb("64XB"), None);
    }

    fn make_cfg(tmp: &TempDir) -> Config {
        let home = tmp.path().join("postgres-home");
        fs::create_dir_all(&home).unwrap();

        let mut cfg = Config {
            pool: vec![NodeConfig {
                id: 0,
                hostname: "local".into(),
            }],
            postgres: PostgresConfig {
                user_home: Some(home),
                pg_install_prefix: Some(tmp.path().join("pg-install")),
                ..Default::default()
            },
            ..Config::default()
        };
        cfg.apply_defaults();
        cfg
    }

    #[tokio::test]
    async fn preflight_no_db_warns_on_db_section() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_cfg(&tmp);
        let r = preflight(&cfg, None).await;
        assert!(
            r.checks
                .iter()
                .any(|c| c.name == "db connection" && c.status == CheckStatus::Warn),
            "expected db connection WARN"
        );
        // Mesh-level validation lives in `pg_agentctl cluster status`,
        // not here — preflight is localhost-scoped.
        assert!(
            !r.checks.iter().any(|c| c.name.contains("peer")),
            "preflight should not emit any peer-* rows"
        );
    }

    // ----- recovery conf include ---------------------------------------

    /// Build a cfg whose `$PGDATA` is a real temp dir, plus a config
    /// directory beside it — the Debian shape (config outside PGDATA)
    /// in miniature, which is the shape that makes the relative-include
    /// trap reachable.
    fn make_include_cfg(tmp: &TempDir) -> (Config, PathBuf, PathBuf) {
        let mut cfg = make_cfg(tmp);
        let data_dir = tmp.path().join("pgdata");
        let conf_dir = tmp.path().join("etc");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&conf_dir).unwrap();
        cfg.postgres.data_dir = Some(data_dir.clone());
        (cfg, data_dir, conf_dir)
    }

    fn include_check(r: &PreflightReport) -> &Check {
        find(&r.checks, "recovery conf include")
    }

    #[tokio::test]
    async fn recovery_include_ok_when_pgdata_conf_names_it() {
        // RHEL shape: postgresql.conf lives inside PGDATA, so the
        // relative spelling BOOTSTRAP prescribes resolves correctly.
        let tmp = TempDir::new().unwrap();
        let (cfg, data_dir, _conf_dir) = make_include_cfg(&tmp);
        fs::write(
            data_dir.join("postgresql.conf"),
            "wal_level = replica\ninclude_if_exists = 'myrecovery.conf'\n",
        )
        .unwrap();

        let r = preflight(&cfg, None).await;
        let c = include_check(&r);
        assert_eq!(c.status, CheckStatus::Ok, "{}", c.detail);
    }

    #[tokio::test]
    async fn recovery_include_err_when_absent() {
        let tmp = TempDir::new().unwrap();
        let (cfg, data_dir, _conf_dir) = make_include_cfg(&tmp);
        fs::write(
            data_dir.join("postgresql.conf"),
            "wal_level = replica\nhot_standby = on\n",
        )
        .unwrap();

        let r = preflight(&cfg, None).await;
        let c = include_check(&r);
        assert_eq!(c.status, CheckStatus::Err, "{}", c.detail);
        assert!(c.detail.contains("never stream"), "{}", c.detail);
        // The message hands the operator the exact line to paste.
        assert!(
            c.detail
                .contains(&format!("include_if_exists = '{}", data_dir.display())),
            "{}",
            c.detail
        );
    }

    #[tokio::test]
    async fn recovery_include_err_when_relative_resolves_outside_pgdata() {
        // The silent one. The config lives outside PGDATA (Debian), so
        // a bare `include_if_exists = 'myrecovery.conf'` names a file
        // in the CONFIG directory — which nothing ever writes. PG
        // starts clean and the standby never streams.
        let tmp = TempDir::new().unwrap();
        let (mut cfg, data_dir, conf_dir) = make_include_cfg(&tmp);
        let conf = conf_dir.join("postgresql.conf");
        fs::write(&conf, "include_if_exists = 'myrecovery.conf'\n").unwrap();
        // No postgresql.conf inside PGDATA; point the probe at the
        // config we wrote by making it the only candidate that exists.
        cfg.postgres.data_dir = Some(data_dir.clone());

        // Probe order is PGDATA first, then the Debian /etc path — in
        // a test neither resolves, so drive the scan directly.
        let want = data_dir.join("myrecovery.conf");
        let hit = scan_for_recovery_include(&conf, &want);
        match hit {
            Some(IncludeHit::Elsewhere { resolved, raw, .. }) => {
                assert_eq!(raw, "myrecovery.conf");
                assert_eq!(resolved, conf_dir.join("myrecovery.conf"));
            }
            other => panic!("expected an Elsewhere hit, got {other:?}"),
        }
    }

    #[test]
    fn recovery_include_ok_on_the_real_debian_shape() {
        // What the acceptance images actually provision, and what
        // BOOTSTRAP now prescribes: config outside PGDATA, the line in
        // a conf.d drop-in, path spelled absolutely. The walk has to
        // cross both hops — include_dir, then the drop-in — and accept
        // the absolute target.
        let tmp = TempDir::new().unwrap();
        let (_cfg, data_dir, conf_dir) = make_include_cfg(&tmp);
        let dropins = conf_dir.join("conf.d");
        fs::create_dir_all(&dropins).unwrap();
        let conf = conf_dir.join("postgresql.conf");
        fs::write(
            &conf,
            format!(
                "data_directory = '{}'\ninclude_dir = 'conf.d'\n",
                data_dir.display()
            ),
        )
        .unwrap();
        fs::write(
            dropins.join("10-pg-agent.conf"),
            format!(
                "wal_keep_size = 512MB\ninclude_if_exists = '{}/myrecovery.conf'\n",
                data_dir.display()
            ),
        )
        .unwrap();

        let want = data_dir.join("myrecovery.conf");
        match scan_for_recovery_include(&conf, &want) {
            Some(IncludeHit::Optional { in_file }) => {
                assert!(in_file.ends_with("10-pg-agent.conf"), "{in_file:?}");
            }
            other => panic!("expected an Optional hit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recovery_include_found_through_include_dir() {
        // The line may live in a conf.d drop-in rather than the main
        // file; the walk must follow include_dir to find it.
        let tmp = TempDir::new().unwrap();
        let (cfg, data_dir, _conf_dir) = make_include_cfg(&tmp);
        let dropins = data_dir.join("conf.d");
        fs::create_dir_all(&dropins).unwrap();
        fs::write(data_dir.join("postgresql.conf"), "include_dir = 'conf.d'\n").unwrap();
        fs::write(
            dropins.join("90-standby.conf"),
            format!(
                "include_if_exists = '{}/myrecovery.conf'\n",
                data_dir.display()
            ),
        )
        .unwrap();

        let r = preflight(&cfg, None).await;
        let c = include_check(&r);
        assert_eq!(c.status, CheckStatus::Ok, "{}", c.detail);
        assert!(c.detail.contains("90-standby.conf"), "{}", c.detail);
    }

    #[tokio::test]
    async fn recovery_include_warns_on_non_optional_include() {
        // Plain `include` works — until the file is absent, which is
        // every primary, and then PostgreSQL refuses to start.
        let tmp = TempDir::new().unwrap();
        let (cfg, data_dir, _conf_dir) = make_include_cfg(&tmp);
        fs::write(
            data_dir.join("postgresql.conf"),
            "include 'myrecovery.conf'\n",
        )
        .unwrap();

        let r = preflight(&cfg, None).await;
        let c = include_check(&r);
        // Working-but-fragile is a warning, not a start-blocking ERR.
        assert_eq!(c.status, CheckStatus::Warn, "{}", c.detail);
        assert!(c.detail.contains("include_if_exists"), "{}", c.detail);
    }

    #[tokio::test]
    async fn recovery_include_warns_when_no_config_file_is_findable() {
        // PG down and no postgresql.conf where either layout keeps it:
        // "cannot confirm" is a warning, not a false accusation.
        let tmp = TempDir::new().unwrap();
        let (cfg, _data_dir, _conf_dir) = make_include_cfg(&tmp);
        let r = preflight(&cfg, None).await;
        let c = include_check(&r);
        assert_eq!(c.status, CheckStatus::Warn, "{}", c.detail);
        assert!(c.detail.contains("could not locate"), "{}", c.detail);
    }

    #[test]
    fn include_directive_parsing_covers_the_spellings_pg_accepts() {
        let p = parse_include_directive;
        assert_eq!(
            p("include_if_exists = 'myrecovery.conf'"),
            Some(("include_if_exists".into(), "myrecovery.conf".into()))
        );
        // No `=`, PG accepts it.
        assert_eq!(
            p("  include 'conf.d/extra.conf'  "),
            Some(("include".into(), "conf.d/extra.conf".into()))
        );
        // Bare (unquoted) value.
        assert_eq!(
            p("include_dir = conf.d"),
            Some(("include_dir".into(), "conf.d".into()))
        );
        // Trailing comment is not part of the value.
        assert_eq!(
            p("include_if_exists = 'myrecovery.conf'  # written by the agent"),
            Some(("include_if_exists".into(), "myrecovery.conf".into()))
        );
        // A `#` inside the quotes is a filename character.
        assert_eq!(
            p("include_if_exists = 'odd#name.conf'"),
            Some(("include_if_exists".into(), "odd#name.conf".into()))
        );
        // Commented-out lines must not count as configuration — this
        // is the one that would turn the whole check into a rubber
        // stamp, since the stanza ships commented in some templates.
        assert_eq!(p("#include_if_exists = 'myrecovery.conf'"), None);
        assert_eq!(p("  # include 'myrecovery.conf'"), None);
        // Not an include directive at all.
        assert_eq!(p("wal_level = replica"), None);
        assert_eq!(p(""), None);
    }

    #[tokio::test]
    async fn tls_check_errors_when_paths_missing() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.tls = TlsConfig {
            ca_cert: Some(tmp.path().join("absent-ca")),
            cert: Some(tmp.path().join("absent-cert")),
            key: Some(tmp.path().join("absent-key")),
        };
        let r = preflight(&cfg, None).await;
        let tls_errs: Vec<_> = r
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::Err && c.name.starts_with("tls material:"))
            .collect();
        // Three missing-file ERRs (ca_cert, cert, key) plus the key-mode probe
        // which also ERRs because the file isn't there to stat.
        assert_eq!(
            tls_errs.len(),
            4,
            "expected 4 TLS material errors, got {tls_errs:?}"
        );
    }

    #[tokio::test]
    async fn tls_check_passes_when_files_present_with_correct_mode() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);

        let ca = tmp.path().join("ca.crt");
        let cert = tmp.path().join("node.crt");
        let key = tmp.path().join("node.key");
        for p in [&ca, &cert, &key] {
            fs::write(p, b"x").unwrap();
        }
        // Key must be 0600.
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();

        cfg.tls = TlsConfig {
            ca_cert: Some(ca),
            cert: Some(cert),
            key: Some(key),
        };

        let r = preflight(&cfg, None).await;
        let tls_errs: Vec<_> = r
            .checks
            .iter()
            .filter(|c| c.name.starts_with("tls material") && c.status == CheckStatus::Err)
            .collect();
        assert!(tls_errs.is_empty(), "unexpected TLS errors: {tls_errs:?}");
    }

    #[tokio::test]
    async fn tls_check_fails_loose_key_mode() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);

        let ca = tmp.path().join("ca.crt");
        let cert = tmp.path().join("node.crt");
        let key = tmp.path().join("node.key");
        for p in [&ca, &cert, &key] {
            fs::write(p, b"x").unwrap();
        }
        // World-readable key — should error.
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();

        cfg.tls = TlsConfig {
            ca_cert: Some(ca),
            cert: Some(cert),
            key: Some(key),
        };

        let r = preflight(&cfg, None).await;
        assert!(r
            .checks
            .iter()
            .any(|c| c.name.contains("tls material: key mode") && c.status == CheckStatus::Err));
    }

    #[tokio::test]
    async fn recovery_tools_err_when_missing() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_cfg(&tmp);
        let r = preflight(&cfg, None).await;
        let errs: Vec<_> = r
            .checks
            .iter()
            .filter(|c| c.name.starts_with("recovery tool") && c.status == CheckStatus::Err)
            .collect();
        assert_eq!(errs.len(), 2, "expected pg_basebackup + pg_rewind errors");
    }

    #[tokio::test]
    async fn recovery_tools_ok_when_executable() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_cfg(&tmp);
        let bin = cfg
            .postgres
            .pg_install_prefix
            .as_deref()
            .unwrap()
            .join("bin");
        fs::create_dir_all(&bin).unwrap();
        for tool in ["pg_basebackup", "pg_rewind"] {
            let p = bin.join(tool);
            fs::write(&p, "#!/bin/sh\nexit 0\n").unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let r = preflight(&cfg, None).await;
        let errs: Vec<_> = r
            .checks
            .iter()
            .filter(|c| c.name.starts_with("recovery tool") && c.status == CheckStatus::Err)
            .collect();
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    }

    #[test]
    fn report_print_formats_columns() {
        let report = PreflightReport {
            checks: vec![
                Check::ok("first", "/path/one"),
                Check::warn("second-longer", "soft"),
                Check::err("third", "boom"),
            ],
        };
        let mut buf = Vec::new();
        report.print(&mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("OK"));
        assert!(s.contains("WARN"));
        assert!(s.contains("ERR"));
        assert!(s.contains("FAIL"));
    }

    #[test]
    fn report_json_shape_stable() {
        let report = PreflightReport {
            checks: vec![Check::err("x", "boom")],
        };
        let j = report.to_json();
        assert_eq!(j["has_errors"], serde_json::Value::Bool(true));
        assert_eq!(j["checks"][0]["status"], "ERR");
        assert_eq!(j["checks"][0]["name"], "x");
    }

    // ----- postgres unit restart policy ------------------------------------

    /// Debian's shape: the line is in the unit but commented out, so
    /// the effective policy is `no` and the agent owns the lifecycle.
    #[test]
    fn restart_policy_accepts_no() {
        let c = restart_policy_verdict(
            "postgresql@17-main.service",
            "Restart=no\nLoadState=loaded\n",
        );
        assert_eq!(c.status, CheckStatus::Ok);
    }

    /// PGDG's shape, and the whole point of the check. The detail has
    /// to carry the remedy: an operator reading an ExecStartPre failure
    /// at 3am should not have to go find the drop-in's filename.
    #[test]
    fn restart_policy_refuses_on_failure_and_names_the_fix() {
        let c = restart_policy_verdict(
            "postgresql-16.service",
            "Restart=on-failure\nLoadState=loaded\n",
        );
        assert_eq!(c.status, CheckStatus::Err);
        assert!(c.detail.contains("dies badly"), "{}", c.detail);
        assert!(c.detail.contains(RESTART_DROPIN), "{}", c.detail);
        assert!(c.detail.contains("daemon-reload"), "{}", c.detail);
    }

    /// Every policy other than `no` can resurrect a postmaster the
    /// cluster has moved past. The check does not rank them: the
    /// remedy is the same file either way.
    #[test]
    fn restart_policy_refuses_every_restarting_policy() {
        for policy in [
            "always",
            "on-abnormal",
            "on-abort",
            "on-watchdog",
            "on-success",
        ] {
            let c = restart_policy_verdict(
                "pg.service",
                &format!("Restart={policy}\nLoadState=loaded\n"),
            );
            assert_eq!(c.status, CheckStatus::Err, "{policy} must be refused");
        }
    }

    /// The false-clean-bill case. `systemctl show` answers for a unit
    /// that does not exist by printing defaults — and the default is
    /// `Restart=no`. Without reading LoadState, a typo'd unit name
    /// would report OK, which is the worst direction for a check to be
    /// wrong in.
    #[test]
    fn restart_policy_will_not_pass_a_unit_that_is_not_loaded() {
        let c = restart_policy_verdict("typo.service", "Restart=no\nLoadState=not-found\n");
        assert_eq!(c.status, CheckStatus::Warn);
        assert!(c.detail.contains("not-found"), "{}", c.detail);
    }

    /// Property order is not promised, and neither is the presence of
    /// either key on an old systemd. Parse, don't index.
    #[test]
    fn restart_policy_reads_properties_in_any_order() {
        let c = restart_policy_verdict("pg.service", "LoadState=loaded\nRestart=always\n");
        assert_eq!(c.status, CheckStatus::Err);
    }

    #[test]
    fn restart_policy_warns_when_systemctl_says_nothing_useful() {
        assert_eq!(
            restart_policy_verdict("pg.service", "").status,
            CheckStatus::Warn
        );
        assert_eq!(
            restart_policy_verdict("pg.service", "LoadState=loaded\n").status,
            CheckStatus::Warn
        );
    }

    // ----- consensus prerequisites -----------------------------------------

    fn raft_checks(cfg: &Config) -> Vec<Check> {
        let mut r = PreflightReport::default();
        raft_prerequisites(cfg, &mut r);
        r.checks
    }

    fn find<'a>(checks: &'a [Check], name: &str) -> &'a Check {
        checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no check named {name}; got {checks:?}"))
    }

    /// The checks are unconditional now. A default config — no `[raft]`
    /// block at all — must still be told whether this node can join
    /// consensus, because there is no longer a configuration in which
    /// it does not have to.
    #[test]
    fn raft_checks_run_without_any_raft_block() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_cfg(&tmp);
        let checks = raft_checks(&cfg);
        assert!(
            !checks.is_empty(),
            "consensus preconditions are not opt-in any more"
        );
        find(&checks, "raft: pool size");
        find(&checks, "raft: transport auth");
        find(&checks, "raft: state dir");
    }

    /// `--dev` is the flag that says "I know this is one node with no
    /// mTLS". Erroring there would make a dev instance unstartable
    /// under the ExecStartPre gate — but the checks must still SAY it,
    /// so the warning survives even though the exit code does not.
    #[test]
    fn dev_mode_downgrades_the_deployment_shape_checks_to_warnings() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.dev_mode = true;
        cfg.pool = vec![NodeConfig {
            id: 0,
            hostname: "solo".into(),
        }];
        cfg.local_node_id = 0;

        let checks = raft_checks(&cfg);
        assert_eq!(find(&checks, "raft: pool size").status, CheckStatus::Warn);
        assert_eq!(
            find(&checks, "raft: transport auth").status,
            CheckStatus::Warn
        );
        let r = PreflightReport { checks };
        assert!(!r.has_errors(), "a dev instance must still be startable");
    }

    /// The same pool WITHOUT `--dev` is a production cluster that
    /// cannot survive a single failure, and must not start.
    #[test]
    fn a_real_deployment_still_errors_on_the_same_shape() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.pool = vec![NodeConfig {
            id: 0,
            hostname: "solo".into(),
        }];
        cfg.local_node_id = 0;

        let checks = raft_checks(&cfg);
        assert_eq!(find(&checks, "raft: pool size").status, CheckStatus::Err);
        assert_eq!(
            find(&checks, "raft: transport auth").status,
            CheckStatus::Err
        );
    }

    /// A leftover `enabled = true` is reported where the operator is
    /// already looking. `false` cannot appear here at all — config load
    /// refuses it, so the daemon never reaches preflight.
    #[test]
    fn raft_warns_about_a_leftover_enabled_key() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.raft.obsolete_enabled = Some(true);

        let c = find(&raft_checks(&cfg), "raft: obsolete config key").clone();
        assert_eq!(c.status, CheckStatus::Warn);
        assert!(c.detail.contains("Delete the line"), "{}", c.detail);
    }

    #[test]
    fn raft_says_nothing_about_the_obsolete_key_when_it_is_absent() {
        let tmp = TempDir::new().unwrap();
        let cfg = make_cfg(&tmp);
        assert!(!raft_checks(&cfg)
            .iter()
            .any(|c| c.name == "raft: obsolete config key"));
    }

    /// Two nodes is the one that matters: a 2-node Raft cluster
    /// tolerates zero failures — losing either loses quorum, so the
    /// survivor cannot confirm its own lease and demotes itself. That
    /// is not a degraded HA cluster, it is one that stops serving when
    /// either half goes away.
    #[test]
    fn raft_refuses_a_pool_smaller_than_three() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.pool = vec![
            NodeConfig {
                id: 0,
                hostname: "a".into(),
            },
            NodeConfig {
                id: 1,
                hostname: "b".into(),
            },
        ];

        let checks = raft_checks(&cfg);
        let c = find(&checks, "raft: pool size");
        assert_eq!(c.status, CheckStatus::Err);
        assert!(c.detail.contains("at least 3"), "{}", c.detail);
    }

    /// The consensus plane rides the peer listener, so no mTLS means
    /// promotion authority is reachable by anyone who can reach the
    /// port. Dev mode is fine for one node and indefensible for a pool.
    #[test]
    fn raft_refuses_to_run_without_mtls() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.pool = (0..3)
            .map(|id| NodeConfig {
                id,
                hostname: format!("n{id}"),
            })
            .collect();

        let c = find(&raft_checks(&cfg), "raft: transport auth").clone();
        assert_eq!(c.status, CheckStatus::Err);
        assert!(c.detail.contains("no TLS configured"), "{}", c.detail);
    }

    /// A node that cannot identify itself in the pool cannot pick a
    /// Raft node id, and everything downstream would be guessing.
    #[test]
    fn raft_refuses_when_the_local_node_is_unresolved() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.pool = (0..3)
            .map(|id| NodeConfig {
                id,
                hostname: format!("nowhere-{id}.invalid"),
            })
            .collect();
        cfg.local_node_id = -1;

        let c = find(&raft_checks(&cfg), "raft: local node id").clone();
        assert_eq!(c.status, CheckStatus::Err);
    }

    /// A healthy three-node mTLS pool passes, and creates the state
    /// directory as a side effect — the vote must be persistable before
    /// the first election, not at it.
    #[test]
    fn raft_passes_on_a_healthy_three_node_pool() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = make_cfg(&tmp);
        cfg.pool = (0..3)
            .map(|id| NodeConfig {
                id,
                hostname: format!("n{id}"),
            })
            .collect();
        cfg.local_node_id = 1;
        cfg.tls = TlsConfig {
            ca_cert: Some(tmp.path().join("ca.pem")),
            cert: Some(tmp.path().join("cert.pem")),
            key: Some(tmp.path().join("key.pem")),
        };

        let checks = raft_checks(&cfg);
        for c in &checks {
            assert_eq!(c.status, CheckStatus::Ok, "unexpected: {c:?}");
        }
        assert!(cfg
            .state_dir
            .as_ref()
            .unwrap()
            .join(crate::raftstore::RAFT_SUBDIR)
            .is_dir());
    }
}
