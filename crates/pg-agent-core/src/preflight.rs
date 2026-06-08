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

use crate::config::{Config, DEFAULT_PGPOOL_NODE_ID_FILE};
use crate::localdb::LocalDb;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

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
            (0, 0) => writeln!(w, "preflight: all checks passed")?,
            (0, _) => writeln!(w, "preflight: {warns} warning(s) — OK with warnings")?,
            _ => writeln!(w, "preflight: {errs} error(s), {warns} warning(s) — FAIL")?,
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
    let path = Path::new(DEFAULT_PGPOOL_NODE_ID_FILE);
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NodeConfig, PostgresConfig, TlsConfig};
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use tempfile::TempDir;

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
}
