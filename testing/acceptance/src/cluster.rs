//! Host-side cluster control: docker compose lifecycle plus `docker
//! exec` into the three nodes. Everything here shells out — the docker
//! CLI is the one interface the bash suite proved, and the API socket
//! keeps working while a node is network-partitioned off `pga-net`
//! (docker exec does not ride the compose network).

use std::process::Stdio;

use anyhow::{bail, Context};
use tokio::process::Command;

pub const NODES: [&str; 3] = ["db0", "db1", "db2"];
pub const COMPOSE: &str = "testing/compose.yaml";

/// Where this cell's PostgreSQL actually lives.
///
/// Read from `/etc/pg-agent-matrix/env` inside the container rather
/// than derived here, because the layout differs by packaging family
/// in six independent ways and only the image knows which it is:
///
/// ```text
///            Debian family                RHEL family
///   unit     postgresql@17-main           postgresql-16
///   data     /var/lib/postgresql/17/main  /var/lib/pgsql/16/data
///   bins     /usr/lib/postgresql/17/bin   /usr/pgsql-16/bin
///   config   /etc/postgresql/17/main      inside PGDATA
///   home     /var/lib/postgresql          /var/lib/pgsql
///   pgpool   pgpool2 + /etc/pgpool2       pgpool-II + /etc/pgpool-II
/// ```
///
/// The suite stops, kills, and inspects these from dozens of places. A
/// cell where half of them guessed the other family's spelling would
/// fail in ways that read as product bugs, which is exactly what a
/// pinned `postgresql@17-main` did on the PostgreSQL 16 cell (finding
/// 27). Asking the image is the fix that does not need repeating.
#[derive(Debug, Clone)]
pub struct Facts {
    pub family: String,
    pub version: String,
    /// Unit name WITHOUT the `.service` suffix.
    pub pg_unit: String,
    pub pg_log: String,
    pub pgdata: String,
    /// The `postgres` OS user's home — where the agent's `state_dir`
    /// (and thus `pg_agent/raft`) is derived from.
    pub pg_home: String,
    pub pgpool_conf_dir: String,
}

static FACTS: std::sync::OnceLock<Facts> = std::sync::OnceLock::new();

/// Read the facts file from `db0` and cache it. Called once, right
/// after the cluster is up and before any scenario runs.
pub async fn init_facts() -> anyhow::Result<()> {
    let raw = exec("db0", "cat /etc/pg-agent-matrix/env")
        .await
        .context("read /etc/pg-agent-matrix/env from db0")?;
    let get = |key: &str| -> anyhow::Result<String> {
        raw.lines()
            .find_map(|l| l.trim().strip_prefix(&format!("{key}=")))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .with_context(|| format!("{key} missing from /etc/pg-agent-matrix/env"))
    };
    // Every key is REQUIRED, including the four only the provisioning
    // scripts consume. Those scripts fall back to Debian's spelling
    // when a key is absent, so on a RHEL image a missing PG_BIN
    // silently becomes /usr/lib/postgresql/16/bin and surfaces much
    // later as a confusing "no such file". Demanding the full key set
    // here turns that into one clear line before the suite starts.
    for required in ["PG_BIN", "PG_CONF_DIR", "PGPOOL_UNIT"] {
        get(required)?;
    }
    let facts = Facts {
        family: get("PG_FAMILY")?,
        version: get("PG_VERSION")?,
        pg_unit: get("PG_UNIT")?,
        pg_log: get("PG_LOG")?,
        pgdata: get("PGDATA")?,
        pg_home: get("PG_HOME")?,
        pgpool_conf_dir: get("PGPOOL_CONF_DIR")?,
    };
    // The package format was chosen host-side before this image
    // existed, so it is the one layout decision that can silently
    // disagree with what actually booted. Catch it here rather than as
    // a puzzling `dnf: command not found` forty seconds later.
    let expected = std::env::var("PG_FAMILY").unwrap_or_else(|_| "debian".to_string());
    anyhow::ensure!(
        facts.family == expected,
        "cell mismatch: host built for PG_FAMILY={expected}, image reports {}",
        facts.family
    );
    println!(
        "  facts: {} PostgreSQL {} — unit {}, data {}",
        facts.family, facts.version, facts.pg_unit, facts.pgdata
    );
    let _ = FACTS.set(facts);
    Ok(())
}

/// Panics if [`init_facts`] has not run — a missing initialisation is a
/// harness bug, and defaulting to Debian here is how a RHEL cell would
/// come to report Debian paths in its failure messages.
pub fn facts() -> &'static Facts {
    FACTS
        .get()
        .expect("cluster::init_facts() must run before any scenario")
}

/// Unit name for the cluster, e.g. `postgresql@17-main` (Debian) or
/// `postgresql-16` (RHEL).
pub fn pg_unit() -> String {
    facts().pg_unit.clone()
}

/// Server log path for the cluster.
pub fn pg_log() -> String {
    facts().pg_log.clone()
}

/// pgpool's config directory: `/etc/pgpool2` or `/etc/pgpool-II`.
pub fn pgpool_conf_dir() -> String {
    facts().pgpool_conf_dir.clone()
}

/// The agent's state directory, derived the same way the agent derives
/// it (`<postgres user home>/pg_agent`).
pub fn agent_state_dir() -> String {
    format!("{}/pg_agent", facts().pg_home)
}

pub fn node_id(node: &str) -> &str {
    node.strip_prefix("db").unwrap_or(node)
}

/// Run a host command to completion; Ok(stdout) iff exit 0.
pub async fn host(argv: &[&str]) -> anyhow::Result<String> {
    let out = Command::new(argv[0])
        .args(&argv[1..])
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("spawn {}", argv[0]))?;
    if !out.status.success() {
        bail!(
            "{}: exit {:?}: {}",
            argv.join(" "),
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `docker exec` as root. Ok(stdout+stderr) iff exit 0; Err carries the
/// combined output so callers can surface it.
pub async fn exec(node: &str, script: &str) -> anyhow::Result<String> {
    exec_as(node, None, script).await
}

/// `docker exec -u postgres` — the agent's user, for psql / pcp /
/// pg_agentctl invocations.
pub async fn exec_pg(node: &str, script: &str) -> anyhow::Result<String> {
    exec_as(node, Some("postgres"), script).await
}

async fn exec_as(node: &str, user: Option<&str>, script: &str) -> anyhow::Result<String> {
    let mut cmd = Command::new("docker");
    cmd.arg("exec");
    if let Some(u) = user {
        cmd.args(["-u", u]);
    }
    cmd.args([&format!("pga-{node}"), "bash", "-c", script])
        .stdin(Stdio::null());
    let out = cmd
        .output()
        .await
        .with_context(|| format!("docker exec pga-{node}"))?;
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    if !out.status.success() {
        bail!(
            "exec on {node} failed ({:?}): {}",
            out.status.code(),
            text.trim()
        );
    }
    Ok(text)
}

/// True iff the command exits 0 (either stream discarded).
pub async fn exec_ok(node: &str, script: &str) -> bool {
    exec(node, script).await.is_ok()
}

pub async fn unit_active(node: &str, unit: &str) -> bool {
    exec_ok(node, &format!("systemctl is-active -q {unit}")).await
}

pub async fn compose_down() {
    let _ = host(&[
        "docker",
        "compose",
        "-f",
        COMPOSE,
        "down",
        "-v",
        "--remove-orphans",
    ])
    .await;
}

pub async fn compose_up() -> anyhow::Result<()> {
    host(&["docker", "compose", "-f", COMPOSE, "up", "-d", "--build"])
        .await
        .map(|_| ())
        .context("compose up")
}

/// The node's IP on `pga-net` — reachable from the host over the docker
/// bridge, which is what lets the harness hold real PostgreSQL
/// connections instead of shelling psql per sample.
pub async fn container_ip(node: &str) -> anyhow::Result<String> {
    let ip = host(&[
        "docker",
        "inspect",
        "-f",
        r#"{{(index .NetworkSettings.Networks "pga-net").IPAddress}}"#,
        &format!("pga-{node}"),
    ])
    .await?;
    let ip = ip.trim().to_string();
    if ip.is_empty() {
        bail!("no pga-net address for {node}");
    }
    Ok(ip)
}

/// The site power blip: SIGKILL PID 1 in every container at once —
/// nothing inside shuts down cleanly (no shutdown checkpoints, no
/// journald goodbye) — then power comes back.
pub async fn power_blip() {
    let _ = host(&["docker", "kill", "pga-db0", "pga-db1", "pga-db2"]).await;
    let _ = host(&["docker", "start", "pga-db0", "pga-db1", "pga-db2"]).await;
}

// --- partial / asymmetric partitions -------------------------------------
//
// `network_disconnect` severs a node completely — both planes, both
// directions, the shape every earlier scenario used. Real failures are
// rarely that tidy: a firewall change closes one port, a NIC drops one
// direction, a security group blocks a subnet. These primitives cut one
// plane at a time so the suite can ask which plane's failure the design
// is actually responding to. Rules are installed INSIDE the target
// container (docker exec rides the API socket, not the compose
// network), so the harness keeps full control of a node it has just
// made unreachable to its peers.

/// Sever `port` to and from `peer_ip`, both directions, on `node`.
pub async fn sever_peer_port(node: &str, peer_ip: &str, port: u16) {
    let _ = exec(
        node,
        &format!(
            "iptables -A OUTPUT -d {peer_ip} -p tcp --dport {port} -j DROP && \
             iptables -A INPUT -s {peer_ip} -p tcp --sport {port} -j DROP"
        ),
    )
    .await;
}

/// Sever `port` to and from EVERY peer on `node` — one plane cut off
/// entirely, the other left untouched.
pub async fn sever_port_everywhere(node: &str, port: u16) {
    let _ = exec(
        node,
        &format!(
            "iptables -A INPUT -p tcp --dport {port} -j DROP && \
             iptables -A OUTPUT -p tcp --dport {port} -j DROP"
        ),
    )
    .await;
}

/// Drop only the connections `node` INITIATES to `port` on anyone.
/// Replies to connections others initiate toward `node` are not matched
/// (their source port is `port`, their destination is ephemeral), so
/// the node becomes unable to ask anything while remaining fully
/// answerable — one-sided blindness that does not depend on which peer
/// happens to hold any particular role.
pub async fn sever_outbound_port(node: &str, port: u16) {
    let _ = exec(
        node,
        &format!("iptables -A OUTPUT -p tcp --dport {port} -j DROP"),
    )
    .await;
}

/// True iff `node` can open a TCP connection to `ip:port` within 2s.
/// Verifies a manufactured cut directly, rather than inferring it from
/// how the cluster reacts — the agent's decision log dedups by variant,
/// so "the state I am waiting for" and "a line announcing it" are not
/// the same thing when the previous scenario left the node in that
/// state already.
pub async fn can_reach(node: &str, ip: &str, port: u16) -> bool {
    exec_ok(node, &format!("timeout 2 bash -c '</dev/tcp/{ip}/{port}'")).await
}

/// Drop every rule this suite installed (the containers run no other
/// firewalling).
pub async fn heal_firewall(node: &str) {
    let _ = exec(node, "iptables -F INPUT && iptables -F OUTPUT").await;
}

pub async fn network_disconnect(node: &str) {
    let _ = host(&[
        "docker",
        "network",
        "disconnect",
        "pga-net",
        &format!("pga-{node}"),
    ])
    .await;
}

pub async fn network_connect(node: &str) {
    let _ = host(&[
        "docker",
        "network",
        "connect",
        "pga-net",
        &format!("pga-{node}"),
    ])
    .await;
}
