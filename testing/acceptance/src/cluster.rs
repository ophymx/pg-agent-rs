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

// --- partial / asymmetric partitions (gap item 6) -----------------------
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
