//! A reader for `pgpool.conf`, shared by every consumer that has to
//! check pgpool's configuration against the agent's own contract:
//! `pg_agentctl check-hooks`, the daemon's `validate-env` preflight,
//! and the supervisor's served-certificate check.
//!
//! It understands exactly as much of the format as those consumers
//! need: `key = value` lines, single-quoted values, `#` comments,
//! last-write-wins on duplicate keys (pgpool's own resolution). It does
//! not follow `include` directives — pgpool-II has none.
//!
//! # The `backend_application_nameN` contract
//!
//! The agent writes `application_name=node<id>` into every standby's
//! `primary_conninfo` and names replication slots `node<id>`; its own
//! membership queries select from `pg_stat_replication` on that
//! pattern. pgpool fills `replication_state` / `sync_state` in
//! `pcp_node_info` and `SHOW POOL NODES` by matching
//! `backend_application_nameN` against the same column, and
//! `delay_threshold_by_time` relies on the same match. A pgpool.conf
//! that carries anything else there — the field report of 2026-11-02
//! had the FQDN — makes pgpool report `none` for a backend that is
//! streaming, and nothing else in the stack contradicts it. The value
//! is therefore part of the contract, with [`backend_application_name`]
//! as its single definition.

use std::collections::HashMap;

/// `backend_application_nameN` the agent-led contract requires for
/// backend `id`: the slot name, which is also the `application_name`
/// the agent writes into `primary_conninfo`. `NodeConfig::slot_name`
/// in pg-agent-core must agree with this; a test there pins it.
pub fn backend_application_name(id: i32) -> String {
    format!("node{id}")
}

/// One backend's addressing as `pgpool.conf` declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backend {
    pub id: i32,
    pub hostname: String,
    pub port: u16,
    /// `backend_application_nameN`, or `None` when the directive is
    /// absent (pgpool then reports `none` for replication state).
    pub application_name: Option<String>,
}

/// Parsed `pgpool.conf` directives, keyed by directive name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PgpoolConf {
    directives: HashMap<String, String>,
}

impl PgpoolConf {
    /// Parse `text`. Never fails: a line that is not `key = value` is
    /// skipped, which is also what pgpool does with lines it cannot
    /// read... up to the point where it refuses to start, which is
    /// pgpool's job to report, not this reader's.
    pub fn parse(text: &str) -> Self {
        Self {
            directives: parse_pgpool_conf(text),
        }
    }

    /// Raw directive value, quotes stripped.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.directives.get(key).map(String::as_str)
    }

    /// Every directive, for consumers that need the map itself.
    pub fn directives(&self) -> &HashMap<String, String> {
        &self.directives
    }

    /// Backend ids declared via `backend_hostnameN`, ascending.
    pub fn backend_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self
            .directives
            .keys()
            .filter_map(|k| k.strip_prefix("backend_hostname"))
            .filter_map(|n| n.parse::<i32>().ok())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The backend declared as `backend_hostnameN`, with its port
    /// (`backend_portN`, pgpool's default 5432 when absent) and
    /// application name.
    pub fn backend(&self, id: i32) -> Option<Backend> {
        let hostname = self.get(&format!("backend_hostname{id}"))?.to_string();
        let port = self
            .get(&format!("backend_port{id}"))
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5432);
        let application_name = self
            .get(&format!("backend_application_name{id}"))
            .map(str::to_string);
        Some(Backend {
            id,
            hostname,
            port,
            application_name,
        })
    }

    /// Every declared backend, ascending by id.
    pub fn backends(&self) -> Vec<Backend> {
        self.backend_ids()
            .into_iter()
            .filter_map(|id| self.backend(id))
            .collect()
    }

    /// The `backend_application_nameN` contract, one row per declared
    /// backend: the directive, the value the contract wants, and what
    /// the file says (`None` = absent). Same shape as the hook rows
    /// `check-hooks` already prints, so a drift here reads the same
    /// way as a drifted `failover_command`.
    pub fn backend_application_name_checks(&self) -> Vec<DirectiveCheck> {
        self.backend_ids()
            .into_iter()
            .map(|id| {
                let key = format!("backend_application_name{id}");
                let got = self.get(&key).map(str::to_string);
                let want = backend_application_name(id);
                DirectiveCheck {
                    ok: got.as_deref() == Some(want.as_str()),
                    key,
                    want,
                    got,
                }
            })
            .collect()
    }
}

/// One directive compared against the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectiveCheck {
    pub key: String,
    pub want: String,
    pub got: Option<String>,
    pub ok: bool,
}

/// Parse `pgpool.conf` enough to extract directive values.
///
/// Matches lines of the form `key = 'value'` or `key = value`, ignoring
/// `#` comments. Last-write-wins on duplicate keys, matching pgpool's
/// own resolution.
pub fn parse_pgpool_conf(text: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for raw in text.lines() {
        let line = raw.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Strip a trailing `# comment`. pgpool's own parser is more
        // lenient, but the directives the agent checks never embed `#`
        // in a quoted value.
        let line = match line.find('#') {
            Some(i) => &line[..i],
            None => line,
        };
        let Some(eq) = line.find('=') else {
            continue;
        };
        let key = line[..eq].trim().to_string();
        let value_raw = line[eq + 1..].trim();
        let value =
            if value_raw.starts_with('\'') && value_raw.ends_with('\'') && value_raw.len() >= 2 {
                value_raw[1..value_raw.len() - 1].to_string()
            } else {
                value_raw.to_string()
            };
        out.insert(key, value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_extracts_quoted_and_bare_values() {
        let s = "\
# leading comment
failover_command = 'pg_agentc failover %d %h %p %P %r %R'

backend_port0 = 5432  # trailing comment
quoted_with_spaces = '  spaces inside  '
        ";
        let m = parse_pgpool_conf(s);
        assert_eq!(
            m.get("failover_command").map(String::as_str),
            Some("pg_agentc failover %d %h %p %P %r %R")
        );
        assert_eq!(m.get("backend_port0").map(String::as_str), Some("5432"));
        assert_eq!(
            m.get("quoted_with_spaces").map(String::as_str),
            Some("  spaces inside  ")
        );
    }

    #[test]
    fn backends_are_read_in_id_order_with_defaults() {
        let conf = PgpoolConf::parse(
            "backend_hostname1 = 'db1'\n\
             backend_port1 = 5433\n\
             backend_application_name1 = 'node1'\n\
             backend_hostname0 = 'db0'\n",
        );
        assert_eq!(conf.backend_ids(), vec![0, 1]);
        let b = conf.backends();
        assert_eq!(b[0].hostname, "db0");
        assert_eq!(b[0].port, 5432, "pgpool's default port applies");
        assert_eq!(b[0].application_name, None);
        assert_eq!(b[1].port, 5433);
        assert_eq!(b[1].application_name.as_deref(), Some("node1"));
    }

    #[test]
    fn application_name_contract_flags_fqdn_and_absence() {
        // The 2026-11-02 shape: the FQDN where the slot name belongs.
        let conf = PgpoolConf::parse(
            "backend_hostname0 = 'db0.example'\n\
             backend_application_name0 = 'db0.example'\n\
             backend_hostname1 = 'db1.example'\n\
             backend_application_name1 = 'node1'\n\
             backend_hostname2 = 'db2.example'\n",
        );
        let checks = conf.backend_application_name_checks();
        assert_eq!(checks.len(), 3);
        assert!(!checks[0].ok);
        assert_eq!(checks[0].got.as_deref(), Some("db0.example"));
        assert_eq!(checks[0].want, "node0");
        assert!(checks[1].ok);
        assert!(!checks[2].ok, "absent is drift, not a pass");
        assert_eq!(checks[2].got, None);
    }
}
