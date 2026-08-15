//! Live PostgreSQL access from the harness. pg_hba trusts the compose
//! subnet (isolated test network), and the docker bridge makes the
//! container IPs routable from the host — so the harness holds real
//! `tokio-postgres` connections as the postgres superuser instead of
//! shelling `psql` per sample.
//!
//! Nodes get killed, fenced, recloned, and partitioned all suite long,
//! so the cache is self-healing: any error drops the cached client and
//! the next call redials. A node that is down simply yields Err, which
//! predicates treat as "not in the asserted state".

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use tokio_postgres::{Client, NoTls};

use crate::cluster::NODES;

pub struct Pg {
    ips: HashMap<&'static str, String>,
    conns: Mutex<HashMap<&'static str, Client>>,
}

impl Pg {
    pub async fn discover() -> anyhow::Result<Self> {
        let mut ips = HashMap::new();
        for n in NODES {
            ips.insert(n, crate::cluster::container_ip(n).await?);
        }
        Ok(Self {
            ips,
            conns: Mutex::new(HashMap::new()),
        })
    }

    async fn connect(&self, node: &'static str) -> anyhow::Result<Client> {
        let ip = self.ips.get(node).context("unknown node")?;
        let config = format!("host={ip} port=5432 user=postgres dbname=postgres connect_timeout=3");
        let (client, conn) = tokio_postgres::connect(&config, NoTls)
            .await
            .with_context(|| format!("connect {node}"))?;
        tokio::spawn(async move {
            // Connection driver; erroring out just means the cached
            // client starts failing and gets redialed.
            let _ = conn.await;
        });
        Ok(client)
    }

    /// Run `sql`, returning the first column of the first row as text.
    /// Reuses a cached connection; one redial on any failure.
    pub async fn scalar(&self, node: &'static str, sql: &str) -> anyhow::Result<String> {
        if let Some(v) = self.try_cached(node, sql).await {
            return Ok(v);
        }
        let client = self.connect(node).await?;
        let row = tokio::time::timeout(Duration::from_secs(5), client.query_one(sql, &[]))
            .await
            .context("query timeout")??;
        let v: String = row_text(&row)?;
        self.conns.lock().unwrap().insert(node, client);
        Ok(v)
    }

    async fn try_cached(&self, node: &'static str, sql: &str) -> Option<String> {
        let client = self.conns.lock().unwrap().remove(node)?;
        let res = tokio::time::timeout(Duration::from_secs(5), client.query_one(sql, &[])).await;
        match res {
            Ok(Ok(row)) => {
                let v = row_text(&row).ok()?;
                self.conns.lock().unwrap().insert(node, client);
                Some(v)
            }
            _ => None, // dropped: redial on the caller's path
        }
    }

    /// Statement with no interesting result (pause/resume/DDL).
    pub async fn execute(&self, node: &'static str, sql: &str) -> anyhow::Result<()> {
        // batch_execute supports multi-statement scripts.
        let client = self.connect(node).await?;
        tokio::time::timeout(Duration::from_secs(60), client.batch_execute(sql))
            .await
            .context("execute timeout")??;
        Ok(())
    }

    pub async fn is_in_recovery(&self, node: &'static str) -> Option<bool> {
        match self
            .scalar(node, "select pg_is_in_recovery()")
            .await
            .ok()?
            .as_str()
        {
            "t" | "true" => Some(true),
            "f" | "false" => Some(false),
            _ => None,
        }
    }

    pub async fn count_primaries(&self) -> usize {
        let mut c = 0;
        for n in NODES {
            if self.is_in_recovery(n).await == Some(false) {
                c += 1;
            }
        }
        c
    }

    pub async fn current_primary(&self) -> Option<&'static str> {
        for n in NODES {
            if self.is_in_recovery(n).await == Some(false) {
                return Some(n);
            }
        }
        None
    }

    /// Standbys streaming from `primary`, excluding pg_basebackup's WAL
    /// stream — an in-flight rebuild masquerades as a caught-up standby
    /// otherwise (bash suite run 11 declared a repair done while the
    /// basebackup was still copying).
    pub async fn streaming_count(&self, primary: &'static str) -> Option<i64> {
        self.scalar(
            primary,
            "select count(*)::text from pg_stat_replication \
             where state='streaming' and application_name <> 'pg_basebackup'",
        )
        .await
        .ok()?
        .parse()
        .ok()
    }

    pub async fn replay_lsn(&self, node: &'static str) -> Option<String> {
        self.scalar(node, "select coalesce(pg_last_wal_replay_lsn()::text, '')")
            .await
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// Receive-vs-replay gap in bytes on a standby.
    pub async fn replay_gap(&self, node: &'static str) -> Option<i64> {
        self.scalar(
            node,
            "select coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), \
             pg_last_wal_replay_lsn()), 0)::bigint::text",
        )
        .await
        .ok()?
        .parse()
        .ok()
    }
}

fn row_text(row: &tokio_postgres::Row) -> anyhow::Result<String> {
    // Everything the harness selects is cast to ::text (or boolean,
    // which tokio-postgres maps to bool — normalize via try_get).
    if let Ok(s) = row.try_get::<_, String>(0) {
        return Ok(s);
    }
    if let Ok(b) = row.try_get::<_, bool>(0) {
        return Ok(if b { "t".into() } else { "f".into() });
    }
    anyhow::bail!("unsupported scalar type");
}
