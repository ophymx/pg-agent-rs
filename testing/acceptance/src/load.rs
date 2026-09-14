//! Continuous write load: the ledger writer. Upgrades
//! the suite's data-survival claim from "one at-rest sentinel
//! survived" to "EVERY acknowledged row survived" — the actual
//! quorum-commit invariant — and lets the deposed primary's
//! hanging-commit behavior be OBSERVED under real concurrency instead
//! of assumed from a single probe.
//!
//! Protocol (per sequence number, strictly ascending):
//! - `INSERT .. ON CONFLICT (seq) DO NOTHING RETURNING seq`;
//! - a returned row is an ACKNOWLEDGMENT — the seq goes in `acked`
//!   and the suite later asserts it exists on the post-failover
//!   primary, every single one;
//! - zero rows returned means an earlier attempt of this seq was
//!   indeterminate (timed out mid-commit) but actually landed: it
//!   goes in `landed` and makes NO survival claim — the client was
//!   never told it committed;
//! - a timeout retries the SAME seq after re-discovering a primary,
//!   so an indeterminate commit can never be double-counted.
//!
//! Discovery is deliberately naive — first node answering
//! `pg_is_in_recovery() = false`, like a client with a static host
//! list — so during a fence-less deposal's dual-serving window the
//! writer really does hit the deposed primary and its commits really
//! do hang in the sync-rep wait (counted in `timeouts`; a per-node
//! cooldown after each timeout keeps the writer making progress
//! instead of starving forever on the corpse).
//!
//! The writer owns its connections (never the shared `Pg` cache): a
//! commit hung in ack starvation would wedge a pipelined cached
//! connection for every later check — G8 learned this once already.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

use crate::cluster::{container_ip, NODES};

const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const NODE_COOLDOWN: Duration = Duration::from_secs(8);
const PACING: Duration = Duration::from_millis(15);

pub struct Load {
    acked: Arc<Mutex<Vec<i64>>>,
    landed: Arc<AtomicUsize>,
    timeouts: Arc<AtomicUsize>,
    conn_errors: Arc<AtomicUsize>,
    max_ack_gap_ms: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

pub struct LoadStats {
    pub acked: Vec<i64>,
    pub landed: usize,
    pub timeouts: usize,
    pub conn_errors: usize,
    pub max_ack_gap_ms: usize,
}

impl Load {
    pub fn start() -> Self {
        let acked = Arc::new(Mutex::new(Vec::new()));
        let landed = Arc::new(AtomicUsize::new(0));
        let timeouts = Arc::new(AtomicUsize::new(0));
        let conn_errors = Arc::new(AtomicUsize::new(0));
        let max_ack_gap_ms = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(writer(
            acked.clone(),
            landed.clone(),
            timeouts.clone(),
            conn_errors.clone(),
            max_ack_gap_ms.clone(),
            stop.clone(),
        ));
        Self {
            acked,
            landed,
            timeouts,
            conn_errors,
            max_ack_gap_ms,
            stop,
            handle,
        }
    }

    pub fn acked_count(&self) -> usize {
        self.acked.lock().unwrap().len()
    }

    pub async fn stop(self) -> LoadStats {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.handle.await;
        LoadStats {
            acked: self.acked.lock().unwrap().clone(),
            landed: self.landed.load(Ordering::SeqCst),
            timeouts: self.timeouts.load(Ordering::SeqCst),
            conn_errors: self.conn_errors.load(Ordering::SeqCst),
            max_ack_gap_ms: self.max_ack_gap_ms.load(Ordering::SeqCst),
        }
    }
}

async fn connect(node: &'static str) -> anyhow::Result<tokio_postgres::Client> {
    let ip = container_ip(node).await?;
    let config = format!("host={ip} port=5432 user=postgres dbname=postgres connect_timeout=3");
    let (client, conn) = tokio_postgres::connect(&config, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// First node claiming NOT-in-recovery, honoring cooldowns. During
/// dual-serving more than one node claims it; the writer takes the
/// first like a naive client would — that is part of the test.
async fn discover(
    cooldown: &HashMap<&'static str, Instant>,
) -> Option<(&'static str, tokio_postgres::Client)> {
    for n in NODES {
        if cooldown.get(n).is_some_and(|t| t.elapsed() < NODE_COOLDOWN) {
            continue;
        }
        let Ok(client) = connect(n).await else {
            continue;
        };
        let probe = tokio::time::timeout(
            Duration::from_secs(3),
            client.query_one("select pg_is_in_recovery()", &[]),
        )
        .await;
        if let Ok(Ok(row)) = probe {
            if !row.get::<_, bool>(0) {
                return Some((n, client));
            }
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn writer(
    acked: Arc<Mutex<Vec<i64>>>,
    landed: Arc<AtomicUsize>,
    timeouts: Arc<AtomicUsize>,
    conn_errors: Arc<AtomicUsize>,
    max_ack_gap_ms: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) {
    let mut seq: i64 = 1;
    let mut target: Option<(&'static str, tokio_postgres::Client)> = None;
    let mut cooldown: HashMap<&'static str, Instant> = HashMap::new();
    let mut last_ack = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        if target.is_none() {
            target = discover(&cooldown).await;
            if target.is_none() {
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue;
            }
        }
        let (node, client) = target.as_ref().unwrap();
        let res = tokio::time::timeout(
            WRITE_TIMEOUT,
            client.query(
                "insert into ledger(seq) values ($1) on conflict (seq) do nothing returning seq",
                &[&seq],
            ),
        )
        .await;
        match res {
            Ok(Ok(rows)) => {
                if rows.is_empty() {
                    landed.fetch_add(1, Ordering::SeqCst);
                } else {
                    let gap = last_ack.elapsed().as_millis() as usize;
                    max_ack_gap_ms.fetch_max(gap, Ordering::SeqCst);
                    last_ack = Instant::now();
                    acked.lock().unwrap().push(seq);
                }
                seq += 1;
                tokio::time::sleep(PACING).await;
            }
            Ok(Err(_)) => {
                // Connection-level error (refused, reset, shutting
                // down): not evidence of starvation, just churn.
                conn_errors.fetch_add(1, Ordering::SeqCst);
                cooldown.insert(node, Instant::now());
                target = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {
                // TIMEOUT: the connection was accepted and the write
                // hung — the ack-starvation shape. Same seq retries
                // elsewhere; if the commit actually landed it will
                // surface as ON CONFLICT (landed, not acked).
                timeouts.fetch_add(1, Ordering::SeqCst);
                cooldown.insert(node, Instant::now());
                target = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
