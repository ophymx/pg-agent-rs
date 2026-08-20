//! The suite's event backbone: every agent-journal and PostgreSQL-log
//! line from every node, tailed live into one ordered in-memory log.
//!
//! This replaces the bash suite's `journalctl --since <timestamp>`
//! re-queries and point-in-time greps with ordering primitives:
//!
//! - a [`Cursor`] marks "now" in the stream (replacing wall-clock
//!   `--since` bounds — scenario windows are event-ordered, not timed);
//! - [`EventLog::await_matching`] blocks until a matching event exists
//!   at or after a cursor (or a liveness budget expires);
//! - [`EventLog::find`] scans history non-blockingly, which is what
//!   absence assertions and post-hoc audits use — over the whole
//!   window, not at a sample instant.
//!
//! Tails run over `docker exec`, which rides the API socket, not
//! `pga-net` — a partitioned node's events keep flowing, which is
//! exactly when they matter most. Each tail restarts itself (without
//! replaying history) if its exec dies.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    Agent,
    Postgres,
}

/// Drop ANSI CSI escape sequences (`ESC [ ... <final byte>`); any
/// other lone ESC is dropped too.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'[') {
            chars.next();
            for t in chars.by_ref() {
                if ('\x40'..='\x7e').contains(&t) {
                    break;
                }
            }
        }
    }
    out
}

#[derive(Clone, Debug)]
pub struct Event {
    /// Position in the suite-wide stream — the ordering the auditor
    /// reasons with. Cross-node order is receipt order, which is
    /// real-time order for live events (tail latency is milliseconds);
    /// only the initial history replay interleaves arbitrarily across
    /// nodes, and no audited claim spans two nodes' replayed history.
    pub seq: usize,
    pub node: &'static str,
    pub source: Source,
    pub line: String,
    /// Host receipt time. Fine for second-granularity liveness
    /// measurements (e.g. the takeover hysteresis gaps), never used as
    /// a safety bound.
    pub at: Instant,
}

/// Position in the event stream. Obtained BEFORE causing something, so
/// "did X happen" is always asked about the events after the cause.
#[derive(Clone, Copy, Debug)]
pub struct Cursor(pub usize);

/// One stream's identity: which node, which of its logs.
pub type StreamKey = (&'static str, Source);

/// What the run actually heard, per stream — the ground truth behind
/// every event-order claim the auditor makes.
pub struct Census {
    /// Events received per stream.
    pub counts: Vec<(StreamKey, usize)>,
    /// Tail deaths per stream. Each one is a hole in `counts` of
    /// unknown size, because the respawn resumes tail-only.
    pub deaths: Vec<(StreamKey, usize)>,
}

#[derive(Default)]
struct Inner {
    events: Vec<Event>,
    /// How many times each stream's tail has died and respawned.
    tail_deaths: Vec<(StreamKey, usize)>,
}

pub struct EventLog {
    inner: Mutex<Inner>,
    notify: Notify,
}

impl EventLog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
        })
    }

    fn record_tail_death(&self, node: &'static str, source: Source) {
        let mut inner = self.inner.lock().unwrap();
        match inner
            .tail_deaths
            .iter_mut()
            .find(|(k, _)| *k == (node, source))
        {
            Some((_, n)) => *n += 1,
            None => inner.tail_deaths.push(((node, source), 1)),
        }
    }

    /// Per-stream event counts and tail deaths, for the end-of-run
    /// census. A stream that went quiet is the difference between "the
    /// cluster did not do it" and "we were not listening", and only
    /// this can tell them apart.
    pub fn census(&self) -> Census {
        let inner = self.inner.lock().unwrap();
        let mut counts: Vec<(StreamKey, usize)> = Vec::new();
        for ev in &inner.events {
            match counts.iter_mut().find(|(k, _)| *k == (ev.node, ev.source)) {
                Some((_, n)) => *n += 1,
                None => counts.push(((ev.node, ev.source), 1)),
            }
        }
        counts.sort_by_key(|((node, source), _)| (*node, format!("{source:?}")));
        let mut deaths = inner.tail_deaths.clone();
        deaths.sort_by_key(|((node, source), _)| (*node, format!("{source:?}")));
        Census { counts, deaths }
    }

    fn append(&self, mut ev: Event) {
        // The daemon's tracing formatter styles field NAMES with ANSI
        // escapes, and journald stores the raw bytes — so a line can
        // contain `\x1b[3mterm\x1b[0m\x1b[2m=\x1b[0m2`, which no
        // substring or field parser should have to know about.
        // Normalize once at ingestion.
        if ev.line.contains('\x1b') {
            ev.line = strip_ansi(&ev.line);
        }
        let mut inner = self.inner.lock().unwrap();
        ev.seq = inner.events.len();
        inner.events.push(ev);
        drop(inner);
        self.notify.notify_waiters();
    }

    /// Marks "now": events appended after this call have seq >= the
    /// returned cursor.
    pub fn cursor(&self) -> Cursor {
        Cursor(self.inner.lock().unwrap().events.len())
    }

    /// Non-blocking scan of `[from..]` for the first match.
    pub fn find<F>(&self, from: Cursor, pred: F) -> Option<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let inner = self.inner.lock().unwrap();
        inner.events[from.0.min(inner.events.len())..]
            .iter()
            .find(|e| pred(e))
            .cloned()
    }

    /// All matches in `[from..]` (for multi-event audits).
    pub fn find_all<F>(&self, from: Cursor, pred: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let inner = self.inner.lock().unwrap();
        inner.events[from.0.min(inner.events.len())..]
            .iter()
            .filter(|e| pred(e))
            .cloned()
            .collect()
    }

    /// Block until an event at/after `from` matches, or `budget`
    /// expires. The budget is a liveness bound on the wait, never part
    /// of the match criteria.
    pub async fn await_matching<F>(&self, from: Cursor, budget: Duration, pred: F) -> Option<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let deadline = Instant::now() + budget;
        let mut scanned = from.0;
        loop {
            {
                let inner = self.inner.lock().unwrap();
                let upto = inner.events.len();
                if let Some(ev) = inner.events[scanned.min(upto)..upto]
                    .iter()
                    .find(|e| pred(e))
                {
                    return Some(ev.clone());
                }
                scanned = upto;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let _ = tokio::time::timeout(deadline - now, self.notify.notified()).await;
        }
    }

    /// Spawn the tails for one node: the pg_agentd journal and the
    /// PostgreSQL server log. History is replayed once (`-n all` /
    /// `-n +1`) so the log covers everything since container start;
    /// respawns after a died exec resume tail-only.
    pub fn spawn_node_tails(self: &Arc<Self>, node: &'static str) {
        self.spawn_tail(
            node,
            Source::Agent,
            "journalctl -u pg_agentd -f -n all --no-pager -o cat".to_string(),
            "journalctl -u pg_agentd -f -n 0 --no-pager -o cat".to_string(),
        );
        let log = crate::cluster::pg_log();
        self.spawn_tail(
            node,
            Source::Postgres,
            format!("tail -F -n +1 {log} 2>/dev/null"),
            format!("tail -F -n 0 {log} 2>/dev/null"),
        );
        // The unit journal, also as Postgres events: a SIGKILLed
        // postmaster writes nothing to its log file — systemd's
        // "Main process exited, code=killed" report is the ONLY event
        // a crash-shape death leaves (G9), and the auditor's serving
        // intervals need it. The journal carries unit lifecycle
        // messages, not the server log, so it cannot duplicate the
        // file tail's serving_start/serving_end lines.
        let unit = crate::cluster::pg_unit();
        self.spawn_tail(
            node,
            Source::Postgres,
            format!("journalctl -u {unit} -f -n all --no-pager -o cat"),
            format!("journalctl -u {unit} -f -n 0 --no-pager -o cat"),
        );
    }

    fn spawn_tail(
        self: &Arc<Self>,
        node: &'static str,
        source: Source,
        first_cmd: String,
        respawn_cmd: String,
    ) {
        let log = Arc::clone(self);
        tokio::spawn(async move {
            let mut cmd = first_cmd;
            loop {
                let child = Command::new("docker")
                    .args(["exec", &format!("pga-{node}"), "bash", "-c", &cmd])
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn();
                if let Ok(mut child) = child {
                    if let Some(stdout) = child.stdout.take() {
                        let mut lines = BufReader::new(stdout).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            log.append(Event {
                                seq: 0, // assigned in append
                                node,
                                source,
                                line,
                                at: Instant::now(),
                            });
                        }
                    }
                    let _ = child.wait().await;
                }
                // Exec died (container recreate, docker hiccup). Tail
                // from "now" — history is already in the log.
                //
                // SAY SO. The respawn resumes tail-only, so every line
                // written between the death and the new tail attaching
                // is gone from the event log for good — and the suite's
                // event awaits would then fail as "the cluster never
                // did X" when the truth is "we stopped listening",
                // which is indistinguishable from a product bug at
                // every call site.
                //
                // Most deaths are legitimate: G10 SIGKILLs PID 1 in all
                // three containers, so every stream dies there by
                // design (18 of them in a clean run, all in G10). The
                // point is not to forbid it, it is to make the gap
                // VISIBLE — see testing/README.md finding 30, where
                // this instrumentation refuted the very hypothesis it
                // was built to confirm.
                log.record_tail_death(node, source);
                println!(
                    "     NOTE: tail died and respawned: {node}/{source:?} — events \
                     logged in the gap are LOST (assertions over this window may \
                     fail spuriously)"
                );
                cmd = respawn_cmd.clone();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
