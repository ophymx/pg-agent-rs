//! Scenario context: PASS/FAIL accounting, timed section headers, and
//! the two wait primitives — a polling `wait_until` for conditions
//! that are only observable by asking (systemd state, pcp maps), and
//! the event-log awaits for anything the cluster *announces*.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::events::{Cursor, Event, EventLog};
use crate::pg::Pg;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[90m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

pub struct Ctx {
    pub log: Arc<EventLog>,
    pub pg: Arc<Pg>,
    pub pass: usize,
    pub failures: Vec<String>,
    /// Declared dual-serving windows for the audit: `(from, node)` —
    /// the scenario asserts that `node` will keep serving as primary
    /// past a rival's promotion (the fence-less agent-death deposal is
    /// the one legitimate case), and the auditor holds it to that
    /// declaration: the overlap must be covered by a window AND the
    /// window must CLOSE (the node's serving eventually ends).
    pub expected_dual_serving: Vec<(Cursor, &'static str)>,
    suite_t0: Instant,
    last_say: Instant,
}

impl Ctx {
    pub fn new(log: Arc<EventLog>, pg: Arc<Pg>) -> Self {
        let now = Instant::now();
        Self {
            log,
            pg,
            pass: 0,
            failures: Vec::new(),
            expected_dual_serving: Vec::new(),
            suite_t0: now,
            last_say: now,
        }
    }

    /// Declare an expected dual-serving window (see the field docs).
    pub fn expect_dual_serving(&mut self, node: &'static str, from: Cursor) {
        self.expected_dual_serving.push((from, node));
    }

    pub fn say(&mut self, title: &str) {
        let now = Instant::now();
        println!(
            "\n{BOLD}== {title}{RESET} {DIM}[+{}s, t={}s]{RESET}",
            (now - self.last_say).as_secs(),
            (now - self.suite_t0).as_secs()
        );
        self.last_say = now;
    }

    pub fn note(&self, msg: &str) {
        println!("     NOTE: {msg}");
    }

    pub fn pass(&mut self, desc: &str) {
        self.pass += 1;
        println!("   {GREEN}PASS{RESET} {desc}");
    }

    pub fn fail(&mut self, desc: &str) {
        self.failures.push(desc.to_string());
        println!("   {RED}FAIL{RESET} {desc}");
    }

    pub fn check(&mut self, desc: &str, cond: bool) {
        if cond {
            self.pass(desc);
        } else {
            self.fail(desc);
        }
    }

    /// Poll `pred` once a second until true (PASS, annotated with the
    /// actual wait) or `budget` seconds elapse (FAIL). Returns success.
    pub async fn wait_until<F, Fut>(&mut self, budget: u64, desc: &str, pred: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let t0 = Instant::now();
        loop {
            if pred().await {
                let waited = t0.elapsed().as_secs();
                if waited > 0 {
                    self.pass(&format!("{desc} ({waited}s)"));
                } else {
                    self.pass(desc);
                }
                return true;
            }
            if t0.elapsed() >= Duration::from_secs(budget) {
                self.fail(&format!("timeout: {desc}"));
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Await an event at/after `from` matching `pred`. PASS with the
    /// wait annotated; FAIL on budget expiry. Returns the event.
    pub async fn await_event<F>(
        &mut self,
        budget: u64,
        desc: &str,
        from: Cursor,
        pred: F,
    ) -> Option<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let t0 = Instant::now();
        let hit = self
            .log
            .await_matching(from, Duration::from_secs(budget), pred)
            .await;
        match hit {
            Some(ev) => {
                let waited = t0.elapsed().as_secs();
                if waited > 0 {
                    self.pass(&format!("{desc} ({waited}s)"));
                } else {
                    self.pass(desc);
                }
                Some(ev)
            }
            None => {
                self.fail(&format!("timeout: {desc}"));
                None
            }
        }
    }

    /// Assert nothing matching `pred` has been logged since `from`.
    /// Absence over a WINDOW of history, not at a sample instant — the
    /// window is bounded by the events the caller has already awaited.
    pub fn check_absent<F>(&mut self, desc: &str, from: Cursor, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        match self.log.find(from, pred) {
            None => self.pass(desc),
            Some(ev) => self.fail(&format!("{desc} — violated by {}: {}", ev.node, ev.line)),
        }
    }

    pub fn summary(&self) -> bool {
        println!("PASS={} FAIL={}", self.pass, self.failures.len());
        for f in &self.failures {
            println!("  - {f}");
        }
        self.failures.is_empty()
    }
}
