//! Post-run event-order auditor: invariants checked over the ENTIRE
//! event history, in sequence order, after all scenarios finish.
//!
//! The scenarios assert liveness ("the takeover happened within its
//! budget"); the auditor asserts *order* — the safety claims that a
//! point sample can pass straight through. The motivating case: a
//! ~2 s dual-primary window (bash-era finding 14) that every sampled
//! `count_primaries` check missed and only manual log forensics
//! found. Here it is a standing assertion.
//!
//! Terms are the backbone: the lease mints them monotonically and
//! they are fencing tokens, so takeover/promotion events carry a
//! logical clock that makes cross-node ordering claims exact —
//! wall-clock receipt order is only needed for the serving-interval
//! overlap check, where millisecond tail latency is noise against the
//! seconds-wide windows the invariant guards.
//!
//! Every parser here is greppy, so a log-format drift could blind the
//! auditor into vacuous green — the non-vacuity floor (at least the
//! three scenario promotions must have been SEEN) turns that failure
//! mode into a loud one.

use std::collections::HashMap;

use std::time::Instant;

use crate::checks::Ctx;
use crate::events::{Cursor, Event, Source};

/// Extract a `term: N` / `term=N` field from an agent log line.
fn term_of(line: &str) -> Option<u64> {
    for pat in ["term: ", "term="] {
        if let Some(idx) = line.find(pat) {
            let digits: String = line[idx + pat.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !digits.is_empty() {
                return digits.parse().ok();
            }
        }
    }
    None
}

/// A node's PostgreSQL entering normal (writable) operation. Exact
/// phrase: the standby variant is "ready to accept read-only
/// connections", which does not contain this string. (The first
/// attempt guarded with `!contains("read")` — but "ready" contains
/// "read", so it matched NOTHING and two invariants were silently
/// vacuous. The non-vacuity floor now covers serving events too.)
fn serving_start(ev: &Event) -> bool {
    ev.source == Source::Postgres && ev.line.contains("is ready to accept connections")
}

/// Write service ENDS at the shutdown *request* — PostgreSQL
/// disconnects clients and refuses new ones right there, while
/// draining walsenders can hold the final "is shut down" for tens of
/// seconds (observed 44 s on a partitioned primary heading toward
/// wal_sender_timeout). Using only the final line would report
/// dual-primary for windows where no client could write. "is shut
/// down" stays as the fallback end for paths with no request line.
/// The crash shape (G9) has no PostgreSQL-side goodbye at all — a
/// SIGKILLed postmaster writes neither of the above — so systemd's
/// unit journal carries the death event instead. Which line that is
/// varies by distro: Debian 13 logs the main process dying
/// ("code=killed"), while Ubuntu 24.04 lets pg_ctlcluster run, find no
/// cluster, and exit 2, recording "Control process exited,
/// code=exited". Both always emit `Failed with result`, so that is the
/// portable marker; `code=killed` stays because matching more ways for
/// a unit to have died can only close intervals earlier, never later,
/// and an unclosed interval is what would raise a false dual-primary.
fn serving_end(ev: &Event) -> bool {
    ev.source == Source::Postgres
        && (ev.line.contains("shutdown request")
            || ev.line.contains("database system is shut down")
            || ev.line.contains("code=killed")
            || ev.line.contains("Failed with result"))
}

/// What a timed-out await turned out to be, once the whole run's log
/// is available to ask.
#[derive(Debug, PartialEq)]
pub enum TimeoutKind {
    /// Nothing ever matched.
    Never,
    /// Matched, but only after the wait gave up — by this much.
    Late(std::time::Duration),
    /// Matched something the log already held when the wait gave up.
    Missed,
}

/// How far before the give-up an event must be stamped before the
/// sweep will call it a harness bug.
///
/// `gave_up_at` is read just *after* `await_matching` returns, so an
/// event appended between its final scan and its deadline check lands
/// microseconds on the wrong side of the line while being nobody's
/// fault. Without a tolerance the MISSED check — the one assertion
/// here — would flap on that race, and a flapping check teaches people
/// to ignore it. A genuine "the log already had it" is seconds early,
/// not milliseconds.
const MISSED_TOLERANCE: std::time::Duration = std::time::Duration::from_millis(250);

/// The verdict, split from the sweep so it is testable without a
/// cluster: everything docker-shaped is in the caller, and the part
/// that decides what a failure MEANS is a function of two timestamps.
pub fn classify_timeout(gave_up_at: Instant, found: Option<&Event>) -> TimeoutKind {
    match found {
        None => TimeoutKind::Never,
        Some(ev) if ev.at + MISSED_TOLERANCE < gave_up_at => TimeoutKind::Missed,
        Some(ev) => TimeoutKind::Late(ev.at.saturating_duration_since(gave_up_at)),
    }
}

/// Ask each node what it actually WROTE, and compare with what the run
/// heard.
///
/// The census counts received events, which answers "were we
/// listening" only in the total-silence case. It cannot size the hole a
/// tail death leaves, and says so: *"a hole in `counts` of unknown
/// size, because the respawn resumes tail-only."* Every event-order
/// claim in this file, and every `check_absent` in every scenario,
/// rests on that hole being small.
///
/// So ask the source. `journalctl -o cat` is the same view the tail
/// consumes, so the counts are comparable line for line, and the
/// difference is the size of what we missed.
///
/// Reported, not asserted. G10 SIGKILLs PID 1 in all three containers
/// by design, so some loss is structural, and a threshold picked
/// without data would be a number pretending to be a rule. What the
/// number is FOR is reading a red run: a node missing hundreds of its
/// own lines explains a failure that looks like the cluster went
/// quiet, and one missing none rules that explanation out.
async fn heard_vs_written(cx: &mut Ctx) {
    let census = cx.log.census();
    let heard = |node: &str, source: Source| -> usize {
        census
            .counts
            .iter()
            .find(|((n, s), _)| *n == node && *s == source)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    };
    let unit = crate::cluster::pg_unit();
    let log_path = crate::cluster::pg_log();
    let mut rows = Vec::new();
    for node in crate::cluster::NODES {
        // Agent: one unit, one tail, a clean comparison.
        let written = count_lines(node, "journalctl -u pg_agentd --no-pager -o cat").await;
        rows.push((node, "Agent", heard(node, Source::Agent), written));
        // Postgres is two tails merged into one source (the server log
        // file and the unit journal), so the comparison has to add the
        // same two things back together.
        let file = count_lines(node, &format!("cat {log_path} 2>/dev/null")).await;
        let journal = count_lines(
            node,
            &format!("journalctl -u {unit} --no-pager -o cat 2>/dev/null"),
        )
        .await;
        rows.push((
            node,
            "Postgres",
            heard(node, Source::Postgres),
            file.zip(journal).map(|(f, j)| f + j),
        ));
    }
    let rendered: Vec<String> = rows
        .iter()
        .map(|(node, source, heard, written)| match written {
            // A negative delta means the source has FEWER lines than we
            // received, which is not us missing anything — it is the
            // journal having been rotated or wiped (the container's
            // journald is volatile, and G10 restarts it). Say that
            // rather than printing a nonsense deficit.
            Some(w) if *w >= *heard => format!("{node}/{source}={heard}/{w}"),
            Some(w) => format!("{node}/{source}={heard}/{w}(rotated)"),
            None => format!("{node}/{source}={heard}/?"),
        })
        .collect();
    cx.note(&format!("heard/written: {}", rendered.join(" ")));
    let missed: Vec<String> = rows
        .iter()
        .filter_map(|(node, source, heard, written)| {
            written
                .filter(|w| w > heard)
                .map(|w| format!("{node}/{source} missed {}", w - heard))
        })
        .collect();
    if !missed.is_empty() {
        cx.note(&format!(
            "lines written but never heard: {} — every absence claim over \
             the affected windows is that much weaker",
            missed.join(", ")
        ));
    }
}

/// `wc -l` over a command's output inside a node, or `None` if the node
/// could not be asked (a torn-down container is not a measurement).
async fn count_lines(node: &str, script: &str) -> Option<usize> {
    crate::cluster::exec(node, &format!("{script} | wc -l"))
        .await
        .ok()
        .and_then(|out| out.trim().parse().ok())
}

/// Re-run every timed-out await against the finished log, and say which
/// kind of failure each one was.
///
/// `await_event` can only report "not within the budget". That is three
/// different findings wearing one label, and finding 30 is what it
/// costs to not separate them:
///
/// - **LATE** — the line is in the log, stamped after the wait gave up.
///   The cluster did announce it; the budget (or the pipe carrying it)
///   was too tight for how slow this run was. Reading these as product
///   failures is what made a slow Rocky cell look broken.
/// - **NEVER** — nothing ever matched. The awaited thing genuinely did
///   not happen, or its stream was not being listened to. This is the
///   only kind worth reading as a product failure without more work.
/// - **MISSED** — the line was already in the log when the wait gave
///   up. That is not a cluster fact at all, it is `await_matching`
///   failing to see something it held; a harness bug, and a check
///   rather than a note.
///
/// The delay is the discriminator, and it is reported rather than
/// thresholded: a match 2s past a 60s budget is the awaited line, while
/// one 400s past it is probably a later occurrence of the same message
/// in a different scenario. The predicate cannot tell those apart — a
/// reader with the number can.
fn late_arrival_sweep(cx: &mut Ctx) {
    let watches = std::mem::take(&mut cx.late_watches);
    if watches.is_empty() {
        return;
    }
    let mut missed = Vec::new();
    for w in &watches {
        let found = cx.log.find(w.from, &*w.pred);
        match classify_timeout(w.gave_up_at, found.as_ref()) {
            TimeoutKind::Never => cx.note(&format!(
                "timeout was NEVER: {} — nothing matched in the whole run \
                 (budget {}s)",
                w.desc,
                w.budget.as_secs()
            )),
            TimeoutKind::Late(by) => cx.note(&format!(
                "timeout was LATE: {} — matched {}s after the {}s budget expired, on {}",
                w.desc,
                by.as_secs(),
                w.budget.as_secs(),
                found.as_ref().map(|e| e.node).unwrap_or("?")
            )),
            TimeoutKind::Missed => {
                missed.push(w.desc.clone());
                cx.note(&format!(
                    "timeout was MISSED: {} — the matching line was already in the log \
                     when the wait gave up ({})",
                    w.desc,
                    found.as_ref().map(|e| e.line.as_str()).unwrap_or("")
                ));
            }
        }
    }
    // Only this one is an assertion. LATE and NEVER describe the
    // cluster (or the budget); MISSED describes the suite, and a suite
    // that cannot see what it is holding invalidates every await in the
    // run, not just the one that reported.
    cx.check(
        &format!(
            "audit: no await timed out on an event the log already had{}",
            if missed.is_empty() {
                String::new()
            } else {
                format!(" — {}", missed.join("; "))
            }
        ),
        missed.is_empty(),
    );
}

pub async fn run(cx: &mut Ctx) {
    cx.say("AUDIT: event-order invariants over the whole run");
    let all = cx.log.find_all(Cursor(0), |_| true);

    // Before any claim about what the cluster did: did we hear every
    // node at all? Every invariant below, and every `check_absent` in
    // every scenario, is only as good as the stream it reads. A node
    // whose agent tail died reports as a well-behaved cluster that
    // simply never acted — silently, and in the *safe*-looking
    // direction, which is the worst way for a test to be wrong.
    let census = cx.log.census();
    let (counts, deaths) = (&census.counts, &census.deaths);
    cx.note(&format!(
        "stream census: {}",
        counts
            .iter()
            .map(|((node, source), n)| format!("{node}/{source:?}={n}"))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    for ((node, source), n) in deaths {
        cx.note(&format!("tail deaths: {node}/{source:?} respawned {n}x"));
    }
    // A re-attach is a death the watchdog got ahead of: the container
    // restarted, so the old exec was attached to a corpse whether or
    // not it had noticed yet. The gap is bounded by the poll interval,
    // where a death's gap is bounded by nothing.
    for ((node, source), n) in &census.reattaches {
        cx.note(&format!(
            "container restarts: {node}/{source:?} re-attached {n}x"
        ));
    }
    // Deliberately a NOTE and not a check: G10 SIGKILLs PID 1 in all
    // three containers, so every stream on every node dies there by
    // design, and a run with zero deaths would mean G10 did not do its
    // job. Measured: 18 deaths in a clean 290/1 run, all of them
    // inside G10. What the census is FOR is the other case — a death
    // in a scenario that never touched the container, which is the
    // one worth reading the surrounding failures with suspicion.
    // A node that contributed no agent events at all never had a
    // stream to lose — a different bug from one that died mid-run, and
    // equally fatal to every claim made about that node.
    late_arrival_sweep(cx);
    heard_vs_written(cx).await;

    let silent: Vec<&'static str> = crate::cluster::NODES
        .iter()
        .copied()
        .filter(|n| {
            !counts
                .iter()
                .any(|((node, source), c)| node == n && *source == Source::Agent && *c > 0)
        })
        .collect();
    cx.check(
        &format!("audit: every node produced agent events{}", {
            if silent.is_empty() {
                String::new()
            } else {
                format!(" — silent: {}", silent.join(", "))
            }
        }),
        silent.is_empty(),
    );

    let takeovers: Vec<(&Event, u64)> = all
        .iter()
        .filter(|ev| ev.source == Source::Agent && ev.line.contains("TookOver"))
        .filter_map(|ev| term_of(&ev.line).map(|t| (ev, t)))
        .collect();
    let promotions: Vec<(&Event, u64)> = all
        .iter()
        .filter(|ev| ev.source == Source::Agent && ev.line.contains("roleexec: promotion complete"))
        .filter_map(|ev| term_of(&ev.line).map(|t| (ev, t)))
        .collect();
    let fences: Vec<&Event> = all
        .iter()
        .filter(|ev| ev.source == Source::Agent && ev.line.contains("FENCING"))
        .collect();
    cx.note(&format!(
        "audited {} events: {} takeovers, {} promotions, {} fences",
        all.len(),
        takeovers.len(),
        promotions.len(),
        fences.len()
    ));

    // Non-vacuity: the suite runs at least three real failovers (G3,
    // G5, G7). Seeing fewer promotions — or fewer PostgreSQL serving
    // starts than promotions + the bootstrap primary — means a parser
    // went blind (log format drift, or a bad guard: the first
    // serving_start matched nothing for exactly this reason), and the
    // invariants below would pass on nothing.
    let serving_starts = all.iter().filter(|ev| serving_start(ev)).count();
    cx.check(
        &format!(
            "audit: parsers saw the suite's promotions and serving starts \
             (non-vacuous: {} promotions, {serving_starts} serving starts)",
            promotions.len()
        ),
        promotions.len() >= 3
            && !takeovers.is_empty()
            && !fences.is_empty()
            && serving_starts > promotions.len(),
    );

    // Every promotion is authorized by an earlier takeover of the SAME
    // term on the SAME node — promotion never happens on spec.
    let unpaired: Vec<String> = promotions
        .iter()
        .filter(|(pev, pterm)| {
            !takeovers
                .iter()
                .any(|(tev, tterm)| tev.node == pev.node && tterm == pterm && tev.seq < pev.seq)
        })
        .map(|(pev, pterm)| format!("{} promoted at term {pterm} without its takeover", pev.node))
        .collect();
    cx.check(
        &format!(
            "audit: every promotion paired with a prior same-term takeover on its node{}",
            if unpaired.is_empty() {
                String::new()
            } else {
                format!(" — {}", unpaired.join("; "))
            }
        ),
        unpaired.is_empty(),
    );

    // Terms are fencing tokens: no term is ever won by two nodes, and
    // takeover terms never regress in event order.
    let mut by_term: HashMap<u64, &'static str> = HashMap::new();
    let mut split_claim = None;
    for (ev, term) in &takeovers {
        match by_term.get(term) {
            Some(owner) if *owner != ev.node => {
                split_claim = Some(format!("term {term} won by both {owner} and {}", ev.node));
            }
            _ => {
                by_term.insert(*term, ev.node);
            }
        }
    }
    cx.check(
        &format!(
            "audit: each term won by exactly one node{}",
            split_claim
                .clone()
                .map(|s| format!(" — {s}"))
                .unwrap_or_default()
        ),
        split_claim.is_none(),
    );
    let regression = takeovers
        .windows(2)
        .find(|w| w[1].1 < w[0].1)
        .map(|w| format!("term {} after term {}", w[1].1, w[0].1));
    cx.check(
        &format!(
            "audit: takeover terms never regress in event order{}",
            regression
                .clone()
                .map(|s| format!(" — {s}"))
                .unwrap_or_default()
        ),
        regression.is_none(),
    );

    // Continuous single-primary: reconstruct per-node "serving as a
    // writable primary" intervals from the PostgreSQL logs and assert
    // no two nodes' intervals ever overlap — over the whole run, not
    // at sample instants. (Only db0 serves during the pre-scenario
    // history replay, so arbitrary cross-node interleaving there
    // cannot fabricate an overlap.)
    //
    // Exemption: a scenario may DECLARE an expected dual-serving
    // window (`Ctx::expect_dual_serving`) — the agent-death deposal is
    // fence-less by construction, so the old primary keeps serving at
    // the PostgreSQL level until the phantom check fences it on agent
    // restart. The declaration is not a blank pass: the overlap must
    // be covered by a window (right node, at/after the declared
    // cursor), and the window must CLOSE — the declared node's serving
    // must END after the rival's start. An overlap that never closes
    // is the real split-brain the invariant exists for.
    let windows = cx.expected_dual_serving.clone();
    let mut open: Option<&'static str> = None;
    let mut overlap = None;
    let mut exempted = 0usize;
    for ev in &all {
        if serving_start(ev) {
            match open {
                Some(existing) if existing != ev.node => {
                    let declared = windows
                        .iter()
                        .any(|(from, node)| *node == existing && ev.seq >= from.0);
                    let closes = declared
                        && all
                            .iter()
                            .any(|e| e.node == existing && e.seq > ev.seq && serving_end(e));
                    if closes {
                        exempted += 1;
                        open = Some(ev.node);
                    } else if declared {
                        overlap = Some(format!(
                            "declared dual-serving window for {existing} never closed \
                             ({} began serving at seq {} and {existing} never stopped)",
                            ev.node, ev.seq
                        ));
                    } else {
                        overlap = Some(format!(
                            "{} began serving while {existing} was still serving (seq {})",
                            ev.node, ev.seq
                        ));
                    }
                }
                _ => open = Some(ev.node),
            }
        } else if serving_end(ev) && open == Some(ev.node) {
            open = None;
        }
    }
    cx.check(
        &format!(
            "audit: no undeclared concurrent primaries ({exempted} declared \
             window{} verified closed){}",
            if exempted == 1 { "" } else { "s" },
            overlap
                .clone()
                .map(|s| format!(" — {s}"))
                .unwrap_or_default()
        ),
        overlap.is_none() && exempted == windows.len(),
    );

    // Every fence completed. The claim is about STATE, not about a
    // shutdown line per fence: if the node was serving when the fence
    // fired, a shutdown must follow before any new serving start. A
    // fence of an already-stopped PostgreSQL (idempotent re-decides
    // every tick while isolated; a fence racing a harness-issued stop)
    // is satisfied trivially.
    let serving_at = |node: &str, seq: usize| -> bool {
        all.iter()
            .filter(|ev| ev.node == node && ev.seq < seq)
            .fold(false, |open, ev| {
                if serving_start(ev) {
                    true
                } else if serving_end(ev) {
                    false
                } else {
                    open
                }
            })
    };
    let unfenced: Vec<String> = fences
        .iter()
        .filter(|fev| {
            if !serving_at(fev.node, fev.seq) {
                return false; // already stopped — nothing to complete
            }
            // Serving at fence time: the node's next serving-state
            // transition must be an end, not another start.
            match all.iter().find(|ev| {
                ev.node == fev.node && ev.seq > fev.seq && (serving_start(ev) || serving_end(ev))
            }) {
                Some(ev) => !serving_end(ev),
                None => true, // never stopped serving after the fence
            }
        })
        .map(|fev| format!("{} (seq {})", fev.node, fev.seq))
        .collect();
    cx.check(
        &format!(
            "audit: every fence of a serving node reached PostgreSQL shutdown{}",
            if unfenced.is_empty() {
                String::new()
            } else {
                format!(" — unfinished: {}", unfenced.join(", "))
            }
        ),
        unfenced.is_empty(),
    );
    cx.check_absent("audit: no fence ever failed", Cursor(0), |ev| {
        ev.source == Source::Agent && ev.line.contains("FENCE FAILED")
    });

    // Finding 22, as a standing invariant: no standby may ever lose
    // its WAL window. The failure is unmistakable in the PostgreSQL
    // log and previously surfaced only as a mysterious wedge that the
    // repair path quietly recloned — a full rebuild where slot timing
    // (now: slots reserved AT promotion) and a wal_keep_size floor
    // should have preserved the stream.
    cx.check_absent(
        "audit: no standby ever lost its WAL window (segment already removed)",
        Cursor(0),
        |ev| {
            ev.source == Source::Postgres
                && ev.line.contains("has already been removed")
                && ev.line.contains("WAL segment")
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ev_at(at: Instant) -> Event {
        Event {
            seq: 0,
            node: "db0",
            source: Source::Agent,
            line: "ha decision".into(),
            at,
        }
    }

    /// The finding-30 case, and the only one the suite could not name
    /// before: the cluster DID announce it, after the budget ran out.
    /// A failure like this says the budget was too tight for how slow
    /// the run was — not that the product stopped working.
    #[test]
    fn a_match_after_the_give_up_is_late_with_the_delay_measured() {
        let gave_up = Instant::now();
        let found = ev_at(gave_up + Duration::from_secs(7));
        assert_eq!(
            classify_timeout(gave_up, Some(&found)),
            TimeoutKind::Late(Duration::from_secs(7))
        );
    }

    /// The only kind that should be read as a product failure without
    /// further work.
    #[test]
    fn no_match_anywhere_is_never() {
        assert_eq!(classify_timeout(Instant::now(), None), TimeoutKind::Never);
    }

    /// A harness bug, not a cluster fact: the log already held the line
    /// when the wait gave up, so `await_matching` failed to see
    /// something in front of it. This one is a check, because it
    /// invalidates every await in the run rather than just its own.
    #[test]
    fn a_match_well_before_the_give_up_is_a_missed_event() {
        let gave_up = Instant::now();
        let found = ev_at(gave_up - Duration::from_secs(1));
        assert_eq!(classify_timeout(gave_up, Some(&found)), TimeoutKind::Missed);
    }

    /// The boundary is the one place this can flap. `gave_up_at` is
    /// read just after the waiter returns, so an event appended between
    /// its last scan and its deadline check is stamped a hair EARLY
    /// through nobody's fault. Inside the tolerance it must not be
    /// called a harness bug — the MISSED check is an assertion, and an
    /// assertion that fires on timing noise gets ignored.
    #[test]
    fn the_give_up_race_is_not_reported_as_a_harness_bug() {
        let gave_up = Instant::now();
        for early_ms in [0, 1, 50, 249] {
            let found = ev_at(gave_up - Duration::from_millis(early_ms));
            assert_eq!(
                classify_timeout(gave_up, Some(&found)),
                TimeoutKind::Late(Duration::ZERO),
                "{early_ms}ms early must not be a harness bug"
            );
        }
    }
}
