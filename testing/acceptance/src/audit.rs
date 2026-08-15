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

/// A node's PostgreSQL entering normal (writable) operation. Genuine
/// primaries only: standbys log the "read only" variant, which the
/// `!read` guard excludes (robust to hyphenation across PG versions).
fn serving_start(ev: &Event) -> bool {
    ev.source == Source::Postgres
        && ev.line.contains("database system is ready to accept")
        && !ev.line.contains("read")
}

fn serving_end(ev: &Event) -> bool {
    ev.source == Source::Postgres && ev.line.contains("database system is shut down")
}

pub fn run(cx: &mut Ctx) {
    cx.say("AUDIT: event-order invariants over the whole run");
    let all = cx.log.find_all(Cursor(0), |_| true);

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

    // Non-vacuity: the suite runs three real failovers (G3, G5, G7).
    // Seeing fewer promotions means the parsers went blind (log format
    // drift), and every invariant below would pass on nothing.
    cx.check(
        "audit: parsers saw the suite's promotions (non-vacuous)",
        promotions.len() >= 3 && !takeovers.is_empty() && !fences.is_empty(),
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
    let mut open: Option<&'static str> = None;
    let mut overlap = None;
    for ev in &all {
        if serving_start(ev) {
            match open {
                Some(existing) if existing != ev.node => {
                    overlap = Some(format!(
                        "{} began serving while {existing} was still serving (seq {})",
                        ev.node, ev.seq
                    ));
                }
                _ => open = Some(ev.node),
            }
        } else if serving_end(ev) && open == Some(ev.node) {
            open = None;
        }
    }
    cx.check(
        &format!(
            "audit: no two nodes ever served as primary concurrently{}",
            overlap
                .clone()
                .map(|s| format!(" — {s}"))
                .unwrap_or_default()
        ),
        overlap.is_none(),
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
}
