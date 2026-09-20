//! Timelines: the control point on disk, history files, and whether a
//! given timeline is one this instance could actually follow.
//!
//! # Why this exists
//!
//! `restore_command` is how a recovering standby asks for WAL, and
//! PostgreSQL uses it for two quite different things. Segments are
//! interchangeable — a segment is identified by timeline and LSN, so
//! whichever peer answers first hands over identical bytes. History
//! files are not. A `NNNNNNNN.history` file is how PostgreSQL *decides
//! which timeline to follow*, and handing one to a standby that cannot
//! reach that timeline does not merely fail to help: it makes the
//! standby adopt a recovery target it will then refuse to start
//! against.
//!
//! That is not hypothetical. A standby stuck on timeline 4 at
//! `0/1D000028`, whose `primary_conninfo` pointed at a perfectly good
//! timeline-4 primary it could have caught up to, asked the pool for
//! `00000005.history`. A third node that had promoted onto timeline 5
//! — forking at `0/1B0001E0`, *before* where the standby's own WAL
//! ended — answered. PostgreSQL took the file at its word, set its
//! recovery target to 5, and died on every start:
//!
//! ```text
//! FATAL:  requested timeline 5 is not a child of this server's history
//! DETAIL: Latest checkpoint is at 0/1D000028 on timeline 4, but in the
//!         history of the requested timeline, the server forked off from
//!         that timeline at 0/1B0001E0.
//! ```
//!
//! The fan-out had no notion of lineage, so a peer on a divergent
//! branch could poison a standby that was otherwise fine.
//!
//! # Where the judgement belongs
//!
//! On the **requesting** side, which is the only side that knows the
//! asker's control point. A peer serving a history file knows nothing
//! about who is asking or what WAL they already have, and adding that
//! to the wire would be asking the wrong node the question.
//!
//! [`reachable`] mirrors PostgreSQL's own check exactly — compare the
//! switchpoint against the latest checkpoint location — so a refusal
//! here predicts precisely the FATAL that would otherwise follow. The
//! standby then stays on its current timeline and keeps following the
//! upstream it was told to follow, which in the case above was all it
//! ever needed to do.

/// What `$PGDATA/global/pg_control` says, readable with PostgreSQL
/// stopped. `0` in either field means unknown — a different major, a
/// localized build, a truncated read — and unknown must never be
/// treated as a value, because both fields are compared across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ControlPoint {
    pub timeline_id: i32,
    /// Latest checkpoint location. The field PostgreSQL itself
    /// compares against a switchpoint, which is why this and not the
    /// minimum recovery point or the last replay position.
    pub checkpoint_lsn: u64,
}

impl ControlPoint {
    pub const UNKNOWN: Self = Self {
        timeline_id: 0,
        checkpoint_lsn: 0,
    };

    /// Both fields known, so a lineage comparison can be trusted.
    pub fn is_known(&self) -> bool {
        self.timeline_id > 0 && self.checkpoint_lsn > 0
    }
}

/// One line of a timeline history file: timeline `tli` ENDED at
/// `switchpoint`, and its successor begins there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineSwitch {
    pub tli: i32,
    pub switchpoint: u64,
}

/// Verdict on "may this instance follow the timeline whose history
/// file we just fetched?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachable {
    /// Follow it. Either it descends from our timeline at or after our
    /// checkpoint, or it is our own timeline (or an ancestor), whose
    /// history PostgreSQL legitimately needs.
    Yes,
    /// It branched off our timeline BEFORE our WAL ends. We have
    /// records it does not; adopting it forks the cluster, and
    /// PostgreSQL would refuse to start anyway.
    Diverged { forked_at: u64 },
    /// Its history does not mention our timeline at all — a different
    /// lineage entirely, not merely a divergent branch of ours.
    NotADescendant,
    /// Our own control point is unreadable, so there is nothing to
    /// compare. Callers serve the file: this check exists to catch a
    /// specific provable divergence, not to withhold WAL whenever the
    /// agent is unsure.
    Unknown,
}

/// Parse the timeline id out of a WAL file name.
///
/// `00000005.history` → 5, `000000050000000000000024` → 5. Both start
/// with the same 8 hex digits, which is the only part read.
pub fn timeline_of(wal_file: &str) -> Option<i32> {
    let head = wal_file.get(..8)?;
    i32::from_str_radix(head, 16).ok().filter(|t| *t > 0)
}

/// True for the names PostgreSQL uses to *choose* a timeline, as
/// opposed to the ones it uses to replay one.
pub fn is_history_file(wal_file: &str) -> bool {
    wal_file.ends_with(".history")
}

/// Parse PostgreSQL's `XXXXXXXX/XXXXXXXX` LSN text into the 64-bit
/// form. Returns `None` rather than a partial value — a misparsed LSN
/// compared against a real one is worse than no comparison.
pub fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.trim().split_once('/')?;
    let hi = u32::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo.trim(), 16).ok()?;
    Some(((hi as u64) << 32) | lo as u64)
}

/// Render an LSN the way PostgreSQL prints it, so a log line can be
/// grepped against `psql` output and against a history file without
/// translation.
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// Parse a timeline history file.
///
/// Each meaningful line is `<tli>\t<lsn>\t<reason>`; blank lines and
/// `#` comments are skipped, as are lines that do not parse — a
/// history file is written by PostgreSQL and a line we cannot read
/// means our parser is behind, which is a reason to know less, not a
/// reason to reject the whole file.
pub fn parse_history(text: &str) -> Vec<TimelineSwitch> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(tli), Some(lsn)) = (fields.next(), fields.next()) else {
            continue;
        };
        let (Ok(tli), Some(switchpoint)) = (tli.parse::<i32>(), parse_lsn(lsn)) else {
            continue;
        };
        out.push(TimelineSwitch { tli, switchpoint });
    }
    out
}

/// May an instance at `local` follow timeline `requested`, given that
/// timeline's history?
///
/// Mirrors PostgreSQL's own test — it refuses when the switchpoint off
/// our timeline is strictly below our latest checkpoint — so a `Yes`
/// here means PostgreSQL will accept the file, and a `Diverged` means
/// it would have thrown `requested timeline %u is not a child of this
/// server's history`.
pub fn reachable(local: ControlPoint, requested: i32, history: &[TimelineSwitch]) -> Reachable {
    if !local.is_known() {
        return Reachable::Unknown;
    }
    // Our own timeline, or one we descend from. PostgreSQL reads the
    // history of its CURRENT target at startup, so refusing these
    // would break ordinary recovery for a node that had done nothing
    // wrong.
    if requested <= local.timeline_id {
        return Reachable::Yes;
    }
    match history.iter().find(|e| e.tli == local.timeline_id) {
        None => Reachable::NotADescendant,
        Some(e) if e.switchpoint < local.checkpoint_lsn => Reachable::Diverged {
            forked_at: e.switchpoint,
        },
        Some(_) => Reachable::Yes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The history file that actually broke a cluster, byte for byte.
    const TL5_HISTORY: &str = "\
1\t0/A0000A0\tno recovery target specified\n\
\n\
2\t0/100000A0\tno recovery target specified\n\
\n\
3\t0/14000238\tno recovery target specified\n\
\n\
4\t0/1B0001E0\tno recovery target specified\n";

    fn at(timeline_id: i32, lsn: &str) -> ControlPoint {
        ControlPoint {
            timeline_id,
            checkpoint_lsn: parse_lsn(lsn).unwrap(),
        }
    }

    #[test]
    fn lsn_round_trips_postgres_text() {
        assert_eq!(parse_lsn("0/1B0001E0"), Some(0x1B00_01E0));
        assert_eq!(parse_lsn("1/FFFFFFFF"), Some(0x1_FFFF_FFFF));
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(parse_lsn("garbage"), None);
        assert_eq!(parse_lsn("0/"), None);
        for text in ["0/1B0001E0", "1/FFFFFFFF", "0/0"] {
            assert_eq!(format_lsn(parse_lsn(text).unwrap()), text);
        }
    }

    #[test]
    fn timeline_comes_off_the_filename() {
        assert_eq!(timeline_of("00000005.history"), Some(5));
        assert_eq!(timeline_of("000000050000000000000024"), Some(5));
        assert_eq!(timeline_of("0000000A.history"), Some(10));
        assert_eq!(timeline_of("00000000.history"), None);
        assert_eq!(timeline_of("short"), None);
    }

    #[test]
    fn history_parses_past_blank_lines() {
        let h = parse_history(TL5_HISTORY);
        assert_eq!(h.len(), 4);
        assert_eq!(
            h[3],
            TimelineSwitch {
                tli: 4,
                switchpoint: 0x1B00_01E0
            }
        );
    }

    #[test]
    fn an_unreadable_line_costs_only_that_line() {
        let h = parse_history("1\t0/A0000A0\tok\nnonsense\n2\tnot-an-lsn\tx\n3\t0/14000238\tok\n");
        assert_eq!(h.len(), 2);
        assert_eq!(h[1].tli, 3);
    }

    // ----- the verdict ---------------------------------------------------

    #[test]
    fn the_divergence_that_broke_the_cluster() {
        // db0: timeline 4, checkpoint 0/1D000028. Timeline 5 forked at
        // 0/1B0001E0 — before that — so db0 holds timeline-4 WAL that
        // timeline 5 never saw.
        let v = reachable(at(4, "0/1D000028"), 5, &parse_history(TL5_HISTORY));
        assert_eq!(
            v,
            Reachable::Diverged {
                forked_at: 0x1B00_01E0
            }
        );
    }

    #[test]
    fn a_standby_behind_the_fork_may_follow() {
        // The whole point of not refusing categorically: this node's
        // WAL ends before the fork, so timeline 5 is a clean
        // continuation of its history.
        assert_eq!(
            reachable(at(4, "0/1A000000"), 5, &parse_history(TL5_HISTORY)),
            Reachable::Yes
        );
    }

    #[test]
    fn a_checkpoint_exactly_at_the_switchpoint_may_follow() {
        // PostgreSQL refuses on `switchpoint < checkpoint`, so equality
        // is allowed. Matching its boundary exactly is the point —
        // being stricter would withhold a file PostgreSQL would have
        // accepted.
        assert_eq!(
            reachable(at(4, "0/1B0001E0"), 5, &parse_history(TL5_HISTORY)),
            Reachable::Yes
        );
    }

    #[test]
    fn our_own_history_is_always_served() {
        // A node already ON timeline 5 reads 00000005.history during
        // ordinary startup. Its own timeline is absent from that file,
        // so the descendant test would say NotADescendant — refusing
        // it would break a node that had done nothing wrong.
        assert_eq!(
            reachable(at(5, "0/25000028"), 5, &parse_history(TL5_HISTORY)),
            Reachable::Yes
        );
        assert_eq!(
            reachable(at(5, "0/25000028"), 4, &parse_history(TL5_HISTORY)),
            Reachable::Yes
        );
    }

    #[test]
    fn a_foreign_lineage_is_refused() {
        // Timeline 9's history never mentions timeline 4: not a
        // divergent branch of ours, a different history entirely.
        let foreign = parse_history("7\t0/5000000\tx\n8\t0/6000000\tx\n");
        assert_eq!(
            reachable(at(4, "0/1D000028"), 9, &foreign),
            Reachable::NotADescendant
        );
    }

    #[test]
    fn an_unknown_control_point_does_not_withhold_wal() {
        // This check exists to catch a provable divergence. When the
        // control file cannot be read there is nothing to prove, and
        // refusing WAL on a guess would strand a standby that might be
        // perfectly able to follow.
        assert_eq!(
            reachable(ControlPoint::UNKNOWN, 5, &parse_history(TL5_HISTORY)),
            Reachable::Unknown
        );
        assert_eq!(
            reachable(at(4, "0/0"), 5, &parse_history(TL5_HISTORY)),
            Reachable::Unknown
        );
    }
}
