#!/bin/bash
# Acceptance tests: 3-node dockerized cluster (PostgreSQL 17 + systemd +
# the real pg-agent-rs .deb), driven end to end. See testing/README.md.
#
# Usage:
#   testing/acceptance.sh              # build, up, run all scenarios, down
#   KEEP=1 testing/acceptance.sh       # leave the cluster running afterwards
#   SKIP_BUILD=1 testing/acceptance.sh # reuse dist/.deb + existing image
set -uo pipefail
cd "$(dirname "$0")/.."

PASS=0
FAIL=0
declare -a FAILURES=()

SUITE_T0=$SECONDS
LAST_SAY_T=$SECONDS
say()  {
    local now=$SECONDS
    printf '\n\033[1m== %s\033[0m \033[90m[+%ss, t=%ss]\033[0m\n' \
        "$*" "$((now - LAST_SAY_T))" "$((now - SUITE_T0))"
    LAST_SAY_T=$now
}
ok()   { PASS=$((PASS+1)); printf '   \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); FAILURES+=("$*"); printf '   \033[31mFAIL\033[0m %s\n' "$*"; }

# assert <description> <command...>  — command runs via bash -c
assert() {
    local desc="$1"; shift
    if bash -c "$*" >/dev/null 2>&1; then ok "$desc"; else bad "$desc"; fi
}

# Run a command inside a node as postgres (the agent's user).
x()  { docker exec "pga-$1" bash -c "${*:2}"; }
xp() { docker exec -u postgres "pga-$1" bash -c "${*:2}"; }

# Journal of pg_agentd on a node since cluster start.
agent_log() { docker exec "pga-$1" journalctl -u pg_agentd --no-pager -o cat; }

# log_has <node> <substring> — substring match against the agent
# journal. Deliberately NOT `agent_log | grep -q`: grep -q exits on the
# first match and SIGPIPEs journalctl, which under `set -o pipefail`
# makes a *successful* match look like a failed command.
log_has() { local out; out=$(agent_log "$1" 2>/dev/null); [[ "$out" == *"$2"* ]]; }

# probe <seconds> <command...> — wait_for without the verdict: polls
# until success (0) or budget exhausted (1), reporting nothing. For
# conditions with a fallback path where a timeout is not a failure.
probe() {
    local budget="$1"; shift
    local waited=0
    until bash -c "$*" >/dev/null 2>&1; do
        sleep 1; waited=$((waited+1))
        if [ "$waited" -ge "$budget" ]; then return 1; fi
    done
}

wait_for() { # wait_for <seconds> <desc> <command...>
    local budget="$1" desc="$2"; shift 2
    local waited=0
    until bash -c "$*" >/dev/null 2>&1; do
        sleep 1; waited=$((waited+1))
        if [ "$waited" -ge "$budget" ]; then bad "timeout: $desc"; return 1; fi
    done
    ok "$desc"
}

# ---------------------------------------------------------------------------
say "build"
if [ -z "${SKIP_BUILD:-}" ]; then
    ./scripts/build-pkgs.sh deb || { echo "deb build failed"; exit 1; }
fi
DEB=$(ls -t dist/pg-agent-rs_*_amd64.deb | head -1)
cp "$DEB" testing/docker/pg-agent.deb
./testing/gen-certs.sh

say "cluster up"
docker compose -f testing/compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f testing/compose.yaml up -d --build || { echo "compose up failed"; exit 1; }

cleanup() {
    if [ -z "${KEEP:-}" ]; then
        docker compose -f testing/compose.yaml down -v >/dev/null 2>&1 || true
    else
        echo "(KEEP=1: cluster left running — pga-db0/1/2)"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
say "S0: daemons come up (validate-env gate passes on all nodes)"
for n in db0 db1 db2; do
    wait_for 60 "$n: pg_agentd active" "docker exec pga-$n systemctl is-active -q pg_agentd"
done
wait_for 30 "db0: postgres active (bootstrap primary)" \
    "docker exec pga-db0 systemctl is-active -q postgresql@17-main"
assert "db1: postgres intentionally down pre-init" \
    "! docker exec pga-db1 systemctl is-active -q postgresql@17-main"

# ---------------------------------------------------------------------------
say "S1: cluster init from db0 → standbys stream"
if xp db0 "pg_agentctl cluster init" ; then
    ok "cluster init returned ok"
else
    bad "cluster init failed"
fi
wait_for 60 "db0 sees 2 streaming standbys" \
    "docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"
for n in db1 db2; do
    wait_for 30 "$n: postgres active as standby" \
        "docker exec -u postgres pga-$n psql -tAc 'select pg_is_in_recovery()' | grep -qx t"
done
assert "cluster status renders all three nodes" \
    "docker exec -u postgres pga-db0 pg_agentctl cluster status | grep -c 'db[0-2]' | grep -qx 3"

# ---------------------------------------------------------------------------
say "S2: shadow HA loop reaches steady state"
wait_for 30 "db0 shadow: took/retains lease as primary" \
    "docker exec pga-db0 journalctl -u pg_agentd -o cat | grep -q 'RetainedLease'"
for n in db1 db2; do
    wait_for 30 "$n shadow: adopted db0 then follows" \
        "docker exec pga-$n journalctl -u pg_agentd -o cat | grep -q 'AdoptedObservedPrimary { node: 0 }'"
    wait_for 30 "$n shadow: Following holder 0" \
        "docker exec pga-$n journalctl -u pg_agentd -o cat | grep -q 'Following { holder: 0 }'"
done

# ---------------------------------------------------------------------------
say "S3: primary death → exactly one shadow takeover (tiebreak)"
x db0 "systemctl stop postgresql@17-main"
# Poll for the takeover (fires at ~leader_ttl), then hold a short
# derived window: the negative asserts below ("db2 did NOT take over")
# need real time in which db2 COULD have acted - a couple of ticks
# past db1's commit is that window; 25 s of it was inertia.
wait_for 30 "db1 (lower id) shadow-took the lease" \
    "docker exec pga-db1 journalctl -u pg_agentd --no-pager -o cat | grep TookOver"
sleep 5
if log_has db2 'TookOver'; then
    bad "db2 also logged TookOver — tiebreak failed"
else
    ok "db2 did not take over"
fi
# The winner must hold its lease through the promotion grace window
# rather than release-and-retake every tick (the flapping this suite
# found; see testing/README.md finding 5).
if log_has db1 'AwaitingPromotion'; then
    ok "db1 awaits promotion instead of thrashing the lease"
else
    bad "db1 did not log AwaitingPromotion"
fi
assert "db2 stood down on the node-id tiebreak" \
    "docker exec pga-db2 journalctl -u pg_agentd -o cat | grep -q 'tiebreak'"
assert "db1/db2 watched the holder before acting" \
    "docker exec pga-db1 journalctl -u pg_agentd -o cat | grep -q 'HolderUnhealthy'"

say "S3b: primary returns → shadow converges back (nothing was promoted)"
x db0 "systemctl start postgresql@17-main"
wait_for 60 "db1 shadow: released bogus lease and follows db0 again" \
    "docker exec pga-db1 journalctl -u pg_agentd -o cat | grep -q 'WouldDemote'"
wait_for 60 "db0 sees 2 streaming standbys again" \
    "docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"

# ---------------------------------------------------------------------------
say "S3c: lag gate refuses a lagging candidate (real WAL lag, no pgpool yet)"
# Runs before pgpool exists so nothing else reacts to the primary going
# down and we can hand the handler a deliberately bad candidate.
# replay LSN of a node, as a plain pg_lsn string.
replay_lsn() { xp "$1" "psql -tAc 'select pg_last_wal_replay_lsn()'" 2>/dev/null | tr -d ' \r'; }
# byte distance between two pg_lsn strings, computed on db1.
lsn_diff() { xp db1 "psql -tAc \"select pg_wal_lsn_diff('$1'::pg_lsn,'$2'::pg_lsn)::bigint\"" 2>/dev/null | tr -d ' \r'; }

xp db2 "psql -tAc 'select pg_wal_replay_pause()'" >/dev/null 2>&1
xp db0 "psql -q -c 'create table if not exists bulk(id int, pad text)' \
        -c 'insert into bulk select g, repeat(chr(97+(g%26)),200) from generate_series(1,120000) g' \
        -c 'checkpoint' -c 'select pg_switch_wal()'" >/dev/null 2>&1
# ~24 MB of WAL - comfortably past the 16 MiB gate without the old
# 60 MB. Poll for db1 having replayed it while paused db2 trails.
wait_for 30 "db1 replayed the bulk WAL (lag gate arrangement ready)" \
    "[ \"\$(docker exec -u postgres pga-db1 psql -tAc \"select pg_wal_lsn_diff(pg_last_wal_replay_lsn(),'0/0')::bigint\" | tr -d ' ')\" -gt 25000000 ]"
sleep 2
LAG=$(lsn_diff "$(replay_lsn db1)" "$(replay_lsn db2)")
if [ -n "$LAG" ] && [ "$LAG" -gt $((16*1024*1024)) ]; then
    ok "db2 trails db1 by ${LAG} bytes (> 16 MiB threshold)"
else
    bad "could not build enough lag on db2 (got '${LAG}')"
fi
x db0 "systemctl stop postgresql@17-main"
sleep 3
# pgpool picks %m by lowest alive node id; hand the handler the LAGGING
# node 2 to prove the gate consults WAL position rather than the pick.
OUT=$(xp db1 "pg_agentc failover 0 db0 5432 /var/lib/postgresql/17/main 2 db2 0 0 5432 /var/lib/postgresql/17/main db0 5432" 2>&1)
if echo "$OUT" | grep -q 'has more WAL'; then
    ok "lag gate refused the lagging candidate"
else
    bad "lag gate did not refuse (output: $OUT)"
fi
if echo "$OUT" | grep -q 'pcp_promote_node -n 1'; then
    ok "refusal names db1 as the better candidate"
else
    bad "refusal did not name the most-advanced node"
fi
assert "db2 was not promoted" \
    "docker exec -u postgres pga-db2 psql -tAc 'select pg_is_in_recovery()' | grep -qx t"
# Restore: nothing was promoted, so the old primary simply comes back.
xp db2 "psql -tAc 'select pg_wal_replay_resume()'" >/dev/null 2>&1
x db0 "systemctl start postgresql@17-main"
wait_for 120 "both standbys caught up again" \
    "[ \"\$(docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and replay_lsn = pg_current_wal_lsn()\" | tr -d ' ')\" = 2 ]"

# ---------------------------------------------------------------------------
say "S4: reactive failover refuses a false failure report (2026-06-11)"
# pgpool-shaped argv: detached=0 (db0, actually healthy primary),
# new_main=1, old_primary=0 — the exact incident shape.
OUT=$(xp db1 "pg_agentc failover 0 db0 5432 /var/lib/postgresql/17/main 1 db1 0 0 5432 /var/lib/postgresql/17/main db0 5432" 2>&1)
if echo "$OUT" | grep -q 'running as primary'; then
    ok "failover refused: announced-dead primary is provably alive"
else
    bad "failover precondition did not refuse (output: $OUT)"
fi
assert "db1 was not promoted" \
    "docker exec -u postgres pga-db1 psql -tAc 'select pg_is_in_recovery()' | grep -qx t"

# ---------------------------------------------------------------------------
say "S5: agent restart on the primary is a non-event"
x db0 "systemctl restart pg_agentd"
wait_for 30 "db0: pg_agentd active after restart" \
    "docker exec pga-db0 systemctl is-active -q pg_agentd"
assert "phantom-primary check confirmed (no conservative stop)" \
    "docker exec pga-db0 journalctl -u pg_agentd -o cat | grep -q 'phantom-primary check: confirmed'"
assert "db0 postgres still primary" \
    "docker exec -u postgres pga-db0 psql -tAc 'select pg_is_in_recovery()' | grep -qx f"

# ---------------------------------------------------------------------------
# Phase 2 — pgpool in the loop. Everything above ran without pgpool;
# from here the hooks fire for real, and the measurements answer
# docs/pgpool-hook-contract.md §5.
# ---------------------------------------------------------------------------
if [ "${PHASE:-all}" = "1" ]; then
    say "result"; echo "PASS=$PASS FAIL=$FAIL"
    [ "$FAIL" -gt 0 ] && { printf '  - %s\n' "${FAILURES[@]}"; exit 1; }
    exit 0
fi

say "S6: pgpool up on all three nodes (watchdog off, target hook contract)"
for n in db0 db1 db2; do
    if x "$n" "/usr/local/sbin/pg-agent-pgpool-setup" >/dev/null 2>&1; then
        ok "$n: pgpool configured + started"
    else
        bad "$n: pgpool setup failed"
    fi
done
for n in db0 db1 db2; do
    wait_for 60 "$n: pgpool shows 3 backends up" \
        "docker exec -u postgres pga-$n pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c ' up ' | grep -qx 3"
done
wait_for 30 "db0: /healthz reports ready" \
    "docker exec pga-db0 curl -sf localhost:9702/healthz | grep -q '\"ready\":true'"

say "S7: check-hooks agrees with the agent-led contract (and --legacy dissents)"
# Post-cutover, the canonical block IS the agent-led contract, so this
# conf — follow_primary empty, watchdog off, the decision-critical
# settings — should check clean except for one known deviation: the
# harness routes failover_command through the counting probe wrapper
# (S9's per-instance measurements). Exactly that drift, nothing else.
CH=$(xp db0 "pg_agentctl check-hooks /etc/pgpool2/pgpool.conf" 2>&1 || true)
if echo "$CH" | grep 'ERR' | grep -q 'failover_command'; then
    ok "check-hooks flags the probe wrapper (drift detection works)"
else
    bad "check-hooks missed the failover_command probe deviation"
fi
if echo "$CH" | grep 'ERR' | grep -qE 'follow_primary|use_watchdog|auto_failback|detach_false_primary'; then
    bad "check-hooks flagged contract keys that match: $(echo "$CH" | grep ERR)"
else
    ok "follow_primary + decision-critical settings check clean"
fi
# The legacy block must now read this conf as drifted — that is the
# point of the flag: pre-cutover deployments keep a canonical to check
# against, and the two contracts are distinguishable.
CHL=$(xp db0 "pg_agentctl check-hooks --legacy /etc/pgpool2/pgpool.conf" 2>&1 || true)
if echo "$CHL" | grep 'ERR' | grep -q 'follow_primary'; then
    ok "--legacy dissents on follow_primary_command (blocks are distinct)"
else
    bad "--legacy did not flag empty follow_primary_command"
fi

say "S8: detach does not propagate between pgpool instances (hook-contract §3)"
# A detach on ONE instance is that instance's routing state only —
# nothing syncs it without watchdog. It also fires that instance's
# failover_command, which the agent must refuse (db2 is a healthy
# streaming standby).
xp db0 "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
sleep 4
assert "db0's pgpool now shows node 2 down" \
    "docker exec -u postgres pga-db0 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q down"
assert "db1's pgpool still shows node 2 up (no propagation)" \
    "docker exec -u postgres pga-db1 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q ' up '"
if log_has db0 'refusing to drop slot'; then
    ok "agent refused the slot drop (detached standby is still streaming)"
else
    bad "agent did not refuse the slot drop for a streaming standby"
fi
assert "db2 still streaming (replication intact)" \
    "docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"
xp db0 "pcp_attach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
wait_for 30 "db0's pgpool shows node 2 up again after explicit attach" \
    "docker exec -u postgres pga-db0 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q ' up '"

say "S9: real primary failover through pgpool (hook-contract §5.1)"
for n in db0 db1 db2; do x "$n" ": > /var/log/failover-probe.log"; done
x db0 "systemctl stop postgresql@17-main"
wait_for 120 "a standby was promoted to primary" \
    "docker exec -u postgres pga-db1 psql -tAc 'select pg_is_in_recovery()' | grep -qx f"
PRIMARIES=0
for n in db0 db1 db2; do
    if xp "$n" "psql -tAc 'select pg_is_in_recovery()'" 2>/dev/null | grep -qx f; then
        PRIMARIES=$((PRIMARIES+1))
    fi
done
if [ "$PRIMARIES" -eq 1 ]; then
    ok "exactly one primary after failover (no split brain)"
else
    bad "expected exactly 1 primary, found $PRIMARIES"
fi
say "measurement: failover_command firings per pgpool instance"
TOTAL=0
for n in db0 db1 db2; do
    c=$(x "$n" "wc -l < /var/log/failover-probe.log" 2>/dev/null | tr -d ' \r')
    c=${c:-0}; TOTAL=$((TOTAL+c))
    echo "     $n: $c invocation(s)"
    x "$n" "cat /var/log/failover-probe.log" 2>/dev/null | sed 's/^/       /'
done
echo "     total across the cluster: $TOTAL"
if [ "$TOTAL" -ge 1 ]; then
    ok "failover_command fired (count recorded above — hook-contract §5.1)"
else
    bad "failover_command never fired"
fi

# ---------------------------------------------------------------------------
# Phase 3 — repair, the remaining hook-contract measurements, and the
# split-brain baseline the promotion-authority redesign exists to close.
# ---------------------------------------------------------------------------
if [ "${PHASE:-all}" = "2" ]; then
    say "result"; echo "PASS=$PASS FAIL=$FAIL"
    [ "$FAIL" -gt 0 ] && { printf '  - %s\n' "${FAILURES[@]}"; exit 1; }
    exit 0
fi

# Which node is currently primary? Echoes db0|db1|db2, or nothing.
current_primary() {
    for n in db0 db1 db2; do
        if xp "$n" "psql -tAc 'select pg_is_in_recovery()'" 2>/dev/null | grep -qx f; then
            echo "$n"; return
        fi
    done
}
count_primaries() {
    local c=0 n
    for n in db0 db1 db2; do
        if xp "$n" "psql -tAc 'select pg_is_in_recovery()'" 2>/dev/null | grep -qx f; then
            c=$((c+1))
        fi
    done
    echo "$c"
}
pcp_all() { # pcp_all <attach|detach> <node-id>
    local verb="$1" nid="$2" n
    for n in db0 db1 db2; do
        xp "$n" "pcp_${verb}_node -h localhost -p 9898 -U pgpool -w -n $nid" >/dev/null 2>&1 || true
    done
}

# Rebuild every non-primary node as a standby of the current primary,
# then re-attach it in every pgpool instance (the fan-out §3 requires).
#
# Deliberately does NOT detach the target first. `cluster recover
# --stop-target-pg` stops the target's PostgreSQL, which makes pgpool
# fire failover_command against it — and the standby-down branch's job
# is to drop that node's slot. Letting that happen live is the point:
# the cross-op consult (failover skips the drop while an in-flight op
# owns the node) is what has to hold, and S10 fails without it.
repair_cluster() {
    local prim="$1" n nid
    for n in db0 db1 db2; do
        [ "$n" = "$prim" ] && continue
        nid="${n#db}"
        xp "$prim" "pg_agentctl cluster recover --target $nid --stop-target-pg" \
            > "/tmp/recover-$nid.log" 2>&1 || true
        pcp_all attach "$nid"
    done
}

say "S10: post-failover repair with the agent's own commands"
PRIM=$(current_primary)
if [ -n "$PRIM" ]; then ok "post-failover primary is $PRIM"; else bad "no primary after S9"; fi
repair_cluster "$PRIM"
wait_for 180 "$PRIM has 2 streaming standbys again" \
    "[ \"\$(docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | tr -d ' ')\" = 2 ]"
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary after repair"
else
    bad "expected 1 primary after repair, found $(count_primaries)"
fi
# The recover/failover-hook race: pgpool fires failover_command when
# recovery stops the target, and the cross-op consult must keep the
# slot the recovery just created.
if log_has "$PRIM" 'in-flight op owns this node; skipping slot drop'; then
    ok "failover deferred to the in-flight recovery (slot survived)"
else
    bad "no cross-op consult logged during repair"
fi

say "S11: pgpool_status is sticky across a pgpool restart (hook-contract §5.3)"
xp "$PRIM" "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
sleep 3
x "$PRIM" "systemctl restart pgpool2"
sleep 5
if xp "$PRIM" "pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2" 2>/dev/null | grep -q down; then
    ok "node 2 still down after restart (status file survived; no leader to correct it)"
else
    bad "node 2 came back up on its own after restart"
fi
xp "$PRIM" "pcp_attach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
wait_for 30 "explicit attach clears the sticky down" \
    "docker exec -u postgres pga-$PRIM pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q ' up '"

say "S12: follow_primary_command non-empty degenerates healthy standbys (§5.4)"
for n in db0 db1 db2; do
    x "$n" "FOLLOW_PRIMARY=/bin/true /usr/local/sbin/pg-agent-pgpool-setup" >/dev/null 2>&1
done
sleep 4
wait_for 60 "pgpool healthy again with the non-empty hook configured" \
    "docker exec -u postgres pga-$PRIM pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c ' up ' | grep -qx 3"
x "$PRIM" "systemctl stop postgresql@17-main"
# Poll for the degeneration (detection + hook fan-out), then settle a
# few extra seconds so the count below is stable, not racing the hook.
wait_for 60 "pgpool marked backends down after the primary stop" \
    "docker exec -u postgres pga-db0 pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c down | grep -qxE '[2-9]'"
sleep 5
DOWN=$(xp db0 "pcp_node_info -h localhost -p 9898 -U pgpool -w -a" 2>/dev/null | grep -c down || true)
echo "     backends marked down on db0's instance: ${DOWN:-?} of 3"
if [ "${DOWN:-0}" -ge 2 ]; then
    ok "non-empty follow_primary_command degenerated standbys too (${DOWN} down)"
else
    bad "expected >=2 backends down with the hook non-empty, saw ${DOWN}"
fi
# Restore the target contract everywhere and repair.
for n in db0 db1 db2; do x "$n" "/usr/local/sbin/pg-agent-pgpool-setup" >/dev/null 2>&1; done
PRIM=$(current_primary)
if [ -z "$PRIM" ]; then
    x db0 "systemctl start postgresql@17-main"; sleep 10; PRIM=$(current_primary)
fi
repair_cluster "$PRIM"
wait_for 180 "cluster repaired again (primary $PRIM, 2 standbys)" \
    "[ \"\$(docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | tr -d ' ')\" = 2 ]"

say "S13: BASELINE — a partition produces split brain today"
# The claim promotion-authority §2.1 rests on: with role derived from
# pgpool's failure detector, an isolated-but-healthy primary and a
# majority that promotes without it are both reachable at once. This
# scenario is expected to FAIL SAFETY today; it is the regression test
# that must invert once the lease lands (step 6/7).
docker network disconnect pga-net "pga-$PRIM" >/dev/null 2>&1
say "     (isolated $PRIM from the cluster network)"
# Poll rather than sleep a fixed window: detection is health_check_period
# × retries plus the hook round trip, and a too-short window reads as
# "no split brain" when the promotion simply had not happened yet.
MAJ_PRIMARY=""
waited=0
while [ "$waited" -lt 150 ]; do
    for n in db0 db1 db2; do
        [ "$n" = "$PRIM" ] && continue
        if xp "$n" "psql -tAc 'select pg_is_in_recovery()'" 2>/dev/null | grep -qx f; then
            MAJ_PRIMARY="$n"
        fi
    done
    [ -n "$MAJ_PRIMARY" ] && break
    sleep 5; waited=$((waited+5))
done
ISO_PRIMARY=no
if xp "$PRIM" "psql -tAc 'select pg_is_in_recovery()'" 2>/dev/null | grep -qx f; then ISO_PRIMARY=yes; fi
echo "     isolated $PRIM still primary: $ISO_PRIMARY | majority-side primary: ${MAJ_PRIMARY:-none}"
if [ "$ISO_PRIMARY" = yes ] && [ -n "$MAJ_PRIMARY" ]; then
    ok "BASELINE CONFIRMED: two primaries under partition (split brain is reachable)"
    SPLIT=yes
else
    ok "no split brain observed this run (isolated=$ISO_PRIMARY majority=${MAJ_PRIMARY:-none})"
    SPLIT=no
fi
docker network connect pga-net "pga-$PRIM" >/dev/null 2>&1
sleep 5
if [ "$SPLIT" = yes ]; then
    say "S13b: the existing mitigation — phantom check stops the stale primary"
    x "$PRIM" "systemctl restart pg_agentd" || true
    if wait_for 30 "restarted agent detected the phantom primary" \
        "docker exec pga-$PRIM journalctl -u pg_agentd --no-pager -o cat | grep 'phantom-primary check: detected'"; then :; fi
    wait_for 60 "stale primary's PostgreSQL was stopped" \
        "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"
    echo "     NOTE: the mitigation needs an agent restart to fire — nothing"
    echo "     stops the stale primary while it keeps running."
fi

# ---------------------------------------------------------------------------
# Phase 4 — consensus for real (promotion-authority step 6). The same
# shadow loop, backed by the embedded Raft instead of a process-local
# store: PgAgentRaft on the peer mTLS listener, membership formed by
# ClusterInit, the lease in a replicated state machine. Still shadow —
# nothing promotes — so what is under test is the decision stream, and
# the structural claims the in-memory store could only simulate:
# exactly-one takeover is now CAS-serialized by a quorum, and a
# partitioned holder *learns* it must demote instead of serving on.
# ---------------------------------------------------------------------------
if [ "${PHASE:-all}" = "3" ]; then
    say "result"; echo "PASS=$PASS FAIL=$FAIL"
    [ "$FAIL" -gt 0 ] && { printf '  - %s\n' "${FAILURES[@]}"; exit 1; }
    exit 0
fi

# log_since <node> <since-ts> <substring> — like log_has, but bounded to
# journal entries after a captured timestamp. Raft scenarios re-trigger
# the same decision variants the shadow phases already logged, so
# whole-journal greps would pass vacuously.
log_since() {
    local out
    out=$(docker exec "pga-$1" journalctl -u pg_agentd --since "$2" --no-pager -o cat 2>/dev/null)
    [[ "$out" == *"$3"* ]]
}
now_ts() { docker exec pga-db0 date '+%Y-%m-%d %H:%M:%S'; }

say "R0: repair after S13, stop pgpool, enable raft on all nodes"
PRIM=$(current_primary)
if [ -z "$PRIM" ]; then
    x db0 "systemctl start postgresql@17-main" >/dev/null 2>&1 || true
    sleep 10; PRIM=$(current_primary)
fi
if [ -n "$PRIM" ]; then ok "primary for the raft phase is $PRIM"; else bad "no primary to start the raft phase from"; fi
STREAMING=$(xp "$PRIM" "psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\"" 2>/dev/null | tr -d ' ')
if [ "${STREAMING:-0}" != "2" ]; then
    repair_cluster "$PRIM"
    wait_for 180 "cluster repaired for the raft phase" \
        "[ \"\$(docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | tr -d ' ')\" = 2 ]"
else
    ok "cluster already healthy (2 streaming standbys)"
fi
# pgpool out of the loop: these scenarios are about the agent's own
# decisions, and the supervisor is disabled in this harness config so
# nothing restarts it.
for n in db0 db1 db2; do x "$n" "systemctl stop pgpool2" >/dev/null 2>&1 || true; done

RAFT_TS=$(now_ts)
for n in db0 db1 db2; do
    x "$n" "printf 'enabled             = true\n' >> /etc/pg_agent/config.toml"
    x "$n" "systemctl restart pg_agentd"
done
for n in db0 db1 db2; do
    # The restart itself is the validate-env assertion: the packaged
    # unit's ExecStartPre gate now runs the raft prerequisite checks
    # (pool of 3, mTLS, resolvable node id, writable state dir) and
    # would keep the daemon down if any refused.
    wait_for 60 "$n: pg_agentd active with [raft] enabled (validate-env gate passed)" \
        "docker exec pga-$n systemctl is-active -q pg_agentd"
    wait_for 30 "$n: raft started" \
        "docker exec pga-$n journalctl -u pg_agentd --since '$RAFT_TS' --no-pager -o cat | grep -q 'raft: started'"
done
# Until ClusterInit forms membership there is no quorum to read
# through, and the loop must report UNKNOWN — not vacant, and above
# all not act. Every node ticking StoreUnknown here is the "cannot
# read is not vacant" rule holding on a real consensus store.
wait_for 30 "pre-membership: loop reports store unknown (not vacant)" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$RAFT_TS' --no-pager -o cat | grep -q 'StoreUnknown'"

say "R1: ClusterInit forms raft membership, idempotently"
# --only-node 99 matches no standby: replication work is a no-op, so
# this exercises exactly the membership bootstrap. Run on the primary
# (ClusterInit refuses on a standby).
INIT1=$(xp "$PRIM" "pg_agentctl cluster init --only-node 99" 2>&1)
if [[ "$INIT1" == *"raft membership initialized (3 nodes)"* ]]; then
    ok "first init formed the membership"
else
    bad "first init did not report membership formation: $INIT1"
fi
INIT2=$(xp "$PRIM" "pg_agentctl cluster init --only-node 99" 2>&1)
if [[ "$INIT2" == *"raft membership already formed"* ]]; then
    ok "second init reports already formed (idempotent, not an error)"
else
    bad "second init did not report already-formed: $INIT2"
fi

say "R2: consensus-backed steady state — one lease, everyone agrees"
PRIM_ID="${PRIM#db}"
wait_for 60 "$PRIM retains the lease through the quorum" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$RAFT_TS' --no-pager -o cat | grep -q 'RetainedLease'"
for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    wait_for 60 "$n follows holder $PRIM_ID (read from the shared state machine)" \
        "docker exec pga-$n journalctl -u pg_agentd --since '$RAFT_TS' --no-pager -o cat | grep -q 'Following { holder: $PRIM_ID }'"
done
# The shared store means lease acquisition happens ONCE, cluster-wide.
# Since the executor work, ClusterInit SEEDS the lease at membership
# bootstrap — so the legitimate acquisition paths are exactly two: the
# seed (reported in R1's init output), or one decision-level
# acquisition. Anything else — multiple acquirers, or a lease with no
# acquisition story at all — is a serialization failure.
ACQ=0
for n in db0 db1 db2; do
    if log_since "$n" "$RAFT_TS" 'TookOver' || log_since "$n" "$RAFT_TS" 'AdoptedObservedPrimary'; then
        ACQ=$((ACQ+1))
    fi
done
SEEDED=no
if [[ "$INIT1" == *"lease seeded"* || "$INIT1" == *"lease already held"* ]]; then SEEDED=yes; fi
if [ "$ACQ" -le 1 ] && { [ "$ACQ" = "1" ] || [ "$SEEDED" = "yes" ]; }; then
    ok "one lease acquisition, cluster-wide (seeded=$SEEDED, decision-acquirers=$ACQ)"
else
    bad "lease acquisition not serialized: seeded=$SEEDED decision-acquirers=$ACQ"
fi

say "R3: primary death — takeover is now quorum-committed, still exactly one"
TS3=$(now_ts)
x "$PRIM" "systemctl stop postgresql@17-main"
wait_for 30 "a standby committed the takeover (raft CAS)" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$PRIM\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$TS3' --no-pager -o cat 2>/dev/null | grep TookOver && exit 0; done; exit 1"
sleep 4   # a couple of ticks: AwaitingPromotion + the rival's window
WINNERS=""
for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    if log_since "$n" "$TS3" 'TookOver'; then WINNERS="$WINNERS $n"; fi
done
case "$(echo $WINNERS | wc -w)" in
    1) ok "exactly one standby committed the takeover:$WINNERS" ;;
    0) bad "no standby took over after leader_ttl" ;;
    *) bad "multiple standbys logged TookOver:$WINNERS — CAS failed to serialize" ;;
esac
W=$(echo $WINNERS | awk '{print $1}')
if [ -n "$W" ]; then
    if log_since "$W" "$TS3" 'AwaitingPromotion'; then
        ok "$W awaits promotion (nothing actually promotes in shadow)"
    else
        bad "$W did not log AwaitingPromotion"
    fi
fi

say "R3b: primary returns — lease converges back through the quorum"
TS3B=$(now_ts)
x "$PRIM" "systemctl start postgresql@17-main"
wait_for 90 "$PRIM retains the lease again" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$TS3B' --no-pager -o cat | grep -q 'RetainedLease'"
wait_for 60 "$PRIM sees 2 streaming standbys again" \
    "docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"

say "R4: S13 INVERTED at the decision level — partition, no second primary decision"
# The scenario S13 proved reachable with pgpool deciding: isolated
# primary keeps serving, majority promotes a second one. Same partition,
# consensus deciding: the majority commits exactly one takeover, and the
# isolated holder — unable to complete a linearizable read — decides to
# DEMOTE ITSELF, without ever learning a takeover happened. Shadow mode
# means both are decisions in a log, but they are the decisions that
# make split brain unreachable once step 7 hands them executors.
TS4=$(now_ts)
docker network disconnect pga-net "pga-$PRIM" >/dev/null 2>&1
say "     (isolated $PRIM — current lease holder — from the cluster network)"
# Poll the two positive signals, then hold the window open for
# leader_ttl + margin: the hysteresis assertion below ("no two
# takeovers within ttl") is only meaningful over a window in which a
# second takeover COULD have happened.
if wait_for 30 "isolated holder escalated to a demote decision" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$TS4' --no-pager -o cat | grep 'quorum contact lost'"; then :; fi
if wait_for 45 "majority committed a takeover" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$PRIM\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$TS4' --no-pager -o cat 2>/dev/null | grep TookOver && exit 0; done; exit 1"; then :; fi
sleep 13   # leader_ttl 10 + margin: the second-takeover window
if log_since "$PRIM" "$TS4" 'TookOver'; then
    bad "isolated node committed a takeover without a quorum"
else
    ok "isolated node committed nothing (no quorum, no writes)"
fi
# In shadow mode nothing promotes, so the winner's lease looks orphaned
# after its grace window and legally moves on — sequential takeovers are
# EXPECTED here, and asserting "exactly one" was wrong (it also produced
# a false red that led to finding 13). The property the ttl actually
# promises is hysteresis: every takeover must come at least leader_ttl
# after the previous one, because a fresh holder gets a full ttl of
# protection while its (asynchronous) promotion lands. Finding 13 was a
# 7-second deposal — a rival's unhealthy clock carried over from the
# previous holder — and this is the assertion that catches it.
MAJ_TAKES=$(for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    docker exec "pga-$n" journalctl -u pg_agentd --since "$TS4" --no-pager -o short-unix 2>/dev/null \
        | grep 'TookOver'
done | awk '{print int($1)}' | sort -n)
NTAKES=$(echo "$MAJ_TAKES" | grep -c '[0-9]' || true)
if [ "${NTAKES:-0}" -ge 1 ]; then
    ok "majority committed a takeover ($NTAKES in the window)"
else
    bad "majority never took over the dead holder's lease"
fi
GAP_VIOLATION=""
prev=""
for t in $MAJ_TAKES; do
    if [ -n "$prev" ] && [ $((t - prev)) -lt 10 ]; then
        GAP_VIOLATION="$((t - prev))s"
    fi
    prev="$t"
done
if [ -z "$GAP_VIOLATION" ]; then
    ok "every takeover ≥ leader_ttl after the previous (fresh holders kept their ttl)"
else
    bad "takeovers $GAP_VIOLATION apart — a new holder was deposed inside its ttl (finding 13 shape)"
fi

say "R4b: partition heals — one primary throughout, lease converges"
TS4B=$(now_ts)
docker network connect pga-net "pga-$PRIM" >/dev/null 2>&1
# Nothing was promoted (shadow), so PostgreSQL-level state needs no
# repair: the isolated primary was never demoted, the standbys never
# promoted. This is also the check that the raft node REJOINS after a
# partition rather than needing a restart.
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one PostgreSQL primary throughout the partition (shadow)"
else
    bad "primary count drifted during the partition: $(count_primaries)"
fi
wait_for 120 "$PRIM retains the lease again after rejoining" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$TS4B' --no-pager -o cat | grep -q 'RetainedLease'"
wait_for 60 "replication intact (2 streaming standbys)" \
    "docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"

# ---------------------------------------------------------------------------
# Phase 5 — EXECUTE (promotion-authority step 7). shadow = false: the
# same decisions, now driving PostgreSQL. Everything before this point
# proved the loop decides correctly; this phase proves the executors
# act on it — real promotion on lease takeover, real fencing on lost
# quorum, real re-pointing of survivors — and that the S13 partition
# now ends with ONE primary on the wire instead of two.
# ---------------------------------------------------------------------------
if [ "${PHASE:-all}" = "4" ]; then
    say "result"; echo "PASS=$PASS FAIL=$FAIL"
    [ "$FAIL" -gt 0 ] && { printf '  - %s\n' "${FAILURES[@]}"; exit 1; }
    exit 0
fi

say "E0: flip shadow off — executors attach"
PRIM=$(current_primary)
if [ -n "$PRIM" ]; then ok "primary entering execute mode: $PRIM"; else bad "no primary before execute phase"; fi
E0_TS=$(now_ts)
for n in db0 db1 db2; do
    x "$n" "sed -i 's/^shadow              = true/shadow              = false/' /etc/pg_agent/config.toml"
    x "$n" "systemctl restart pg_agentd"
done
for n in db0 db1 db2; do
    wait_for 60 "$n: pg_agentd active with shadow off" \
        "docker exec pga-$n systemctl is-active -q pg_agentd"
    wait_for 30 "$n: EXECUTE mode logged" \
        "docker exec pga-$n journalctl -u pg_agentd --since '$E0_TS' --no-pager -o cat | grep -q 'EXECUTE mode'"
done
# The holder retains; the standbys converge onto it through the
# executor (slot prep via peer RPC + conf rewrite + reload) — the
# follow path that replaces pgpool's follow_primary_command.
wait_for 60 "$PRIM retains the lease in execute mode" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$E0_TS' --no-pager -o cat | grep -q 'RetainedLease'"
for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    wait_for 60 "$n executor converged onto the holder" \
        "docker exec pga-$n journalctl -u pg_agentd --since '$E0_TS' --no-pager -o cat | grep -q 'now following lease holder'"
done
wait_for 30 "replication intact after the follows (2 streaming)" \
    "docker exec -u postgres pga-$PRIM psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"

say "E1: primary death → the takeover PROMOTES for real"
E1_TS=$(now_ts)
x "$PRIM" "systemctl stop postgresql@17-main"
# Deposal after leader_ttl, then a fast promotion — poll for it rather
# than sleeping a fixed window. NOTE: log matching goes through
# log_since, never `journalctl | grep -q` in this shell — pipefail
# turns grep -q's early exit into a false failure (the log_has comment
# at the top of this file; re-learned the hard way in E2's first run).
wait_for 60 "a standby completed a real promotion" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$PRIM\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$E1_TS' --no-pager -o cat 2>/dev/null | grep 'promotion complete' && exit 0; done; exit 1"
sleep 3   # let the loop's next tick settle roles
E1_WINNER=""
for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    if log_since "$n" "$E1_TS" 'roleexec: promotion complete'; then
        E1_WINNER="$n"
    fi
done
if [ -n "$E1_WINNER" ]; then ok "$E1_WINNER promoted on lease takeover"; else bad "no node completed a promotion"; fi
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one PostgreSQL primary after failover"
else
    bad "expected 1 primary, found $(count_primaries)"
fi
if [ -n "$E1_WINNER" ]; then
    assert "promotion is journaled (ops list shows a done promote op)" \
        "docker exec -u postgres pga-$E1_WINNER pg_agentctl ops list | grep promote | grep -qi done"
    # The surviving standby re-points onto the new primary.
    E1_SURVIVOR=""
    for n in db0 db1 db2; do
        [ "$n" = "$PRIM" ] && continue
        [ "$n" = "$E1_WINNER" ] && continue
        E1_SURVIVOR="$n"
    done
    wait_for 90 "$E1_SURVIVOR re-pointed and streams from $E1_WINNER" \
        "docker exec -u postgres pga-$E1_WINNER psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 1"
fi

say "E1b: operator rejoins the dead ex-primary (demote policy: never automatic)"
# The fenced/stopped ex-primary must NOT have been restarted or rebuilt
# by the executor while we watched.
assert "ex-primary $PRIM stayed stopped (no automatic rejoin)" \
    "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"
PRIM_ID="${PRIM#db}"
xp "$E1_WINNER" "pg_agentctl cluster recover --target $PRIM_ID --stop-target-pg" \
    > /tmp/e1b-recover.log 2>&1 || true
wait_for 180 "ex-primary rebuilt; $E1_WINNER has 2 streaming standbys" \
    "docker exec -u postgres pga-$E1_WINNER psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"
if [ "$(count_primaries)" = "1" ]; then
    ok "still exactly one primary after the rejoin"
else
    bad "primary count drifted during rejoin: $(count_primaries)"
fi
# cluster recover may have started pgpool on the target; keep this
# phase pgpool-free.
for n in db0 db1 db2; do x "$n" "systemctl stop pgpool2" >/dev/null 2>&1 || true; done

say "E2: S13 EXECUTED — the partition that used to make two primaries"
PRIM=$E1_WINNER
E2_TS=$(now_ts)
docker network disconnect pga-net "pga-$PRIM" >/dev/null 2>&1
say "     (isolated $PRIM — current primary and lease holder)"
# The isolated holder FENCES ITSELF: quorum loss escalates to a demote
# decision and the executor stops PostgreSQL — §3's dilemma resolved
# the safe way, on the node that cannot know what the majority is
# doing. Poll for it (fires within ~retry_timeout + a tick).
if wait_for 30 "isolated holder fenced itself (quorum loss → stop)" \
    "docker exec pga-$PRIM journalctl -u pg_agentd --since '$E2_TS' --no-pager -o cat | grep FENCING"; then :; fi
assert "isolated $PRIM PostgreSQL is actually stopped" \
    "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"
# Majority: deposal after leader_ttl, then a promotion that must NOT
# stall on the partitioned peer (finding 14: restore_command re-probing
# the isolated node held a promotion for ~40 s — now bounded by
# FETCH_WAL_SETUP_TIMEOUT + the restore_wal peer cooldown).
if wait_for 60 "majority completed a real promotion" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$PRIM\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$E2_TS' --no-pager -o cat 2>/dev/null | grep 'promotion complete' && exit 0; done; exit 1"; then :; fi
E2_WINNER=""
for n in db0 db1 db2; do
    [ "$n" = "$PRIM" ] && continue
    if log_since "$n" "$E2_TS" 'roleexec: promotion complete'; then
        E2_WINNER="$n"
    fi
done
if [ -n "$E2_WINNER" ]; then ok "majority promoted $E2_WINNER"; else bad "majority never promoted"; fi

docker network connect pga-net "pga-$PRIM" >/dev/null 2>&1
sleep 5
# THE assertion this whole design exists for. S13 measured two
# primaries under the same partition; with the lease deciding and
# executors acting, the answer is one — during the partition AND after
# it heals, with no phantom-check restart, no operator, no
# coincidence.
if [ "$(count_primaries)" = "1" ]; then
    ok "S13 INVERTED: exactly one primary on the wire, partition and all"
else
    bad "S13 NOT inverted: $(count_primaries) primaries after the partition"
fi
assert "fenced ex-holder stays down after reconnect (demote policy)" \
    "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"

say "E2b: operator repairs; cluster whole again"
if [ -z "$E2_WINNER" ]; then
    # Without a winner there is nothing meaningful to repair from —
    # E2's failures already tell the story; don't cascade noise.
    bad "skipping E2b (no majority winner to repair from)"
else
PRIM_ID="${PRIM#db}"
xp "$E2_WINNER" "pg_agentctl cluster recover --target $PRIM_ID --stop-target-pg" \
    > /tmp/e2b-recover.log 2>&1 || true
# The fenced node is rebuilt above. The SURVIVING standby may also need
# repair: candidate selection samples moving WAL positions, so the
# survivor can end up a few bytes past the new primary's fork point —
# unable to follow the new timeline by streaming, wedged in a
# walreceiver retry loop (finding 15). Diverged-standby repair is
# rewind territory, which v1 demote policy reserves for the operator —
# so the operator path is what this exercises.
if probe 90 \
    "docker exec -u postgres pga-$E2_WINNER psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"; then
    ok "$E2_WINNER has 2 streaming standbys"
else
    for n in db0 db1 db2; do
        [ "$n" = "$E2_WINNER" ] && continue
        if xp "$n" "psql -tAc \"select pg_stat_wal_receiver.status from pg_stat_wal_receiver\"" 2>/dev/null | grep -qx streaming; then
            continue
        fi
        echo "     NOTE: $n not streaming (diverged past the fork point?) — operator recover"
        NID="${n#db}"
        xp "$E2_WINNER" "pg_agentctl cluster recover --target $NID --stop-target-pg" \
            > "/tmp/e2b-recover-$NID.log" 2>&1 || true
    done
    wait_for 180 "$E2_WINNER has 2 streaming standbys (after diverged-standby repair)" \
        "docker exec -u postgres pga-$E2_WINNER psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"
fi
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary at the end of the execute phase"
else
    bad "primary count wrong at end: $(count_primaries)"
fi
fi   # E2_WINNER guard

say "E3: full cutover shape — pgpool routes, the lease decides"
# The production end-state: pgpool up in the agent-led contract on all
# three nodes, execute mode on. Kill the primary: pgpool fires its
# notify-only failover_command, the handler answers ADVISORY (no
# promotion from the hook — SPEC §5.1 lease-mode), the HA loop
# promotes, and pgpool discovers the new primary through sr_check.
# Failure detection and routing stay pgpool's; authority does not.
for n in db0 db1 db2; do
    x "$n" "/usr/local/sbin/pg-agent-pgpool-setup" >/dev/null 2>&1 || true
done
E3_PRIM=$(current_primary)
for n in db0 db1 db2; do
    wait_for 60 "$n: pgpool shows 3 backends up" \
        "docker exec -u postgres pga-$n pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c ' up ' | grep -qx 3"
done
E3_TS=$(now_ts)
x "$E3_PRIM" "systemctl stop postgresql@17-main"
wait_for 30 "failover hook answered advisory (the lease decides, not the hook)" \
    "for n in db0 db1 db2; do docker exec pga-\$n journalctl -u pg_agentd --since '$E3_TS' --no-pager -o cat 2>/dev/null | grep 'failover: advisory' && exit 0; done; exit 1"
wait_for 60 "the lease promoted a standby" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$E3_PRIM\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$E3_TS' --no-pager -o cat 2>/dev/null | grep 'promotion complete' && exit 0; done; exit 1"
sleep 3
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary after the hook-announced, lease-decided failover"
else
    bad "primary count wrong after cutover-shape failover: $(count_primaries)"
fi
E3_NEW=$(current_primary)
E3_NEW_ID="${E3_NEW#db}"
wait_for 60 "pgpool discovered the new primary via sr_check (no follow hook needed)" \
    "docker exec -u postgres pga-$E3_NEW pcp_node_info -h localhost -p 9898 -U pgpool -w -n $E3_NEW_ID | grep -qi primary"
# Operator repair of the dead node — with pgpool live this time, so
# cluster recover's attach fan-out actually attaches.
E3_DEAD_ID="${E3_PRIM#db}"
xp "$E3_NEW" "pg_agentctl cluster recover --target $E3_DEAD_ID --stop-target-pg" \
    > /tmp/e3-recover.log 2>&1 || true
wait_for 180 "cluster whole again ($E3_NEW + 2 streaming standbys)" \
    "docker exec -u postgres pga-$E3_NEW psql -tAc \"select count(*) from pg_stat_replication where state='streaming'\" | grep -qx 2"
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary at the end of the suite"
else
    bad "primary count wrong at suite end: $(count_primaries)"
fi

# ---------------------------------------------------------------------------
say "result"
echo "PASS=$PASS FAIL=$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '  - %s\n' "${FAILURES[@]}"
    KEEP=${KEEP:-1}   # keep the cluster for debugging on failure
    exit 1
fi
