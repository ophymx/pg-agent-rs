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

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
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

wait_for() { # wait_for <seconds> <desc> <command...>
    local budget="$1" desc="$2"; shift 2
    local waited=0
    until bash -c "$*" >/dev/null 2>&1; do
        sleep 2; waited=$((waited+2))
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
# leader_ttl 10s + loop_wait 2s + margin
sleep 25
if log_has db1 'TookOver'; then
    ok "db1 (lower id) shadow-took the lease"
else
    bad "db1 did not log TookOver"
fi
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
        -c 'insert into bulk select g, repeat(chr(97+(g%26)),200) from generate_series(1,300000) g' \
        -c 'checkpoint' -c 'select pg_switch_wal()'" >/dev/null 2>&1
sleep 12
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

say "S7: gen-pgpool's canonical hook block vs the target contract"
# check-hooks compares pgpool.conf against the canonical block. We
# deliberately override follow_primary_command to empty (the target
# contract), so a *detected* difference is the correct outcome — and
# proves the tool notices drift.
CH=$(xp db0 "pg_agentctl check-hooks /etc/pgpool2/pgpool.conf" 2>&1 || true)
if echo "$CH" | grep -qi "follow_primary"; then
    ok "check-hooks flags follow_primary_command drift (canonical vs target)"
else
    bad "check-hooks did not flag the deliberate follow_primary_command override"
fi

say "S8: detach does not propagate between pgpool instances (hook-contract §3)"
# A detach on ONE instance is that instance's routing state only —
# nothing syncs it without watchdog. It also fires that instance's
# failover_command, which the agent must refuse (db2 is a healthy
# streaming standby).
xp db0 "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
sleep 8
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
sleep 5
x "$PRIM" "systemctl restart pgpool2"
sleep 8
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
sleep 8
wait_for 60 "pgpool healthy again with the non-empty hook configured" \
    "docker exec -u postgres pga-$PRIM pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c ' up ' | grep -qx 3"
x "$PRIM" "systemctl stop postgresql@17-main"
sleep 40
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
    sleep 10; waited=$((waited+10))
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
sleep 10
if [ "$SPLIT" = yes ]; then
    say "S13b: the existing mitigation — phantom check stops the stale primary"
    x "$PRIM" "systemctl restart pg_agentd" || true
    sleep 20
    if log_has "$PRIM" 'phantom-primary check: detected'; then
        ok "restarted agent detected the phantom primary"
    else
        bad "phantom check did not flag the stale primary"
    fi
    wait_for 60 "stale primary's PostgreSQL was stopped" \
        "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"
    echo "     NOTE: the mitigation needs an agent restart to fire — nothing"
    echo "     stops the stale primary while it keeps running."
fi

# ---------------------------------------------------------------------------
say "result"
echo "PASS=$PASS FAIL=$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '  - %s\n' "${FAILURES[@]}"
    KEEP=${KEEP:-1}   # keep the cluster for debugging on failure
    exit 1
fi
