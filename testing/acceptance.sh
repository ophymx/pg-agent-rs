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
say "result"
echo "PASS=$PASS FAIL=$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '  - %s\n' "${FAILURES[@]}"
    KEEP=${KEEP:-1}   # keep the cluster for debugging on failure
    exit 1
fi
