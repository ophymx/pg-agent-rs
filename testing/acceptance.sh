#!/bin/bash
# Acceptance tests: 3-node dockerized cluster (PostgreSQL 17 + systemd +
# the real pg-agent-rs .deb), driven end to end. See testing/README.md.
#
# THE MODEL UNDER TEST IS LEASE-DRIVEN ROLES. Nodes boot with
# [raft] enabled = true, shadow = false — the greenfield deployment
# shape — and every failure/recovery scenario runs under the raft
# consensus lease: pgpool is present strictly as the router it is
# post-cutover (advisory failover hook, empty follow hook, sr_check
# discovery). There is no migration narrative here; the pgpool-led
# promote path is deleted from the codebase, not merely untested.
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

# Run a command inside a node as root / as postgres (the agent's user).
x()  { docker exec "pga-$1" bash -c "${*:2}"; }
xp() { docker exec -u postgres "pga-$1" bash -c "${*:2}"; }

# Journal of pg_agentd on a node since cluster start.
agent_log() { docker exec "pga-$1" journalctl -u pg_agentd --no-pager -o cat; }

# log_has <node> <substring> — substring match against the agent
# journal. Deliberately NOT `agent_log | grep -q`: grep -q exits on the
# first match and SIGPIPEs journalctl, which under `set -o pipefail`
# makes a *successful* match look like a failed command.
log_has() { local out; out=$(agent_log "$1" 2>/dev/null); [[ "$out" == *"$2"* ]]; }

# log_since <node> <since-ts> <substring> — like log_has, bounded to
# journal entries after a captured timestamp, so scenarios don't pass
# vacuously on an earlier phase's identical decision.
log_since() {
    local out
    out=$(docker exec "pga-$1" journalctl -u pg_agentd --since "$2" --no-pager -o cat 2>/dev/null)
    [[ "$out" == *"$3"* ]]
}
now_ts() { docker exec pga-db0 date '+%Y-%m-%d %H:%M:%S'; }

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

# streaming_count <primary> — standbys streaming from it.
streaming_count() {
    xp "$1" "psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\"" 2>/dev/null | tr -d ' '
}

# pcp_attach_everywhere — attach every node on every instance and wait
# until each instance shows 3 backends up. `cluster recover` attaches
# only through the LOCAL pcp (hook-contract §3's fan-out obligation —
# TODO.md), so after failovers+rejoins the per-instance maps are
# legitimately stale; scenarios that assert on pgpool's map normalize
# first.
pcp_attach_everywhere() {
    local n i prim prim_id
    prim=$(current_primary)
    prim_id="${prim#db}"
    for n in db0 db1 db2; do
        # Primary's backend first: attaching any OTHER node runs
        # pgpool's failback, which blocks in find_primary_node_repeatedly
        # (search_primary_node_timeout, 300 s) when the instance's map
        # has no up primary — finding 16's wedge.
        for i in $prim_id 0 1 2; do
            # Attach ONLY if that instance holds the node down: blindly
            # attaching an already-up backend (worst: the primary, on
            # its own instance) makes pgpool re-run its failover
            # processing and transiently degenerate healthy backends —
            # run 12's G8 watched it mark the primary down and then
            # refuse a detach for want of candidates.
            if xp "$n" "pcp_node_info -h localhost -p 9898 -U pgpool -w -n $i" 2>/dev/null | grep -q down; then
                xp "$n" "pcp_attach_node -h localhost -p 9898 -U pgpool -w -n $i" >/dev/null 2>&1 || true
            fi
        done
    done
    for n in db0 db1 db2; do
        wait_for 60 "$n: pgpool map normalized (3 backends up)" \
            "docker exec -u postgres pga-$n pcp_node_info -h localhost -p 9898 -U pgpool -w -a | grep -c ' up ' | grep -qx 3"
    done
}

# NOTE on every "N streaming" count below: pg_basebackup's WAL stream
# appears in pg_stat_replication as state='streaming', so an in-flight
# rebuild masquerades as a caught-up standby — run 11's G7 declared its
# repair done while the basebackup was still copying, and G8 raced it.
# The application_name filter excludes backup connections.

# repair_standbys <primary> <since-desc> — operator path: rebuild every
# non-streaming standby via `cluster recover`. Handles the
# diverged-survivor case (finding 15): candidate selection samples
# moving WAL positions, so a survivor can end up past the new primary's
# fork point and wedge — rewind territory, reserved for the operator by
# demote policy, which is exactly the path this exercises.
repair_standbys() {
    local prim="$1" ctx="$2" n nid
    # Short probe on purpose: a healthy re-point streams within ~10 s,
    # and the diverged-survivor wedge (finding 15) is the COMMON
    # post-takeover case — both majority standbys stream the same WAL
    # until the cut, so the non-winner is a coin flip to be past the
    # winner's fork point. Waiting longer just delays the repair the
    # operator path exists to run.
    if probe 20 \
        "[ \"\$(docker exec -u postgres pga-$prim psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 2 ]"; then
        ok "$prim has 2 streaming standbys ($ctx)"
        return
    fi
    for n in db0 db1 db2; do
        [ "$n" = "$prim" ] && continue
        if xp "$n" "psql -tAc \"select status from pg_stat_wal_receiver\"" 2>/dev/null | grep -qx streaming; then
            continue
        fi
        echo "     NOTE: $n not streaming — operator recover ($ctx)"
        nid="${n#db}"
        xp "$prim" "pg_agentctl cluster recover --target $nid --stop-target-pg" \
            > "/tmp/recover-$ctx-$nid.log" 2>&1 || true
    done
    wait_for 180 "$prim has 2 streaming standbys ($ctx, after repair)" \
        "[ \"\$(docker exec -u postgres pga-$prim psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 2 ]"
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
say "G0: greenfield boot — execute mode from the first start"
# Nodes are provisioned with [raft] enabled = true, shadow = false.
# validate-env (the systemd ExecStartPre gate) therefore runs the raft
# prerequisite checks on every start; the daemons coming up IS that
# assertion. Before ClusterInit, there is no membership and hence no
# quorum: the loop must tick StoreUnknown — never vacant, and above
# all never act. Executors attached to a store that answers "unknown"
# do nothing: that is the greenfield safety posture.
for n in db0 db1 db2; do
    wait_for 60 "$n: pg_agentd active (validate-env gate incl. raft checks)" \
        "docker exec pga-$n systemctl is-active -q pg_agentd"
done
wait_for 30 "db0: postgres active (bootstrap primary)" \
    "docker exec pga-db0 systemctl is-active -q postgresql@17-main"
assert "db1: postgres intentionally down pre-init" \
    "! docker exec pga-db1 systemctl is-active -q postgresql@17-main"
assert "db2: postgres intentionally down pre-init" \
    "! docker exec pga-db2 systemctl is-active -q postgresql@17-main"
for n in db0 db1 db2; do
    wait_for 30 "$n: raft started, EXECUTE mode" \
        "docker exec pga-$n journalctl -u pg_agentd --no-pager -o cat | grep -q 'EXECUTE mode'"
done
wait_for 30 "pre-membership: loop reports store unknown (not vacant, no action)" \
    "docker exec pga-db0 journalctl -u pg_agentd --no-pager -o cat | grep -q 'StoreUnknown'"
assert "no executor action before membership exists" \
    "! docker exec pga-db0 journalctl -u pg_agentd --no-pager -o cat | grep -qE 'FENCING|roleexec: promoting'"

# ---------------------------------------------------------------------------
say "G1: cluster init — replication + membership + lease, one command"
INIT1=$(xp db0 "pg_agentctl cluster init" 2>&1)
if echo "$INIT1" | grep -q 'cluster_init complete'; then
    ok "cluster init: standbys initialised"
else
    bad "cluster init failed: $INIT1"
fi
if echo "$INIT1" | grep -q 'raft membership initialized (3 nodes)'; then
    ok "raft membership formed by init"
else
    bad "init did not form raft membership: $INIT1"
fi
if echo "$INIT1" | grep -q 'lease seeded'; then
    ok "lease seeded for the bootstrap primary"
else
    bad "init did not seed the lease: $INIT1"
fi
wait_for 60 "db0 has 2 streaming standbys" \
    "[ \"\$(docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 2 ]"
wait_for 30 "db0 retains the lease (execute-mode steady state)" \
    "docker exec pga-db0 journalctl -u pg_agentd --no-pager -o cat | grep -q 'RetainedLease'"
for n in db1 db2; do
    wait_for 60 "$n executor converged onto the holder" \
        "docker exec pga-$n journalctl -u pg_agentd --no-pager -o cat | grep -q 'now following lease holder'"
done
INIT2=$(xp db0 "pg_agentctl cluster init --only-node 99" 2>&1)
if echo "$INIT2" | grep -q 'already formed'; then
    ok "re-running init is idempotent (membership already formed)"
else
    bad "second init not idempotent: $INIT2"
fi

say "G1b: pgpool up as the router (agent-led contract)"
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

# ---------------------------------------------------------------------------
say "G2: the hook contract holds (check-hooks fully clean)"
# The deployed conf ends with the canonical gen-pgpool block, no
# overrides — so check-hooks must pass verbatim: hooks AND the
# decision-critical settings.
if xp db0 "pg_agentctl check-hooks /etc/pgpool2/pgpool.conf" >/dev/null 2>&1; then
    ok "check-hooks passes on the deployed conf (exit 0)"
else
    CH=$(xp db0 "pg_agentctl check-hooks /etc/pgpool2/pgpool.conf" 2>&1 || true)
    bad "check-hooks found drift: $(echo "$CH" | grep ERR)"
fi

# ---------------------------------------------------------------------------
say "G2b: pgpool stays a router — detach neither propagates nor breaks replication"
# Steady state, pristine maps: this scenario is about ROUTING semantics
# (per-instance detach, no watchdog sync, agent refusing to break a
# healthy standby) and gets tested before any failover has rearranged
# the per-instance maps. Post-failover map hygiene is finding 16's
# territory, exercised by the failover scenarios themselves.
G2B_TS=$(now_ts)
xp db0 "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
sleep 4
assert "db0's pgpool shows node 2 down" \
    "docker exec -u postgres pga-db0 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q down"
assert "db1's pgpool still shows node 2 up (no propagation)" \
    "docker exec -u postgres pga-db1 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q ' up '"
if log_since db0 "$G2B_TS" 'refusing to drop slot'; then
    ok "agent refused the slot drop (detached standby is still streaming)"
else
    bad "agent did not refuse the slot drop for a streaming standby"
fi
assert "replication intact (2 streaming)" \
    "[ \"\$(docker exec -u postgres pga-db0 psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 2 ]"
xp db0 "pcp_attach_node -h localhost -p 9898 -U pgpool -w -n 2" >/dev/null 2>&1 || true
wait_for 30 "explicit attach clears the detach" \
    "docker exec -u postgres pga-db0 pcp_node_info -h localhost -p 9898 -U pgpool -w -n 2 | grep -q ' up '"

# ---------------------------------------------------------------------------
say "G3: primary death — hook advises, the lease decides, pgpool discovers"
PRIM=db0
G3_TS=$(now_ts)
x "$PRIM" "systemctl stop postgresql@17-main"
wait_for 30 "failover hook answered advisory (notify-only, as contracted)" \
    "for n in db0 db1 db2; do docker exec pga-\$n journalctl -u pg_agentd --since '$G3_TS' --no-pager -o cat 2>/dev/null | grep 'failover: advisory' && exit 0; done; exit 1"
wait_for 60 "the lease promoted a standby" \
    "for n in db1 db2; do docker exec pga-\$n journalctl -u pg_agentd --since '$G3_TS' --no-pager -o cat 2>/dev/null | grep 'promotion complete' && exit 0; done; exit 1"
sleep 3
W1=""
for n in db1 db2; do
    if log_since "$n" "$G3_TS" 'roleexec: promotion complete'; then W1="$n"; fi
done
if [ -n "$W1" ]; then ok "winner: $W1"; else bad "no promotion winner found"; W1=db1; fi
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one PostgreSQL primary after failover"
else
    bad "expected 1 primary, found $(count_primaries)"
fi
assert "promotion is journaled (ops list shows a done promote op)" \
    "docker exec -u postgres pga-$W1 pg_agentctl ops list | grep promote | grep -qi done"
wait_for 90 "the surviving standby re-pointed and streams from $W1" \
    "[ \"\$(docker exec -u postgres pga-$W1 psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 1 ]"
W1_ID="${W1#db}"
wait_for 60 "pgpool discovered the new primary via sr_check (no follow hook)" \
    "docker exec -u postgres pga-$W1 pcp_node_info -h localhost -p 9898 -U pgpool -w -n $W1_ID | grep -qi primary"
# Finding 16: failover_on_backend_error can degenerate the winner's own
# backend on its own instance (~20 s post-promote), and auto_failback
# off makes that permanent — unless the executor's self-attach probe
# converges it. Role alone (above) doesn't prove routability; status
# must be up. 60 s comfortably covers the degeneration window plus one
# 10 s probe interval.
wait_for 60 "the winner's own pgpool routes to it (backend up — finding 16)" \
    "docker exec -u postgres pga-$W1 pcp_node_info -h localhost -p 9898 -U pgpool -w -n $W1_ID | grep -q ' up '"

# ---------------------------------------------------------------------------
say "G4: operator rejoin — demote policy, the slot-race guard, repair"
assert "dead ex-primary $PRIM stayed stopped (rejoin is never automatic)" \
    "! docker exec pga-$PRIM systemctl is-active -q postgresql@17-main"
PRIM_ID="${PRIM#db}"
xp "$W1" "pg_agentctl cluster recover --target $PRIM_ID --stop-target-pg" \
    > /tmp/recover-g4.log 2>&1 || true
repair_standbys "$W1" "g4"
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary after the rejoin"
else
    bad "primary count drifted during rejoin: $(count_primaries)"
fi

say "G4b: reclone a LIVE standby — the slot-race guard (finding 9)"
# The recover/failover-hook race needs pgpool to believe the target is
# UP when recovery stops it: the stop then fires the standby-down hook,
# whose job is to drop the very slot the recovery just created, and the
# cross-op consult must defer. (Rejoining an already-dead node cannot
# fire the hook — pgpool stopped watching it long ago.) This is also
# the operator's reclone-a-suspect-standby flow, verbatim.
pcp_attach_everywhere
G4B_TARGET=""
for n in db0 db1 db2; do
    [ "$n" = "$W1" ] && continue
    G4B_TARGET="$n"; break
done
G4B_TS=$(now_ts)
G4B_TID="${G4B_TARGET#db}"
xp "$W1" "pg_agentctl cluster recover --target $G4B_TID --stop-target-pg" \
    > /tmp/recover-g4b.log 2>&1 || true
if log_since "$W1" "$G4B_TS" 'in-flight op owns this node; skipping slot drop'; then
    ok "failover hook deferred to the in-flight recovery (slot survived)"
else
    bad "no cross-op consult during the live reclone"
fi
repair_standbys "$W1" "g4b"

# ---------------------------------------------------------------------------
say "G5: partition of the holder — fence + one takeover + one primary"
G5_TS=$(now_ts)
docker network disconnect pga-net "pga-$W1" >/dev/null 2>&1
say "     (isolated $W1 — current primary and lease holder)"
if wait_for 30 "isolated holder fenced itself (quorum loss → stop)" \
    "docker exec pga-$W1 journalctl -u pg_agentd --since '$G5_TS' --no-pager -o cat | grep FENCING"; then :; fi
assert "isolated $W1 PostgreSQL is actually stopped" \
    "! docker exec pga-$W1 systemctl is-active -q postgresql@17-main"
if wait_for 60 "majority completed a real promotion" \
    "for n in db0 db1 db2; do [ \"\$n\" = \"$W1\" ] && continue; docker exec pga-\$n journalctl -u pg_agentd --since '$G5_TS' --no-pager -o cat 2>/dev/null | grep 'promotion complete' && exit 0; done; exit 1"; then :; fi
W2=""
for n in db0 db1 db2; do
    [ "$n" = "$W1" ] && continue
    if log_since "$n" "$G5_TS" 'roleexec: promotion complete'; then W2="$n"; fi
done
if [ -n "$W2" ]; then ok "majority promoted $W2"; else bad "no majority winner"; W2=db0; fi
if log_since "$W1" "$G5_TS" 'TookOver'; then
    bad "isolated node committed a takeover without a quorum"
else
    ok "isolated node committed nothing (no quorum, no writes)"
fi
# Hysteresis: every takeover ≥ leader_ttl after the previous — a fresh
# holder's protection window (finding 13) held under real execution.
MAJ_TAKES=$(for n in db0 db1 db2; do
    [ "$n" = "$W1" ] && continue
    docker exec "pga-$n" journalctl -u pg_agentd --since "$G5_TS" --no-pager -o short-unix 2>/dev/null \
        | grep 'TookOver'
done | awk '{print int($1)}' | sort -n)
GAP_VIOLATION=""
prev=""
for t in $MAJ_TAKES; do
    if [ -n "$prev" ] && [ $((t - prev)) -lt 10 ]; then GAP_VIOLATION="$((t - prev))s"; fi
    prev="$t"
done
if [ -z "$GAP_VIOLATION" ]; then
    ok "every takeover ≥ leader_ttl after the previous (hysteresis held)"
else
    bad "takeovers $GAP_VIOLATION apart — a fresh holder was deposed inside its ttl"
fi

say "G5b: partition heals — one primary throughout, operator rejoins"
docker network connect pga-net "pga-$W1" >/dev/null 2>&1
sleep 5
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one PostgreSQL primary during and after the partition"
else
    bad "primary count wrong after the partition: $(count_primaries)"
fi
assert "fenced ex-holder stays down after reconnect (demote policy)" \
    "! docker exec pga-$W1 systemctl is-active -q postgresql@17-main"
W1_ID="${W1#db}"
xp "$W2" "pg_agentctl cluster recover --target $W1_ID --stop-target-pg" \
    > /tmp/recover-g5.log 2>&1 || true
repair_standbys "$W2" "g5"
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary after the rejoin"
else
    bad "primary count wrong after rejoin: $(count_primaries)"
fi

# ---------------------------------------------------------------------------
say "G6: agent restarts are non-events"
G6_TS=$(now_ts)
x "$W2" "systemctl restart pg_agentd"
wait_for 30 "$W2: pg_agentd active after restart" \
    "docker exec pga-$W2 systemctl is-active -q pg_agentd"
assert "phantom-primary check confirmed (no conservative stop)" \
    "docker exec pga-$W2 journalctl -u pg_agentd --since '$G6_TS' --no-pager -o cat | grep -q 'phantom-primary check: confirmed'"
assert "$W2 postgres untouched by the agent restart" \
    "docker exec -u postgres pga-$W2 psql -tAc 'select pg_is_in_recovery()' | grep -qx f"
wait_for 30 "$W2 retains the lease after its agent restart" \
    "docker exec pga-$W2 journalctl -u pg_agentd --since '$G6_TS' --no-pager -o cat | grep -q 'RetainedLease'"
G6_STANDBY=""
for n in db0 db1 db2; do
    [ "$n" = "$W2" ] && continue
    G6_STANDBY="$n"; break
done
x "$G6_STANDBY" "systemctl restart pg_agentd"
wait_for 30 "$G6_STANDBY: pg_agentd active after restart" \
    "docker exec pga-$G6_STANDBY systemctl is-active -q pg_agentd"
sleep 5
assert "no takeover churn from the restarts" \
    "! docker exec pga-$G6_STANDBY journalctl -u pg_agentd --since '$G6_TS' --no-pager -o cat | grep -q 'TookOver'"
assert "replication intact (2 streaming)" \
    "[ \"\$(docker exec -u postgres pga-$W2 psql -tAc \"select count(*) from pg_stat_replication where state='streaming' and application_name <> 'pg_basebackup'\" | tr -d ' ')\" = 2 ]"

# ---------------------------------------------------------------------------
say "G7: candidate selection refuses a lagging standby (§2.2 under the lease)"
# The §2.2 defect was pgpool picking a candidate by lowest node id with
# no WAL comparison. Under the lease, candidacy runs the most-advanced
# check itself. Arrange real lag: pause replay on one standby, write
# ~24 MB on the primary, kill the primary — the caught-up standby MUST
# win, and the lagging one must stand down naming the gap.
S_LAG=""
S_OK=""
for n in db0 db1 db2; do
    [ "$n" = "$W2" ] && continue
    if [ -z "$S_LAG" ]; then S_LAG="$n"; else S_OK="$n"; fi
done
xp "$S_LAG" "psql -tAc 'select pg_wal_replay_pause()'" >/dev/null 2>&1
xp "$W2" "psql -q -c 'create table if not exists bulk(id int, pad text)' \
        -c 'insert into bulk select g, repeat(chr(97+(g%26)),200) from generate_series(1,120000) g' \
        -c 'checkpoint' -c 'select pg_switch_wal()'" >/dev/null 2>&1
wait_for 30 "$S_LAG trails by > 16 MiB (replay paused, still receiving)" \
    "[ \"\$(docker exec -u postgres pga-$S_LAG psql -tAc \"select pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint\" | tr -d ' ')\" -gt 16777216 ]"
G7_TS=$(now_ts)
x "$W2" "systemctl stop postgresql@17-main"
wait_for 60 "the caught-up standby $S_OK won the takeover" \
    "docker exec pga-$S_OK journalctl -u pg_agentd --since '$G7_TS' --no-pager -o cat | grep 'promotion complete'"
if log_since "$S_LAG" "$G7_TS" 'roleexec: promotion complete'; then
    bad "the LAGGING standby was promoted — §2.2 reopened"
else
    ok "the lagging standby was not promoted"
fi
# The stand-down log is timing-dependent: it only appears if the
# lagging node's candidacy tick fires before the winner's CAS lands —
# afterwards it just follows the new holder. The enforced property is
# the two asserts above; the log, when present, is corroboration.
if log_since "$S_LAG" "$G7_TS" 'is ahead by'; then
    ok "$S_LAG stood down naming the WAL gap (corroboration)"
else
    echo "     NOTE: $S_LAG never reached candidacy (winner's CAS landed first)"
fi
xp "$S_LAG" "psql -tAc 'select pg_wal_replay_resume()'" >/dev/null 2>&1
W2_ID="${W2#db}"
xp "$S_OK" "pg_agentctl cluster recover --target $W2_ID --stop-target-pg" \
    > /tmp/recover-g7.log 2>&1 || true
repair_standbys "$S_OK" "g7"
if [ "$(count_primaries)" = "1" ]; then
    ok "exactly one primary after the lag-gated failover + rejoin"
else
    bad "primary count wrong: $(count_primaries)"
fi

# ---------------------------------------------------------------------------
say "result"
echo "PASS=$PASS FAIL=$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '  - %s\n' "${FAILURES[@]}"
    KEEP=${KEEP:-1}   # keep the cluster for debugging on failure
    exit 1
fi
