#!/bin/bash
# OS / PostgreSQL matrix for the acceptance suite. ON DEMAND ONLY.
#
# `testing/acceptance.sh` is unchanged and is still the everyday entry
# point: with no environment set it builds the baseline cell
# (debian:trixie, PostgreSQL 17) exactly as before. Nothing here runs
# unless this script is invoked directly. That is deliberate — the
# matrix costs one full suite run per cell, and its value is in
# answering "does this still work on the other distro" occasionally,
# not in taxing every iteration.
#
# Runs the full suite once per cell, sequentially (the cells share the
# docker daemon, the host's cgroups, and the compose project name — they
# cannot run concurrently). Each cell rebuilds the image for its base and
# tags it per cell, so a failure means "this combination is broken", not
# "the previous cell's image was still lying around".
#
# Each base is paired with the PostgreSQL version it ships NATIVELY. No
# PGDG repo: the point is to test what distributions actually deliver,
# and to keep a build failure meaningful instead of a repo-drift alarm.
#
# Usage:
#   testing/matrix.sh                 # every cell
#   testing/matrix.sh trixie-pg17     # named cells only
#   KEEP_GOING=1 testing/matrix.sh    # run all cells even after a failure
#
# Exit status is non-zero if any cell failed.
set -uo pipefail
cd "$(dirname "$0")/.."

# tag              base image        pg
CELLS=(
    "trixie-pg17   debian:trixie     17"
    "noble-pg16    ubuntu:24.04      16"
    "bookworm-pg15 debian:bookworm   15"
)
#
# bookworm was excluded while the package was dynamically linked: it
# carried the build host's glibc floor, declared no dependency, and so
# installed on Debian 12 (glibc 2.36) only to die at exec with
# `GLIBC_2.39 not found` — finding 26. The build is static musl now, so
# the floor is gone and the cell is real. It is also the cell that
# proves it stays gone: if anyone reverts the build to a dynamic
# target, this is where it fails.

want=("$@")
keep_going="${KEEP_GOING:-}"
outdir="${MATRIX_OUT:-target/matrix}"
mkdir -p "$outdir"

results=()
failed=0

for cell in "${CELLS[@]}"; do
    read -r tag base pg <<<"$cell"
    if [ ${#want[@]} -gt 0 ] && ! printf '%s\n' "${want[@]}" | grep -qx "$tag"; then
        continue
    fi

    log="$outdir/$tag.log"
    printf '\n\033[1m=== matrix cell: %s (%s, PostgreSQL %s)\033[0m\n' "$tag" "$base" "$pg"
    echo "    log: $log"

    # A cell starts from nothing: any container from the previous cell
    # runs the previous cell's PostgreSQL.
    docker compose -f testing/compose.yaml down -v --remove-orphans >/dev/null 2>&1

    start=$(date +%s)
    MATRIX_TAG="$tag" BASE_IMAGE="$base" PG_VERSION="$pg" \
        ./testing/acceptance.sh >"$log" 2>&1
    rc=$?
    elapsed=$(( $(date +%s) - start ))

    tally=$(grep -a -o 'PASS=[0-9]* FAIL=[0-9]*' "$log" | tail -1)
    [ -z "$tally" ] && tally="did not reach the summary"

    if [ $rc -eq 0 ]; then
        printf '\033[32m    PASS\033[0m  %s  (%ss)\n' "$tally" "$elapsed"
        results+=("PASS  $tag  $tally  ${elapsed}s")
    else
        printf '\033[31m    FAIL\033[0m  %s  (%ss)\n' "$tally" "$elapsed"
        results+=("FAIL  $tag  $tally  ${elapsed}s")
        failed=1
        # Surface the failing checks inline: the whole point of a matrix
        # is noticing that cell N broke where cell 1 passed.
        grep -a "FAIL" "$log" | head -15 | sed 's/^/      /'
        [ -z "$keep_going" ] && break
    fi
done

docker compose -f testing/compose.yaml down -v --remove-orphans >/dev/null 2>&1

printf '\n\033[1m=== matrix summary\033[0m\n'
for r in "${results[@]}"; do echo "  $r"; done
exit $failed
