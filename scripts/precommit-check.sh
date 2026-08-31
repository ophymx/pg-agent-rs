#!/usr/bin/env bash
#
# scripts/precommit-check.sh — run the standard checks before a commit.
# Mirrors the cadence of the Go project's precommit-check.sh: fmt, lint,
# build (subsumed by clippy/test), test, ending with `git status`.
#
# Usage:
#   ./scripts/precommit-check.sh             # check-only (default)
#   ./scripts/precommit-check.sh --fix       # apply rustfmt and clippy --fix
#   ./scripts/precommit-check.sh --quick     # skip cargo doc (the slow check)
#   ./scripts/precommit-check.sh --release   # also build a release of every crate
#   ./scripts/precommit-check.sh --help
#
# Exits non-zero on the first failed check. Run from anywhere; the script
# locates the workspace root from its own path.

set -euo pipefail

ROOT="$(cd "$(dirname "$(realpath "$0")")/.." && pwd)"
cd "$ROOT"

# ----- arg parsing ----------------------------------------------------------

FIX=0
QUICK=0
RELEASE=0

usage() {
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
}

for arg in "$@"; do
  case "$arg" in
    --fix)     FIX=1 ;;
    --quick)   QUICK=1 ;;
    --release) RELEASE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "precommit-check: unknown option: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

# ----- helpers --------------------------------------------------------------

if [ -t 2 ]; then
  BOLD=$'\e[1m'
  RESET=$'\e[0m'
else
  BOLD=''
  RESET=''
fi

step() {
  printf '\n%s==>%s %s\n' "$BOLD" "$RESET" "$*"
}

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "precommit-check: required tool missing: $1" >&2
    [ -n "${2-}" ] && echo "  install hint: $2" >&2
    exit 1
  fi
}

require cargo  "rustup component add cargo"
# No `require protoc`: the .proto sources compile against the vendored
# binary (protoc-bin-vendored, a build-dependency of pg-agent-proto), so a
# checkout needs nothing but a Rust toolchain. The guard that used to be
# here existed only to turn "command not found" into a legible message,
# and there is no longer a command to not find.

# rustup components — surface a useful error instead of cargo's own.
if ! cargo fmt --version >/dev/null 2>&1; then
  echo "precommit-check: rustfmt component missing" >&2
  echo "  install hint: rustup component add rustfmt" >&2
  exit 1
fi
if ! cargo clippy --version >/dev/null 2>&1; then
  echo "precommit-check: clippy component missing" >&2
  echo "  install hint: rustup component add clippy" >&2
  exit 1
fi

# ----- fmt ------------------------------------------------------------------

step "cargo fmt"
if [ "$FIX" -eq 1 ]; then
  cargo fmt --all
else
  cargo fmt --all --check
fi

# ----- clippy ---------------------------------------------------------------
# All targets (lib, bin, tests, benches, examples). -D warnings makes any
# lint a hard error so we don't accumulate noise.

step "cargo clippy --workspace --all-targets"
if [ "$FIX" -eq 1 ]; then
  cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged -- -D warnings
else
  cargo clippy --workspace --all-targets -- -D warnings
fi

# ----- tests ----------------------------------------------------------------
# Compiles every target along the way, so a separate `cargo check` is
# redundant.

step "cargo test --workspace"
cargo test --workspace

# ----- docs (slow, skipped under --quick) -----------------------------------
# `-D warnings` catches broken intra-doc links — the most common silent
# documentation regression.

if [ "$QUICK" -ne 1 ]; then
  step "cargo doc --workspace --no-deps"
  RUSTDOCFLAGS="-D warnings" cargo doc \
    --workspace --no-deps --document-private-items
fi

# ----- licenses -------------------------------------------------------------
# Skipped rather than failed when cargo-deny is absent: it is a separate
# `cargo install`, and a contributor without it should still get a usable
# precommit run. The release workflow runs the same check unconditionally,
# so a widened license set cannot reach a tag through this gap.

if command -v cargo-deny >/dev/null 2>&1; then
  step "cargo deny check licenses"
  cargo deny check licenses
else
  step "cargo deny check licenses (SKIPPED — cargo-deny not installed)"
  echo "  install hint: cargo install --locked cargo-deny" >&2
fi

# ----- release build (opt-in) -----------------------------------------------

if [ "$RELEASE" -eq 1 ]; then
  step "cargo build --workspace --release"
  cargo build --workspace --release
fi

# ----- final status ---------------------------------------------------------
# Always end with `git status` so the operator notices anything `--fix`
# touched or any new build artefact.

step "git status"
git status

printf '\n%sprecommit-check.sh: all checks passed%s\n' "$BOLD" "$RESET"
