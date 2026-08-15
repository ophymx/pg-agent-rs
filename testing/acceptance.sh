#!/bin/bash
# Thin launcher for the acceptance harness (testing/acceptance, Rust).
# The harness drives the dockerized 3-node cluster end to end; see
# testing/README.md and the crate's module docs.
#
# Usage:
#   testing/acceptance.sh              # build, up, run all scenarios, down
#   KEEP=1 testing/acceptance.sh       # leave the cluster running afterwards
#   SKIP_BUILD=1 testing/acceptance.sh # reuse dist/.deb + existing image
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo run -q -p acceptance
