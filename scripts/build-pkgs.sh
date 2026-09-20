#!/bin/sh
# Build the pg-agent-rs `.deb` and `.rpm` via cargo-deb and
# cargo-generate-rpm. Package metadata lives in
# crates/pg-agentd/Cargo.toml; see packaging/README.md.
#
# Usage:
#   scripts/build-pkgs.sh              # both formats
#   scripts/build-pkgs.sh deb          # just the .deb
#   scripts/build-pkgs.sh rpm          # just the .rpm
#   VERSION=1.2.3 scripts/build-pkgs.sh   # override version (default:
#                                       # Cargo workspace version)
#
# Run from the repository root (it relies on relative paths).

set -eu

cd "$(dirname "$0")/.."

# Check the tool for the format actually requested, so building just
# the .deb does not demand the RPM packager. Checked at all because the
# failure mode otherwise is a bare "command not found" for a cargo
# subcommand, which reads like a broken toolchain rather than a missing
# one-line install.
need() {
    command -v "$1" >/dev/null 2>&1 && return 0
    echo "error: $1 not found in PATH" >&2
    echo "       install with: cargo install $1" >&2
    exit 1
}

# Pick a version. CLI arg of "1.2.3-rc1" isn't supported here — set
# VERSION as an env var instead.
if [ -z "${VERSION:-}" ]; then
    VERSION=$(awk -F'"' '/^version =/ { print $2; exit }' Cargo.toml)
fi
if [ -z "$VERSION" ]; then
    echo "error: could not derive VERSION from Cargo.toml" >&2
    exit 1
fi
export VERSION

# Build the release binaries once, STATICALLY against musl.
#
# Not a preference — a correctness fix. A dynamically-linked build
# carries the build host's glibc floor and the package declares none,
# so it installs on an older distro and dies at exec with
# `GLIBC_2.39 not found` (testing/FINDINGS.md finding 26). Debian 12 and
# RHEL 9 are both below a Debian 13 builder's floor, which rules out
# the .rpm target entirely. Static musl removes the floor rather than
# documenting it: one artifact verified running on Debian 12, Ubuntu
# 24.04, Rocky 9 and Alpine.
#
# Feasible here because the dependency set is pure Rust apart from
# ring's C/asm — see the tokio-rustls note in the workspace Cargo.toml,
# which keeps it that way. Requires `musl-tools` and
# `rustup target add x86_64-unknown-linux-musl`.
#
# NSS plugins are unsupported by construction: a static musl binary
# resolves via DNS and /etc/hosts, not nsswitch.conf. That is a stated
# boundary (packaging/README.md), and the deployments this targets
# discover peers through DNS.
TARGET="${TARGET:-x86_64-unknown-linux-musl}"
export TARGET

# Package architecture, derived from the target rather than assumed.
#
# The two formats spell the same machine differently — Debian says
# amd64/arm64 where RPM says x86_64/aarch64 — and BOTH names have to be
# right for reasons beyond the filename: the architecture recorded
# INSIDE the package is what apt and dnf use to refuse a package built
# for another machine. A mislabelled arm64 build would install happily
# on x86_64 and die at exec, which is finding 26's failure mode wearing
# a different hat.
#
# Unknown targets stop here. Guessing an architecture name is exactly
# the kind of helpfulness that produces a package claiming to be
# something it is not.
case "$TARGET" in
    x86_64-unknown-linux-musl)  DEB_ARCH=amd64; RPM_ARCH=x86_64 ;;
    aarch64-unknown-linux-musl) DEB_ARCH=arm64; RPM_ARCH=aarch64 ;;
    *)
        echo "error: no package-architecture mapping for TARGET=$TARGET" >&2
        echo "       add one to scripts/build-pkgs.sh rather than letting the" >&2
        echo "       packagers guess" >&2
        exit 2
        ;;
esac

echo "==> cargo build --release --target ${TARGET}"
cargo build --release --target "$TARGET"

mkdir -p dist

# Stage the built binaries at a FIXED path, because neither packager
# expands environment variables in asset paths and cargo-deb resolves
# them relative to the manifest — so the target triple cannot be
# templated into the metadata. Staging keeps one build feeding both
# packagers, and keeps the packaged artifact traceable to it.
mkdir -p dist/staging
for b in pg_agentd pg_agentc pg_agentctl; do
    cp -f "target/${TARGET}/release/${b}" "dist/staging/${b}"
done
echo "==> staged $(file -b dist/staging/pg_agentd | cut -d, -f1-2)"

build_deb() {
    need cargo-deb
    out="dist/pg-agent-rs_${VERSION}_${DEB_ARCH}.deb"
    # --no-build: the static musl build above IS the artifact. Letting
    # cargo-deb rebuild would produce a host-native dynamic binary —
    # finding 26 walking straight back in.
    #
    # --target is NOT a build instruction here (--no-build already
    # settled that); it is how cargo-deb learns which architecture to
    # stamp on the package. The assets are fixed paths under
    # dist/staging, so it changes nothing else.
    echo "==> cargo deb → ${out}"
    cargo deb --no-build --no-strip --target "$TARGET" -p pg-agentd --output "$out"
}

build_rpm() {
    need cargo-generate-rpm
    out="dist/pg-agent-rs-${VERSION}-1.${RPM_ARCH}.rpm"
    # -a rather than --target: --target would send it looking for
    # artifacts under a target subdirectory, and the binaries it
    # packages are staged at a fixed path. Only the recorded
    # architecture needs to change.
    echo "==> cargo generate-rpm → ${out}"
    cargo generate-rpm -a "$RPM_ARCH" -p crates/pg-agentd --output "$out"
}

case "${1:-all}" in
    all)
        build_deb
        build_rpm
        ;;
    deb)
        build_deb
        ;;
    rpm)
        build_rpm
        ;;
    *)
        echo "usage: $0 [all|deb|rpm]" >&2
        exit 2
        ;;
esac

echo ""
echo "==> dist/"
ls -lh dist/
